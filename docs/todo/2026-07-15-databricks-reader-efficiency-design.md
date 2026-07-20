# Databricks reader efficiency: one AS JSON read path for both backends

Status: **approved design, ready for implementation planning**
Date: 2026-07-15
Branch: `claude/databricks-reader-efficiency-75bf90`
Supersedes: PR #239 (shared observed-table assembly) — folded into this work rather than merged.

## Goal

Rebuild the Databricks reader so both backends (Spark and SQL warehouse) construct an
`ObservedTable` from **one primary description** plus a small, fixed set of supplementary
metadata queries — reducing both the number of query calls and the number of reader
functions, and putting the shared logic behind one deep module.

The lever is `DESCRIBE TABLE EXTENDED <table> AS JSON`, which returns a single JSON document
carrying almost all table-local state as structured data.

## Non-goals

- No change to the domain model (`ObservedTable`, `ObservedColumn`, `PrimaryKeyConstraint`,
  `ForeignKeyConstraint`, `ForeignKeyReference`), the diff, validation, planning, or the write
  path. This is a read-adapter change only.
- No cross-table / per-catalog batching (roadmap #17's other idea) — still evidence-gated.
- No new managed metadata. We populate exactly the fields the domain already models.

## Decisions (resolved during brainstorming)

| Decision                   | Choice                                              | Why                                                                                                                                                                                                                                                                                                                                          |
| -------------------------- | --------------------------------------------------- | -------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------- |
| Sequencing vs PR #239      | **Subsume** #239                                    | The AS JSON rewrite reshapes the very `read.py` assembly #239 extracts; extracting the old shape first is wasted motion.                                                                                                                                                                                                                     |
| Which backends use AS JSON | **Both**                                            | Truest "one abstraction"; deletes the per-aspect info_schema column/PK/FK queries and the DDL type parser.                                                                                                                                                                                                                                   |
| OSS-Spark local e2e        | **Lightweight native test reader**                  | OSS Spark rejects `AS JSON` for Delta v2 tables (`NOT_SUPPORTED_COMMAND_FOR_V2_TABLE`, verified locally). Production Spark runs on Databricks where it works; the ~20 engine e2e tests keep running by injecting a test-only reader that reads OSS Delta via native `spark.table().schema` + `DESCRIBE DETAIL`, reusing the shared assembly. |
| Clustering source          | **AS JSON** (`clustering_columns`)                  | Real Databricks output carries top-level `clustering_columns` (the research missed it).                                                                                                                                                                                                                                                      |
| Property source            | **AS JSON** (`table_properties`, policy-projected) | Lets us drop `DESCRIBE DETAIL` from the read path (4 round-trips, not 5). Gated on a live check — see Risk R1.                                                                                                                                                                                                                               |
| Supplementary round-trips  | **3 clean separate queries**                        | Repo policy: don't optimise to a UNION without latency evidence (`todo.md:11`).                                                                                                                                                                                                                                                              |
| Existence detection        | **Derive from the AS JSON not-found error**         | No separate existence probe; fewest ops.                                                                                                                                                                                                                                                                                                     |

## Final read shape

Per present table: **4 round-trips**, down from ~8. `DESCRIBE DETAIL` is removed from the
read path.

1. `DESCRIBE TABLE EXTENDED <table> AS JSON` → columns (structured types, nullability,
   comments), table comment, `partition_columns`, `clustering_columns`, `table_properties`
   (projected through the property policy), and `table_constraints` (PK + outbound FKs, as an
   embedded string).
2. `information_schema.table_tags`
3. `information_schema.column_tags`
4. `information_schema` referencing-FK query (inbound FKs — not in the JSON).

An **absent** table costs only the failed AS JSON attempt (its not-found error → `TableAbsent`).

## Architecture

Both readers collapse to thin backend shells over a shared, PySpark-free core — the read-side
twin of the write-side `execution.execute_statements(run, statements)`.

```
SparkReader / WarehouseReader                 (backend-specific: execute SQL, extract result,
  │   classify not-found → TableAbsent)        classify errors)
  ▼
parse_table_snapshot(json_text, qn) ──► TableSnapshot   (sql/describe_json.py — pure, shared)
  │                                    (+ parse_table_constraints, sql/constraints.py)
  ▼
observed_table_from_snapshot(snapshot, run_info_schema_query) ──► ObservedTable
      (adapters/databricks/read.py — attaches tags + inbound FKs, validates consistency)
```

### New modules

- **`sql/describe_json.py`** — `parse_table_snapshot(json_text: str, qualified_name: QualifiedName) -> TableSnapshot`.
  Parses the JSON once. Owns the structured-type → domain `DataType` mapper, the column
  skip/raise policy (previously in `column_from_catalog`), the `table_properties` policy
  projection, comment handling, partition/clustering extraction, and delegation to the constraint
  parser. Raises a typed `MetadataParseError` on malformed structure — never silently drops.
- **`sql/constraints.py`** — `parse_table_constraints(value: str | None) -> ParsedConstraints`.
  The one embedded-string field, isolated behind a narrow interface and documented as less
  structurally stable than the rest of the JSON.
- **`TableSnapshot`** (frozen dataclass, in `sql/`) — the neutral table-local form:
  `qualified_name, columns (tuple[ObservedColumn], tags empty), comment, partitioned_by,
  `clustered_by, properties (already policy-projected), primary_key, foreign_keys`.
  Reuses `ObservedColumn` — **no new `ObservedColumnBase`**.

### Reshaped

- **`adapters/databricks/read.py`** — created fresh (not #239's version):
  `observed_table_from_snapshot(snapshot, *, run_info_schema_query) -> ObservedTable`.
  Attaches column tags, table tags, and inbound FKs (via `run_info_schema_query`), validates
  that tagged / key / layout columns exist, returns the domain object. Stays PySpark-free.

### Backend shells (`spark/reader.py`, `warehouse/reader.py`)

Each supplies only:

- **execute + extract**: Spark `spark.sql(describe_json_query(qn)).first()[0]`; warehouse
  cursor `execute`/`fetchone`. Supplementary rows: Spark `spark.sql(q).collect()`; warehouse
  `cursor.execute(q); fetchall()`.
- **error classification**: Spark via `AnalysisException.getCondition()`; warehouse via the
  connector's `[CONDITION]` message prefix (the connector-4.x reality already recorded in
  `todo.md`). Centralise the not-found classification in `adapters/databricks/errors.py`.
- `run_info_schema_query`: warehouse = cursor fetch; Spark = direct `spark.sql(...).collect()`
  (production Spark is always Unity Catalog — see scope change below).

Both then call `parse_table_snapshot` + `observed_table_from_snapshot`.

## The AS JSON parser (`describe_json.py`)

Verified against three real Databricks tables (Appendix A).

- **Identity**: use the input `qualified_name`; optionally assert the JSON's
  `catalog_name`/`schema_name`/`table_name` match (sanity check, not a data source).
- **Columns** (`columns[]`): each `{name, type, nullable, comment?}`. Map `type` (a structured
  object keyed by `name`) to a domain `DataType`; casefold the column name; `comment` defaults
  to `""` when the key is **omitted** (columns omit it; the table's own `comment` may instead be
  present as `""`). Preserve declared order.
- **Structured type mapping** (`data_type_from_json`): primitives with aliases
  (`int`/`integer`→`Integer`, `bigint`/`long`→`Long`, `timestamp`/`timestamp_ltz`→`Timestamp`,
  `timestamp_ntz`→`TimestampNtz`, …); `decimal` (`precision`/`scale`); `string`/`char`/`varchar`
  → `String` (ignore `collation`, `length`); `array` (`element_type`, `element_nullable`);
  `map` (`key_type`, `value_type`, `value_nullable`); `struct` (`fields[]` each
  `{name, type, nullable}`, casefold field names — **this is what fixes the struct
  special-character round-trip bug** `todo.md:7`, since names arrive as JSON strings, not
  embedded in a DDL string). Returns `None` for unmappable types (interval, void, geo, future).
- **Column skip/raise policy** (moved from `column_from_catalog`): an unmappable
  **non-partition** column is skipped with a warning; an unmappable **partition** column raises
  (partitioning must be complete or the read is wrong). The parser knows partition membership
  from `partition_columns`.
- **Table comment**: top-level `comment`; `""` and a missing key both mean no comment.
- **Partitioning**: top-level `partition_columns` (ordered array; casefold; `()` when absent).
- **Clustering**: top-level `clustering_columns` (array; casefold; `()` when absent). Ignore the
  stringified `table_properties.clusteringColumns` duplicate.
- **Properties**: top-level `table_properties` (string→string map) projected through
  `DELTA_PROPERTY_POLICY`. The synthesized protocol/feature keys (`delta.feature.*`,
  `minReaderVersion`, `enableDeletionVectors`, …) are not managed keys and drop out. See R1.
- **Constraints**: `table_constraints` string → `parse_table_constraints`.
- **Malformed input** (missing `columns`, a type object with no `name`, a constraint string that
  won't parse) raises `MetadataParseError`, which the reader boundary turns into `ReadFailed`.

## The constraint-string parser (`constraints.py`)

`table_constraints` is a bracketed list of `(constraint_name, BODY)` pairs; BODY is DDL-like
text. Confirmed format (Appendix A, `order_fact`):

```
[(pk_dev_gold_order_fact,PRIMARY KEY (`order_id`)), (fk_…,FOREIGN KEY (`product_id`) REFERENCES `dev`.`gold`.`product_dimension` (`product_id`))]
```

Must handle: a space after the tuple-separating comma; `PRIMARY KEY (`c1`, `c2`)` and
`FOREIGN KEY (`local…`) REFERENCES `cat`.`sch`.`tbl` (`ref…`)`; backtick-quoted identifiers with
**doubled backticks** for a literal backtick; commas **inside** quoted names and inside the
column lists (so it cannot naively split on commas); constraint names containing `,` `(` `)`
`@` `-`; composite keys paired positionally (local ↔ referenced); a 3-part backticked referenced
table. Returns `ParsedConstraints(primary_key_columns, primary_key_name, foreign_keys[...])`;
`describe_json` builds the domain `PrimaryKeyConstraint` / `ForeignKeyConstraint` from it,
carrying the catalog's constraint names as data. `None`/empty (missing key, or a pre-17.3
runtime that omits it) → no constraints.

## Existence & error handling

- **Absent**: the AS JSON attempt on a missing relation raises a not-found condition
  (`TABLE_OR_VIEW_NOT_FOUND`, and schema/catalog-not-found variants). The reader classifies
  those → `TableAbsent`. No separate existence probe.
- **Unreadable**: any other exception (including foreign/federated tables'
  `UNSUPPORTED_FEATURE`, a parse failure, a permission error) → `ReadFailed` carrying the table
  name, stage, and underlying exception. Both `fetch_state` boundaries stay total.
- **Fallback if error-classification proves unreliable** on the warehouse (message-prefix
  parsing): reintroduce a single cheap existence probe. Preferred path is derive-from-error.

## Scope & runtime changes (document, do not preflight)

- **Runtime floor**: `table_constraints` requires **DBR 17.3+ or a SQL warehouse**; base AS JSON
  requires DBR 16.2+. On an older Spark cluster (16.2–17.2) AS JSON succeeds but omits
  constraints → PK/FK read as absent. Document the floor (matching the existing liquid-clustering
  precedent); no version gating in code.
- **hive_metastore reads dropped**: both readers are now Unity-Catalog-only (AS JSON does not
  apply to non-UC tables). The Spark reader's `information_schema` availability probe/cache is
  removed. Update `reference-limitations.md`, which currently says a hive_metastore table is
  readable through the Spark backend.

## Risks

- **R1 — property synthesis (live-gated).** An observed **managed** property that a declaration
  does not declare is a **hard validation failure** (`PropertyUndeclared` finding →
  the `PropertyMustBeDeclared` validation rule), not churn. AS JSON's `table_properties` carries a
  synthesized blob; the danger is a managed key with a platform **default**
  (`logRetentionDuration`, `deletedFileRetentionDuration`, `dataSkippingNumIndexedCols`) being
  emitted when not truly set. Evidence across three real tables is strongly negative: the only
  managed key ever present was `columnMapping.mode`, and only where genuinely set; two tables
  use the default 32-column data-skipping yet omit `dataSkippingNumIndexedCols`.
  **Gate**: a live check that creates a table setting none of the 6 policy-managed keys and confirms
  `table_properties` carries none of them (especially the two retention keys, not exercised by
  the samples). **Fallback if it ever bites**: read properties from `DESCRIBE DETAIL` for that
  key (loud, clear failure message; trivial fix). Keep the change to the property source a
  one-function swap.
- **R2 — constraint-string format drift.** `table_constraints` is undocumented officially.
  Mitigate with the isolated parser + real fixtures; a format change is a loud parse failure
  (`ReadFailed`), not silent bad data.
- **R3 — JSON schema drift.** Databricks guarantees the JSON is automation-stable but it has
  already diverged from the published doc. Parse defensively: tolerate unknown keys and missing
  optional keys.

## Testing

Read correctness moves from local e2e into unit tests over the pure parsers + assembly, using
**real Databricks JSON fixtures** (Appendix A).

- **`test_describe_json.py`** — every type shape (primitive incl. `bigint`/`double`,
  decimal, array, map, nested struct, struct field-name casefolding + special chars),
  nullable/not-null, column comment omitted vs present, table `comment` `""` vs present,
  partitioning, `clustering_columns`, `table_properties` policy projection (incl. the synthesized
  blob dropping out), unmappable non-partition column skipped, unmappable partition column
  raises, malformed JSON → `MetadataParseError`.
- **`test_constraints.py`** — no constraints (`None`/empty), single-column PK, composite PK,
  single FK, composite FK, multiple constraints (space after comma), 3-part backticked
  reference, doubled-backtick and comma-in-name identifiers, malformed string → error.
- **`test_reader.py` (warehouse & spark)** — routed fakes now return an AS JSON string +
  supplementary rows; assert the parsed `ObservedTable` and the **≤4-query contract**; absent via
  the not-found condition; any backend exception → `ReadFailed`.
- **Engine e2e (`tests/e2e/`)** — unchanged in intent; inject a lightweight `_NativeSparkReader`
  (in `tests/`, not shipped) that reads OSS Delta via `spark.table().schema` + `DESCRIBE DETAIL`
  and feeds `observed_table_from_snapshot`. Needs a small `StructType → DataType` mapper
  (test-only). Keeps the full read→diff→plan→execute round-trip local and credential-free.

## Obsolete code to remove

Once the new path is green:

- `sql/queries.py`: `columns_query`, `primary_key_query`, `foreign_keys_query`,
  `table_row_query`, `describe_detail_query`, `information_schema_probe_query`.
- `sql/rows.py`: `column_from_catalog`, `primary_key_from_rows`, `foreign_keys_from_rows`,
  `managed_properties_from_detail_row`, `clustering_columns_from_detail_row`.
- `sql/parse.py` (`parse_data_type`) and `tests/.../test_parse.py` — the DDL type parser loses
  its only user (`column_from_catalog`); the native test reader uses `StructType`, not DDL.
  (`render_data_type` on the write path is untouched.)
- The Spark reader's `information_schema` availability probe/cache and `_ColumnMapping`.
- **Kept**: `referencing_foreign_keys_query`, `table_tags_query`, `column_tags_query` and their
  mappers (`table_tags_from_rows`, `column_tags_from_rows`, `referencing_foreign_keys_from_rows`).

## Docs to update

- `reference-limitations.md` — hive_metastore reads dropped; DBR 17.3+/warehouse floor for
  constraint observation.
- `how-to-implement-adapter.md`, `explanation-architecture.md` — the reader now describes via
  AS JSON + a shared snapshot/assembly; `execution.execute_statements` gains a read-side twin.
- `docs/todo/roadmap.md` #17 and `todo.md` — mark the per-table reduction done; note the
  struct special-character bug (`todo.md:7`) fixed on the parse side; record R1's live gate.

## Acceptance criteria

1. Every `ObservedTable` / `ObservedColumn` / key field is populated correctly from the new path.
2. A present UC Delta table costs **4 round-trips** (1 AS JSON + 3 supplementary); an absent
   table costs only the failed AS JSON attempt. Reader tests assert the query count.
3. Both readers share `parse_table_snapshot`, `parse_table_constraints`, and
   `observed_table_from_snapshot`; the backend shells differ only in execution + error
   classification.
4. Complex types are read from the structured JSON; observed properties are policy-projected;
   composite key/column order preserved.
5. `DESCRIBE DETAIL` and the DDL type parser are gone from the read path.
6. Existing public reader behaviour (`TablePresent`/`TableAbsent`/`ReadFailed`) is preserved.
7. Full suite green: `uv run pytest`, `ruff check`, `mypy src`, `lint-imports`, docs `-W`.

## Appendix A — real Databricks fixtures

Three verified `DESCRIBE TABLE EXTENDED … AS JSON` outputs to seed test fixtures. Store under
`tests/adapters/databricks/sql/fixtures/`.

- `demo_table` — clustered by `id`, single-column PK, `comment: ""`, `columnMapping.mode=name`.
- `users_data` — 14 columns (bigint/double/string), table comment set, columns omit `comment`,
  default 32-col data-skipping present in `statistics` but **not** in `table_properties`, no
  constraints.
- `order_fact` — column comments, mixed nullability, PK + FK in `table_constraints`
  (`REFERENCES \`dev\`.\`gold\`.\`product_dimension\``), `columnMapping.mode=name`.

Full JSON for all three is persisted in `fixtures-describe-json-2026-07-15.md` (this directory);
normalize to strict JSON when creating the fixtures.
