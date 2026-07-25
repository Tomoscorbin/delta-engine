# Business logic, Delta, and Databricks correctness review

**Status:** Fresh sweep complete; original item 3 and fresh findings 9–15
await agreement and implementation

**Original review:** 2026-07-14

**Fresh sweep:** 2026-07-20

**Fresh-sweep implementation PR:** Not opened

This began as the second phase of the codebase review. Seven of the original
eight findings have since been implemented, as recorded in their resolution
notes below. On 2026-07-20 the current `main` branch was swept again from the
public API through domain validation, dependency resolution, diffing,
observation, SQL compilation, and execution reporting. The fresh findings were
checked against the live Databricks and Delta documentation linked in each
section.

## Scope

This review asks whether the engine:

- accepts only declarations that it can reconcile correctly on Databricks;
- observes enough catalog state to detect drift without silently omitting it;
- generates valid Delta and Databricks SQL for supported changes;
- makes permanent Delta table-feature changes explicit in the plan; and
- reports success only when the requested postcondition has been established.

It does not propose new product features merely because Databricks supports
them. Partition-to-liquid-clustering conversion, `RELY`, `UNIQUE`, and richer
schema adoption are therefore outside this review unless an existing engine
path currently produces an incorrect result.

## Summary

| # | Severity | Finding | Failure mode |
| --- | --- | --- | --- |
| 1 ✅ | High | Non-Delta objects are not rejected | Invalid or partial plans against views, Iceberg, or other formats |
| 2 ✅ | High | Unparseable columns are silently omitted | Existing drift can be reported as synchronized |
| 3 | High | Required Delta table features are not planned | Valid-looking plans fail during execution |
| 4 ✅ | High | Foreign-key types are checked against the wrong parent object | Invalid constraints pass declaration and resolution |
| 5 ✅ | Medium | Clearing a column comment generates invalid SQL | Warehouse execution fails on `UNSET COMMENT` |
| 6 ✅ | Medium | Identifier normalization disagrees with Unity Catalog | Valid names can change identity; invalid object names pass locally |
| 7 ✅ | Medium | Layout and map-type validation is too permissive | Unsupported declarations reach execution |
| 8 ✅ | Medium | `CREATE TABLE IF NOT EXISTS` can report false success | A concurrent incompatible create is treated as success |
| 9 | High | Non-default `STRING` collations are erased on observation | Collation drift can be reported as synchronized |
| 10 ✅ | Medium | Column tags are not removed before dropping a column | Governed-tagged column drops fail during execution |
| 11 | Medium | Tag declarations omit Databricks tag constraints | Invalid tag declarations reach execution |
| 12 | Medium | Some validation runs before identifier normalization | Invalid layouts pass and valid foreign keys can be rejected |
| 13 | Medium | Generated constraint names can be invalid or collide | Constraint creation fails on Unity Catalog |
| 14 | Medium | Dependency traversal is recursive | A valid deep graph can abort synchronization with `RecursionError` |
| 15 | Low | `Decimal` accepts non-integer precision and scale | The model can compile invalid `DECIMAL` SQL |

## 1. Reject views and non-Delta tables at the read boundary

### Cause

`table_row_query` establishes that a catalog object exists but does not
distinguish a table from a view. Both readers subsequently fetch `DESCRIBE
DETAIL`, but they ignore its `format` field when constructing the observed
table.

Relevant code:

- `src/delta_engine/adapters/databricks/sql/queries.py` (`table_row_query`)
- `src/delta_engine/adapters/databricks/warehouse/reader.py` (`fetch_state`)
- `src/delta_engine/adapters/databricks/spark/reader.py` (`fetch_state`)

A view reaches `DESCRIBE DETAIL` and fails with a confusing read error. A
non-Delta table for which detail succeeds is more dangerous: the engine can
diff it as though it were Delta and plan properties, constraints, clustering,
or other Delta-specific DDL against it.

`DESCRIBE DETAIL` exposes `format` as `delta` or `iceberg`, so the format is
already available at the point where the reader decides what kind of state it
has observed. See the
[Databricks table-detail reference](https://docs.databricks.com/gcp/en/tables/operations/table-details).

### Proposed solution

1. Read the catalog object type and reject views with a specific `ReadError`.
2. Require the detail format to be `delta` in both readers.
3. Raise a clear `ReadError` for every other format before mapping columns or
   planning changes.

Add shared mapper tests plus live coverage for a view and, where the test
workspace supports it, a non-Delta table.

### Resolved (2026-07-16)

Superseded in shape by the AS JSON reader (PR #241) and closed by the relation
acceptance policy in `read.py`: the shared read admits only relations with
`type` MANAGED or EXTERNAL and `provider` delta — the facts are carried on
`TableDescription` from the AS JSON document's own fields (no extra query) and
judged in `read_catalog_state` — and raises `UnsupportedRelationError` for
everything else — views, materialized views, streaming tables, foreign
tables, non-Delta formats, and any relation kind Databricks adds later — which
surfaces as `ReadError` at the typed read boundary. An acceptance set rather
than a rejection list, so unknown kinds fail closed by construction. Existing
EXTERNAL tables are read and reconciled; creating one is a tracked follow-up
(todo.md). Unit coverage in `sql/test_describe.py` and `test_read.py`; live
coverage for a view, a streaming table, and an Iceberg table in
`tests/live/test_sql_warehouse_live_supported_relations.py`. The earlier
standalone attempt (PR #238, guards over `table_row_query` + `DESCRIBE DETAIL`)
was closed as stale when those read paths were deleted.

## 2. Fail closed when any observed column type cannot be mapped

### Cause

The shared row mapper raises for an unparseable partition column but logs and
returns `None` for an ordinary unparseable column. The caller filters out that
`None`, making the column invisible to synchronization.

Relevant code:

- `src/delta_engine/adapters/databricks/sql/rows.py` (`map_column_row`)
- `tests/adapters/databricks/sql/test_rows.py`
- `tests/live/test_sql_warehouse_live_platform_assumptions.py`

For example, if the catalog contains `id INT, geo GEOGRAPHY` and the desired
declaration contains only `id`, the current reader can report the table as
synchronized after dropping `geo` from the observed state. The same lossy path
is exercised by nested struct field names containing special characters:
Unity Catalog accepts the declaration with column mapping, but
`information_schema` returns a type string that the current parser cannot
round-trip.

### Proposed solution

1. Treat every unmappable observed column as a read failure; no column may be
   silently omitted.
2. Until the reader is upgraded, reject nested field names that the textual
   parser cannot round-trip, even when column mapping is enabled.
3. Change the existing skip tests to assert a typed read failure and add a live
   convergence test for the nested-name case.

A later, separate improvement should use structured schema data. Databricks
supports `DESCRIBE TABLE ... AS JSON`, including structured `type_json`, on
current runtimes and SQL warehouses. See the
[DESCRIBE TABLE reference](https://docs.databricks.com/gcp/en/sql/language-manual/sql-ref-syntax-aux-describe-table).
That larger reader refactor is not required to make the current behavior safe.

### Resolved (2026-07-16)

Fixed in [PR #241](https://github.com/Tomoscorbin/delta-engine/pull/241), which
rebuilt both readers on `DESCRIBE … AS JSON` and then made the column reader
fail closed on every unmappable observed column. Malformed column entries,
malformed type shapes, and malformed layout lists fail the read; and any type
the domain cannot model — an unknown or future type name (for example
`geography`), an unrepresentable nested type, or a domain-constructor rejection
— now also fails the parse rather than being skipped, surfacing as `ReadError`
at the typed read boundary. No observed column is silently omitted, so the
earlier idea of surfacing "skipped columns" in the report is dropped as moot:
there is no shrunken observed state left to report.

The nested-name round-trip case (item 2 of the proposed solution) is subsumed:
the structured AS JSON types avoid the textual type-string parsing that dropped
those columns, and any struct the domain still cannot represent fails the read
rather than being skipped.

## 3. Plan required Delta table-feature enablement

### Cause

The desired type and widening rules admit operations that need a Delta table
feature, but the observed model does not carry authoritative feature state and
the plan has no feature-enablement action. The AS JSON reader currently removes
`delta.feature.*` entries while building managed table properties, and
`ObservedTable` has no separate feature field. This affects at least:

- adding `TIMESTAMP_NTZ` anywhere in the type tree of an existing table;
- widening `DATE` to `TIMESTAMP_NTZ`; and
- adding `VARIANT` anywhere in the type tree of an existing table.

The successful live widening case still enables `timestampNtz` out of band.
Without that prerequisite, the warehouse rejects the same otherwise-supported
change.

Relevant code:

- `src/delta_engine/adapters/databricks/read.py`
  (`DELTA_PROPERTY_POLICY.project_observed` and observed-table construction)
- `src/delta_engine/domain/model/table.py` (`ObservedTable`)
- `src/delta_engine/application/validation.py` (type-widening matrix)
- `src/delta_engine/domain/plan/actions.py` and
  `src/delta_engine/domain/plan/diff.py`
- `tests/live/test_sql_warehouse_live_safety.py`
- `tests/live/test_sql_warehouse_live_types_and_layout.py`

Databricks documents explicit feature enablement for existing tables using
[TIMESTAMP_NTZ](https://docs.databricks.com/aws/en/sql/language-manual/data-types/timestamp-ntz-type)
or [VARIANT](https://docs.databricks.com/aws/en/tables/features/variant).
`DESCRIBE DETAIL` exposes the enabled feature list in `tableFeatures`; see the
[table-detail schema](https://docs.databricks.com/aws/en/delta/table-details).

### Proposed solution

1. Observe enabled table features authoritatively, preferably from
   `DESCRIBE DETAIL.tableFeatures`, and preserve them in `ObservedTable`.
   Retaining an AS JSON property is acceptable only if it is documented to be
   equivalent for every feature the engine supports.
2. Determine required features recursively from the complete desired type tree,
   including array elements, map keys and values, and struct fields.
3. Add an explicit `EnableTableFeature` action before every dependent column
   action when the feature is absent.
4. Compile that action to the documented `delta.feature.* = 'supported'` table
   property and include it in dry-run output.
5. Do nothing when the feature is already enabled, and do not emit a redundant
   action for table creation when Databricks enables the feature from the
   created schema.
6. Add focused planner/compiler tests and live cases for both absent and
   already-enabled feature state.

Feature activation is a permanent protocol change, so it must be visible in
the plan. Rejecting the declaration and requiring an out-of-band command would
also be safe, but would not provide declarative convergence.

## 4. Validate foreign keys against the registered parent definition

### Cause

`DeltaTable` validates child column types against the particular object held by
`ForeignKey.references`. Dependency resolution then finds the registered table
with the same qualified name and verifies its primary-key column names, but not
their types.

Relevant code:

- `src/delta_engine/api/delta_table.py` (`_validate_foreign_keys`)
- `src/delta_engine/application/dependency_resolution.py`

It is therefore possible to construct a foreign key using one parent object,
register a different parent declaration under the same qualified name, and
pass preparation and dependency resolution even though the registered parent
has incompatible column types. Databricks rejects the constraint later because
each foreign-key column type must match the referenced column type. See the
[Databricks constraint reference](https://docs.databricks.com/aws/en/sql/language-manual/sql-ref-syntax-ddl-create-table-constraint).

### Proposed solution

1. During dependency resolution, compare each local child type with the type
   on the actual registered parent declaration.
2. Add a typed foreign-key failure reason such as
   `REFERENCED_COLUMN_TYPE_MISMATCH`.
3. Propagate the parent-resolution failure to dependent tables using the
   existing failure mechanism.

Cover the mismatch through resolver tests, engine tests, and the public
declaration surface.

### Resolved (2026-07-17)

Fixed in [PR #256](https://github.com/Tomoscorbin/delta-engine/pull/256).
Dependency resolution now compares each local column's type with the
referenced column's type on the registered parent declaration and fails the
table with the new `REFERENCED_COLUMN_TYPE_MISMATCH` reason. The check runs
after `REFERENCED_COLUMNS_NOT_A_KEY` — which guarantees every referenced
column exists on the registered parent — and before cycle classification,
mirroring the existing structural-check precedence; dependents of a
mismatched table are blocked through the existing propagation pass.
Resolver, engine, and failure-detail coverage all construct the mismatch
through the public declaration surface (an FK declared against one parent
object while a differently-typed declaration is registered under the same
qualified name).

## 5. Compile an empty column comment as `COMMENT ''`

### Cause

The SQL compiler emits `ALTER COLUMN ... UNSET COMMENT` when the desired
comment is empty. Databricks SQL warehouses reject that syntax. The live
lifecycle test currently avoids the empty-comment transition because it was
already demonstrated to fail on the real platform.

Relevant code:

- `src/delta_engine/adapters/databricks/sql/compile.py`
- `tests/live/test_sql_warehouse_live.py`

The supported form is `ALTER COLUMN ... COMMENT '...'`; see the
[ALTER TABLE reference](https://docs.databricks.com/aws/en/sql/language-manual/sql-ref-syntax-ddl-alter-table).

### Proposed solution

Always compile the comment action with a string literal, including
`COMMENT ''`. Restore the empty-comment transition to the live lifecycle test.

### Resolved (2026-07-14)

Shipped in commit 05952ce, reapplying the fix reverted out of PR #213: the
compiler always emits a string literal, so an empty desired comment compiles
to `COMMENT ''` rather than `UNSET COMMENT`, which SQL warehouses reject
(verified live 2026-07-12). `''` round-trips as the empty comment the reader
observes, keeping resyncs idempotent on both backends. The live lifecycle
test's empty-comment transition on `account_code` was restored with the fix.

## 6. Align identifier normalization with Unity Catalog

### Cause

Declared and observed identifiers use `casefold()` as though it meant
lowercase. It is a stronger Unicode transformation: the lowercase identifier
`straße`, for example, is rejected on declaration and becomes `strasse` on
observation. Those are different identifiers to quote and send back to the
catalog.

At the same time, qualified object-name parts can contain characters or lengths
that Unity Catalog rejects, despite the SQL compiler quoting them.

Relevant code:

- `src/delta_engine/domain/model/qualified_name.py`
- `src/delta_engine/domain/model/column.py`
- `src/delta_engine/adapters/databricks/sql/rows.py`

Unity Catalog stores object names in lowercase, preserves column-name casing,
and treats column references case-insensitively. It also limits object names to
255 characters and forbids periods, spaces, slashes, control characters, and
DEL. See the
[Unity Catalog requirements](https://docs.databricks.com/aws/en/data-governance/unity-catalog/requirements).

### Proposed solution

1. Centralize Databricks identifier validation and normalization.
2. Use `lower()` rather than `casefold()` for the engine's chosen lowercase
   canonical form on both declaration and observation paths.
3. Validate catalog, schema, and table parts against Unity Catalog length and
   forbidden-character rules.
4. Retain the existing column-mapping-specific rules for column and nested-field
   characters rather than applying table-name restrictions to columns.

Add Unicode regression tests that prove normalization never changes a valid
lowercase identifier into a different spelling.

### Resolved (2026-07-25)

The 2026-07-17 fix first replaced `casefold()` with `lower()` and normalized
all identifiers in constructors. The completed policy now separates identity
from spelling: qualified catalog, schema, and table parts still normalize with
`str.lower()` because Unity Catalog stores them that way, while column-like
identifiers preserve their declared or observed spelling.

All case-insensitive comparisons, duplicate checks, and lookups derive an
explicit `identifier_key`; nested data types use a recursive semantic identity.
Executable plans bind column references to the exact post-sync physical
spelling before SQL compilation. This fixes both the Unicode rewrite and the
managed-constraint paths that require exact camelCase spelling. The behavior is
covered from domain constructors through API, diff, planning, adapters, and
the live Databricks primary-key and foreign-key cases. See
`2026-07-24-column-identifier-spelling-design.md`.

## 7. Separate partition and clustering type rules, and validate map keys

### Cause

Partitioning and clustering currently share one tuple of unsupported complex
types. Databricks supports Boolean and Binary partition columns, but its liquid
clustering type list excludes them. Both are accepted as clustering keys by the
current declaration model.

The `Map` model also accepts another `Map` as its key type, while Databricks
allows any key type except `MAP`.

Relevant code:

- `src/delta_engine/api/delta_table.py` (`UNSUPPORTED_LAYOUT_TYPES` and layout
  validation)
- `src/delta_engine/domain/model/data_type.py` (`Map`)

Platform references:

- [Liquid clustering supported types](https://docs.databricks.com/aws/en/delta/clustering)
- [Partition column restrictions](https://docs.databricks.com/gcp/en/tables/partitions)
- [MAP key restrictions](https://docs.databricks.com/aws/en/sql/language-manual/data-types/map-type)

### Proposed solution

1. Give partitioning and clustering separate supported-type rules.
2. Reject Boolean and Binary clustering keys at declaration time.
3. Add `Map.__post_init__` validation that rejects a `Map` key type.

These are deterministic declaration errors and need no runtime probing.

### Resolved (2026-07-17)

The shared `_TYPES_UNUSABLE_AS_LAYOUT_KEYS` tuple was split into
`_TYPES_UNUSABLE_AS_PARTITION_KEYS` (Array/Map/Struct/Variant, unchanged) and
`_TYPES_UNUSABLE_AS_CLUSTERING_KEYS`, which adds Boolean and Binary. The
clustering exclusion was verified against the liquid-clustering supported-type
list, whose enumerated types omit Boolean and Binary even though both are valid
partition columns. `Map` gained a `__post_init__` that rejects a `Map` key
type, matching the platform rule that a MAP key may be any type except MAP; a
MAP value may still be a MAP.

Coverage: API-layer tests assert clustering rejects Boolean and Binary while
partitioning still accepts them (the two rules are now distinct), and
domain-layer tests assert the Map key rejection and that a Map value type is
still allowed. The shared AS JSON type-document strategy was narrowed to stop
generating map-keyed maps, which the domain now forbids. Both changes are
declaration-time errors with no runtime probing, so no live coverage is
required; the live partition-rejection assumption test was reworded to name the
now-separate partition and clustering type lists.

## 8. Remove the false-success race from table creation

**Status:** Done — `IF NOT EXISTS` removed from `compile_create_table`.

### Cause

Creation compiles to `CREATE TABLE IF NOT EXISTS`. If another writer creates an
incompatible table after the engine observes absence but before this statement
runs, Databricks performs a no-op and the executor reports the planned create as
successful. The requested table postcondition has not been established.

Relevant code:

- `src/delta_engine/adapters/databricks/sql/compile.py` (`compile_create_table`)

### Proposed solution

Remove `IF NOT EXISTS`. A concurrent create should produce an `ExecutionFailure`
instead of a false success. The user can rerun synchronization, at which point
the reader will observe and diff the table that actually exists.

Re-reading and reconciling after a possible no-op would also be correct, but it
would add a second read/execution protocol solely to preserve the guard. Failing
the race explicitly is simpler and consistent with the engine's no-retry
policy.

## Fresh-sweep findings (2026-07-20)

The following findings are against the implementation after the seven resolved
items above. They are not restatements of the original defects.

## 9. Fail closed on unsupported `STRING` collations

### Cause

The AS JSON type parser recognizes `string` as a simple scalar and returns
`String()` before considering the type document's `collation` field. A catalog
column using a non-default collation is therefore observed as an ordinary
string. The diff can report synchronization even though the effective string
comparison and ordering semantics differ.

The loss occurs recursively too: a collated string nested in an array, map, or
struct follows the same simple-type path.

Relevant code:

- `src/delta_engine/adapters/databricks/sql/types.py`
  (`data_type_from_json` and the simple-type mapping)
- `src/delta_engine/domain/model/data_type.py` (`String`)
- `tests/adapters/databricks/sql/test_types.py`

Databricks includes the effective string collation in the structured type
document returned by
[`DESCRIBE TABLE ... AS JSON`](https://docs.databricks.com/aws/en/sql/language-manual/sql-ref-syntax-aux-describe-table)
and supports collation in column definitions and alterations; see
[manage column clauses](https://docs.databricks.com/aws/en/sql/language-manual/sql-ref-syntax-ddl-alter-table-manage-column).

### Proposed solution

1. Inspect `collation` before returning the simple `String` domain type at
   every depth of the type tree.
2. For the present product boundary, accept only an absent/default
   `UTF8_BINARY` collation and fail the table read for any other value. Do not
   silently coerce it to `String()`.
3. If declarative collation support is added later, model the collation on
   `String` and carry it through equality, SQL compilation, and alteration as
   one complete change.
4. Add parser tests for default, non-default, and nested collations, plus a live
   observation pin for a non-default collated column.

Failing closed is the narrow correctness fix; adding collation declarations is
a separate product capability.

## 10. Remove column tags before dropping governed-tagged columns

### Cause

`_diff_column_tags` iterates desired columns only. When full reconciliation
drops an observed-only column, the plan emits `DropColumn` without unsetting
that column's observed tags. The action ordering would still be wrong if the
unset were added naively: `DROP_COLUMN` currently precedes the column-tag
phase.

Databricks requires governed tags to be removed before a tagged column can be
dropped. See
[governed-tag restrictions](https://docs.databricks.com/aws/en/database-objects/tags).

Relevant code:

- `src/delta_engine/domain/plan/diff.py` (`_diff_column_tags` and column
  removal)
- `src/delta_engine/domain/plan/actions.py` (`ActionPhase`)
- `tests/domain/plan/test_diff.py`

### Proposed solution

1. In the scope that manages column structure, emit `UnsetColumnTag` for every
   observed tag on every observed-only column.
2. Order those unsets before `DropColumn`. If necessary, give tag unsets and tag
   sets separate phases so ordinary updates retain their existing order.
3. Keep tag-only and metadata-only scopes non-destructive: they must not infer a
   column drop.
4. Add a plan-order regression test. Add a live governed-tag drop test when the
   test principal can create governed tags; otherwise retain an explicit
   platform-assumption pin and exercise the SQL order with ordinary tags.

### Implemented

Observed tags on removed columns now produce `UnsetColumnTag` actions alongside
the `DropColumn`. Action ordering places column-tag sets first, tag unsets next,
and column drops last, preserving ordinary tag reconciliation while ensuring
cleanup runs before removal. Domain regression coverage pins both fact
production and plan ordering; live governed-tag coverage remains unavailable
until the test principal can create governed tags.

## 11. Enforce Databricks tag declaration constraints

### Cause

`_validate_tags` enforces the existing per-object count and length rules, but it
does not enforce all Databricks syntax and aggregate limits. In particular:

- tag keys containing `.`, `,`, `-`, `=`, `/`, or `:` are accepted;
- leading or trailing spaces in tag keys or values are accepted; and
- the limit of 1,000 total column-tag assignments on one table is not checked.

The current test suite even treats `any.custom-key` as valid, although the
platform forbids both the period and hyphen. These declarations survive local
validation and fail only when Databricks executes the DDL.

Relevant code:

- `src/delta_engine/api/delta_table.py` (`_validate_tags` and table-wide tag
  validation)
- `tests/api/test_delta_table.py`

Platform reference:
[Databricks tag constraints](https://docs.databricks.com/aws/en/database-objects/tags).

### Proposed solution

1. Reject every documented forbidden character in a tag key.
2. Reject leading or trailing spaces in both keys and values without silently
   trimming user input.
3. Count tag assignments across all columns and reject totals above 1,000.
4. Retain the existing per-object count and length checks.
5. Replace the stale permissive test and add a boundary matrix for characters,
   whitespace, 1,000 assignments, and 1,001 assignments.

These are deterministic declaration errors and do not need runtime discovery.

## 12. Normalize identifiers before layout and foreign-key validation

This is a residual public-boundary gap after items 6 and 7; it does not reopen
their domain-level fixes.

### Cause

Two validation paths inspect raw API input before the normalized domain objects
are constructed:

1. `_validate_layout` checked raw `partitioned_by` and `clustered_by` names,
   then `DesiredTable` lowercased them. A mixed-case spelling could bypass a
   type restriction or the partition-all-columns check. For example, a Binary
   column named `flag` with `clustered_by=["FLAG"]` could be accepted even
   though Binary is not a supported liquid-clustering type.
2. `ForeignKey._to_constraint` compares raw mapping names with normalized parent
   key columns and performs raw local-column lookups. A valid mapping such as
   `{"parent_id": "ID"}` can therefore be rejected even though Databricks
   identifiers are case-insensitive.

Relevant code:

- `src/delta_engine/api/delta_table.py` (`_validate_layout`,
  `ForeignKey._to_constraint`, and `DeltaTable.__init__`)
- `src/delta_engine/domain/model/table.py`
- `src/delta_engine/domain/model/constraints.py`

Databricks' identifier behavior is documented in
[Names and identifiers](https://docs.databricks.com/aws/en/sql/language-manual/sql-ref-names);
the clustering type restriction is documented in
[Liquid clustering](https://docs.databricks.com/aws/en/delta/clustering).

### Proposed solution

1. Canonicalize layout names and both sides of every foreign-key mapping at the
   beginning of their API conversion paths.
2. Run membership, duplicate, all-columns, key-equivalence, and type checks only
   against canonical names.
3. Pass those same canonical values into the domain constructors; validation
   and storage must not see different spellings.
4. Add mixed-case positive and negative tests, including the Binary clustering
   bypass and a mixed-case referenced primary-key column.

### Resolved (2026-07-25)

Declaration validation, domain structure checks, and foreign-key resolution now
index columns by `identifier_key` while preserving the input spelling. The
mixed-case layout and foreign-key matrices cover the former bypasses, and
planning binds accepted references to the resulting physical schema.

## 13. Stop synthesizing unsafe physical constraint names

### Cause

Primary-key and foreign-key names are synthesized by concatenating table and
column names:

- `{table_name}_pk`; and
- `{owner_table}_{local_columns}_fk`.

That construction is not closed over the valid public declaration space. A
255-character table name already produces an over-length primary-key name. A
valid column name such as `net/gross` can introduce a character that Unity
Catalog forbids in an object identifier. Separator ambiguity can also produce
the same schema-wide constraint name from different table/column combinations.

Relevant code:

- `src/delta_engine/domain/model/constraints.py`
- `src/delta_engine/adapters/databricks/sql/compile.py`
- `src/delta_engine/adapters/databricks/read.py`

Databricks makes the constraint name optional in
[`ADD CONSTRAINT`](https://docs.databricks.com/aws/en/sql/language-manual/sql-ref-syntax-ddl-alter-table-add-constraint)
and requires a supplied name to be unique within the schema. Unity Catalog
identifier limits are described in
[Names and identifiers](https://docs.databricks.com/aws/en/sql/language-manual/sql-ref-names).

### Proposed solution

1. Prefer omitting the physical constraint name in create/add DDL and let
   Databricks allocate it. The reader already observes the resulting name, and
   constraint equality/diffing should remain content-based.
2. If explicit names prove necessary, introduce a platform-safe, bounded,
   unambiguous generator with a stable digest and account for schema-wide
   collision scope. Simple truncation is not sufficient.
3. Add cases for a 255-character table name, special characters valid only for
   columns, and ambiguous table/column concatenations.
4. Confirm live that generated platform names can be observed and subsequently
   used for drop/reconciliation.

## 14. Make dependency-cycle detection iterative

### Cause

The strongly connected components traversal is recursive. A valid acyclic
dependency chain near Python's recursion limit aborts the whole synchronization
before per-table results can be produced. A local probe with 1,050 tables
raised `RecursionError: maximum recursion depth exceeded`.

Relevant code:

- `src/delta_engine/application/dependency_resolution.py`
  (`_strongly_connected_components`)
- `src/delta_engine/application/engine.py` (`resolve`)
- `tests/application/test_dependency_resolution.py`

### Proposed solution

1. Replace the recursive traversal with an iterative SCC implementation that
   keeps discovery, low-link, and component state explicitly.
2. Preserve the current deterministic component and failure ordering.
3. Add a regression graph comfortably above the interpreter recursion limit,
   covering both a deep acyclic chain and a deep graph containing a cycle.
4. Do not raise the process recursion limit as a library side effect.

## 15. Require integer `Decimal` precision and scale

### Cause

`Decimal.__post_init__` checks numeric ranges but not runtime types.
`Decimal(10.5, 0)` and `Decimal(True, 0)` are accepted and can render invalid or
misleading `DECIMAL` SQL. Python type annotations do not enforce this at
runtime, and `bool` is an `int` subclass.

Relevant code:

- `src/delta_engine/domain/model/data_type.py` (`Decimal`)
- `tests/domain/model/test_data_type.py`

Databricks requires integer precision and scale in the
[`DECIMAL` type](https://docs.databricks.com/aws/en/sql/language-manual/data-types/decimal-type).

### Proposed solution

1. Require `type(precision) is int` and `type(scale) is int` before applying the
   existing range checks.
2. Raise the normal declaration-time `ValueError` for floats, booleans, strings,
   and other non-integer values.
3. Add a type/value matrix while retaining the current precision and scale
   boundary tests.

## Residual concurrency limitation

Synchronization is still an observe-plan-execute sequence of independent
statements, not a transaction over the complete table declaration. Removing
`IF NOT EXISTS` in item 8 correctly turns the concurrent-create no-op into a
failure, but another actor can still alter an existing table between observation
and any later DDL, or immediately after the final action. A successful report
therefore means that the planned statements succeeded, not that the complete
desired state was re-read and verified.

This sweep does not classify that as a deterministic implementation defect
without first choosing a concurrency contract. Before claiming a stronger
postcondition, choose and document one of:

- a single-writer/no-concurrent-DDL contract; or
- post-execution observation and reconciliation verification, with a typed
  failure when the desired state is not established at that verification
  point.

A verification read narrows the race and detects interleaving; it cannot prevent
a later external writer from changing the table.

## Fresh-sweep verification

The sweep was performed on `main` at
`406a51243e02b30bf457e7fa2c4bea0add2853d5`. Before this document edit:

- `uv run pytest -q` completed with 956 passed and 63 credentialed/live tests
  deselected;
- `uv run ruff check .` passed;
- `uv run mypy` passed for 56 source files; and
- `uv run lint-imports` passed all seven architecture contracts.

Targeted local probes reproduced the collation loss, governed-tag drop plan,
invalid tag acceptance, mixed-case layout and foreign-key behavior, unsafe
constraint-name generation, deep-graph recursion failure, and non-integer
`Decimal` acceptance.

The credentialed SQL warehouse suite was not run during this sweep because it
mutates a real Unity Catalog workspace. Databricks/Delta-specific conclusions
were instead compared with the official documentation linked above as retrieved
on 2026-07-20. The new platform-sensitive cases remain required live coverage
for their implementation PR.

## Fresh-sweep implementation boundary

The next correctness work should cover original item 3 and fresh items 9–15.
It may be split into reviewable PRs, but no item should be marked resolved
without its observation, planning, compilation, and ordering consequences being
handled together.

Recommended grouping:

1. observed semantics and Delta protocol: items 3 and 9;
2. tag validity and action ordering: items 10 and 11;
3. declaration normalization and physical names: items 12 and 13; and
4. general robustness: items 14 and 15.

Deliberate limits:

- fail closed on unsupported collations now; do not bundle first-class collation
  declarations;
- model required table-feature enablement because it is part of converging an
  already-supported declared type, but do not add general runtime/version
  preflight;
- enforce current Databricks constraints without adding unrelated declaration
  capabilities; and
- decide the concurrency contract separately rather than implying that these
  deterministic corrections make a multi-statement sync transactional.

Before an implementation PR is ready for merge, run:

- focused regression tests for each changed API, domain, resolver, reader,
  planner, compiler, and ordering path;
- the full non-live suite and normal lint, type, and architecture checks; and
- the live SQL warehouse cases for table features, collations, governed tags,
  and platform-generated constraint names against Unity Catalog.

## Agreement checklist

- [ ] Original item 3 and fresh items 9–15 are accepted as correctness defects.
- [ ] Table-feature enablement will be an observed, visible planned action
      rather than an out-of-band prerequisite.
- [ ] Unsupported collations will fail closed until first-class collation
      declarations are intentionally added.
- [ ] Governed column tags will be unset before drops, and declarations will
      enforce the documented tag constraints.
- [ ] API identifiers will be canonicalized before validation, and physical
      constraint names will be delegated to Databricks unless a safe generator
      is demonstrated.
- [ ] Dependency traversal will be iterative and `Decimal` will enforce runtime
      integer inputs.
- [ ] The concurrency contract and success postcondition will be documented
      explicitly.
- [ ] Implementation will remain isolated from unrelated product and
      documentation work.
