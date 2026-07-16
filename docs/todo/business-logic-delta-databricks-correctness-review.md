# Business logic, Delta, and Databricks correctness review

**Status:** Review complete; findings and proposed solutions await agreement

**Reviewed:** 2026-07-14

**Implementation PR:** Not opened

This is the second phase of the codebase review. The general correctness work is
tracked separately in [PR #227](https://github.com/Tomoscorbin/delta-engine/pull/227),
and the documentation review will follow after this phase is agreed and
implemented.

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

| #    | Severity | Finding                                                       | Failure mode                                                       |
| ---- | -------- | ------------------------------------------------------------- | ------------------------------------------------------------------ |
| 1    | High     | Non-Delta objects are not rejected                            | Invalid or partial plans against views, Iceberg, or other formats  |
| 2    | High     | Unparseable columns are silently omitted                      | Existing drift can be reported as synchronized                     |
| 3    | High     | Required Delta table features are not planned                 | Valid-looking plans fail during execution                          |
| 4    | High     | Foreign-key types are checked against the wrong parent object | Invalid constraints pass declaration and resolution                |
| 5    | Medium   | Clearing a column comment generates invalid SQL               | Warehouse execution fails on `UNSET COMMENT`                       |
| 6    | Medium   | Identifier normalization disagrees with Unity Catalog         | Valid names can change identity; invalid object names pass locally |
| 7    | Medium   | Layout and map-type validation is too permissive              | Unsupported declarations reach execution                           |
| 8 ✅ | Medium   | `CREATE TABLE IF NOT EXISTS` can report false success         | A concurrent incompatible create is treated as success             |

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

1. Read the catalog object type and reject views with a specific `ReadFailed`.
2. Require the detail format to be `delta` in both readers.
3. Return a clear `ReadFailed` for every other format before mapping columns or
   planning changes.

Add shared mapper tests plus live coverage for a view and, where the test
workspace supports it, a non-Delta table.

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

### Follow-up (2026-07-16)

The structured-reader improvement anticipated above shipped as
[PR #241](https://github.com/Tomoscorbin/delta-engine/pull/241), which rebuilt
both readers on `DESCRIBE … AS JSON` and narrowed this finding: malformed
column entries, malformed type shapes, and malformed layout lists now fail the
read, and a skipped column that partitioning, clustering, or a key names fails
`ObservedTable` construction. The one remaining silent path is a column whose
well-formed type name the domain does not model (for example `geography`): it
is skipped with a log warning only, on the grounds that such a type cannot be
declared in a desired table either, so no drift is fabricated.

- [ ] Surface reader-skipped columns in the sync report so a shrunken observed
      schema is visible without log access: carry `skipped_columns` on
      `TablePresent`, derive `has_read_warnings` on `TableRunReport` and
      `SyncReport`, project both into `to_dict()` (bumping `schema_version`
      to 3), and render a "Read warnings" section in `render_report`. Table
      status stays `SUCCESS`; the flag gives CI a separate gate. A first
      implementation was reverted on 2026-07-16 as standing surface not yet
      justified; revisit with live evidence (a type-shape survey table holding
      one column of every modeled type, asserting zero skips).

## 3. Plan required Delta table-feature enablement

### Cause

The desired type and widening rules can admit operations that need a Delta
table feature without inspecting or changing `DESCRIBE DETAIL.tableFeatures`.
This affects at least:

- adding `TIMESTAMP_NTZ` to an existing table;
- widening `DATE` to `TIMESTAMP_NTZ`; and
- adding `VARIANT` to an existing table.

The current successful live widening test enables `timestampNtz` out of band,
while the safety test demonstrates that the warehouse rejects the same change
without that prerequisite.

Relevant code:

- `src/delta_engine/application/validation.py` (type-widening matrix)
- `tests/live/test_sql_warehouse_live_safety.py`
- `tests/live/test_sql_warehouse_live_types_and_layout.py`

Databricks requires explicit feature enablement for existing tables using
[TIMESTAMP_NTZ](https://docs.databricks.com/gcp/en/sql/language-manual/data-types/timestamp-ntz-type)
or [VARIANT](https://docs.databricks.com/aws/en/tables/features/variant).

### Proposed solution

1. Preserve the observed `tableFeatures` in the reader model.
2. Determine required features recursively from the desired type tree.
3. Add an explicit `EnableTableFeature` action before any dependent column
   action when the feature is absent.
4. Compile that action to the documented `delta.feature.* = supported` table
   property and include it in dry-run output.
5. Do nothing when the feature is already enabled.

The protocol change is permanent, so it must be visible in the plan. The
alternative—rejecting the declaration and requiring an out-of-band command—is
safe but does not provide declarative convergence. The proposed action is the
preferred solution.

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

Remove `IF NOT EXISTS`. A concurrent create should produce an `ExecutionFailed`
instead of a false success. The user can rerun synchronization, at which point
the reader will observe and diff the table that actually exists.

Re-reading and reconciling after a possible no-op would also be correct, but it
would add a second read/execution protocol solely to preserve the guard. Failing
the race explicitly is simpler and consistent with the engine's no-retry
policy.

## Proposed implementation boundary

The implementation PR for this review should contain all eight corrections,
with these deliberate limits:

- fail closed on lossy type parsing now; do not bundle the structured JSON
  schema-reader refactor;
- model required table-feature enablement because it is part of converging a
  supported declared type, but do not add general runtime/version preflight;
- enforce the current Databricks platform constraints without adding new
  declaration capabilities; and
- keep documentation restructuring and general documentation accuracy work for
  the final documentation-review phase.

Before the implementation PR is ready for merge, run:

- focused unit tests for each changed declaration, resolver, reader, and
  compiler path;
- the full non-live suite and normal lint, type, and architecture checks; and
- the live SQL warehouse suite, including the restored comment transition and
  new feature/reader guard cases, against Unity Catalog.

## Agreement checklist

- [ ] The eight items above are accepted as correctness defects.
- [ ] The proposed solution for each item is accepted.
- [ ] Feature enablement will be represented as a visible planned action rather
      than an out-of-band prerequisite.
- [ ] Lossy schema parsing will fail closed now; structured JSON observation is
      deferred.
- [ ] Once agreed, implementation will be isolated in its own PR.
