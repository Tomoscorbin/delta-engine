---
tags:
  - reference
---

# Safe-change rules

The engine validates the computed diff before executing any SQL. These rules block changes that cannot be made safely in place. Each produces a validation failure and a `PLANNING_FAILED` status with a message naming the rule and the affected column or table. They are the validation-rules layer of [the safety model](explanation-safety-model.md), which explains how they fit alongside declaration-time checks and managed aspects.

| Rule                                    | What it blocks                                                                                                                                                                            | How to resolve                                                                                         |
| --------------------------------------- | ----------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------- | ------------------------------------------------------------------------------------------------------ |
| `NonNullableColumnAdd`                  | Adding a `NOT NULL` column to an existing table                                                                                                                                           | Add the column as nullable, backfill, set `NOT NULL` outside the engine, then declare `nullable=False` |
| `NullabilityTighteningOnExistingColumn` | Changing an existing nullable column to `NOT NULL`                                                                                                                                        | Backfill existing NULLs, set `NOT NULL` outside the engine, then declare `nullable=False`              |
| `NonWideningColumnTypeChange`           | Changing a column's declared data type outside the widening matrix ([type widening](how-to-configure-table.md#type-widening))                                                             | Drop and recreate the table out of band, then re-sync                                                  |
| `TypeWideningRequiredForTypeChange`     | A widening type change without `delta.enableTypeWidening='true'` declared                                                                                                                 | Declare the property (it may be set in the same sync as the widen)                                     |
| `PartitioningChangeNotSupported`        | Changing `partitioned_by` on an existing table                                                                                                                                            | Rewrite/recreate it, or convert it to clustering out of band, then re-sync                             |
| `PropertyTransitionNotSupported`        | A property transition that is unsafe in place — the catalog rejects it, or applying it would rewrite the table (e.g. `delta.columnMapping.mode` `name` → `none` rewrites every data file) | Update the declaration to match the catalog value                                                      |
| `PropertyMustBeDeclared`                | A managed property set on the table but missing from the declaration                                                                                                                      | Declare it (or declare it `None` to remove it, where removal is possible)                              |
| `ColumnMappingRequiredForDrop`          | A plan drops a column but the declaration lacks `delta.columnMapping.mode='name'`                                                                                                         | Declare the property (it may be set in the same sync as the drop)                                      |
| `AmbiguousColumnRename`                 | A declared rename whose old and new column both exist on the table                                                                                                                        | Remove the `renamed_from` hint and drop the old column in its own sync                                 |
| `PrimaryKeyReferencedByForeignKeys`     | Dropping or changing a primary key while foreign keys reference it (same-table FKs dropped in the same sync are exempt)                                                                   | Sync the referencing tables without those foreign keys first, then change the key                      |

## Clustering is not a blocked change

Unlike `partitioned_by`, changing a table's liquid clustering keys has no
validation rule blocking it: Delta reconciles clustering keys with `ALTER TABLE
... CLUSTER BY (...)` (or `CLUSTER BY NONE` to remove them), so the engine plans
this in place. `PartitioningChangeNotSupported` reflects delta-engine's
create-only partitioning model. Changing one partition specification to another
requires a data rewrite. Databricks SQL and Databricks Runtime 18.1 and above
can convert a partitioned table to liquid clustering with `REPLACE PARTITIONED
BY WITH CLUSTER BY`, but the engine does not model that conversion.

Re-clustering only affects data written after the change: existing files
keep their old clustering layout until they are rewritten by a subsequent
`OPTIMIZE` (optionally `OPTIMIZE FULL` to rewrite the whole table
immediately). The engine issues the `ALTER TABLE` but does not run
`OPTIMIZE`; query performance on old data improves only once you optimize.

## Column renames

A rename is declared with `renamed_from` on the new column (see
[renaming a column](how-to-configure-table.md#renaming-a-column)). The differ
relabels the observed column through the hint before comparing, so a rename
emits a single rename action rather than a drop plus an add. The hint applies
only when the old name is observed and the new one is not; once applied it is
inert, so it is safe to keep as declaration history and correct on a fresh
catalog (remove the hint if the declaration's `scope` is later narrowed). If
both names are observed the rename cannot apply and `AmbiguousColumnRename`
blocks the sync.

Partitioning and clustering metadata follow a mapped column's identity, so a
layout key rename does not create layout drift. Primary and foreign keys
involving the column are replaced explicitly: the plan drops each key before
the rename and re-adds the declared key afterwards, rather than relying on
the implicit drops Databricks performs during `RENAME COLUMN`. An inbound
foreign key still blocks a primary-key rename via
`PrimaryKeyReferencedByForeignKeys`.

Renaming requires `delta.columnMapping.mode='name'`. Downstream consumers are
affected in ways the engine cannot see and does not manage: streaming reads
need a `schemaTrackingLocation` to survive a rename, and change data feed has
limitations on column-mapped tables. These are consumer concerns, documented
rather than validated. CHECK constraints and generated columns are also
outside the model: change dependent expressions first, or Databricks rejects
the rename at execution.

Four further checks are laws rather than rules — they define what a declaration
is allowed to govern and how it must name the table's columns, and always run
regardless of the rule set. They are the `ELIGIBILITY_CHECKS` in
`application/validation.py`, evaluated before any safety rule:

| Law                              | What it blocks                                                                                          | How to resolve                                                                        |
| -------------------------------- | ------------------------------------------------------------------------------------------------------- | ------------------------------------------------------------------------------------- |
| `ColumnSpellingMustMatchCatalog` | A declared column (or `renamed_from` reference) spelled differently from the catalog — case must match exactly | Update the declaration to the catalog's exact spelling (`DESCRIBE TABLE` shows it)     |
| `UnmanagedAspectDrift`           | An unmanaged aspect (e.g. column structure) has drifted from the declaration in a restricted-scope sync — the failure names each difference | Update the declaration to match the live table, or widen its scope to manage this aspect |
| `MissingTableUnmanaged`          | The table does not exist but this definition does not manage table existence                            | Create the table out-of-band first, or manage it fully                                |
| `StreamingTableAnnotationsOnly`  | The observed table is a streaming table and the declaration manages more than comments and tags | Declare it with `scope="annotations"` or `scope="tags"`; the table's schema, properties, and keys belong to its owning pipeline |

`ColumnSpellingMustMatchCatalog` is judged at every scope, because a misspelled
column reference is a defect in the declaration rather than drift in one of the
table's aspects: a declaration managing only keys or tags still names its
columns, and naming them wrongly is wrong whatever else it declines to manage.
It is reported first when several of these fire, so the actionable root defect
leads rather than a consequence of it.

`StreamingTableAnnotationsOnly` judges the declaration against the observed
relation kind, not against drift, so it fires even when the streaming table is
currently in sync. The line it draws is the defining SQL: schema, properties,
and keys belong to `CREATE OR REFRESH` and stay unmanageable, while comments
and Unity Catalog tags are alterable from outside the pipeline and are
manageable. A key the pipeline declared must be mirrored in the declaration to
keep it from reading as unmanaged drift.

## Declaration-time checks

Some invalid states are rejected before any sync — a `ValueError` when the
`DeltaTable` (or a type) is constructed, because no catalog state could make
them succeed:

| Check                                                | What it rejects                                                                                                                                                                                                                                                                      |
| ---------------------------------------------------- | ------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------ |
| Nullable primary key                                 | A primary key column declared `nullable=True`                                                                                                                                                                                                                                        |
| Duplicate foreign keys                               | Two foreign keys over the same local columns                                                                                                                                                                                                                                         |
| Foreign key type match                               | Local column types differing from the referenced primary key's column types                                                                                                                                                                                                          |
| Unmanaged property key                               | A property key outside `DELTA_PROPERTY_POLICY` (e.g. a typo)                                                                                                                                                                                                                          |
| Property value format                                | A value the catalog would reject: boolean keys (`delta.enableChangeDataFeed`, `delta.enableTypeWidening`) must be lowercase `true`/`false`, retention durations `interval <n> <unit>`, `delta.dataSkippingNumIndexedCols` an integer >= -1, `delta.columnMapping.mode` `none`/`name` |
| Column and struct field names needing column mapping | Special characters (spaces, `,;{}()=`, tabs, newlines) in a column name or any nested struct field name (reported as a dotted path, e.g. `payload.order id`) without `delta.columnMapping.mode='name'` declared                                                                      |
| Rename hint without column mapping                   | A `renamed_from` hint on any column without `delta.columnMapping.mode='name'` declared — `RENAME COLUMN` requires column mapping, and the hint is visible at declaration time (unlike a drop, which is judged at sync time)                                                          |
| Incoherent rename hint                               | A `renamed_from` naming the column itself, matching a still-declared column, duplicated across columns, or declared on a restricted scope that does not manage column structure                                                                                                      |
| CDF-reserved column names                            | `_change_type`, `_commit_version`, `_commit_timestamp` while `delta.enableChangeDataFeed` is declared `true`                                                                                                                                                                         |
| Map key type                                         | A `Map` used as another `Map`'s key type                                                                                                                                                                                                                                               |
| Nested `NOT NULL` placement                          | A non-null struct field whose containing column or struct field is nullable, or any non-null struct field below an `Array` or `Map`                                                                                                                                                   |
| Tag limits                                           | More than 50 tags on the table or a column, or a tag key or value over 256 characters                                                                                                                                                                                                |
| Decimal precision                                    | `Decimal` precision above 38                                                                                                                                                                                                                                                         |
| Partitioning                                         | Partition columns of complex type (`Array`, `Map`, `Struct`, `Variant`), or partitioning by every column                                                                                                                                                                             |
| Clustering                                           | More than four `clustered_by` keys, a clustering key of unsupported type (`Array`, `Map`, `Struct`, `Variant`, `Boolean`, or `Binary`), or declaring both `partitioned_by` and `clustered_by` on the same table — see [limitations](reference-limitations.md)                                                    |

Validation runs before any SQL executes. A failed validation means the table is unchanged.
