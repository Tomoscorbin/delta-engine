---
tags:
  - reference
---

# Safe-change rules

The engine validates the computed diff before executing any SQL. These rules block changes that cannot be made safely in place. Each fires a `VALIDATION_FAILED` status with a message naming the rule and the affected column or table. They are the validation-rules layer of [the safety model](explanation-safety-model.md), which explains how they fit alongside declaration-time checks and managed aspects.

| Rule                                    | What it blocks                                                                                                                                          | How to resolve                                                                                         |
| --------------------------------------- | ------------------------------------------------------------------------------------------------------------------------------------------------------- | ------------------------------------------------------------------------------------------------------ |
| `NonNullableColumnAdd`                  | Adding a `NOT NULL` column to an existing table                                                                                                         | Add the column as nullable, backfill, set `NOT NULL` outside the engine, then declare `nullable=False` |
| `NullabilityTighteningOnExistingColumn` | Changing an existing nullable column to `NOT NULL`                                                                                                      | Backfill existing NULLs, set `NOT NULL` outside the engine, then declare `nullable=False`              |
| `ColumnDataTypeChangeNotSupported`      | Changing a column's declared data type                                                                                                                  | Drop and recreate the table out of band, then re-sync                                                  |
| `PartitioningChangeNotSupported`        | Changing `partitioned_by` on an existing table                                                                                                          | Drop and recreate the table out of band, then re-sync                                                  |
| `PropertyTransitionNotSupported`        | A property transition the catalog rejects — a value change (e.g. `delta.columnMapping.mode` `name` → `none`) or a removal of a key that cannot be unset | Update the declaration to match the catalog value                                                      |
| `PropertyMustBeDeclared`                | A managed property set on the table but missing from the declaration                                                                                    | Declare it (or declare it `None` to remove it, where removal is possible)                              |
| `ColumnMappingRequiredForDrop`          | A plan drops a column but the declaration lacks `delta.columnMapping.mode='name'`                                                                       | Declare the property (it may be set in the same sync as the drop)                                      |
| `PrimaryKeyReferencedByForeignKeys`     | Dropping or changing a primary key while foreign keys reference it (same-table FKs dropped in the same sync are exempt)                                 | Sync the referencing tables without those foreign keys first, then change the key                      |

## Clustering is not a blocked change

Unlike `partitioned_by`, changing a table's liquid clustering keys has no
validation rule blocking it, because there is nothing unsafe about it: Delta
reconciles clustering keys with `ALTER TABLE ... CLUSTER BY (...)` (or
`CLUSTER BY NONE` to remove them), so the engine plans this in place instead
of failing validation. `PartitioningChangeNotSupported` blocks
`partitioned_by` because Delta has no equivalent in-place `ALTER TABLE` for
partition columns — changing them means physically rewriting every data
file — while a re-cluster is a metadata change that later `OPTIMIZE` runs
apply lazily.

Re-clustering only affects data written after the change: existing files
keep their old clustering layout until they are rewritten by a subsequent
`OPTIMIZE` (optionally `OPTIMIZE FULL` to rewrite the whole table
immediately). The engine issues the `ALTER TABLE` but does not run
`OPTIMIZE`; query performance on old data improves only once you optimize.

Two further checks are scope invariants rather than rules — they define what a
declaration is allowed to govern and always run, regardless of the rule set:

| Invariant               | What it blocks                                                                                          | How to resolve                                                           |
| ----------------------- | ------------------------------------------------------------------------------------------------------- | ------------------------------------------------------------------------ |
| `UnmanagedAspectDrift`  | An unmanaged aspect (e.g. column structure) has drifted from the declaration in a restricted-scope sync | Sync the table fully, or update the declaration to match the live schema |
| `MissingTableUnmanaged` | The table does not exist but this definition does not manage column structure                           | Create the table out-of-band first, or manage it fully                   |

## Declaration-time checks

Some invalid states are rejected before any sync — a `ValueError` when the
`DeltaTable` (or a type) is constructed, because no catalog state could make
them succeed:

| Check                                                | What it rejects                                                                                                                                                                                                                                                  |
| ---------------------------------------------------- | ---------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------- |
| Nullable primary key                                 | A primary key column declared `nullable=True`                                                                                                                                                                                                                    |
| Duplicate foreign keys                               | Two foreign keys over the same local columns                                                                                                                                                                                                                     |
| Foreign key type match                               | Local column types differing from the referenced primary key's column types                                                                                                                                                                                      |
| Unmanaged property key                               | A property key outside the managed registry (e.g. a typo)                                                                                                                                                                                                        |
| Property value format                                | A value the catalog would reject: `delta.enableChangeDataFeed` (the only boolean key) must be lowercase `true`/`false`, retention durations `interval <n> <unit>`, `delta.dataSkippingNumIndexedCols` an integer >= -1, `delta.columnMapping.mode` `none`/`name` |
| Column and struct field names needing column mapping | Special characters (spaces, `,;{}()=`, tabs, newlines) in a column name or any nested struct field name (reported as a dotted path, e.g. `payload.order id`) without `delta.columnMapping.mode='name'` declared                                                  |
| CDF-reserved column names                            | `_change_type`, `_commit_version`, `_commit_timestamp` while `delta.enableChangeDataFeed` is declared `true`                                                                                                                                                     |
| Tag limits                                           | More than 50 tags on the table or a column, or a tag value over 1000 characters                                                                                                                                                                                  |
| Decimal precision                                    | `Decimal` precision above 38                                                                                                                                                                                                                                     |
| Partitioning                                         | Partition columns of complex type (`Array`, `Map`, `Struct`, `Variant`), or partitioning by every column                                                                                                                                                         |
| Clustering                                           | More than four `clustered_by` keys, a clustering key of complex type (`Array`, `Map`, `Struct`, `Variant`), or declaring both `partitioned_by` and `clustered_by` on the same table — see [limitations](reference-limitations.md)                                |

Validation runs before any SQL executes. A failed validation means the table is unchanged.
