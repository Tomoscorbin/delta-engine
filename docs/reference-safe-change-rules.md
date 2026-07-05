---
tags:
  - reference
---

# Safe-change rules

The engine validates the computed diff before executing any SQL. These rules block changes that cannot be made safely in place. Each fires a `VALIDATION_FAILED` status with a message naming the rule and the affected column or table.

| Rule | What it blocks | How to resolve |
|---|---|---|
| `NonNullableColumnAdd` | Adding a `NOT NULL` column to an existing table | Add the column as nullable, backfill, then tighten nullability |
| `NullabilityTighteningOnExistingColumn` | Changing an existing nullable column to `NOT NULL` | Backfill existing NULLs first, then update the declaration |
| `ColumnDataTypeChangeNotSupported` | Changing a column's declared data type | Drop and recreate the table out of band, then re-sync |
| `PartitioningChangeNotSupported` | Changing `partitioned_by` on an existing table | Drop and recreate the table out of band, then re-sync |
| `PropertyTransitionNotSupported` | A property transition the catalog rejects — a value change (e.g. `delta.columnMapping.mode` `name` → `none`) or a removal of a key that cannot be unset | Update the declaration to match the catalog value |
| `PropertyMustBeDeclared` | A managed property set on the table but missing from the declaration | Declare it (or declare it `None` to remove it, where removal is possible) |

Two further checks are scope invariants rather than rules — they define what a
declaration is allowed to govern and always run, regardless of the rule set:

| Invariant | What it blocks | How to resolve |
|---|---|---|
| `UnmanagedAspectDrift` | An unmanaged aspect (e.g. column structure) has drifted from the declaration in a metadata-only sync | Sync the table fully, or update the declaration to match the live schema |
| `MissingTableUnmanaged` | The table does not exist but this definition does not manage column structure | Create the table out-of-band first, or manage it fully |
| `ColumnMappingRequiredForDrop` | A plan drops a column but the declaration lacks `delta.columnMapping.mode='name'` | Declare the property (it may be set in the same sync as the drop) |

A nullable primary key column is rejected earlier still — when the `DeltaTable` is constructed (`ValueError` at definition time), not as a plan-validation rule — because a nullable primary key is not a well-formed table definition. See [how-to-declare-primary-keys.md](how-to-declare-primary-keys.md).

Validation runs before any SQL executes. A failed validation means the table is unchanged.
