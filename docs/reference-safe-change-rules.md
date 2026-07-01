---
tags:
  - reference
---

# Safe-change rules

The engine validates a computed plan before executing any SQL. These four rules block changes that cannot be made safely in place. Each fires a `VALIDATION_FAILED` status with a message naming the rule and the affected column or table.

| Rule | What it blocks | How to resolve |
|---|---|---|
| `NonNullableColumnAdd` | Adding a `NOT NULL` column to an existing table | Add the column as nullable, backfill, then tighten nullability |
| `NullabilityTighteningOnExistingColumn` | Changing an existing nullable column to `NOT NULL` | Backfill existing NULLs first, then update the declaration |
| `UnsupportedColumnTypeChange` | Changing a column's declared data type | Drop and recreate the table out of band, then re-sync |
| `DisallowPartitioningChange` | Changing `partitioned_by` on an existing table | Drop and recreate the table out of band, then re-sync |

A nullable primary key column is rejected earlier still — when the `DeltaTable` is constructed (`ValueError` at definition time), not as a plan-validation rule — because a nullable primary key is not a well-formed table definition. See [how-to-declare-primary-keys.md](how-to-declare-primary-keys.md).

Validation runs before any SQL executes. A failed validation means the table is unchanged.
