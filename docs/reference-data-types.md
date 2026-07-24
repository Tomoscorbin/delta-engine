---
tags:
  - reference
---

# Data types

The types you can declare on a `Column`, and the Spark SQL type each compiles
to. A column whose catalog type falls outside this set is handled as described
under [Unsupported types](#unsupported-types).

| `delta_engine` type                      | Spark SQL type         | Notes                                                                |
| ---------------------------------------- | ---------------------- | -------------------------------------------------------------------- |
| `Integer()`                              | `INT`                  |                                                                      |
| `Long()`                                 | `BIGINT`               |                                                                      |
| `Float()`                                | `FLOAT`                |                                                                      |
| `Double()`                               | `DOUBLE`               |                                                                      |
| `Boolean()`                              | `BOOLEAN`              |                                                                      |
| `String()`                               | `STRING`               |                                                                      |
| `Date()`                                 | `DATE`                 |                                                                      |
| `Timestamp()`                            | `TIMESTAMP`            |                                                                      |
| `Decimal(precision, scale)`              | `DECIMAL(p, s)`        | Both required; precision 1–38, scale 0–precision (Delta/Spark limit) |
| `Array(element_type)`                    | `ARRAY<T>`             | `element_type` must be a supported type                              |
| `Map(key_type, value_type)`              | `MAP<K, V>`            | Both arguments must be supported types                               |
| `Byte()`                                 | `TINYINT`              |                                                                      |
| `Short()`                                | `SMALLINT`             |                                                                      |
| `Binary()`                               | `BINARY`               |                                                                      |
| `TimestampNtz()`                         | `TIMESTAMP_NTZ`        | Creation enables the feature; existing tables need it enabled first  |
| `Variant()`                              | `VARIANT`              | Creation enables the feature; existing tables need it enabled first  |
| `Struct([StructField(name, type), ...])` | `STRUCT<name: T, ...>` | Field nullability/comments not modeled; fields are created nullable  |

`Map` declarations should use a non-`Map` key type. The current declaration
gate does not reject `Map(Map(...), value_type)`, but Databricks does not accept
a map as another map's key, so that shape fails at execution.

Any change to a struct's fields (adding, removing, renaming, or retyping a
field) surfaces as a column type change on the owning column and is blocked
by `NonWideningColumnTypeChange`, the same as any other non-widening type
change — structs are never widened as a whole; recreate the table to change
a struct.

## Unsupported types

A column whose catalog type is outside the table above (`VOID`, `INTERVAL`,
geospatial types, a future Spark type, etc.) fails that table's read: the sync
reports `READ_FAILED` for the table instead of planning against a partial view
of it, and no column is ever silently skipped. The engine manages a table's
full column set — an observed column absent from the declaration is planned as
a `DROP COLUMN` — so an omitted column would make its drift invisible and
could report a table as in sync when it is not. A failed read affects only
that table; the other tables in the run still sync normally.

Hitting this usually means the catalog's type vocabulary is ahead of this
engine version: new Spark types (recent precedents: `TIMESTAMP_NTZ`,
`VARIANT`) reach tables before tools that pin a type model.

Observed `CHAR(n)`/`VARCHAR(n)` columns are treated as `String`: the length bound is not modeled, produces no drift, and is never altered. The reasoning — facts that cannot round-trip declaration → catalog → observation are normalized out on both sides — is explained in [explanation-architecture.md](explanation-architecture.md).

For an existing Delta table, Databricks does not enable `TIMESTAMP_NTZ` or
`VARIANT` support merely because an `ADD COLUMN` statement names the type.
Delta-engine observes the table's supported features and enables a missing
schema-required feature before adding the dependent column. New tables
containing either type enable their required feature as part of creation, so
they need no separate enablement action.
