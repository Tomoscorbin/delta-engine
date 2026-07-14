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
| `TimestampNtz()`                         | `TIMESTAMP_NTZ`        | Creation enables the feature; existing tables need it enabled first |
| `Variant()`                              | `VARIANT`              | Creation enables the feature; existing tables need it enabled first |
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

A non-partition column whose Spark type is outside this table (`VOID`,
`INTERVAL`, geospatial types, etc.) is skipped with a logged warning. The engine
leaves it unmanaged — it neither creates, alters, nor drops it — and cannot
detect drift in that column. This is a current fail-open limitation at the read
boundary; do not rely on delta-engine as a complete drift gate for a table that
contains an unsupported type. All mappable columns are still managed normally.

Observed `CHAR(n)`/`VARCHAR(n)` columns are treated as `String`: the length bound is not modeled, produces no drift, and is never altered. The reasoning — facts that cannot round-trip declaration → catalog → observation are normalized out on both sides — is explained in [explanation-architecture.md](explanation-architecture.md).

A table where every column is unsupported surfaces as a `READ_FAILED` for that table alone.

A table whose partition column has an unsupported type also surfaces as `READ_FAILED`, rather than being skipped: silently dropping a partition column would leave the observed partitioning incomplete, and the engine would report a false partitioning change instead of an honest "could not determine state". Hitting this usually means the catalog's type vocabulary is ahead of this engine version — new Spark types (recent precedents: `TIMESTAMP_NTZ`, `VARIANT`) reach tables before tools that pin a type model — or the name resolves to a non-Delta relation (a federated or legacy Hive table) with a broader partitioning vocabulary.

For an existing Delta table, Databricks does not enable `TIMESTAMP_NTZ` or
`VARIANT` support merely because an `ADD COLUMN` statement names the type.
Enable the corresponding table feature out of band before asking delta-engine
to add that column. New tables containing either type enable their required
feature as part of creation. Delta-engine does not currently plan these feature
upgrades for existing tables, so a missing feature surfaces as
`EXECUTION_FAILED`.
