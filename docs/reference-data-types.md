---
tags:
  - reference
---

# Data types

| `delta_engine` type                      | Spark SQL type         | Notes                                                               |
| ---------------------------------------- | ---------------------- | ------------------------------------------------------------------- |
| `Integer()`                              | `INT`                  |                                                                     |
| `Long()`                                 | `BIGINT`               |                                                                     |
| `Float()`                                | `FLOAT`                |                                                                     |
| `Double()`                               | `DOUBLE`               |                                                                     |
| `Boolean()`                              | `BOOLEAN`              |                                                                     |
| `String()`                               | `STRING`               |                                                                     |
| `Date()`                                 | `DATE`                 |                                                                     |
| `Timestamp()`                            | `TIMESTAMP`            |                                                                     |
| `Decimal(precision, scale)`              | `DECIMAL(p, s)`        | Both arguments required                                             |
| `Array(element_type)`                    | `ARRAY<T>`             | `element_type` must be a supported type                             |
| `Map(key_type, value_type)`              | `MAP<K, V>`            | Both arguments must be supported types                              |
| `Byte()`                                 | `TINYINT`              |                                                                     |
| `Short()`                                | `SMALLINT`             |                                                                     |
| `Binary()`                               | `BINARY`               |                                                                     |
| `TimestampNtz()`                         | `TIMESTAMP_NTZ`        | No timezone; the table feature is enabled automatically on creation |
| `Variant()`                              | `VARIANT`              | Requires a runtime with variant support                             |
| `Struct((StructField(name, type), ...))` | `STRUCT<name: T, ...>` | Field nullability/comments not modeled; fields are created nullable |

Any change to a struct's fields (adding, removing, renaming, or retyping a
field) surfaces as a column type change on the owning column and is blocked
by `ColumnDataTypeChangeNotSupported`, the same as any other unsupported type
change; recreate the table to change a struct.

## Unsupported types

A column whose Spark type is outside this table (`VOID`, `INTERVAL`, geospatial types, etc.) is skipped with a logged warning. The engine leaves it unmanaged — it neither creates, alters, nor drops it. All other columns on the table are still managed normally.

Observed `CHAR(n)`/`VARCHAR(n)` columns are treated as `String`: the length bound is not modeled, produces no drift, and is never altered. The reasoning — facts that cannot round-trip declaration → catalog → observation are normalized out on both sides — is explained in [explanation-architecture.md](explanation-architecture.md).

A table where every column is unsupported surfaces as a `READ_FAILED` for that table alone.

A table whose partition column has an unsupported type also surfaces as `READ_FAILED`, rather than being skipped: silently dropping a partition column would leave the observed partitioning incomplete, and the engine would report a false partitioning change instead of an honest "could not determine state".
