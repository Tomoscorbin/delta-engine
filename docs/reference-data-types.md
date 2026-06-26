---
tags:
  - reference
---

# Data types

| `delta_engine` type | Spark SQL type | Notes |
|---|---|---|
| `Integer()` | `INT` | |
| `Long()` | `BIGINT` | |
| `Float()` | `FLOAT` | |
| `Double()` | `DOUBLE` | |
| `Boolean()` | `BOOLEAN` | |
| `String()` | `STRING` | |
| `Date()` | `DATE` | |
| `Timestamp()` | `TIMESTAMP` | |
| `Decimal(precision, scale)` | `DECIMAL(p, s)` | Both arguments required |
| `Array(element_type)` | `ARRAY<T>` | `element_type` must be a supported type |
| `Map(key_type, value_type)` | `MAP<K, V>` | Both arguments must be supported types |

## Unsupported types

A column whose Spark type is outside this table (`STRUCT`, `BINARY`, `VARIANT`, `TIMESTAMP_NTZ`, etc.) is skipped with a logged warning. The engine leaves it unmanaged — it neither creates, alters, nor drops it. All other columns on the table are still managed normally.

A table where every column is unsupported surfaces as a `READ_FAILED` for that table alone.
