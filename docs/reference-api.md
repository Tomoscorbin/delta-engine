---
tags:
  - reference
---

# API reference

The public API is reached through two import surfaces: `delta_engine.schema`
for declaring tables (no PySpark required) and `delta_engine.databricks` for
Spark conversion, building an engine, and running syncs. Result and error
types are re-exported from the top-level `delta_engine` package.

The full per-module reference is generated from the source tree; see
{doc}`autoapi/delta_engine/index`. The entry points:

| You want to…                  | Start at                                                                |
| ----------------------------- | ----------------------------------------------------------------------- |
| Declare tables, columns, keys | {doc}`delta_engine.schema <autoapi/delta_engine/schema/index>`          |
| Convert for Spark or sync     | {doc}`delta_engine.databricks <autoapi/delta_engine/databricks/index>`  |
| Inspect results and errors    | {doc}`delta_engine <autoapi/delta_engine/index>` (top-level re-exports) |

## Convert a declaration to a PySpark schema

Use the declaration as the authoritative DataFrame schema without coupling
the declaration module itself to PySpark:

```python
from delta_engine.databricks import to_spark_schema

result = transform(source).to(to_spark_schema(customers))
```

`to_spark_schema()` returns a PySpark `StructType` preserving declared column
order, name spelling, data types, and nullability. Table and column comments
and tags remain catalog annotations; they are not copied into Spark field
metadata. Array elements and map values are nullable because declarations do
not model their nullability separately. Importing the function does not
require PySpark, but calling it does.

## Notes on `DeltaTable`

### `scope` (str, default `"full"`)

Selects what the declaration manages. `"full"` manages the whole table.
`"metadata"` restricts the sync to catalog metadata: comments, tags, and
primary/foreign key constraints. `"annotations"` restricts it to table and
column comments and tags, and `"tags"` restricts it further to table and
column tags. The scopes nest: `tags ⊂ annotations ⊂ metadata ⊂ full`.
Only `"annotations"` and `"tags"` may target streaming tables. A restricted
scope still declares the full table shape; aspects outside the scope are never
changed, and any unmanaged drift causes validation to fail. Properties are
the exception: a declaration that does not manage properties never compares
them at all.

### `clustered_by` (read-only accessor)

The tuple of liquid clustering key column names, in declaration order,
reflecting the `clustered_by` constructor argument. A table-level list, the
sibling of `partitioned_by` and mutually exclusive with it; at most four keys.
Key order is not significant. See
[clustering](how-to-configure-table.md#clustering).
