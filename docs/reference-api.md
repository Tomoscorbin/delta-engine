---
tags:
  - reference
---

# API reference

The public API is reached through two import surfaces: `delta_engine.schema`
for declaring tables (no PySpark required) and `delta_engine.databricks` for
building an engine and running syncs. Result and error types are re-exported
from the top-level `delta_engine` package.

The full per-module reference is generated from the source tree; see
{doc}`autoapi/delta_engine/index`. The entry points:

| You want to…                  | Start at                                                                |
| ----------------------------- | ----------------------------------------------------------------------- |
| Declare tables, columns, keys | {doc}`delta_engine.schema <autoapi/delta_engine/schema/index>`          |
| Build an engine and sync      | {doc}`delta_engine.databricks <autoapi/delta_engine/databricks/index>`  |
| Inspect results and errors    | {doc}`delta_engine <autoapi/delta_engine/index>` (top-level re-exports) |

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
