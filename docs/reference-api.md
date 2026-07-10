---
tags:
  - reference
---

# API reference

The public API is reached through two import surfaces: `delta_engine.schema`
for declaring tables (no PySpark required) and `delta_engine.databricks` for
building an engine and running syncs. Result and error types are re-exported
from the top-level `delta_engine` package.

Each entry below links to a full page generated from the object's docstring.

## Schema declarations

```{eval-rst}
.. autosummary::
   :toctree: generated
   :nosignatures:

   delta_engine.schema.DeltaTable
   delta_engine.schema.Column
   delta_engine.schema.ForeignKey
   delta_engine.schema.Property
```

```{eval-rst}
.. autodata:: delta_engine.schema.Self
```

### Notes on `DeltaTable`

#### `scope` (str, default `"full"`)

Selects what the declaration manages. `"full"` manages the whole table.
`"metadata"` restricts the sync to catalog metadata: comments, tags, and
primary/foreign key constraints. `"tags"` restricts it to table and column
tags. A restricted scope still declares the full table shape; aspects outside
the scope are never changed, and any unmanaged drift causes validation to
fail. Properties are the exception: a declaration that does not manage
properties never compares them at all.

#### `clustered_by` (read-only accessor)

The tuple of liquid clustering key column names, in declaration order,
reflecting the `clustered_by` constructor argument. A table-level list, the
sibling of `partitioned_by` and mutually exclusive with it; at most four keys.
Key order is not significant. See
[clustering](how-to-configure-table.md#clustering).

## Engine

```{eval-rst}
.. autosummary::
   :toctree: generated
   :nosignatures:

   delta_engine.Engine
```

## Results and errors

`SyncReport` is a pure data object. To turn one into human-readable text, pass
it to `render_report` or `render_diff`.

```{eval-rst}
.. autosummary::
   :toctree: generated
   :nosignatures:

   delta_engine.SyncReport
   delta_engine.render_report
   delta_engine.render_diff
   delta_engine.TableRunStatus
   delta_engine.SyncFailedError
   delta_engine.Failure
```

## Databricks adapter

```{eval-rst}
.. autosummary::
   :toctree: generated
   :nosignatures:

   delta_engine.databricks.build_engine
   delta_engine.databricks.configure_logging
```
