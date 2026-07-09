---
tags:
  - reference
---

# API reference

The public API is reached through two import surfaces: `delta_engine.schema`
for declaring tables (no PySpark required) and `delta_engine.databricks` for
building an engine and running syncs. Result and error types are re-exported
from the top-level `delta_engine` package.

## Schema declarations

### DeltaTable

```{eval-rst}
.. autoclass:: delta_engine.schema.DeltaTable
   :members:
   :undoc-members:
   :show-inheritance:
```

#### `metadata_only` (bool, default `False`)

When `True`, restricts the sync to catalog metadata: comments, tags, and
primary/foreign key constraints. Column structure, properties, partitioning,
and clustering are never changed. The live schema must match the declaration
exactly — any structural drift causes validation to fail.

#### `clustered_by` (read-only accessor)

The tuple of liquid clustering key column names, in declaration order,
reflecting the `clustered_by` constructor argument. A table-level list, the
sibling of `partitioned_by` and mutually exclusive with it; at most four keys.
Key order is not significant. See
[clustering](how-to-configure-table.md#clustering).

### Column

```{eval-rst}
.. autoclass:: delta_engine.schema.Column
   :members:
   :undoc-members:
```

### ForeignKey

```{eval-rst}
.. autoclass:: delta_engine.schema.ForeignKey
   :members:
   :undoc-members:
```

### Self

```{eval-rst}
.. autodata:: delta_engine.schema.Self
```

### Property

```{eval-rst}
.. autoclass:: delta_engine.schema.Property
   :members:
```

## Engine

### Engine

```{eval-rst}
.. autoclass:: delta_engine.Engine
   :members:
```

## Results and errors

### SyncReport

```{eval-rst}
.. autoclass:: delta_engine.SyncReport
   :members:
```

### Rendering a report

`SyncReport` is a pure data object. To turn one into human-readable text, pass
it to one of these functions:

```{eval-rst}
.. autofunction:: delta_engine.render_report
```

```{eval-rst}
.. autofunction:: delta_engine.render_diff
```

### TableRunStatus

```{eval-rst}
.. autoclass:: delta_engine.TableRunStatus
   :members:
```

### SyncFailedError

```{eval-rst}
.. autoexception:: delta_engine.SyncFailedError
   :members:
```

### Failure

```{eval-rst}
.. autoclass:: delta_engine.Failure
   :members:
```

## Databricks adapter

```{eval-rst}
.. autofunction:: delta_engine.databricks.build_engine
```

```{eval-rst}
.. autofunction:: delta_engine.databricks.configure_logging
```
