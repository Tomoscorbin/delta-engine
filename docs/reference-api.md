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
```

#### `scope` (str, default `"full"`)

Selects what the declaration manages. `"full"` manages the whole table.
`"metadata"` restricts the sync to catalog metadata: comments, tags, and
primary/foreign key constraints. `"tags"` restricts it to table and column
tags. A restricted scope still declares the full table shape; aspects outside
the scope are never changed, and any unmanaged drift causes validation to
fail. Properties are the exception: a declaration that does not manage
properties never compares them at all.

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
