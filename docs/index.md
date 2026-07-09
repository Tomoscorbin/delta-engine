# delta-engine

Declarative schema management for Delta Lake tables on Databricks. You declare
the state a table should have; the engine reads the state it actually has,
computes the difference, checks that the difference is safe to apply, and runs
exactly the DDL needed to close the gap.

```python
from delta_engine.databricks import build_engine
from delta_engine.schema import Column, DeltaTable, Integer, String

customers = DeltaTable(
    catalog="dev",
    schema="silver",
    name="customers",
    columns=[
        Column("id", Integer(), nullable=False),
        Column("name", String()),
    ],
)

engine = build_engine(spark)
engine.sync(customers)  # creates the table, or no-ops if it already matches
```

There is no migration script to write and no DDL to hand-order. The declaration
is the source of truth; `sync` reconciles the catalog to it, every run.

## What a sync does

The engine reads the current catalog state, diffs it against your declaration,
validates that the drift is safe to fix in place, plans deterministic DDL
actions, orders tables so foreign-key dependencies are created first, executes,
and returns a per-table report. Unsafe changes — dropping data, changing a
column's type, repartitioning — fail validation with a named rule before any
SQL runs. [How a sync works](explanation-sync-lifecycle.md) walks through the
phases; [the safety model](explanation-safety-model.md) explains what gets
blocked and why.

## Backend support

delta-engine currently targets one backend: Delta Lake tables on Databricks
with Unity Catalog. The planning core is deliberately backend-free — backends
plug in as adapters that read catalog state and execute plans — so additional
backends, such as open-source Unity Catalog, can be added without changing the
model. See [Architecture](explanation-architecture.md) for the design and
[how to implement an adapter](how-to-implement-adapter.md) for the extension
points.

## Where to go next

| You want to…                                   | Read                                                                            |
| ---------------------------------------------- | ------------------------------------------------------------------------------- |
| Install the package and sync your first table  | [Installation](installation.md), [Getting started](tutorial-getting-started.md) |
| Understand what a sync does before running one | [How a sync works](explanation-sync-lifecycle.md)                               |
| Check whether the engine supports something    | [Capabilities and limitations](reference-limitations.md)                        |
| Declare keys, properties, tags, or comments    | The how-to guides in the sidebar                                                |
| See why a change was rejected                  | [Safe-change rules](reference-safe-change-rules.md)                             |
| Understand the internals or add a backend      | [Architecture](explanation-architecture.md)                                     |

```{toctree}
:hidden:
:caption: Getting started

installation
tutorial-getting-started
```

```{toctree}
:hidden:
:caption: Concepts

explanation-sync-lifecycle
explanation-safety-model
```

```{toctree}
:hidden:
:caption: How-to guides

how-to-configure-table
how-to-declare-primary-keys
how-to-declare-foreign-keys
how-to-configure-properties
how-to-deploy-metadata-only
how-to-preview-changes
how-to-handle-sync-failures
```

```{toctree}
:hidden:
:caption: Reference

reference-limitations
reference-data-types
reference-safe-change-rules
reference-api
```

```{toctree}
:hidden:
:caption: Architecture

explanation-architecture
how-to-implement-adapter
how-to-add-action-type
```
