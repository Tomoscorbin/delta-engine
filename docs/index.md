# delta-engine

Declarative schema management for Delta Lake tables on Databricks. You declare
the state a table should have; the engine reads the state it actually has,
computes the difference, checks that the difference is safe to apply, and runs
exactly the DDL needed to close the gap.

```python
from delta_engine.databricks import build_spark_engine
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

engine = build_spark_engine(spark)
engine.sync(customers)  # creates the table, or no-ops if it already matches
```

There is no migration script to write and no DDL to hand-order. The declaration
is the source of truth; `sync` reconciles the catalog to it, every run.

## Install

```bash
pip install delta-engine
```

Declaring schemas is pure Python. A sync can use either a Databricks Spark
session or a Databricks SQL warehouse connection; the latter needs no PySpark.
See [Installation](installation.md) for the Databricks compute and `[sql]`
paths.

## What a sync does

The engine reads the current catalog state, diffs it against your declaration,
validates that the drift is safe to fix in place, plans deterministic DDL
actions, orders tables so foreign-key dependencies are created first, executes,
and returns a per-table report. Unsafe changes — dropping data, narrowing a
column's type, repartitioning — fail validation with a named rule before any
SQL runs. [How a sync works](explanation-sync-lifecycle.md) walks through the
phases; [the safety model](explanation-safety-model.md) explains what gets
blocked and why.

## Backend support

delta-engine targets Delta Lake tables on Databricks with Unity Catalog today.
Backends plug in as adapters that read catalog state and execute plans, and the
planning core takes no backend imports — so a Delta-compatible backend, such as
open-source Unity Catalog, can be added by implementing those adapters. A
genuinely different table format, such as Iceberg, would also need Delta-specific
policy lifted out of the application layer first. See
[Architecture](explanation-architecture.md#import-purity-versus-semantic-coupling)
for what is and isn't backend-neutral, and
[how to implement an adapter](how-to-implement-adapter.md) for the ports.

## Where to go next

| You want to…                                   | Read                                                                            |
| ---------------------------------------------- | ------------------------------------------------------------------------------- |
| Install the package and sync your first table  | [Installation](installation.md), [Getting started](tutorial-getting-started.md) |
| Run read-only schema plans in GitHub Actions    | [CLI reference](reference-cli.md)                                              |
| Understand what a sync does before running one | [How a sync works](explanation-sync-lifecycle.md)                               |
| Check whether the engine supports something    | [Capabilities and limitations](reference-limitations.md), [runtime compatibility](reference-runtime-compatibility.md) |
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
how-to-deploy-metadata-only
how-to-preview-changes
how-to-gate-changes-in-ci
how-to-handle-sync-failures
```

```{toctree}
:hidden:
:caption: Reference

reference-limitations
reference-runtime-compatibility
reference-data-types
reference-safe-change-rules
reference-run-report
reference-cli
reference-api
autoapi/delta_engine/index
```

```{toctree}
:hidden:
:caption: Architecture

explanation-architecture
how-to-implement-adapter
how-to-add-action-type
```
