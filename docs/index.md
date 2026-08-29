# delta-engine

Declarative schema management for Delta Lake tables on Databricks. You declare
the state a table should have; the engine reads the state it actually has,
computes the difference, checks that the difference is safe to apply, and runs
exactly the DDL needed to close the gap.

## What it is for

Delta Engine turns a table's physical schema and Unity Catalog metadata into a
version-controlled contract. It is useful when tables are long-lived and a team
wants the same columns, properties, comments, tags, and constraints across
environments, with drift and unsafe changes visible before DDL runs.

It does not transform or load data. Existing PySpark jobs, declarative
pipelines, dbt models, or other systems can keep producing the rows while Delta
Engine reconciles the catalog state around them. A one-off DDL statement is
usually simpler for a one-off change; the value here comes from keeping the
declarations and re-running them in CI or deployment workflows.

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
report = engine.sync(customers)  # creates the table, or no-ops if it already matches
```

There is no migration script to write and no DDL to hand-order. The declaration
is the source of truth; `sync` reconciles the catalog to it, every run. It
returns a structured report rather than printing output. The
[getting-started tutorial](tutorial-getting-started.md) shows the report, diff,
and exact planned SQL and explains where to keep the declaration.

## Choose how to run it

Delta Engine runs where your release or data workflow calls it. See
[Ways to use Delta Engine](explanation-ways-to-use-delta-engine.md) for
release-time reconciliation, ETL readiness checks, and restricted governance
deployments.

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
| Choose a release or workflow pattern           | [Ways to use Delta Engine](explanation-ways-to-use-delta-engine.md)             |
| Run read-only schema plans in GitHub Actions    | [CLI reference](reference-cli.md)                                              |
| Understand what a sync does before running one | [How a sync works](explanation-sync-lifecycle.md)                               |
| Check whether the engine supports something    | [Capabilities and limitations](reference-limitations.md)                        |
| Declare keys, properties, tags, or comments    | The how-to guides in the sidebar                                                |
| See why a change was rejected                  | [Safe-change rules](reference-safe-change-rules.md)                             |
| Understand the internals or add a backend      | [Architecture](explanation-architecture.md)                                     |
| Understand the environment support policy      | [Runtime compatibility](explanation-runtime-compatibility.md)                   |

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
explanation-runtime-compatibility
explanation-ways-to-use-delta-engine
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
how-to-add-lint-rule
```
