# delta-engine

Declarative schema management for Delta Lake tables on Databricks. You declare
the state a table should have; the engine reads the state it actually has,
computes the difference, checks that the difference is safe to apply, and runs
exactly the DDL needed to close the gap.

There is no migration script to write and no DDL to hand-order. The declaration
is the source of truth, and every `sync` reconciles the catalog to it.

**Backends:** delta-engine targets Delta Lake on Databricks with Unity Catalog
today. The planning core is deliberately backend-free — backends plug in as
adapters that read catalog state and execute plans — so other backends, such as
open-source Unity Catalog, can be added without changing the model.

## Install

```bash
pip install delta-engine
```

The base package is pure Python with no runtime dependencies: declaring and
planning schemas needs no PySpark. Running a sync needs a Databricks
environment, which provides Spark and Delta. See the
[installation guide](https://github.com/Tomoscorbin/delta-engine/blob/main/docs/installation.md)
for the `[databricks]` extra used for local development.

## Quickstart

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

engine = build_engine(spark)  # `spark` is provided by your Databricks notebook
engine.sync(customers)         # creates the table, or no-ops if it already matches
```

## What a sync does

Every `sync` runs the same phase chain for each table: read the current catalog
state, diff it against your declaration, validate that the drift is safe to fix
in place, plan deterministic DDL, order tables so foreign-key dependencies are
created first, execute, and return a per-table `SyncReport`. Unsafe changes —
dropping data, changing a column's type, repartitioning — fail validation with
a named rule before any SQL runs.

## Documentation

Start with [how a sync works](https://github.com/Tomoscorbin/delta-engine/blob/main/docs/explanation-sync-lifecycle.md)
for the model, or jump to what you need:

**Getting started**

- [Installation](https://github.com/Tomoscorbin/delta-engine/blob/main/docs/installation.md)
- [Getting started tutorial](https://github.com/Tomoscorbin/delta-engine/blob/main/docs/tutorial-getting-started.md) — define a table and run your first sync

**Concepts**

- [How a sync works](https://github.com/Tomoscorbin/delta-engine/blob/main/docs/explanation-sync-lifecycle.md) — the phases between calling `sync` and getting a report
- [The safety model](https://github.com/Tomoscorbin/delta-engine/blob/main/docs/explanation-safety-model.md) — what the engine blocks, and why

**How-to guides**

- [Configure a table](https://github.com/Tomoscorbin/delta-engine/blob/main/docs/how-to-configure-table.md) — properties, tags, comments, keys, and partitioning
- [Deploy metadata only](https://github.com/Tomoscorbin/delta-engine/blob/main/docs/how-to-deploy-metadata-only.md) — roll out governance metadata with no schema change
- [Preview changes with a dry run](https://github.com/Tomoscorbin/delta-engine/blob/main/docs/how-to-preview-changes.md)
- [Handle sync failures](https://github.com/Tomoscorbin/delta-engine/blob/main/docs/how-to-handle-sync-failures.md) — inspect `SyncReport` and act on each status

**Reference**

- [Capabilities and limitations](https://github.com/Tomoscorbin/delta-engine/blob/main/docs/reference-limitations.md) — what the engine can and cannot manage
- [Data types](https://github.com/Tomoscorbin/delta-engine/blob/main/docs/reference-data-types.md) — supported types and Spark SQL equivalents
- [Safe-change rules](https://github.com/Tomoscorbin/delta-engine/blob/main/docs/reference-safe-change-rules.md) — changes the engine blocks at validation
- [API reference](https://github.com/Tomoscorbin/delta-engine/blob/main/docs/reference-api.md)

**Architecture** (for contributors)

- [Architecture](https://github.com/Tomoscorbin/delta-engine/blob/main/docs/explanation-architecture.md) — layers, ports and adapters, design decisions
- [Implement a custom adapter](https://github.com/Tomoscorbin/delta-engine/blob/main/docs/how-to-implement-adapter.md) — the `CatalogStateReader` and `PlanExecutor` ports
- [Add a new action type](https://github.com/Tomoscorbin/delta-engine/blob/main/docs/how-to-add-action-type.md) — extend `Action`, `ActionPhase`, and the compiler
