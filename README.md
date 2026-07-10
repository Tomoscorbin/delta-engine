# delta-engine

Declarative, safety-first schema and metadata management for Delta Lake tables on Databricks.

Define the state your tables should have in Python. Delta Engine reads their current Unity Catalog state, calculates the difference, validates whether each change is safe, and executes only the DDL needed to reconcile them.

There are no migration scripts to maintain and no DDL statements to hand-order. Your declarations remain the source of truth across the lifetime of the table.

## What Delta Engine gives you

- *Desired-state reconciliation:* declare the complete table state rather than a sequence of migrations.
- *Safe in-place evolution:* unsafe changes, such as type narrowing, repartitioning or destructive schema changes, are rejected before any SQL runs.
- *Reviewable plans:* preview the semantic changes and exact DDL with a dry run before applying them.
- *Drift detection:* compare version-controlled declarations against the live Unity Catalog state in local workflows or CI.
- *Scoped ownership:* manage governance metadata without taking ownership of a table’s schema or data lifecycle.
- *Dependency-aware execution:* synchronise groups of tables in the correct order when primary-key and foreign-key relationships exist.

Delta Engine does not need to own how a table’s data is produced. It can manage the catalog state around tables populated by PySpark jobs, declarative pipelines, dbt models or other systems.

## Install

```bash
pip install delta-engine
```

The base package is pure Python with no runtime dependencies: declaring and
planning schemas needs no PySpark. Running a sync needs a Databricks
environment, which provides Spark and Delta. See the
[installation guide](https://tomoscorbin.github.io/delta-engine/installation.html)
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
dropping data, narrowing a column's type, repartitioning — fail validation
with a named rule before any SQL runs.

## Documentation

Start with [how a sync works](https://tomoscorbin.github.io/delta-engine/explanation-sync-lifecycle.html)
for the model, or jump to what you need:

**Getting started**

- [Installation](https://tomoscorbin.github.io/delta-engine/installation.html)
- [Getting started tutorial](https://tomoscorbin.github.io/delta-engine/tutorial-getting-started.html) — define a table and run your first sync

**Concepts**

- [How a sync works](https://tomoscorbin.github.io/delta-engine/explanation-sync-lifecycle.html) — the phases between calling `sync` and getting a report
- [The safety model](https://tomoscorbin.github.io/delta-engine/explanation-safety-model.html) — what the engine blocks, and why

**How-to guides**

- [Configure a table](https://tomoscorbin.github.io/delta-engine/how-to-configure-table.html) — properties, tags, comments, keys, and partitioning
- [Deploy metadata only](https://tomoscorbin.github.io/delta-engine/how-to-deploy-metadata-only.html) — roll out governance metadata with no schema change
- [Preview changes with a dry run](https://tomoscorbin.github.io/delta-engine/how-to-preview-changes.html)
- [Gate schema changes in CI](https://tomoscorbin.github.io/delta-engine/how-to-gate-changes-in-ci.html) — fail a pull request when declarations and the catalog disagree
- [Handle sync failures](https://tomoscorbin.github.io/delta-engine/how-to-handle-sync-failures.html) — inspect `SyncReport` and act on each status

**Reference**

- [Capabilities and limitations](https://tomoscorbin.github.io/delta-engine/reference-limitations.html) — what the engine can and cannot manage
- [Data types](https://tomoscorbin.github.io/delta-engine/reference-data-types.html) — supported types and Spark SQL equivalents
- [Safe-change rules](https://tomoscorbin.github.io/delta-engine/reference-safe-change-rules.html) — changes the engine blocks at validation
- [Run report schema](https://tomoscorbin.github.io/delta-engine/reference-run-report.html) — the `to_dict()` payload, field by field
- [API reference](https://tomoscorbin.github.io/delta-engine/reference-api.html)

**Architecture**

- [Architecture](https://tomoscorbin.github.io/delta-engine/explanation-architecture.html) — layers, ports and adapters, design decisions
- [Implement a custom adapter](https://tomoscorbin.github.io/delta-engine/how-to-implement-adapter.html) — the `CatalogStateReader` and `PlanExecutor` ports
- [Add a new action type](https://tomoscorbin.github.io/delta-engine/how-to-add-action-type.html) — extend `Action`, `ActionPhase`, and the compiler
