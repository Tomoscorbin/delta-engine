# delta-engine

[![Live Databricks Tests](https://github.com/Tomoscorbin/delta-engine/actions/workflows/live.yaml/badge.svg?branch=main)](https://github.com/Tomoscorbin/delta-engine/actions/workflows/live.yaml)

Declarative, safety-first schema and metadata management for Delta Lake tables on Databricks.

Define the state your tables should have in Python. Delta Engine reads their current Unity Catalog state, calculates the difference, validates whether each change is safe, and executes only the DDL needed to reconcile them.

## What problem does it solve?

Tables often outlive the notebook, job, or pipeline that first created them.
Their columns, properties, comments, tags, and constraints can then drift across
environments or change through unreviewed DDL. Delta Engine gives those catalog
facts a version-controlled source of truth and makes each proposed change
reviewable before it reaches Databricks.

Delta Engine is a reconciler, not a data pipeline or a migration ledger. Your
existing jobs, declarative pipelines, dbt models, or other systems continue to
produce table data. Delta Engine manages the table schema and catalog metadata
around that data.

It is most useful when a team wants repeatable governance, drift detection, and
safe evolution across a continuing table estate. You can use it for a one-off
change, but a notebook or migration script is usually simpler if you do not
intend to keep and re-run the declaration. Do not let two tools manage the same
table aspects; use [scoped ownership](https://tomoscorbin.github.io/delta-engine/how-to-deploy-metadata-only.html)
when another system owns the schema.

## What Delta Engine gives you

- **Desired-state reconciliation:** declare the complete table state rather than a sequence of migrations.
- **Safe in-place evolution:** unsafe changes, such as type narrowing, repartitioning or destructive schema changes, are rejected before any SQL runs.
- **Reviewable plans:** preview semantic changes and compiled DDL with a dry run before applying them.
- **Drift detection:** compare version-controlled declarations against the live Unity Catalog state in local workflows or CI.
- **Scoped ownership:** manage governance metadata without taking ownership of a table’s schema or data lifecycle.
- **Dependency-aware execution:** synchronise groups of tables in the correct order when primary-key and foreign-key relationships exist.

Delta Engine does not need to own how a table’s data is produced. It can manage the catalog state around tables populated by PySpark jobs, declarative pipelines, dbt models or other systems.

## Install

```bash
pip install delta-engine
```

The base package is pure Python with no runtime dependencies: declaring and
planning schemas needs no PySpark. Running a sync needs either a Spark
session supplied by Databricks Runtime or a Databricks SQL warehouse
connection. On Databricks compute, install only the
base package and use the runtime's Spark and Delta libraries. Outside
Databricks compute, install the `[sql]` extra to sync through a SQL warehouse
— for example, schema sync from CI without a Spark session or cluster. See the
[installation guide](https://tomoscorbin.github.io/delta-engine/installation.html)
for the supported installation paths and their requirements.

Install `delta-engine[cli]` to run
`delta-engine plan MODULE:ATTRIBUTE` through any standard Databricks
unified-auth configuration. The CLI always shows the semantic diff, report,
and planned SQL and never applies the generated plan. See the
[CLI reference](https://tomoscorbin.github.io/delta-engine/reference-cli.html).

## Quickstart

```python
from delta_engine import render_report
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

engine = build_spark_engine(spark)  # `spark` is provided by your Databricks notebook
report = engine.sync(customers)     # creates the table, or no-ops if it already matches
print(render_report(report))
```

A first sync that creates this table renders like this (elapsed time varies):

```text
SYNC REPORT
===========

TABLE                 STATUS   STATEMENTS  DETAIL
dev.silver.customers  SUCCESS  1/1         2 columns

1 table: 1 applied (0.0s)
```

`sync` returns the structured `SyncReport`; it does not print this text itself.
Use `render_report` for status, `render_diff` for semantic changes,
`report.planned_sql_statements` for exact DDL, or `report.to_dict()` for JSON-safe
automation data. The [getting-started tutorial](https://tomoscorbin.github.io/delta-engine/tutorial-getting-started.html)
shows all four and explains where declarations fit in a pipeline.

Validation happens before execution. When a table contains an unsafe change, Delta Engine does not execute a partially valid plan for that table.

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
- [Gate schema changes in CI](https://tomoscorbin.github.io/delta-engine/how-to-gate-changes-in-ci.html) — report planned changes and fail unreadable or unsafe plans
- [Handle sync failures](https://tomoscorbin.github.io/delta-engine/how-to-handle-sync-failures.html) — inspect `SyncReport` and act on each status

**Reference**

- [CLI](https://tomoscorbin.github.io/delta-engine/reference-cli.html) — the read-only plan command, connection contract, output, and exit codes
- [Capabilities and limitations](https://tomoscorbin.github.io/delta-engine/reference-limitations.html) — what the engine can and cannot manage
- [Data types](https://tomoscorbin.github.io/delta-engine/reference-data-types.html) — supported types and Spark SQL equivalents
- [Safe-change rules](https://tomoscorbin.github.io/delta-engine/reference-safe-change-rules.html) — changes the engine blocks at validation
- [Run report schema](https://tomoscorbin.github.io/delta-engine/reference-run-report.html) — the `to_dict()` payload, field by field
- [API reference](https://tomoscorbin.github.io/delta-engine/reference-api.html)

**Architecture**

- [Architecture](https://tomoscorbin.github.io/delta-engine/explanation-architecture.html) — layers, ports and adapters, design decisions
- [Implement a custom adapter](https://tomoscorbin.github.io/delta-engine/how-to-implement-adapter.html) — the `CatalogStateReader` and `PlanExecutor` ports
- [Add a new action type](https://tomoscorbin.github.io/delta-engine/how-to-add-action-type.html) — extend `Action`, `ActionPhase`, and the compiler
