---
tags:
  - tutorial
---

# Getting started with delta-engine

This tutorial walks you through defining your first Delta table and syncing it
to Databricks. By the end you will have created a table in Unity Catalog and
seen the sync report that confirms it.

## Prerequisites

- Python 3.12 or later
- A Databricks workspace with Unity Catalog enabled
- An active `SparkSession` (a Databricks notebook provides one automatically as `spark`)

## Define a table

Import the building blocks and describe your table:

```python
from delta_engine.schema import Column, DeltaTable, Integer, String

customers = DeltaTable(
    catalog="dev",
    schema="silver",
    name="customers",
    columns=[
        Column("id", Integer()),
        Column("name", String()),
    ],
)
```

`DeltaTable` describes what you want. No SQL runs yet.

## Sync

Build an engine and pass your table definitions straight to `sync`. The engine reads the current catalog state, computes direct actions, accepts or rejects the plan through validation, and executes any DDL needed:

```python
from delta_engine.databricks import build_spark_engine

engine = build_spark_engine(spark)
report = engine.sync(customers)
```

If the table does not exist, the engine creates it. If it already matches your declaration, `sync` is a no-op.

## Check the result

`sync` returns a `SyncReport` describing what happened to each table. Render it
to see the outcome:

```python
from delta_engine import render_report

print(render_report(report))
```

The report above describes that first sync, including the table creation. Run
`engine.sync(customers)` again and its report shows no changes — the declaration
and the catalog now agree.

## Enable logging (optional)

Call `configure_logging()` before `sync` to see colored progress output:

```python
from delta_engine.databricks import configure_logging

configure_logging()
engine.sync(customers)
```

## What to do when sync fails

If any table fails planning or execution, `sync` raises `SyncFailedError`. The exception message shows which tables failed and why. See [how to handle sync failures](how-to-handle-sync-failures.md) for how to inspect the report programmatically.

## Next steps

- [How a sync works](explanation-sync-lifecycle.md) — what happens between calling `sync` and getting a report back.
- [How to configure a table](how-to-configure-table.md) — properties, tags, comments, keys, and partitioning.
- [Preview changes with a dry run](how-to-preview-changes.md) — inspect the current plan without touching the catalog.
