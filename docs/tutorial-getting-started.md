---
tags:
  - tutorial
---

# Getting started with delta-engine

This tutorial walks you through defining your first Delta table, registering it, and syncing it to Databricks. By the end you will have a table created in your Unity Catalog and a sync report confirming success.

## Prerequisites

- Python 3.12 or later
- A Databricks workspace with Unity Catalog enabled
- An active `SparkSession` (a Databricks notebook provides one automatically as `spark`)

## Databricks feature compatibility

delta-engine does not check Databricks Runtime or Delta Lake protocol versions before
emitting supported DDL. Features such as informational primary and foreign keys,
change data feed, and table properties depend on the runtime and table version in
your workspace. If Databricks rejects a statement because the runtime or table does
not support it, the engine surfaces that backend error in the sync failure report.

## Install delta-engine

```bash
pip install delta-engine
```

## Define a table

Import the building blocks and describe your table:

```python
from delta_engine import Column, DeltaTable, Integer, String

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

## Declare partitioning (optional)

Pass `partitioned_by` to `DeltaTable` when the table should be partitioned at
creation time:

```python
from delta_engine import Column, Date, DeltaTable, String

events = DeltaTable(
    catalog="dev",
    schema="silver",
    name="events",
    columns=[
        Column("event_date", Date()),
        Column("event_type", String()),
        Column("payload", String()),
    ],
    partitioned_by=["event_date"],
)
```

Every name in `partitioned_by` must appear in `columns`. Partition columns are
declared as regular columns too: `partitioned_by` names them, and `columns`
defines their types and nullability.

Partitioning is fixed once the table exists. Declaring a different
`partitioned_by` value for an existing table raises `SyncFailedError` during
validation, before any SQL runs. To change partitioning, drop and recreate the
table out of band, then re-sync.

## Register the table

A `Registry` holds the set of tables the engine will manage:

```python
from delta_engine import Registry

registry = Registry()
registry.register(customers)
```

## Sync

Build an engine and call `sync`. The engine reads the current catalog state, computes a plan, validates it, and executes any DDL needed:

```python
from delta_engine import build_databricks_engine

engine = build_databricks_engine(spark)
engine.sync(registry)
```

If the table does not exist, the engine creates it. If it already matches your declaration, `sync` is a no-op.

## Enable logging (optional)

Call `configure_logging()` before `sync` to see colored progress output:

```python
from delta_engine import configure_logging

configure_logging()
engine.sync(registry)
```

## What to do when sync fails

If any table fails validation or execution, `sync` raises `SyncFailedError`. The exception message shows which tables failed and why. See [how-to-handle-sync-failures.md](how-to-handle-sync-failures.md) for how to inspect the report programmatically.
