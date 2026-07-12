---
tags:
  - tutorial
---

# Installation

## Requirements

| Requirement                               | Needed for                                                                              |
| ----------------------------------------- | --------------------------------------------------------------------------------------- |
| Python 3.12 or later                      | Everything                                                                              |
| A Databricks workspace with Unity Catalog | Running syncs against a real catalog                                                    |
| An active `SparkSession`                  | Running syncs through the Spark backend (a Databricks notebook provides one as `spark`) |
| A Databricks SQL warehouse connection     | Running syncs through the SQL warehouse backend instead — no PySpark needed             |

The base package has no runtime dependencies. Declaring schemas, planning, and
inspecting reports are pure Python — PySpark is only needed if you sync
through the Spark backend. The SQL warehouse backend below needs no PySpark
at all.

## Install

```bash
pip install delta-engine
```

In a Databricks notebook:

```python
%pip install delta-engine
```

Databricks provides Spark and Delta at runtime, so the base package is all you
need there.

## Local development against the Spark backend

To use the Spark backend outside Databricks — for example, running local
Spark in tests — install the `spark` extra, which adds PySpark and
Delta:

```bash
pip install "delta-engine[spark]"
```

## Syncing through a SQL warehouse

To sync without PySpark — for example from a plain CI runner — install the
`sql` extra, which adds the Databricks SQL connector:

```bash
pip install "delta-engine[sql]"
```

This syncs through a Databricks SQL warehouse connection instead of a Spark
session, over `databricks-sql-connector`. The extra is named `sql`, while the
backend package it installs is `delta_engine.adapters.databricks.warehouse` —
both name the same backend; the mismatch follows the Databricks product's own
name for this compute, "SQL warehouse".

This path requires Unity Catalog: every read runs through
`information_schema`, which a catalog such as `hive_metastore` does not
expose (see [limitations](reference-limitations.md)).

## Verify

```python
from delta_engine.schema import Column, DeltaTable, Integer

DeltaTable(
    catalog="dev",
    schema="silver",
    name="smoke_test",
    columns=[Column("id", Integer())],
)
print("delta-engine is installed")
```

If this runs, you are ready for [Getting started](tutorial-getting-started.md).
