---
tags:
  - tutorial
---

# Installation

## Requirements

| Requirement                               | Needed for                                                    |
| ----------------------------------------- | ------------------------------------------------------------- |
| Python 3.12 or later                      | Everything                                                    |
| A Databricks workspace with Unity Catalog | Running syncs against a real catalog                          |
| An active `SparkSession`                  | Running syncs (a Databricks notebook provides one as `spark`) |

The base package has no runtime dependencies. Declaring schemas, planning, and
inspecting reports are pure Python — PySpark is only needed when you actually
sync against Databricks.

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

## Local development against the Databricks adapter

To use the Databricks adapter outside Databricks — for example, running local
Spark in tests — install the `databricks` extra, which adds PySpark and
Delta:

```bash
pip install "delta-engine[databricks]"
```

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
