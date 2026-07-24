---
tags:
  - tutorial
---

# Installation

## Requirements

| Requirement                               | Needed for                                                                  |
| ----------------------------------------- | --------------------------------------------------------------------------- |
| Python 3.12 or later                      | Everything                                                                  |
| A Databricks workspace with Unity Catalog | Running syncs against a real catalog                                        |
| Databricks Runtime 16.2+ with a `SparkSession` | Running syncs through the Spark backend                                  |
| A Databricks SQL warehouse connection     | Running syncs through the SQL warehouse backend instead — no PySpark needed |

The base package has no runtime dependencies. Declaring schemas, planning, and
inspecting reports are pure Python. The Spark backend uses the PySpark and
Delta libraries supplied by Databricks Runtime; the SQL warehouse backend
needs no PySpark at all.

## Install

```bash
pip install delta-engine
```

The unpinned commands in this guide are convenient for evaluation. For a
repeatable job or application deployment, pin the Delta Engine release (for
example, `delta-engine[cli]==X.Y.Z`) and commit the complete environment to
your application's lock file. Delta Engine's dependency ranges select
supported major lines; they are not a replacement for an application lock.

In a Databricks notebook:

```python
%pip install "delta-engine==X.Y.Z"  # replace X.Y.Z with the release you deploy
dbutils.library.restartPython()
```

Databricks provides Spark and Delta at runtime, so the base package is all you
need there. Do not install `pyspark` or `delta-spark` over the runtime's
mutually compatible versions. Delta Engine deliberately has no `spark` extra:
its production Spark reader depends on Databricks Runtime and Unity Catalog
features that local open-source Spark does not provide.

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

Create a connector connection and pass it to `build_sql_engine`. This minimal
example uses a personal access token; the Databricks SQL connector also supports
OAuth credential providers for production workloads.

```python
import os

from databricks import sql
from delta_engine import render_report
from delta_engine.databricks import build_sql_engine

from myproject.tables import customers

with sql.connect(
    server_hostname=os.environ["DATABRICKS_SERVER_HOSTNAME"],
    http_path=os.environ["DATABRICKS_HTTP_PATH"],
    access_token=os.environ["DATABRICKS_TOKEN"],
) as connection:
    engine = build_sql_engine(connection)
    report = engine.sync(customers)

print(render_report(report))
```

The server hostname and HTTP path are available from the SQL warehouse's
Connection Details. Keep the connection open for the duration of `sync`; the
engine uses it for both catalog reads and DDL execution.

## Installing the CLI

The `cli` extra adds the read-only `delta-engine plan` command on top of the
SQL warehouse backend:

```bash
pip install "delta-engine[cli]"
```

The command builds a plan for one explicit declaration collection against the
live catalog, prints its semantic diff, report, and planned SQL, and never
executes the planned SQL. The CLI extra contains only Typer, `databricks-sdk`, and
`databricks-sql-connector`; the base package remains dependency-free.

Configure `DATABRICKS_SQL_WAREHOUSE_ID`, then configure the Databricks SDK's
standard unified authentication through environment variables or a profile.
The CLI adds no connection flags and does not choose an authentication method.
See the [CLI reference](reference-cli.md), or the
[GitHub Actions OIDC example](how-to-gate-changes-in-ci.md) for a read-only CI
deployment.

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
