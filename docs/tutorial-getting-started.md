---
tags:
  - tutorial
---

# Getting started with delta-engine

This tutorial walks you through defining your first Delta table, previewing its
creation, and syncing it to Databricks. It also explains where that declaration
lives after the first run. By the end you will have created a table in Unity
Catalog and seen the semantic diff, exact DDL, and sync report that confirm it.

## Before you start: what Delta Engine owns

Delta Engine manages the physical schema and Unity Catalog metadata of a Delta
table. It does not schedule a pipeline, transform data, or write table rows.
For example, a PySpark job can continue writing customer records while Delta
Engine keeps the table's columns, properties, comments, tags, and constraints
aligned with a Python declaration.

This is most valuable as a continuing contract:

1. Keep declarations in version control with the application or data platform
   code that owns them.
2. Preview them in CI with a dry run.
3. Apply them once from a controlled deployment step, or at the start of a
   single-writer data job before that job writes rows.
4. Run them again after a declaration changes, or periodically to detect and
   repair safe drift.

A one-off notebook is valid too. Defining a `DeltaTable` does not register a
background controller, and removing a declaration from a later `sync` does not
drop the Databricks table; Delta Engine only considers the declarations passed
to that invocation. If another system already owns the schema, use
[scoped ownership](how-to-deploy-metadata-only.md) so the two tools do not
manage the same aspects.

## Prerequisites

- Python 3.12 or later
- A Databricks workspace with Unity Catalog enabled
- An active `SparkSession` (a Databricks notebook provides one automatically as `spark`)

## Define a table in a Python module

Declarations are ordinary Python objects. A small project might keep them in
`myproject/tables.py` and export the complete ordered collection used by CI and
deployment:

```python
# myproject/tables.py
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

all_tables = (customers,)
```

`DeltaTable` describes what you want. Importing this module and constructing the
object runs no SQL. The same declaration can be imported by a notebook, job,
deployment command, or the read-only CLI.

## Build an engine

On Databricks compute, build an engine from the notebook's Spark session:

```python
from delta_engine.databricks import build_spark_engine
from myproject.tables import all_tables

engine = build_spark_engine(spark)
```

See [Installation](installation.md) to build an engine from a Databricks SQL
warehouse connection instead. The declaration and report behavior are the same
for both backends.

## Preview the first sync

Start with a dry run. It reads live catalog state, validates the change, and
compiles exact SQL without executing it:

```python
preview = engine.sync(*all_tables, dry_run=True)

print(preview.render_diff())
print(preview.render())
```

For a missing `dev.silver.customers` table, `preview.render_diff()` shows the
semantic change rather than SQL:

```text
DIFF
====

dev.silver.customers  (CREATE)
  columns
    + id    Integer  NOT NULL
    + name  String
```

`preview.render()` shows the plan status and statement count. The `PLAN` banner
is the visible assurance that no planned SQL ran:

```text
SYNC REPORT
===========

PLAN — no planned SQL executed

TABLE                 STATUS   STATEMENTS  DETAIL
dev.silver.customers  SUCCESS  1           2 columns

1 table: 1 changed, 0 unchanged, 0 deferred, 0 failed (0.0s)
```

Elapsed time varies. `sync` returns the `SyncReport` object; it does not print
either rendering. The object supports four complementary views:

| View | Use |
| --- | --- |
| `preview.render_diff()` | Human-readable semantic changes |
| `preview.render()` | Per-table status, failures, and statement counts |
| `preview.planned_sql_statements` | Exact DDL grouped by table |
| `preview.to_dict()` | Versioned, JSON-safe data for CI and logging |

For this preview, the exact planned SQL is:

```python
{
    "dev.silver.customers": (
        "CREATE TABLE `dev`.`silver`.`customers` "
        "(`id` INT NOT NULL, `name` STRING) USING delta",
    )
}
```

## Apply the declaration

Run the same declaration without `dry_run=True`. A real sync re-reads live
state, plans again, and then executes accepted DDL; it does not replay a saved
preview:

```python
report = engine.sync(*all_tables)
print(report.render())
```

The first successful application renders:

```text
SYNC REPORT
===========

TABLE                 STATUS   STATEMENTS  DETAIL
dev.silver.customers  SUCCESS  1/1         2 columns

1 table: 1 applied (0.0s)
```

`1/1` means one of one planned statements was applied. If the table already
matches, the next run is a no-op:

```text
SYNC REPORT
===========

TABLE                 STATUS   STATEMENTS  DETAIL
dev.silver.customers  SUCCESS  0           no changes

1 table: 1 unchanged (0.0s)
```

Keep one write-capable sync per table at a time. Multi-statement DDL is not
transactional, and the current engine does not lock out another writer; review
[capabilities and limitations](reference-limitations.md) before unattended
production use.

## Enable logging (optional)

Call `configure_logging()` before `sync` to see colored progress logs in
addition to the returned report:

```python
from delta_engine.databricks import configure_logging

configure_logging()
engine.sync(*all_tables)
```

Logging is operational progress; `SyncReport.render()`,
`SyncReport.render_diff()`, and `SyncReport.to_dict()` remain the stable ways to
inspect the outcome.

## What to do when sync fails

If any table fails planning or execution, `sync` raises `SyncFailedError`. The exception message shows which tables failed and why. See [how to handle sync failures](how-to-handle-sync-failures.md) for how to inspect the report programmatically.

## Next steps

- [How a sync works](explanation-sync-lifecycle.md) — what happens between calling `sync` and getting a report back.
- [How to configure a table](how-to-configure-table.md) — properties, tags, comments, keys, and partitioning.
- [Preview changes with a dry run](how-to-preview-changes.md) — inspect the current plan without touching the catalog.
