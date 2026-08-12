---
tags:
  - how-to
---

# How to preview changes with a dry run

`sync(..., dry_run=True)` runs read, diff, accepted/rejected planning, SQL
compilation, and dependency resolution, then skips execution. Nothing in the
catalog changes. The report shows the plan, its exact compiled SQL, and every
pre-execution failure discoverable from the catalog snapshot it read; it cannot
predict a Databricks error that would occur only while executing SQL.

## Run a dry run

```python
report = engine.sync(customers, orders, dry_run=True)
```

A dry run never raises `SyncFailedError`, even when it finds a read, validation,
or foreign-key failure — the point is to return the complete preview report.

## See what would change

`render_diff` shows every table's planned changes as `+`/`-`/`~` blocks;
`render_report` shows the per-table statuses and any failures:

```python
from delta_engine import render_diff, render_report

print(render_diff(report))
print(render_report(report))
```

For example, previewing creation of the `customers` declaration from the
[getting-started tutorial](tutorial-getting-started.md) produces:

```text
DIFF
====

dev.silver.customers  (CREATE)
  columns
    + id    Integer  NOT NULL
    + name  String
```

```text
SYNC REPORT
===========

PLAN — no planned SQL executed

TABLE                 STATUS   STATEMENTS  DETAIL
dev.silver.customers  SUCCESS  1           2 columns

1 table: 1 changed, 0 unchanged, 0 failed (0.0s)
```

Elapsed time varies. `sync` returns the report object and neither renderer is
called automatically. The diff answers *what would change*; the report answers
*whether the plan succeeded and how much SQL it contains*.

Each table's `plan` records the DDL actions compiled for that observed snapshot.
The `execution` field stays `None` on every table, because nothing ran.

## Check a dry run programmatically

The result is the same `SyncReport` type a real run returns, so the same
inspection applies — see
[how to handle sync failures](how-to-handle-sync-failures.md). A useful CI
gate is "no failures":

```python
report = engine.sync(customers, orders, dry_run=True)

if report.has_failures:
    raise SystemExit(render_report(report))
```

`report.has_changes` reports whether any table has a planned change, and
`report.planned_sql_statements` maps each table's name to the SQL preview
compiled for the current snapshot. For a machine-readable view of the whole
run — status, planned actions, and SQL as plain JSON — call `report.to_dict()` (see
[the run report schema](reference-run-report.md)). To turn a dry run into a
red/green pull-request check, see
[how to gate schema changes in CI](how-to-gate-changes-in-ci.md).

## Dry run first, then apply

A dry run and a real run use the same declaration and decision pipeline, so the
natural deployment shape is: dry-run in CI to review the plan, then run the same
`sync` call without `dry_run` to apply it. The real run re-reads and re-plans
live state, so its actions can differ if drift appeared after the preview. It
can also report execution failures that no dry run can discover.
