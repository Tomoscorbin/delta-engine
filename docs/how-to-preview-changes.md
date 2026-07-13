---
tags:
  - how-to
---

# How to preview changes with a dry run

`sync(..., dry_run=True)` runs the full decision-making — read, diff,
accepted/rejected planning and dependency resolution — and skips only execution. Nothing
in the catalog changes, and the returned report shows exactly what a real run
would do, including any failures it would have.

## Run a dry run

```python
report = engine.sync(customers, orders, dry_run=True)
```

A dry run never raises `SyncFailedError`, even when a table would fail — the
point is to see everything, so the report is always returned.

## See what would change

`render_diff` shows every table's planned changes as `+`/`-`/`~` blocks;
`render_report` shows the per-table statuses and any failures:

```python
from delta_engine import render_diff, render_report

print(render_diff(report))
print(render_report(report))
```

Each table's `plan` records the DDL actions a real run would execute. The
`execution` field stays `None` on every table, because nothing ran.

## Check a dry run programmatically

The report is the same `SyncReport` a real run returns, so the same
inspection applies — see
[how to handle sync failures](how-to-handle-sync-failures.md). A useful CI
gate is "no failures":

```python
report = engine.sync(customers, orders, dry_run=True)

if report.has_failures:
    raise SystemExit(render_report(report))
```

`report.has_changes` reports whether any table has a planned change, and
`report.planned_sql_statements` maps each table's name to the exact DDL a
real run would execute. For a machine-readable view of the whole run — status, planned
actions, and SQL as plain JSON — call `report.to_dict()` (see
[the run report schema](reference-run-report.md)). To turn a dry run into a
red/green pull-request check, see
[how to gate schema changes in CI](how-to-gate-changes-in-ci.md).

## Dry run first, then apply

A dry run and a real run make the same decisions from the same declarations,
so the natural deployment shape is: dry-run in CI to review the plan, then run
the same `sync` call without `dry_run` to apply it. The real run re-reads live
state at that point, so drift that appears between the two runs is picked up
rather than blindly replayed.
