---
tags:
  - how-to
---

# How to handle sync failures

`engine.sync(registry)` raises `SyncFailedError` when one or more tables fail. This guide shows how to catch it and act on the structured report it carries.

## Catch the error

```python
from delta_engine import SyncFailedError

try:
    engine.sync(registry)
except SyncFailedError as error:
    report = error.report
```

## Inspect the report

`error.report` is a `SyncReport`. Iterate it to see each table's outcome:

```python
for table_report in report:
    print(table_report.qualified_name, table_report.status)
```

`table_report.status` is one of five `TableRunStatus` values:

| Status | Meaning |
|---|---|
| `SUCCESS` | Table synced without issues |
| `READ_FAILED` | Could not read current catalog state |
| `VALIDATION_FAILED` | Plan was rejected before any SQL ran |
| `FOREIGN_KEY_FAILED` | A foreign key could not be applied, or a dependency won't build |
| `EXECUTION_FAILED` | SQL ran but a statement failed |

## Read failure details

```python
from delta_engine import TableRunStatus

for table_report in report:
    if table_report.status == TableRunStatus.VALIDATION_FAILED:
        for failure in table_report.all_failures:
            print("\n".join(failure.format_lines()))
```

`table_report.all_failures` returns every `Failure` across all phases. Each `Failure` renders itself via `format_lines()`.

## Check which tables failed

```python
for qualified_name, failures in report.failures_by_table.items():
    print(f"{qualified_name}:")
    for failure in failures:
        for line in failure.format_lines():
            print(f"  {line}")
```

`report.failures_by_table` is a `dict[QualifiedName, tuple[Failure, ...]]` containing only the tables that failed.

## Act on validation failures

Validation failures mean no SQL ran for that table. The failure message names the rule and explains what to fix:

```python
for table_report in report:
    if table_report.status == TableRunStatus.VALIDATION_FAILED:
        # Safe to retry after fixing the declaration
        for failure in table_report.all_failures:
            print(failure.format_lines()[0])
```

See [reference-safe-change-rules.md](reference-safe-change-rules.md) for the full list of validation rules and how to resolve each one.

## Act on foreign key failures

A `FOREIGN_KEY_FAILED` table ran no SQL. The cause is one of: a reference to an unregistered table, a dependency cycle, or a dependency that won't reach its desired state this sync. When a dependency fails, every table downstream of it is blocked too — so fix the upstream table first, then re-run.

```python
for table_report in report:
    if table_report.status == TableRunStatus.FOREIGN_KEY_FAILED:
        for failure in table_report.all_failures:
            print(failure.format_lines()[0])
```

See [how-to-declare-foreign-keys.md](how-to-declare-foreign-keys.md) for how dependency ordering and all-or-nothing blocking work.

## Act on execution failures

Execution failures are partial: actions before the failure ran and committed; actions after were not attempted. Fix the root cause and re-run — the engine re-reads live state and plans only the remaining drift.
