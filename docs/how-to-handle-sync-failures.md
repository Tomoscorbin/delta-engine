---
tags:
  - how-to
---

# How to handle sync failures

On a real run, `engine.sync(...)` raises `SyncFailedError` when one or more tables fail; the exception carries the full `SyncReport`. (A [dry run](how-to-preview-changes.md) never raises — it returns the same report type for you to inspect.) This guide shows how to catch it and act on the report.

A table's status names the [phase](explanation-sync-lifecycle.md) at which it failed, so handling a failure is mostly a matter of mapping status to cause.

## Catch the error

```python
from delta_engine import SyncFailedError

try:
    engine.sync(customers, orders)
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

| Status               | Meaning                                                         |
| -------------------- | --------------------------------------------------------------- |
| `SUCCESS`            | Table synced without issues                                     |
| `READ_FAILED`        | Could not read current catalog state                            |
| `PLANNING_FAILED`    | Plan was rejected before any SQL ran                            |
| `FOREIGN_KEY_FAILED` | A foreign key could not be applied, or a dependency won't build |
| `EXECUTION_FAILED`   | SQL ran but a statement failed                                  |

## Read failure details

```python
from delta_engine import TableRunStatus

for table_report in report:
    if table_report.status == TableRunStatus.READ_FAILED:
        for failure in table_report.failures:
            print("\n".join(failure.format_lines()))
```

`table_report.failures` is the single phase-ordered stream of every `Failure` for that table (read → planning → foreign key → execution). Each `Failure` renders itself via `format_lines()`. The concrete failure classes — `ReadFailure`, `ValidationFailure`, `ForeignKeyFailure`, and `ExecutionFailure` — are importable from `delta_engine`, so a caller can branch on failure type with `isinstance` rather than on `status` alone.

For a machine-readable view of the whole run — each table's status, planned changes, SQL, and failures as plain JSON — call `report.to_dict()`; the `failures` list in each table record carries the `phase`, `type`, and `message` of every failure. See [the run report schema](reference-run-report.md).

Both backends are Unity Catalog only: after the initial describe, tag and key
metadata is always read from `information_schema`, unconditionally. A failing
metadata query (for example a permissions gap on `information_schema`) fails
that table's read rather than silently reporting the table as having no
constraints or tags. If you see a read failure naming an `information_schema`
view, check the running principal's grants on that catalog's
`information_schema`.

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
    if table_report.status == TableRunStatus.PLANNING_FAILED:
        # Safe to retry after fixing the declaration
        for failure in table_report.failures:
            print(failure.format_lines()[0])
```

See [reference-safe-change-rules.md](reference-safe-change-rules.md) for the full list of validation rules and how to resolve each one.

## Act on foreign key failures

A `FOREIGN_KEY_FAILED` table ran no SQL. The cause is one of: a reference to an unregistered table, a dependency cycle, a foreign key that does not target the referenced table's primary key, a foreign-key column whose type does not match the referenced column on the registered table, or a dependency that won't reach its desired state this sync — whether it failed before execution (read or planning) or while executing earlier in the same run. When a dependency fails, every table downstream of it is blocked too — so fix the upstream table first, then re-run.

```python
for table_report in report:
    if table_report.status == TableRunStatus.FOREIGN_KEY_FAILED:
        for failure in table_report.failures:
            print(failure.format_lines()[0])
```

See [Foreign keys](how-to-configure-table.md#foreign-keys) for how dependency ordering and all-or-nothing blocking work.

## Act on execution failures

Execution failures are partial: statements before the failure ran and committed; statements after were not attempted. Tables whose foreign keys depend on the failed table are blocked in the same run and report `FOREIGN_KEY_FAILED`. Fix the root cause and re-run — the engine re-reads live state and plans only the remaining drift.

(runtime-and-delta-feature-compatibility)=

## Runtime and Delta feature compatibility

The Spark factory first enforces the global DBR range documented in
[runtime compatibility](reference-runtime-compatibility.md). It fails before
constructing an engine if the runtime is unsupported or cannot be identified.

Within that supported range, delta-engine does not preflight the runtime or
Delta table protocol requirement for every feature it can declare. If you use a
Databricks feature such as key constraints, tags, deletion vectors, or change
data feed, make sure your workspace, runtime, table protocol, and privileges
support that feature.

When Databricks rejects a statement because the runtime or Delta table version is
too old, the engine reports it as an `EXECUTION_FAILED` table with the original
exception type, message, and SQL. Upgrade or configure the Databricks
environment, then re-run `sync`.
