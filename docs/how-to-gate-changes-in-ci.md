---
tags:
  - how-to
---

# How to gate schema changes in CI

Run a dry run against the live catalog when a pull request opens, fail the job
when the declarations and the catalog disagree, and surface the exact changes —
including the SQL that would run — in the job output. A dry run makes no catalog
changes and never raises, so the whole result is available as data for the gate
to act on.

## The gate

```python
import json
import sys

from delta_engine.databricks import build_engine

from myproject.tables import all_tables  # your DeltaTable declarations

report = build_engine(spark).sync(*all_tables, dry_run=True)

print(json.dumps(report.to_dict(), indent=2))

if report.has_failures or report.has_changes:
    sys.exit(1)
```

The two booleans state different facts:

- `report.has_failures` — a table could not be read, failed validation, or was
  blocked by a foreign-key dependency. Its drift was **not** planned.
- `report.has_changes` — at least one table has a validated, planned change
  waiting to apply.

A green job means the catalog already matches the declarations. Fail on
`has_failures` to catch declarations that cannot apply; fail on `has_changes` to
require that changes are reviewed and applied deliberately rather than drifting
in.

## Show the SQL that would run

`report.planned_sql_statements` maps each table's dotted name to the exact statements a
real sync would execute — full text, in execution order, untruncated:

```python
for name, statements in report.planned_sql_statements.items():
    print(f"-- {name}")
    for statement in statements:
        print(statement)
```

The same statements appear per table in `report.to_dict()`, so a single JSON
payload carries the status, the planned actions, and the DDL together.

## Discriminate failures by type

The concrete failure types are importable, so a gate can treat failure classes
differently — for example tolerating a flaky read while hard-failing unsafe
drift:

```python
from delta_engine import ReadFailure, ValidationFailure

for table_report in report:
    for failure in table_report.failures:
        if isinstance(failure, ValidationFailure):
            sys.exit(1)  # never merge an unsafe change
        if isinstance(failure, ReadFailure):
            print(f"transient read failure on {table_report.qualified_name}")
```

## Render the report yourself

delta-engine deliberately ships no PR-comment or markdown renderer. `to_dict()`
is stable, JSON-serialisable data (see
[the run report schema](reference-run-report.md)); formatting it into a PR
comment, a Slack message, or a log line is your pipeline's job.
