---
tags:
  - how-to
---

# How to gate schema changes in CI

Run `delta-engine plan` against the live catalog when a pull request opens: it
dry-runs your declarations, prints the drift, and exits non-zero when the
declarations and the catalog disagree. A dry run makes no catalog changes, so
the gate is safe to run on every push.

## The gate

```bash
pip install "delta-engine[cli]"
delta-engine plan myproject.tables
```

`myproject.tables` is a module containing your `DeltaTable` declarations —
every table bound at the module's top level is included. Target an explicit
aggregate with `myproject.tables:all_tables`, or pass several modules.

The exit code tells the story:

| Exit code | Meaning                                                       |
| --------- | ------------------------------------------------------------- |
| 0         | The catalog already matches the declarations                  |
| 1         | A table failed — unreadable, invalid drift, or a config error |
| 2         | Validated changes are pending, waiting to be applied          |

Failing on any non-zero code means a green job guarantees no drift. A pipeline
that wants to treat "changes pending" differently from "broken" can branch on
the two codes.

Connection settings come from the environment: `DATABRICKS_SERVER_HOSTNAME`,
`DATABRICKS_HTTP_PATH`, and `DATABRICKS_TOKEN`. The token is env-only by
design; `--server-hostname` and `--http-path` flags override their variables.

## A complete GitHub Actions workflow

Gate pull requests, apply on merge to main:

````yaml
name: schema

on:
  pull_request:
  push:
    branches: [main]

env:
  DATABRICKS_SERVER_HOSTNAME: ${{ vars.DATABRICKS_SERVER_HOSTNAME }}
  DATABRICKS_HTTP_PATH: ${{ vars.DATABRICKS_HTTP_PATH }}
  DATABRICKS_TOKEN: ${{ secrets.DATABRICKS_TOKEN }}

jobs:
  plan:
    if: github.event_name == 'pull_request'
    runs-on: ubuntu-latest
    steps:
      - uses: actions/checkout@v4
      - uses: actions/setup-python@v5
        with:
          python-version: "3.12"
      - run: pip install "delta-engine[cli]"
      - name: Plan
        run: |
          { echo '```'; delta-engine plan myproject.tables; echo '```'; } >> "$GITHUB_STEP_SUMMARY"

  apply:
    if: github.event_name == 'push'
    runs-on: ubuntu-latest
    steps:
      - uses: actions/checkout@v4
      - uses: actions/setup-python@v5
        with:
          python-version: "3.12"
      - run: pip install "delta-engine[cli]"
      - run: delta-engine apply myproject.tables
````

The plan step writes the report into the job's step summary, so the drift is
readable from the PR checks page. The pipe preserves the CLI's exit code, so
pending changes still fail the gate.

## Show the SQL that would run

`--show-sql` appends each table's exact planned statements to the text report:

```bash
delta-engine plan myproject.tables --show-sql
```

## Machine-readable output

`--output json` prints the full run report — the same
[`to_dict()` payload](reference-run-report.md) the Python API returns — as the
only thing on stdout:

```bash
delta-engine plan myproject.tables --output json | jq '.tables[] | {name, status}'
```

## The Python API, for custom gates

The CLI covers the common gate. For custom policy — tolerating a flaky read
while hard-failing unsafe drift, say — call the engine directly and inspect
the report:

```python
import os
import sys

from databricks import sql

from delta_engine import ValidationFailure
from delta_engine.databricks import build_sql_engine

from myproject.tables import all_tables  # your DeltaTable declarations

with sql.connect(
    server_hostname=os.environ["DATABRICKS_SERVER_HOSTNAME"],
    http_path=os.environ["DATABRICKS_HTTP_PATH"],
    access_token=os.environ["DATABRICKS_TOKEN"],
) as connection:
    report = build_sql_engine(connection).sync(*all_tables, dry_run=True)

for table_report in report:
    for failure in table_report.failures:
        if isinstance(failure, ValidationFailure):
            sys.exit(1)  # never merge an unsafe change
```

On a Databricks cluster the same gate runs through the Spark backend: build
the engine with `build_spark_engine(spark)` and everything else — the report,
the booleans, the failure types — is identical.

## Render the report yourself

The CLI emits text and JSON; formatting a PR comment, a Slack message, or a
log line from the JSON remains your pipeline's job. `to_dict()` is stable,
JSON-serialisable data — see [the run report schema](reference-run-report.md).
