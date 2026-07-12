---
tags:
  - how-to
---

# How to gate schema changes in CI

Run `delta-engine plan` against the live catalog when a pull request opens. It
reads Unity Catalog, prints the semantic diff and report, and fails when a table
cannot be read or changed safely. Pending valid changes remain mergeable by
default; use `--fail-on-changes` for a scheduled or explicit drift gate.

A plan executes no DDL, but it is a live warehouse operation and can start
compute. Give its identity only the catalog and warehouse permissions needed to
read state.

## Run a plan

```bash
pip install "delta-engine[cli]"
delta-engine plan myproject.tables:all_tables
```

Every declaration argument must use `MODULE:ATTRIBUTE`. The attribute may be a
single `DeltaTable` or a non-empty sequence (list or tuple) of them.
Bare-module scanning is not supported; see the
[CLI reference](reference-cli.md#declaration-arguments) for the full grammar
and duplicate checks.

The default exit codes are:

| Exit code | Meaning                                                        |
| --------- | -------------------------------------------------------------- |
| 0         | The plan is valid, whether in sync or carrying pending changes |
| 1         | A table or configuration failed                                |
| 2         | Pending valid changes and `--fail-on-changes` was supplied     |

Click usage errors also exit `2`, so treat that code as drift only when a plan
report was produced.

For a strict drift gate:

```bash
delta-engine plan myproject.tables:all_tables --fail-on-changes
```

This is useful in a scheduled reconciliation check or after an apply. On a pull
request, the default command usually gives the better workflow: a proposed
declaration change can show the DDL it would apply without making the PR red.

## Configure unified authentication

The CLI delegates authentication to `databricks.sdk.core.Config`. In GitHub
Actions, use [Databricks workload identity federation for GitHub OIDC](https://docs.databricks.com/aws/en/dev-tools/auth/provider-github)
instead of storing a Databricks token. The job needs:

- `permissions: id-token: write` so GitHub can issue an OIDC token
- `permissions: contents: read` for checkout
- `DATABRICKS_AUTH_TYPE=github-oidc`
- `DATABRICKS_HOST`, `DATABRICKS_CLIENT_ID`, and `DATABRICKS_HTTP_PATH`

Use two Databricks service principals and federation policies:

- A **plan identity** for trusted same-repository pull requests. Grant it
  warehouse use and read-only catalog metadata access, but no schema-changing
  privileges.
- An **apply identity** with the required write privileges. Bind its federation
  policy to a protected GitHub `production` environment and keep its client ID
  in that environment's variables.

Fork pull requests contain untrusted code and do not receive the live plan
identity in the workflow below. Their plan job is skipped; run ordinary offline
lint and unit tests for forks instead.

## Complete GitHub Actions workflow

This workflow plans trusted pull requests and applies after a push to `main`.
It uses the currently supported [`actions/checkout@v7`](https://github.com/actions/checkout)
major and does not store `DATABRICKS_TOKEN`.

````yaml
name: schema

on:
  pull_request:
  push:
    branches: [main]

permissions:
  contents: read
  id-token: write

env:
  DATABRICKS_HOST: ${{ vars.DATABRICKS_HOST }}
  DATABRICKS_HTTP_PATH: ${{ vars.DATABRICKS_HTTP_PATH }}

jobs:
  plan:
    # Never run repository code from a fork with the live catalog identity.
    if: >-
      github.event_name == 'pull_request' &&
      github.event.pull_request.head.repo.full_name == github.repository
    runs-on: ubuntu-latest
    env:
      DATABRICKS_AUTH_TYPE: github-oidc
      DATABRICKS_CLIENT_ID: ${{ vars.DATABRICKS_PLAN_CLIENT_ID }}
    steps:
      - uses: actions/checkout@v7
      - uses: actions/setup-python@v6
        with:
          python-version: "3.12"
          cache: pip
          cache-dependency-path: pyproject.toml
      - run: pip install "delta-engine[cli]"
      - name: Plan
        shell: bash
        run: |
          set +e
          output="$(delta-engine plan myproject.tables:all_tables 2>&1)"
          status=$?
          set -e

          {
            echo '```text'
            printf '%s\n' "$output"
            echo '```'
          } >> "$GITHUB_STEP_SUMMARY"

          printf '%s\n' "$output"
          exit "$status"

  apply:
    if: github.event_name == 'push' && github.ref == 'refs/heads/main'
    runs-on: ubuntu-latest
    environment: production
    env:
      DATABRICKS_AUTH_TYPE: github-oidc
      DATABRICKS_CLIENT_ID: ${{ vars.DATABRICKS_APPLY_CLIENT_ID }}
    steps:
      - uses: actions/checkout@v7
      - uses: actions/setup-python@v6
        with:
          python-version: "3.12"
          cache: pip
          cache-dependency-path: pyproject.toml
      - run: pip install "delta-engine[cli]"
      - run: delta-engine apply myproject.tables:all_tables
````

The plan step captures output and status separately. It disables the shell's
fail-fast mode only while running the command, always writes the closing code
fence, prints the same report to the job log, and finally exits with the
original CLI status. A pipe or grouped `echo` command can otherwise hide the
status or leave a broken step summary when the plan fails.

Protect the `production` environment with the reviewers and branch rules
appropriate for your deployment. The environment gate prevents the
write-capable client ID and matching OIDC subject from being released merely
because a push job was created.

## Show the SQL that would run

`--show-sql` appends each table's exact planned statements to text output:

```bash
delta-engine plan myproject.tables:all_tables --show-sql
```

SQL is compiled before dependency resolution. A table that is later blocked by
a failed dependency can still carry planned statements in the report; those
statements were not executed.

## Machine-readable output

`--output json` prints the full
[`to_dict()` payload](reference-run-report.md) as the only content on stdout:

```bash
delta-engine plan myproject.tables:all_tables --output json \
  | jq '.tables[] | {name, status}'
```

Declaration/authentication prints and engine logs are redirected to stderr in
JSON mode. The JSON already contains `planned_sql_statements`, so adding
`--show-sql` does not change it.

## The Python API for custom gates

For custom policy, construct the same unified-auth connection explicitly and
inspect the report:

```python
import os
import sys

from databricks import sql
from databricks.sdk.core import Config

from delta_engine import ValidationFailure
from delta_engine.databricks import build_sql_engine
from myproject.tables import all_tables

config = Config()
with sql.connect(
    server_hostname=config.host,
    http_path=os.environ["DATABRICKS_HTTP_PATH"],
    credentials_provider=lambda: config.authenticate,
) as connection:
    report = build_sql_engine(connection).sync(*all_tables, dry_run=True)

for table_report in report:
    for failure in table_report.failures:
        if isinstance(failure, ValidationFailure):
            sys.exit(1)
```

On a Databricks cluster, build the engine with `build_spark_engine(spark)`
instead. The report and failure types are identical.
