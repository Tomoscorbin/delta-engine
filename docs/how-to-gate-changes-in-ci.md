---
tags:
  - how-to
---

# How to report schema plans in CI

Run `delta-engine plan` when a trusted same-repository pull request opens. The
command reads the live Unity Catalog state, prints the target, semantic diff,
sync report, and exact planned SQL without executing the planned DDL.

Pending valid changes exit successfully. Catalog-read, validation,
foreign-key-resolution, connection, and configuration failures exit
unsuccessfully, so one command can act as the PR check without making an
intentional schema change look like a broken build.

## Prepare one declaration collection

Expose the tables for the workflow as one non-empty ordered sequence:

```python
# myproject/tables.py
from delta_engine.schema import Column, DeltaTable, Integer

orders = DeltaTable(
    "production",
    "silver",
    "orders",
    columns=(Column("id", Integer(), nullable=False),),
)

all_tables = [orders]
```

The command takes the collection's exact `MODULE:ATTRIBUTE` reference:

```bash
pip install "delta-engine[cli]"
delta-engine plan myproject.tables:all_tables
```

The CLI accepts one reference only. A single table, empty or unordered
collection, mixed sequence, and duplicate qualified table names fail before a
connection opens. See the [CLI reference](reference-cli.md) for the complete
contract.

## Configure GitHub OIDC for this workflow

Create one Databricks service principal for plans and grant only the warehouse
and catalog permissions needed to read the declared tables' metadata. Do not
grant schema-changing privileges.

Configure Databricks workload identity federation for the repository and pull
request subjects you trust. The GitHub job needs `permissions: id-token: write`
so GitHub supplies its OIDC request URL and token. Store these non-secret target
values as repository variables:

- `DATABRICKS_HOST`
- `DATABRICKS_PLAN_CLIENT_ID`
- `DATABRICKS_SQL_WAREHOUSE_ID`

The workflow selects GitHub OIDC through the Databricks SDK's standard
`DATABRICKS_AUTH_TYPE` setting. The CLI itself has no GitHub-specific
authentication branch; it delegates credential resolution to the SDK.

Fork pull requests execute code controlled by another repository. Skip the
live plan for forks so that code never receives the federated catalog identity;
run the project's ordinary offline lint and test jobs for those PRs instead.

## Add the GitHub Actions workflow

This complete workflow has one read-only plan job, no apply path, and no
write-capable identity:

````yaml
name: schema-plan

on:
  pull_request:

jobs:
  plan:
    # Do not give fork-controlled code the live catalog identity.
    if: github.event.pull_request.head.repo.full_name == github.repository
    runs-on: ubuntu-latest
    permissions:
      contents: read
      id-token: write
    env:
      DATABRICKS_HOST: ${{ vars.DATABRICKS_HOST }}
      DATABRICKS_CLIENT_ID: ${{ vars.DATABRICKS_PLAN_CLIENT_ID }}
      DATABRICKS_AUTH_TYPE: github-oidc
      DATABRICKS_SQL_WAREHOUSE_ID: ${{ vars.DATABRICKS_SQL_WAREHOUSE_ID }}
    steps:
      - uses: actions/checkout@v6
      - uses: actions/setup-python@v6
        with:
          python-version: "3.12"
          cache: pip
          cache-dependency-path: pyproject.toml
      - run: pip install "delta-engine[cli]"
      - name: Plan schemas
        shell: bash
        run: |
          set +e
          delta-engine plan myproject.tables:all_tables > plan-report.txt
          status=$?
          set -e

          {
            echo '## Delta Engine plan'
            echo
            echo '```text'
            cat plan-report.txt
            echo '```'
          } >> "$GITHUB_STEP_SUMMARY"

          cat plan-report.txt
          exit "$status"
````

The command's complete report is stdout, so the step stores it before writing
the fenced summary and replaying it to the job log. Imported-code output, logs,
configuration errors, and tracebacks remain on stderr and therefore go directly
to the log. The original exit status is preserved after the summary is written.

## Interpret the result

| Exit code | Result                                                                       |
| --------- | ---------------------------------------------------------------------------- |
| 0         | The live plan completed, with or without pending changes                     |
| 1         | Configuration, authentication, reading, validation, or FK resolution failed  |
| 2         | The command line was malformed, such as a missing argument or removed option |

Review the `DIFF` section for semantic intent and `PLANNED SQL` for the DDL
compiled from the catalog snapshot read by this plan. A `VALIDATION_FAILED`,
`READ_FAILED`, or `FOREIGN_KEY_FAILED` row is a failed check and includes detail
in the report. A later write-capable sync re-reads and re-plans live state, so
the preview is not a replay artifact and cannot predict execution failures.

`delta-engine plan` always calls the engine with `dry_run=True`; there is no
apply command or flag to turn the generated plan into a write. Declaration
modules are ordinary Python, which is why the workflow uses trusted code and a
read-only identity. Applying declarations remains a separate Python API
workflow with its own explicit connection and permissions.
