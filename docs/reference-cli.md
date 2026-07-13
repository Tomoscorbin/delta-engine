---
tags:
  - reference
---

# CLI reference

The `delta-engine` command has one read-only, CI-first workflow:

```bash
delta-engine plan myproject.tables:all_tables
```

It loads one explicit declaration collection, reads one live Unity Catalog
target through a Databricks SQL warehouse, and runs
`Engine.sync(..., dry_run=True)`. It never executes DDL or otherwise mutates
catalog state. Use the Python API when you intend to apply changes.

Install the optional CLI dependencies first:

```bash
pip install "delta-engine[cli]"
```

The base distribution remains dependency-free. If the console script is run
without the extra, it prints the install command instead of importing Typer,
the Databricks SDK, or the SQL connector.

## Command

```text
delta-engine plan MODULE:ATTRIBUTE
```

Routine use has no options. The root command retains `--help` and `--version`,
and `plan --help` describes its one argument. Shell-completion installation
options are disabled.

The command performs these operations in order:

1. Import the selected declaration module from the current checkout.
2. Validate that the attribute contains one non-empty ordered sequence of
   `DeltaTable` declarations.
3. Open a SQL warehouse connection using GitHub Actions OIDC.
4. Read catalog state and build a dry-run plan.
5. Print the target identity, semantic diff, sync report, and any planned SQL.

A stopped warehouse may start when the plan reads catalog metadata, so the
command can incur compute cost despite being read-only.

## Declaration reference

The argument is always one `MODULE:ATTRIBUTE` reference. The attribute must be
a non-empty ordered sequence, such as a list or tuple:

```python
from delta_engine.schema import Column, DeltaTable, Integer

orders = DeltaTable(
    "dev",
    "silver",
    "orders",
    columns=(Column("id", Integer(), nullable=False),),
)

all_tables = [orders]
```

Point the command at the collection, even when it currently holds one table:

```bash
delta-engine plan myproject.tables:all_tables
```

A single `DeltaTable`, an empty collection, an unordered collection such as a
set, a mixed sequence, or duplicate qualified table names is a configuration
error. The CLI does not scan module globals and does not accept multiple
declaration references.

The current working directory takes import precedence, so repository code does
not need to be installed first. Declaration imports execute arbitrary Python;
run plans only for code you trust. A missing target module or attribute is a
short configuration error. Exceptions raised by the selected module, including
a missing dependency imported by that module, retain their original traceback.

## GitHub Actions OIDC target

The CLI supports exactly one authentication path. Every invocation requires:

| Environment variable                | Meaning                                      |
| ----------------------------------- | -------------------------------------------- |
| `DATABRICKS_HOST`                   | Workspace URL                                |
| `DATABRICKS_CLIENT_ID`              | Read-only Databricks service principal       |
| `DATABRICKS_SQL_WAREHOUSE_ID`       | Warehouse ID, not a connector HTTP path      |
| `ACTIONS_ID_TOKEN_REQUEST_URL`      | Supplied by GitHub for `id-token: write`     |
| `ACTIONS_ID_TOKEN_REQUEST_TOKEN`    | Supplied by GitHub for `id-token: write`     |

The workflow job must grant `permissions: id-token: write`. The CLI constructs
`databricks.sdk.core.Config` with `auth_type="github-oidc"`; PATs, profiles,
OAuth client secrets, local user authentication, and generic unified-auth
selection are not supported. Variables for those methods are ignored.

The warehouse ID becomes `/sql/1.0/warehouses/<id>` inside the connection
boundary. Users never configure connector transport paths directly.

Authentication and connection failures are rendered as one-line configuration
errors. Identity and OIDC values are redacted from failure details. A local
file that shadows the installed `databricks` packages is also reported as a
configuration error.

## Output

Every completed plan writes these text sections to stdout in order:

1. `TARGET`: normalized host, warehouse ID, and declaration reference
2. `DIFF`: semantic changes for each table
3. `SYNC REPORT`: statuses, failures, and summary
4. `PLANNED SQL`: exact statements, when the plan compiled any

The client ID and GitHub OIDC values are never rendered. Planned SQL is shown
by default; there is no SQL display flag or JSON mode.

Imported-code output, SDK or connector output, engine logs, configuration
errors, and tracebacks go to stderr. This keeps the complete human-readable
plan together on stdout while preserving diagnostics separately.

SQL is compiled before cross-table dependency resolution. A table later
blocked by a failed dependency can therefore carry table-local planned SQL;
the report explains why those statements were not eligible to run.

## Exit codes

| Code | Meaning                                                                       |
| ---- | ----------------------------------------------------------------------------- |
| 0    | The plan completed successfully, whether in sync or carrying pending changes |
| 1    | Configuration, catalog read, or validation failed                            |
| 2    | Typer/Click rejected malformed command-line usage                            |

Unexpected declaration-code and engine defects propagate with tracebacks and
exit non-zero. The connection is still closed; a cleanup failure is logged and
never replaces the completed report or primary exception.
