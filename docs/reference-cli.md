---
tags:
  - reference
---

# CLI reference

The `delta-engine` command plans and applies Python table declarations through
a Databricks SQL warehouse. Install the optional CLI dependencies first:

```bash
pip install "delta-engine[cli]"
```

The base distribution still has no runtime dependencies. If the console script
is run without the extra, it prints the install command instead of importing
Typer, the Databricks SDK, or the SQL connector.

## Declaration arguments

Every declaration argument has the form `MODULE:ATTRIBUTE`:

```bash
delta-engine plan myproject.tables:orders
delta-engine plan myproject.tables:core_tables myproject.events:event_tables
```

The attribute must be one `DeltaTable` or a sequence (a list or tuple)
containing at least one `DeltaTable`. A bare module is invalid; the CLI never
scans module globals. Empty sequences, unordered containers such as sets, and
non-table items are configuration errors.

The same qualified table may be selected only once. This includes the same
object repeated in one sequence, aliases selected through separate attributes,
and distinct `DeltaTable` objects with the same `catalog.schema.table` name.
The CLI reports both argument/item origins and does not silently deduplicate
them. These checks finish before authentication or connection setup begins.

The current working directory is prepended to `sys.path`, so a declaration
module in a repository checkout can be loaded without installing that project.

**Declaration imports execute arbitrary Python.** Run the CLI only against code
you trust. A genuinely missing target module or attribute is rendered as a
short configuration error. Exceptions raised while the module imports,
including a missing dependency of that module, propagate with their original
traceback.

## `plan`

```text
delta-engine plan [OPTIONS] MODULE:ATTRIBUTE...
```

`plan` opens a live warehouse connection and runs the engine with
`dry_run=True`. It reads current Unity Catalog state, computes and validates the
diff, compiles planned SQL, and resolves table dependencies, but executes no
DDL. A valid plan exits successfully whether or not changes are pending:

```bash
delta-engine plan myproject.tables:all_tables
```

Use `--fail-on-changes` only when pending drift should fail the job:

```bash
delta-engine plan myproject.tables:all_tables --fail-on-changes
```

Although no DDL runs, `plan` is not an offline command. Its live reads can
start a stopped SQL warehouse and may incur compute cost.

## `apply`

```text
delta-engine apply [OPTIONS] MODULE:ATTRIBUTE...
```

`apply` runs the same read, diff, validation, planning, and dependency phases,
then executes each eligible plan:

```bash
delta-engine apply myproject.tables:all_tables
```

An apply is **not transactional across tables or statements**. The executor
stops a table after its first failed statement, but work completed earlier is
not rolled back. A failed run can therefore partially succeed; inspect the
emitted report before retrying.

## Options

Both commands accept:

| Option                | Meaning                                               |
| --------------------- | ----------------------------------------------------- |
| `--output text\|json` | Select text (the default) or structured report output |
| `--show-sql`          | Append planned statements to text output              |
| `--host HOST`         | Override `DATABRICKS_HOST`                            |
| `--http-path PATH`    | Override `DATABRICKS_HTTP_PATH`                       |
| `--profile PROFILE`   | Override `DATABRICKS_CONFIG_PROFILE`                  |
| `--verbose`, `-v`     | Send INFO-level engine progress to stderr             |
| `--help`              | Show command help                                     |

`plan` additionally accepts `--fail-on-changes`. The root command accepts
`--version` and `--help`.

## Exit codes

| Code | `plan`                                                             | `apply`                            |
| ---- | ------------------------------------------------------------------ | ---------------------------------- |
| 0    | The plan is valid; it may be in sync or contain pending changes    | Every table completed successfully |
| 1    | A table or configuration failed                                    | A table or configuration failed    |
| 2    | Valid changes are pending **and** `--fail-on-changes` was supplied | Not used by a completed apply      |

Click also uses exit code `2` for command-line usage errors, such as a missing
declaration argument or invalid option value. Code `2` therefore means
"changes pending" only when the command actually produced a valid plan report.
Unexpected exceptions from user declarations or programming defects propagate
normally and the Python process exits non-zero with a traceback.

## Databricks unified authentication

The CLI constructs [`databricks.sdk.core.Config`](https://databricks-sdk-py.readthedocs.io/en/stable/authentication.html)
and gives its authentication callback to the Databricks SQL connector. It does
not implement its own token selection. Unified authentication can therefore use
environment variables, Databricks configuration profiles, PATs, OAuth M2M, or
GitHub Actions OIDC.

CLI values take precedence as follows:

1. `--host` overrides `DATABRICKS_HOST` and `--profile` overrides
   `DATABRICKS_CONFIG_PROFILE`.
2. Without those flags, the SDK reads Databricks environment variables and
   then the selected or default `.databrickscfg` profile according to its
   unified-auth rules.
3. `--http-path` overrides `DATABRICKS_HTTP_PATH`. The HTTP path is a SQL
   warehouse connector setting, so it is resolved separately from SDK auth.

There is deliberately no token flag. For local named-profile authentication:

```bash
export DATABRICKS_HTTP_PATH=/sql/1.0/warehouses/abc123
delta-engine plan myproject.tables:all_tables --profile development
```

For OAuth M2M:

```bash
export DATABRICKS_HOST=https://example.cloud.databricks.com
export DATABRICKS_HTTP_PATH=/sql/1.0/warehouses/abc123
export DATABRICKS_CLIENT_ID=...
export DATABRICKS_CLIENT_SECRET=...
delta-engine plan myproject.tables:all_tables
```

For GitHub Actions workload identity federation, set
`DATABRICKS_AUTH_TYPE=github-oidc`, `DATABRICKS_HOST`,
`DATABRICKS_CLIENT_ID`, and `DATABRICKS_HTTP_PATH`, and grant the workflow
`id-token: write`. See the [CI guide](how-to-gate-changes-in-ci.md) for separate
plan/apply identities and fork handling.

PAT authentication remains available as the unified-auth legacy fallback:

```bash
export DATABRICKS_HOST=https://example.cloud.databricks.com
export DATABRICKS_HTTP_PATH=/sql/1.0/warehouses/abc123
export DATABRICKS_TOKEN=...
delta-engine plan myproject.tables:all_tables
```

Prefer short-lived OAuth or OIDC credentials for automation.

Configuration failures found before connecting — unresolvable authentication
and a missing HTTP path — are reported together in one error. Failures while
establishing the connection (unreachable host, rejected credentials) are also
rendered as one-line configuration errors rather than tracebacks. If a file in
the working directory shadows the installed `databricks` packages (a project
file named `databricks.py`, say), the error names that file.

## Output streams and planned SQL

Text output writes the diff/report to stdout. `--show-sql` appends each table's
planned statements. JSON mode writes the complete
[`SyncReport.to_dict()` payload](reference-run-report.md) as the only content on
stdout; declaration prints, authentication output, sync-time prints, logs, and
configuration errors go to stderr. This makes stdout safe to pipe to `jq`:

```bash
delta-engine plan myproject.tables:all_tables --output json \
  | jq '.tables[] | {name, status, planned_sql_statements}'
```

JSON already contains `planned_sql_statements`, so `--show-sql` is redundant in
JSON mode.

SQL is compiled before cross-table dependency resolution. A report can
therefore contain planned SQL for a table that is subsequently blocked by a
failed or unresolved dependency. Planned SQL describes the table-local work the
engine prepared; it is not proof that the statements were or could be executed
in that run.
