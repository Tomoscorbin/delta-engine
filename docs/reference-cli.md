---
tags:
  - reference
---

# CLI reference

The `delta-engine` command has four workflows — three read-only, one that
writes:

```bash
delta-engine plan myproject.tables:all_tables
delta-engine apply myproject.tables:all_tables
delta-engine generate dev.silver.orders > orders.py
delta-engine lint myproject.tables:all_tables
```

`plan` loads one explicit declaration collection, reads one live Unity Catalog
target through a Databricks SQL warehouse, and runs
`Engine.sync(..., dry_run=True)`. The engine invocation never executes planned
DDL. Declaration modules are ordinary Python and remain responsible for their
own import-time behaviour.

`apply` runs the same pipeline with `dry_run=False`: it re-reads live state,
re-plans, and executes the compiled DDL. The same safety rules veto unsafe
changes — there is no flag to force a rejected plan. Run it with an identity
that holds the required schema-changing privileges.

`generate` reads one live table and prints an importable declaration module —
the adoption on-ramp for bringing an existing table under management without
hand-transcribing its schema.

`lint` checks the declarations themselves against governance rules — comments,
primary keys, required tags. It never opens a connection, so it needs no
credentials and can run first in CI.

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

The root command retains `--help` and `--version`, and `plan --help`
describes its argument and options. Shell-completion installation options
are disabled.

`plan` takes two options; neither changes what the command reads, and
nothing it plans is ever executed:

| Option                  | Effect                                                                              |
| ----------------------- | ----------------------------------------------------------------------------------- |
| `--output [text\|json]` | Report format on stdout; `json` emits the [run report](reference-run-report.md)     |
| `--fail-on-changes`     | Exit 1 when a valid plan contains pending changes, so CI can gate on catalog drift |

The command performs these operations in order:

1. Import the selected declaration module from the current checkout.
2. Validate that the attribute contains one non-empty ordered sequence of
   `DeltaTable` declarations.
3. Open a SQL warehouse connection using Databricks unified authentication.
4. Read catalog state and build a plan without executing it.
5. Print the target identity, semantic diff, sync report, and any planned SQL.

A stopped warehouse may start when the plan reads catalog metadata, so the
command can incur compute cost despite being read-only.

## Declaration reference

The argument is always one `MODULE:ATTRIBUTE` reference. The attribute is a
non-empty ordered sequence, such as a list or tuple:

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

A bare `DeltaTable` attribute is also accepted and loads as a one-table
collection, so a single declaration plans without a wrapper list:

```bash
delta-engine plan myproject.tables:orders
```

An empty collection, an unordered collection such as a set, a mixed sequence,
or duplicate qualified table names is a configuration error. The CLI does not
scan module globals and does not accept multiple declaration references.

Declaration order never changes the plan: the engine reports tables in
sorted qualified-name order and derives execution order from foreign-key
dependencies. The sequence requirement exists so error messages can point
at a stable item index, not because position carries meaning.

The current working directory takes import precedence, so repository code does
not need to be installed first. Declaration imports execute arbitrary Python;
run plans only for code you trust. A missing target module or attribute is a
short configuration error. Exceptions raised by the selected module, including
a missing dependency imported by that module, retain their original traceback.

## delta-engine apply

```text
delta-engine apply MODULE:ATTRIBUTE
```

`apply` takes the same single declaration reference as `plan` and resolves its
connection the same way. The run differs only after planning: compiled
statements execute against the catalog in dependency order, each table
independently, halting a table at its first failed statement while unrelated
tables continue.

`apply` accepts the same `--output` option as `plan`. There is no
`--fail-on-changes`: executing pending changes is the command's purpose.

Output matches `plan` with two differences: the report carries no dry-run
banner and summarises real outcomes (`applied`, `partially applied`,
`not applied`), and the SQL section is headed `EXECUTED SQL`. A table blocked
by a failed dependency still shows its compiled SQL; the report explains why
those statements did not run.

Safety rules are enforced exactly as in `plan`: a rejected table reports
`PLANNING_FAILED` and executes nothing. There is no force flag.

## Generating a declaration from a live table

```text
delta-engine generate CATALOG.SCHEMA.TABLE
```

The command reads one live table's observed state and prints a Python module
to stdout. The module declares the table through `delta_engine.schema`
vocabulary, bound to a variable named after the table, so its output is
immediately usable:

```bash
delta-engine generate dev.silver.orders > orders.py
delta-engine plan orders:orders
```

A table name that is not a valid Python identifier is sanitised for the
variable binding (`2024-orders` becomes `_2024_orders`); the declared table
name is always the real one.

A correctly generated module plans no changes against its source table.
Warnings go to stderr, so redirecting stdout to a file never captures them.

Connection configuration is identical to `plan`: `DATABRICKS_SQL_WAREHOUSE_ID`
plus Databricks unified authentication.

### Foreign keys

Each observed foreign key is declared in `foreign_keys`: a key to another
table references it by its full `"catalog.schema.table"` name, and a
self-referencing key uses `Self`. The module never constructs the referenced
`DeltaTable`, so it stays self-contained, and planning it as written keeps
every constraint.

### Streaming tables

A streaming table's structure, properties, and keys belong to its owning
pipeline, so the generated module declares `scope="annotations"` and omits
properties. A warning on stderr states this. The module manages comments and
tags only, which is the widest scope validation admits for streaming tables.

### Tables that cannot be generated

Some live tables carry state the engine cannot declare; generation fails with
a one-line error naming the table and the reason rather than emitting a module
that cannot plan. Known cases: `delta.columnMapping.mode = 'id'` (legacy
Hive-metastore migrations), compound interval property values such as
`'interval 1 hour 30 minutes'`, and `NOT NULL` struct fields inside arrays or
maps.

### Fidelity caveats

- `CHAR(n)`/`VARCHAR(n)` columns are declared as `String()`; the engine reads
  both sides the same way, so the plan stays clean, but the length bound is
  not preserved.
- Catalog, schema, and table names are rendered in canonical lowercase, as
  Unity Catalog stores them. Column spelling is preserved.
- Only engine-managed property keys appear; other properties a table carries
  are not engine state.

## Linting declarations

```text
delta-engine lint [MODULE:ATTRIBUTE]
```

`lint` loads the same declaration collection as `plan` and checks each table
against the enabled rules. Nothing is read from the catalog and no connection
is opened, so the command works without any Databricks configuration.

| Rule id          | Checks                                                              | Default |
| ---------------- | ------------------------------------------------------------------- | ------- |
| `table-comment`     | The table has a comment                                             | error   |
| `column-comment`    | Every column has a comment (one finding per column)                 | error   |
| `primary-key`       | The table declares a primary key                                    | error   |
| `naming-convention` | The table name and every column name match a pattern (snake_case by default) | off     |
| `required-tag`      | The table carries each configured tag key (values are not checked) | off     |

Configure severities in `pyproject.toml`; each rule takes `"error"`,
`"warning"`, or `"off"`:

```toml
[tool.delta-engine.lint]
declarations = "myproject.tables:all_tables"
column-comment = "warning"
naming-convention = { pattern = "[a-z][a-z0-9_]*" }
required-tag = { keys = ["owner"] }
```

`naming-convention` is off until you name it. It matches the whole name against
`pattern` (the default is snake_case), so a partial match does not pass; an
invalid regular expression is a configuration error. `required-tag` is off until
its keys are listed; `severity` inside its inline
table is optional and defaults to `"error"`. An unknown rule name or an
invalid severity is a configuration error, so a typo cannot silently disable
a rule.

### Per-table overrides

Override blocks change the policy for the tables they match, leaving every
other table on the top-level settings:

```toml
[[tool.delta-engine.lint.overrides]]
tables = ["dev.bronze.*"]
primary-key = "off"
column-comment = "warning"

[[tool.delta-engine.lint.overrides]]
tables = ["prod.gold.*"]
naming-convention = "error"
```

`tables` is a list of `catalog.schema.table` globs. A pattern always has three
dot-separated segments and a `*` matches within its segment only, so
`dev.bronze.*` covers one schema and every table in a catalog is spelled
`dev.*.*`. Matching is case-insensitive, like the names themselves.

Rule settings inside a block take the same form as the top level — a severity
string or an inline table with parameters — so an override can disable a rule,
change its severity, reconfigure its parameters, or enable a rule that is off
globally. When several blocks match one table they apply in file order, each
changing only the rules it names; the last block to name a rule wins.

A block with an empty `tables` list, no rule settings, an unknown rule name,
or a malformed pattern is a configuration error. A pattern that matches none
of the declared tables is not.

The argument is optional when the config declares a `declarations` target; an
explicit argument wins. The config is read from `./pyproject.toml` in the
working directory, or from the file named with `--config`.

`lint` accepts the same `--output` option as `plan`. Text output groups
findings per table and ends with a summary line. `--output json` emits one
document carrying `tables_checked`, `error_count`, `warning_count`, and a
`findings` list; each finding carries `rule`, `severity`, `table`, and
`message`.

Warnings never change the exit code: exit 0 means no error-severity findings,
exit 1 means at least one error finding or a configuration error. To gate CI
on a rule, leave it at `error`; to adopt gradually, downgrade it to
`warning`.

Programmatic use needs no CLI: `delta_engine.lint.lint_tables(*all_tables)`
returns the same report, so a declarations repository can assert
`not report.has_errors` in its own tests.

The rule set is built in. Adding a rule is one class in `lint/rules.py` —
see [how to add a lint rule](how-to-add-lint-rule.md).

## Databricks connection

Every invocation requires one CLI-specific target setting:

| Environment variable          | Meaning                                 |
| ----------------------------- | --------------------------------------- |
| `DATABRICKS_SQL_WAREHOUSE_ID` | Warehouse ID, not a connector HTTP path |

The CLI constructs `databricks.sdk.core.Config()` without choosing an
authentication method. The SDK resolves the workspace and credentials from its
standard environment variables or configuration profiles. Authentication is
therefore deployment configuration, not a CLI option or code path.

For example, a GitHub Actions job can select workload identity federation with
`DATABRICKS_HOST`, `DATABRICKS_CLIENT_ID`, and
`DATABRICKS_AUTH_TYPE=github-oidc`; see [the CI guide](how-to-gate-changes-in-ci.md).
Local profiles and other Databricks unified-auth configurations use the same
command without flags.

The warehouse ID becomes `/sql/1.0/warehouses/<id>` inside the connection
boundary. Users never configure connector transport paths directly.

Authentication and connection failures are rendered as one-line configuration
errors. Secret-looking environment values are redacted from failure details. A
local file that shadows the installed `databricks` packages is also reported as
a configuration error.

## Output

Every completed run writes these text sections to stdout in order:

1. `TARGET`: normalized host, warehouse ID, and declaration reference
2. `DIFF`: semantic changes for each table
3. `SYNC REPORT`: statuses, failures, and summary
4. `PLANNED SQL`: exact statements, when the plan compiled any (headed
   `EXECUTED SQL` on an apply run)

On a dry run the sync report labels this boundary `PLAN — no planned SQL
executed`. Catalog reads still occur and may start the warehouse; only the
generated statements are guaranteed not to execute.

Credentials are never intentionally rendered. Planned SQL is always shown;
there is no SQL display flag.

With `--output json` the text sections are replaced by one JSON document:
the versioned run report that `SyncReport.to_dict()` emits (see
[the run report schema](reference-run-report.md)). The target identity is
not part of the document. Diagnostics still go to stderr, so a completed
run's stdout parses directly with `json.loads`.

Imported-code output, SDK or connector output, engine logs, configuration
errors, and tracebacks go to stderr. This keeps the complete human-readable
plan together on stdout while preserving diagnostics separately.

Cross-table dependency resolution orders the tables first; SQL is then
compiled per table before anything would execute. A table later blocked by a
failed dependency can therefore carry table-local planned SQL; the report
explains why those statements were not eligible to run.

## Exit codes

| Code | Meaning                                                                                                                                  |
| ---- | ----------------------------------------------------------------------------------------------------------------------------------------- |
| 0    | The run completed with no failures: a plan in sync or carrying pending changes (without `--fail-on-changes`), an apply that executed fully, or a lint with no error-severity findings |
| 1    | Configuration, catalog read, planning, execution, or generation failed — `--fail-on-changes` found pending changes, or lint found error-severity findings |
| 2    | Typer/Click rejected malformed command-line usage                                                                                           |

Unexpected declaration-code and engine defects propagate with tracebacks and
exit non-zero. The connection is still closed; a cleanup failure is logged and
never replaces the completed report or primary exception.
