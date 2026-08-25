---
tags:
  - reference
---

# CLI reference

The `delta-engine` command has two read-only workflows:

```bash
delta-engine plan myproject.tables:all_tables
delta-engine generate dev.silver.orders > orders.py
```

`plan` loads one explicit declaration collection, reads one live Unity Catalog
target through a Databricks SQL warehouse, and runs
`Engine.sync(..., dry_run=True)`. The engine invocation never executes planned
DDL. Declaration modules are ordinary Python and remain responsible for their
own import-time behaviour. Use the Python API when you intend to apply changes.

`generate` reads one live table and prints an importable declaration module —
the adoption on-ramp for bringing an existing table under management without
hand-transcribing its schema.

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

A correctly generated module plans no changes against its source table, with
one exception: foreign keys, which plan as drops until wired up by hand (see
below). Warnings go to stderr, so redirecting stdout to a file never captures
them.

Connection configuration is identical to `plan`: `DATABRICKS_SQL_WAREHOUSE_ID`
plus Databricks unified authentication.

### Foreign keys are not rendered

A `ForeignKey` declaration references another `DeltaTable` object, which a
single-table module cannot construct. Each observed foreign key is instead
emitted as a commented hint inside the constructor call and as a stderr
warning. **Planning the module as written drops those constraints** — the
engine owns the full key set, so an undeclared key reads as removal. Wire the
referenced tables (or `Self` for a self-referencing key) into `foreign_keys`
before applying anything.

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

Every completed plan writes these text sections to stdout in order:

1. `TARGET`: normalized host, warehouse ID, and declaration reference
2. `DIFF`: semantic changes for each table
3. `SYNC REPORT`: statuses, failures, and summary
4. `PLANNED SQL`: exact statements, when the plan compiled any

The sync report labels this boundary `PLAN — no planned SQL executed`. Catalog
reads still occur and may start the warehouse; only the generated statements
are guaranteed not to execute.

Credentials are never intentionally rendered. Planned SQL is shown by default;
there is no SQL display flag or JSON mode.

Imported-code output, SDK or connector output, engine logs, configuration
errors, and tracebacks go to stderr. This keeps the complete human-readable
plan together on stdout while preserving diagnostics separately.

Cross-table dependency resolution orders the tables first; SQL is then
compiled per table before anything would execute. A table later blocked by a
failed dependency can therefore carry table-local planned SQL; the report
explains why those statements were not eligible to run.

## Exit codes

| Code | Meaning                                                                      |
| ---- | ---------------------------------------------------------------------------- |
| 0    | The plan completed successfully, whether in sync or carrying pending changes |
| 1    | Configuration, catalog read, planning, or generation failed                  |
| 2    | Typer/Click rejected malformed command-line usage                            |

Unexpected declaration-code and engine defects propagate with tracebacks and
exit non-zero. The connection is still closed; a cleanup failure is logged and
never replaces the completed report or primary exception.
