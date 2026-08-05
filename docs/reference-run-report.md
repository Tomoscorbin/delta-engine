---
tags:
  - reference
---

# Run report schema

`SyncReport.to_dict()` projects a whole run as plain, JSON-serialisable data —
`dict`, `list`, `str`, `int`, `bool`, and `None` only, so
`json.dumps(report.to_dict())` works directly. It is the machine-readable view
of a run, for CI gates, run-history persistence, and structured logging. The
human-readable views are `render_report` and `render_diff`.

`TableRun.to_dict()` projects a single table's record, the same object
that appears in the run-level `tables` list.

## Stability

The fields and enumerated values below are a **public contract**, versioned by
`schema_version`. Adding a field is backwards-compatible; renaming or removing
one, or renaming an enumerated value, is a breaking change and increments
`schema_version`. The projection is deterministic: tables appear in run order,
and changes and statements in action-plan order, so two projections of the same
report compare equal — successive dry-run outputs can be diffed.

Version 2 renamed the planning-phase status from `VALIDATION_FAILED` to
`PLANNING_FAILED` and the corresponding failure phase from `VALIDATION` to
`PLANNING`.

`rejected_changes` was added without a version bump: adding a field is
backwards-compatible, and a reader that does not know the key sees exactly the
payload it saw before.

The per-type failure fields (2026-08-05) were likewise added without a bump:
every failure record still carries `phase`, `type`, and `message` with
unchanged meaning, and the additional keys sit beside them.

## Consistency

`SyncReport` rejects combinations that a completed engine run cannot produce.
A dry run cannot contain execution results. A real run with a non-empty plan
must either contain its execution result or a failure explaining why execution
did not run. Empty plans and plans rejected before compilation require no
execution result.

These checks apply when constructing a report directly as well as when the
engine assembles one. `ExecutionSummary` separately validates the statement
history inside an execution result.

## Table change states

For Python callers, `SyncReport.table_change_states` returns one
`TableChangeState` per table run, in the same order as
`SyncReport.table_runs`. The aggregate owns this view because the distinction
between a planned dry-run change and an unapplied real-run change depends on
the run's `dry_run` mode, not on the table report alone.

| Member              | Value               | Meaning                                                        |
| ------------------- | ------------------- | -------------------------------------------------------------- |
| `NOT_PLANNED`       | `not planned`       | Reading or planning failed before a plan was accepted          |
| `UNCHANGED`         | `unchanged`         | The accepted plan contained no catalog changes                 |
| `PLANNED`           | `planned`           | A dry run compiled a non-empty plan without executing it       |
| `NOT_APPLIED`       | `not applied`       | A real-run change was blocked, or its first statement failed   |
| `PARTIALLY_APPLIED` | `partially applied` | Some statements succeeded before a later statement failed      |
| `APPLIED`           | `applied`           | Every statement in a non-empty real-run plan succeeded         |

Change state is deliberately separate from `TableRunStatus`: status explains
which phase failed, while change state describes the effect on the catalog. A
table can therefore be `EXECUTION_FAILED` with either `NOT_APPLIED` or
`PARTIALLY_APPLIED`, and an unchanged table can still carry a foreign-key
failure. Import the enum with `from delta_engine import TableChangeState`.

The human-readable renderers use these states on real runs. `render_diff`
marks non-empty plans that were not applied or only partially applied, while
the `render_report` footer counts catalog outcomes. A compiled plan blocked
before execution shows statement progress as `0/n`. Dry-run diff blocks and
their changed/unchanged/failed footer keep describing planned work instead.

This Python-level derived view does not add fields to `SyncReport.to_dict()` or
`TableRun.to_dict()`; the structured schema below remains version 2.

## Run-level fields

`SyncReport.to_dict()` returns:

| Field            | Type         | Meaning                                       |
| ---------------- | ------------ | --------------------------------------------- |
| `schema_version` | `int`        | Version of this payload schema; currently `2` |
| `started_at`     | `str`        | ISO 8601 timestamp when the run began         |
| `ended_at`       | `str`        | ISO 8601 timestamp when the run ended         |
| `dry_run`        | `bool`       | Whether execution was skipped                 |
| `has_changes`    | `bool`       | True if any table has a planned change        |
| `has_failures`   | `bool`       | True if any table failed a phase              |
| `tables`         | `list[dict]` | Per-table records, in run order (see below)   |

## Table-level fields

Each entry in `tables`, and the whole of `TableRun.to_dict()`:

| Field                    | Type             | Meaning                                                      |
| ------------------------ | ---------------- | ------------------------------------------------------------ |
| `name`                   | `str`            | Dotted, unquoted qualified name, e.g. `cat.schema.orders`    |
| `status`                 | `str`            | A `TableRunStatus` value (`SUCCESS`, `PLANNING_FAILED`, …)  |
| `has_changes`            | `bool`           | True if this table has a planned change                      |
| `has_failures`           | `bool`           | True if this table failed a phase                            |
| `changes`                | `list[dict]`     | Summaries of the planned changes, in plan order (see below)  |
| `rejected_changes`       | `list[dict]`     | Differences found but refused, when the plan was rejected; empty otherwise |
| `planned_sql_statements` | `list[str]`      | Full compiled DDL the plan lowers to, in order               |
| `failures`               | `list[dict]`     | Failure records, in phase order (see below)                  |
| `execution`              | `dict` \| `None` | Execution counts, or `None` on a dry run or a skipped table  |

### Change records

Each entry in `changes` summarises part of a planned change, derived from the
same interpretation the text renderers use. They are human-oriented summaries,
not one record per plan action (a table creation expands into several), and
not a complete description of the change — the authoritative, complete
description is `planned_sql_statements`:

| Field       | Type  | Meaning                                                                            |
| ----------- | ----- | ---------------------------------------------------------------------------------- |
| `kind`      | `str` | Change category: `columns`, `keys`, `clustering`, `partitioning`, `features`, `properties`, `tags`, `comments` |
| `operation` | `str` | `add`, `remove`, or `change`                                                       |
| `subject`   | `str` | What the change targets: the name of a column, property, tag, or table feature; `column <name>` or `column <name>.<tag>` when the target is scoped to a column; or one of `table`, `primary key`, `foreign key ...`, `clustering`, `partitioning` |
| `detail`    | `str` | How it changed, e.g. `Integer → Long` or `= 'true' (was 'false')`; empty when there is none |

### Rejected change records

When a table's diff is rejected, no plan exists, so `changes` is empty. The
differences the engine did find are projected into `rejected_changes` in the
same record shape, so a reader can see *what* was refused alongside the
`failures` list that says *why*. It includes both the actions the engine would
have taken and the differences no action can close (a column spelled
differently from the catalog, a property set but undeclared, a partitioning
change). It is always empty for a table that planned successfully.

### Failure records

Each entry in `failures`:

| Field     | Type  | Meaning                                                                      |
| --------- | ----- | ---------------------------------------------------------------------------- |
| `phase`   | `str` | The phase that produced it: `READ`, `PLANNING`, `FOREIGN_KEY`, `EXECUTION` |
| `type`    | `str` | The concrete failure class name, e.g. `ValidationFailure`                    |
| `message` | `str` | The rendered failure message                                                 |

Records are not all-string: the per-type fields below include an integer
(`statement_index`) and lists (`details`, `columns`).

#### Additional keys by type

`ReadFailure`:

| Field            | Type  | Meaning                                                        |
| ---------------- | ----- | -------------------------------------------------------------- |
| `exception_type` | `str` | The backend exception class name                               |
| `diagnostic`     | `str` | The complete backend message; `message` may truncate long text |

`ValidationFailure`:

| Field     | Type        | Meaning                                                             |
| --------- | ----------- | ------------------------------------------------------------------- |
| `rule`    | `str`       | The rule that rejected the diff, e.g. `NonWideningColumnTypeChange` |
| `subject` | `str`       | The column, property, or aspect judged; empty for whole-table rules |
| `details` | `list[str]` | One line per difference behind a summary judgment                   |

`ExecutionFailure`:

| Field             | Type  | Meaning                                                        |
| ----------------- | ----- | -------------------------------------------------------------- |
| `exception_type`  | `str` | The backend exception class name                               |
| `diagnostic`      | `str` | The complete backend message; `message` may truncate long text |
| `statement_index` | `int` | 0-based index into the table's `planned_sql_statements`        |
| `sql`             | `str` | The statement that failed                                      |

`ForeignKeyFailure`:

| Field        | Type        | Meaning                                                   |
| ------------ | ----------- | --------------------------------------------------------- |
| `reason`     | `str`       | Machine code for why, e.g. `BLOCKED_BY_FAILED_DEPENDENCY` |
| `columns`    | `list[str]` | The declaring table's foreign-key columns                 |
| `references` | `str`       | Dotted name of the referenced table                       |

### Execution record

When a table executed, `execution` is:

| Field     | Type  | Meaning                                       |
| --------- | ----- | --------------------------------------------- |
| `applied` | `int` | Statements that ran successfully              |
| `total`   | `int` | Statements planned (`planned_sql_statements`) |

It is `None` for a dry run and for any table skipped by an earlier-phase
failure or blocked by a failed foreign-key dependency. The engine executes statement by statement and stops at the first
failure, so `applied < total` means the trailing statements were never
attempted.

## The planned SQL property

Alongside the projection, `SyncReport.planned_sql_statements` is a
`dict[str, tuple[str, ...]]` mapping each table's dotted name to its compiled
statements, omitting tables with no planned change. It is the same statement
text that appears per table under `planned_sql_statements` in `to_dict()`.

Planned is not executed: a table blocked after planning (for example by a
foreign-key dependency failure) still reports the SQL its plan compiles to.
Whether the statements ran is answered by `execution` and `has_failures`.

See [how to gate schema changes in CI](how-to-gate-changes-in-ci.md) for the
projection in use.
