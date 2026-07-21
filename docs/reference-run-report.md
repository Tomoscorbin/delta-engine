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

`TableRunReport.to_dict()` projects a single table's record, the same object
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

Each entry in `tables`, and the whole of `TableRunReport.to_dict()`:

| Field                    | Type             | Meaning                                                      |
| ------------------------ | ---------------- | ------------------------------------------------------------ |
| `name`                   | `str`            | Dotted, unquoted qualified name, e.g. `cat.schema.orders`    |
| `status`                 | `str`            | A `TableRunStatus` value (`SUCCESS`, `PLANNING_FAILED`, …)  |
| `has_changes`            | `bool`           | True if this table has a planned change                      |
| `has_failures`           | `bool`           | True if this table failed a phase                            |
| `changes`                | `list[dict]`     | Summaries of the planned changes, in plan order (see below)  |
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
| `kind`      | `str` | Change category: `columns`, `keys`, `clustering`, `properties`, `tags`, `comments` |
| `operation` | `str` | `add`, `remove`, or `change`                                                       |
| `subject`   | `str` | What the change targets, e.g. a column name or `table: '...'`                      |
| `detail`    | `str` | Extra detail, e.g. `Integer → Long`; empty when there is none                      |

### Failure records

Each entry in `failures`:

| Field     | Type  | Meaning                                                                      |
| --------- | ----- | ---------------------------------------------------------------------------- |
| `phase`   | `str` | The phase that produced it: `READ`, `PLANNING`, `FOREIGN_KEY`, `EXECUTION` |
| `type`    | `str` | The concrete failure class name, e.g. `ValidationFailure`                    |
| `message` | `str` | The rendered failure message                                                 |

### Execution record

When a table executed, `execution` is:

| Field     | Type  | Meaning                                       |
| --------- | ----- | --------------------------------------------- |
| `applied` | `int` | Statements that ran successfully              |
| `total`   | `int` | Statements planned (`planned_sql_statements`) |

It is `None` for a dry run and for any table skipped by an earlier-phase
failure. The engine executes statement by statement and stops at the first
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
