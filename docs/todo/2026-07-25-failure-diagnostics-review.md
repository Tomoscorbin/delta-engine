---
tags:
  - todo
  - usability
  - reporting
---

# Failure diagnostics review

## Overall assessment

The failure model is reliable but not yet diagnostic enough. Failures are
typed, phase ordered, attached to the correct table, and shown in both
exceptions and reports. Execution failures already retain the failed SQL, and
most safety rules name the unsafe subject and suggest a recovery.

The main problem is that the report is shaped around an *accepted plan*. When
planning is rejected, the exact diff is omitted from every user-facing view.
That makes `UnmanagedAspectDrift` especially opaque: the engine knows every
different column, property, tag, or comment, but the output says only that an
aspect drifted and then labels the DIFF as `no changes`.

Read failures have a separate context problem. They retain the backend
exception type and message, but not which catalog-read step failed. The CLI
also prints an expected read failure up to three times: an adapter warning with
a traceback, an engine error, and the final report.

The first improvement should therefore be richer report evidence, not a broad
rewrite of exception handling.

## Review method

This review traced the four persistent failure values from their producers
through:

- `Failure.format_lines()` and `headline()`;
- `SyncFailedError`;
- the status grid and full `Failures` section;
- the CLI `plan` command; and
- `SyncReport.to_dict()`.

Focused tests passed unchanged:

```text
204 passed in 1.75s
```

The exercised files were the failure, rendering, report, validation, CLI plan,
Databricks read, and Databricks execution test modules. The findings below are
therefore gaps in the current intended behaviour, not failing tests.

## Current diagnostic quality

| Failure | What is already useful | What is missing |
| --- | --- | --- |
| `ReadFailure` | Table context, backend type, backend message | Read step/query, stable backend condition, a cause-first headline, one non-duplicated presentation |
| `ValidationFailure` | Rule code; most rules name the subject and remediation | Rejected diff; exact subjects for unmanaged drift and column-drop prerequisites |
| `ForeignKeyFailure` | Owning table, local columns, referenced table, broad reason | Constraint name, referenced columns, actual key, exact type mismatches, direct upstream cause |
| `ExecutionFailure` | Backend type/message, failed SQL, applied/planned progress | Human numbering is zero-based; truncation is silent; structured output flattens the fields |

## Prioritized findings

### 1. Rejected drift is hidden and described as “no changes” — high

The engine computes and retains `_TableRun.diff`, but `to_report()` does not
carry it into `TableRunReport`. A rejected planning outcome has no `plan`, so:

- `render_diff_block()` prints `(no changes — see failures)`;
- `has_changes` is false;
- `to_dict()["changes"]` is empty; and
- only the validation rule's prose remains.

For `UnmanagedAspectDrift`, validation then groups raw differences by
`TableAspect` and emits one generic message per aspect. Two different columns
produce the same output as twenty:

```text
DIFF
====

dev.silver.test
  (no changes — see failures)

Validation failed: UnmanagedAspectDrift - Operation not allowed:
column structure has drifted but is not managed by this definition.
```

This explains why the failure cannot answer “what was being changed?” The
detail is absent from both the DIFF view and the failure.

#### Recommendation

Carry the computed `TableDiff` into `TableRunReport` as the observed drift,
independently of whether it became an accepted `ActionPlan`.

Keep the existing meaning of `has_changes`—accepted, executable changes—so CI
gates do not silently change semantics. Add a separate `has_drift`/`drift`
projection and render failed planning as a rejected diff, for example:

```text
dev.silver.orders  (REJECTED)
  columns
    + new_status     String
    - legacy_status

Validation blocked: declaration does not manage column structure
  The rejected diff would add `new_status` and drop `legacy_status`.
  Align the declaration and live table, or explicitly use `scope="full"`
  and review that plan before applying it.
```

The raw diff should show all observed differences. The failure should identify
which entries are unmanaged, so managed and unmanaged drift cannot be confused
when both are present.

This is preferable to only lengthening the existing
`UnmanagedAspectDrift.message`: exact drift is useful for every planning
failure, and the DIFF view is its natural home.

### 2. Read failures lose the operation context and are repeated by the CLI — high

`read_catalog_state()` wraps the complete read in one broad error boundary.
`ReadError` and `ReadFailure` receive only:

- `exception_type`; and
- the rendered exception `message`.

A current read can fail while describing the relation, checking schema
existence, or reading column tags, table tags, primary keys, outbound foreign
keys, or inbound foreign keys. Unless Databricks happens to name the object in
its message, the report cannot say which step failed.

The shared exception inspector already extracts a Databricks condition for
missing-relation classification, but that condition is discarded for all
other errors. This leaves the compact grid showing implementation-level types
such as:

```text
READ_FAILED  Read error: ServerOperationError
```

There is also a presentation conflict. Under the CLI's normal logging setup,
one expected transport failure currently renders as:

1. an adapter `WARNING` with the full traceback;
2. an engine `ERROR` repeating the type and message; and
3. the structured report repeating the failure again.

The traceback is the only place that incidentally reveals that the describe
query failed, while the actual failure value lacks that context.

#### Recommendation

Add explicit read context at the shared Databricks read boundary:

- a small stable step value such as `DESCRIBE_TABLE`, `SCHEMA_EXISTS`,
  `COLUMN_TAGS`, `TABLE_TAGS`, `PRIMARY_KEY`, `FOREIGN_KEYS`, or
  `REFERENCING_FOREIGN_KEYS`;
- the Databricks condition when available from `getCondition()` or the
  bracketed warehouse-message prefix;
- the backend exception type and complete raw message as diagnostics; and
- optionally the SQL query in detailed/debug output.

Use the step and condition in the headline:

```text
Read failed while reading column tags [INSUFFICIENT_PERMISSIONS]
```

The full section can then retain the backend message and type. Expected
failures should render once by default through the report. Adapter tracebacks
belong behind an explicit debug/verbose mode; engine phase logs should not
repeat failures that the report is about to print.

### 3. The machine-readable failure record is lossy — medium-high

`_failure_records()` reduces every failure subtype to `phase`, class-name
`type`, and one flattened rendered `message`.

That discards useful structured fields already present on the values:

- validation `rule_name`;
- read/execution `exception_type`;
- execution `statement_index` and `statement`;
- foreign-key `reason`, local columns, and referenced table.

It also silently applies the five-line display truncation to read and execution
messages. The full message survives on the in-memory failure object, but not in
the documented JSON intended for run-history persistence and structured
logging.

#### Recommendation

Add subtype-specific structured details to the failure record while retaining
the current `message` for display compatibility. At minimum:

```text
code                 stable rule/reason/backend condition
summary              concise cause-first text
raw_message          complete backend message, when applicable
details              subtype-specific plain JSON fields
```

Adding fields is already documented as backwards-compatible. Do not silently
truncate `raw_message`; if a display is shortened, mark it as truncated and
tell the caller where the complete diagnostic remains.

The public `code` should not rely solely on Python class names. Validation rule
names and `ForeignKeyFailureReason` are already suitable stable codes; read and
execution failures can prefer the backend condition and fall back to the
exception type.

### 4. Headlines lead with implementation vocabulary instead of the cause — medium

The grid intentionally omits detail messages and shows only `headline()`.
Consequently its two most common planning/read summaries are:

```text
Read error: ServerOperationError
Validation failed: UnmanagedAspectDrift
```

These identify implementation mechanisms, not the problem. The full failure
section is better, but users scan the grid first and automation often captures
only compact output.

#### Recommendation

Make headlines cause-first and retain technical codes secondarily:

```text
Read failed: insufficient permissions while reading table tags
Planning blocked: unmanaged column structure drift (2 differences)
Execution failed: statement 1 rejected [DELTA_UNSUPPORTED_DROP_COLUMN]
Foreign key blocked: referenced table cat.sch.customers failed
```

Keep rule and backend codes in structured output and, where useful, in
parentheses. They are valuable for searching and support, but should not be the
only explanation.

### 5. Two validation rules omit the exact affected subjects — medium

Most validation rules are already specific. Type changes include the column
and both types; partitioning includes current and requested layouts; property
rules include keys and values; rename conflicts and primary-key blockers name
their objects.

The notable exceptions are:

- `UnmanagedAspectDrift`, which reports only the aspect; and
- `ColumnMappingRequiredForDrop`, which reports that “dropping a column”
  requires column mapping but does not name the column or columns.

`NonNullableColumnAdd` names the column but gives no recovery path, unlike the
more complete nullability-tightening rule.

#### Recommendation

Require every validation diagnostic to answer:

1. What exact subject differs?
2. What are the live and declared/requested states where relevant?
3. Why is the operation blocked?
4. What is the safest next action?

For rules that deliberately aggregate multiple actions, list the affected
subjects rather than returning an anonymous aggregate.

### 6. Foreign-key structural failures lack the evidence used to decide them — medium

The resolver has the complete `ForeignKeyConstraint` and both registered table
definitions when it classifies a failure, but `_foreign_key_failure()` retains
only local columns, referenced table, and broad reason.

For `REFERENCED_COLUMN_TYPE_MISMATCH`, the user is told only that types differ.
For `REFERENCED_COLUMNS_NOT_A_KEY`, the requested referenced columns and the
registered primary key are absent. This forces the user to reconstruct the
resolver's comparison manually.

#### Recommendation

Retain the constraint name, local-to-referenced column mapping, and
reason-specific evidence:

- exact `(local column, local type) -> (referenced column, referenced type)`
  mismatches;
- requested referenced columns and the registered primary-key columns; and
- the upstream table/status for dependency blocking.

The existing broad reason remains useful as the stable code.

### 7. Execution diagnostics are strong but have two presentation defects — low

Execution failures are the best of the four types: they show progress, backend
message, and exact SQL. Two details remain:

- `statement_index` is zero-based internally and displayed directly, producing
  “statement 0” for the first statement; and
- a message longer than five lines is truncated with no ellipsis or pointer to
  the complete value.

Keep the stored index zero-based if that is the API contract, but display
`statement_index + 1` and, ideally, `1 of N`. Mark truncated diagnostics
explicitly.

## Suggested implementation sequence

1. Add black-box output tests for the two reported cases: a read-step failure
   and multi-entry unmanaged drift. Pin a single concise CLI presentation.
2. Carry `TableDiff` through `TableRunReport`; add rejected-diff rendering and
   an additive machine-readable drift projection.
3. Add read step and condition to `ReadError`/`ReadFailure`, then remove the
   default duplicate traceback/error logging.
4. Make failure records subtype-aware without removing the existing three
   fields.
5. Fill the remaining subject/remediation gaps in validation and foreign-key
   diagnostics; change execution display numbering to one-based.

## Acceptance criteria

- A user can identify the exact columns/properties/tags/comments behind
  `UnmanagedAspectDrift` without recreating the diff in Python.
- A planning failure never labels observed drift as “no changes”.
- A read failure identifies the table, failing read step, stable condition
  when available, backend message, and backend type.
- The CLI shows one expected failure presentation by default and no traceback
  unless debug output is requested.
- `to_dict()` retains complete backend diagnostics and subtype-specific fields.
- Every validation failure names its affected subject(s) and a safe next step.
- Foreign-key type/key failures show the exact comparison that failed.
- The first failed execution statement is displayed as statement 1, while any
  stored zero-based index remains stable.
