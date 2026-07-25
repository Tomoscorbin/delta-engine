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
| `ValidationFailure` | Rule code; most rules name the subject and remediation | Comparison evidence; exact subjects for unmanaged drift and column-drop prerequisites |
| `ForeignKeyFailure` | Owning table, local columns, referenced table, broad reason | Constraint name, referenced columns, actual key, exact type mismatches, direct upstream cause |
| `ExecutionFailure` | Backend type/message, failed SQL, applied/planned progress | Stable backend condition, one-based human numbering, explicit truncation, structured fields |

## Vocabulary before implementation

The current model has the right data, but `result`, `outcome`, `summary`, and
`resolution` are too close to be useful as peer concepts. Use one generic term,
**outcome**, for the terminal state retained from a phase:

```text
desired/observed state
        |
        v
       diff
        |
        v
planning judgment + declaration context
        |
        +----------- accepted --> plan
        |
        +----------- rejected --> failures
        |
        v
dependency outcome
        |
        v
execution outcome
        |
        v
      report
```

The canonical semantic model is:

| Term | Meaning | Examples and constraints |
| --- | --- | --- |
| **state** | A snapshot asserted by one side of the comparison | `DesiredTable` is the declared target; `ObservedTable` is normalized catalog state |
| **difference** | One discrepancy between desired and observed state | An `Action` or `Unresolvable` inside a diff; evidence, not yet approved work |
| **diff** | The complete comparison artifact for one table | `TableDiff`; preferably `TableInSync`, `TableMissing`, or non-empty `TableDrift` |
| **drift** | Differences on an existing table | Reserve for the `TableDrift` arm. A missing table is a difference but not drift, so avoid a new generic `has_drift` report field |
| **action** | A candidate remedy associated with a resolvable difference | An action becomes approved executable work only when planning accepts the complete diff into an `ActionPlan` |
| **plan** | An accepted, ordered set of actions | `ActionPlan`; only successful planning produces one. An empty plan is a successful no-op |
| **outcome** | The terminal state retained or derived for a phase after orchestration | Succeeded, failed, blocked, skipped, or no-op; the one generic lifecycle term instead of treating result and summary as peer concepts |
| **failure** | A persistent, typed value explaining why a phase did not succeed | `ReadFailure`, `ValidationFailure`, `ForeignKeyFailure`, or `ExecutionFailure`; report data, not an exception |
| **report** | The immutable public aggregate of retained outcomes and projections | `TableRunReport` and `SyncReport`; the source presented to callers after orchestration |
| **status** | A coarse label derived from retained outcomes | `TableRunStatus`; useful for scanning and gates, but never the detailed cause |

Two supporting terms stay outside the persistent model:

- an **error** is an exception crossing a boundary: `ReadError` and
  `ExecutionError` are transient adapter signals converted into failures, and
  `SyncFailedError` carries the completed report;
- a **diagnostic** is a human- or machine-readable projection of differences
  and failures, such as a headline, backend condition, or raw message.

`Result` adds little as a semantic category because every function returns a
result. `Summary` should be reserved for an actually condensed projection;
`ExecutionSummary` retains the detailed statement history and is closer to an
execution attempt or record. `Resolution` remains useful only as
dependency-domain vocabulary, not as another generic category.

If internal names are later aligned with this vocabulary, the direction is:

| Current name | Clearer role-based name |
| --- | --- |
| `ReadResult` | `ReadOutcome` |
| `ValidationResult` | `ValidationVerdict` |
| `PlanningResult` | `PlanningOutcome` |
| `ExecutionResult` | `StatementOutcome` |
| `ExecutionSummary` | `ExecutionAttempt` |
| `ExecutionOutcome` | keep |
| `TableResolution` | keep, or `DependencyOutcome` |
| `ResolveResult` | `DependencyOutcomes`, or no named aggregate alias |

These internal renames are optional consistency work, not a prerequisite for
better diagnostics.

The phase vocabulary then becomes:

| Phase | Positive outcome | Negative outcome | Persistent evidence |
| --- | --- | --- | --- |
| Read | table present or confirmed absent | failed | `CatalogState` or `ReadFailure` |
| Diff | comparison completed | not run after a read failure | `TableDiff`; no success/failure value is needed |
| Plan | diff and declaration accepted, including an empty no-op plan | diff or declaration rejected | `PlanningSucceeded(ActionPlan)` or `PlanningFailed(ValidationFailure...)` |
| Compile | statements produced, possibly none | unexpected programming defect | Compiled SQL; no expected failure value |
| Resolve | dependencies resolved | failed or blocked | `TableResolution` and `ForeignKeyFailure` |
| Execute | attempted statements succeeded | failed or dependency-blocked | `ExecutionSummary`, `ExecutionFailure`, or `ExecutionBlockedByDependency` |

### Empty and no-op cases

“Produced no actions” is not sufficient to identify a no-op. A diff containing
only an unresolvable difference also has no actions, but planning must reject
it. Use these distinct states:

| State | Representation | Meaning |
| --- | --- | --- |
| Diff not run | `diff is None` after a read failure | No comparison exists |
| In sync | Currently `TableDrift(actions=(), unresolvable=())`; preferably a `TableInSync` diff arm | Comparison completed and found no differences |
| Actionless difference | Empty `actions`, non-empty `unresolvable` | Differences exist but no candidate remedy can close them; planning will reject |
| No-op plan | Successful planning with an empty `ActionPlan` | The diff was accepted and there is nothing to execute |
| In-sync declaration rejection | `TableInSync` plus `PlanningFailed`; no `ActionPlan` | There is no drift, but the declaration itself claims unsupported authority |
| Rejected planning | `PlanningFailed`; no `ActionPlan` | The comparison or declaration was not accepted; differences need not exist |
| Planned changes | Successful planning with a non-empty `ActionPlan` | Accepted work is ready to compile |

There is value in making the in-sync case explicit, but do not call it `NoOp`:
no-op is a planning outcome, while the diff's neutral fact is **in sync**.
Prefer:

```text
TableDiff = TableInSync | TableMissing | TableDrift
```

`TableInSync` should retain the desired and observed endpoints needed to build
the target-bearing empty plan. `TableDrift` should then require at least one
action or unresolvable difference, making an empty value invalid rather than
using emptiness as an implicit sentinel.

`TableInSync` must still pass through mandatory declaration and scope gates
before planning succeeds. `StreamingTableTagsOnly` deliberately judges the
declaration's claimed aspects rather than its actions, so an in-sync streaming
table declared with broader-than-tag scope remains a planning failure. Only an
accepted `TableInSync` produces the existing empty `ActionPlan`, which remains
the canonical no-op plan.

This adds one union variant and requires an extra branch at every exhaustive
match, but makes three states unambiguous: comparison not run (`None`),
comparison found no differences (`TableInSync`), and comparison found
differences (`TableMissing` or `TableDrift`). A derived `has_differences` is
then false only for `TableInSync`; a derived `is_noop` is true only after
planning accepts an empty plan.

These distinctions also constrain report naming:

- `has_changes` continues to mean that an accepted plan contains actions.
- A rejected planning outcome has no planned changes, but may or may not have
  differences.
- When differences exist, user-facing language should say **rejected
  differences**, not imply that candidate actions were planned or “would”
  execute.
- When an in-sync declaration is rejected, say **in sync; declaration
  rejected** rather than inventing rejected differences.
- If machine consumers need a predicate later, `has_differences` is the
  accurate concept across missing tables, drift, actions, and unresolvable
  differences. It should not be added until a concrete consumer needs it.

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

Retain the computed `TableDiff` as comparison evidence independently of
whether it became an accepted `ActionPlan`. Prefer making both
`PlanningSucceeded` and `PlanningFailed` retain the diff they judged, then
expose it through a read-only `TableRunReport.diff` property. The planning
variants are internal, while `TableRunReport` is public; this preserves the
public report constructor and makes a diff part of every post-read planning
outcome instead of adding a defaulted report field that permits a successful
read with no comparison.

Keep the existing meaning of `has_changes`—accepted, executable changes—so CI
gates do not silently change semantics. Do not add a generic `has_drift`
predicate: `TableMissing` and unresolvable differences make that name
inaccurate.

Render a failed `TableMissing` or `TableDrift` as rejected differences, for
example:

```text
dev.silver.orders
  REJECTED DIFFERENCES
  columns (unmanaged)
    + new_status     String
    - legacy_status

Validation blocked: declaration does not manage column structure
  Differences: `new_status` is absent; `legacy_status` is undeclared.
  Align the declaration and live table, or explicitly use `scope="full"`
  and review that plan before applying it.

  No DDL was planned.
```

Render a failed `TableInSync` separately:

```text
dev.silver.streaming_orders
  (in sync; declaration rejected — no DDL was planned)
```

The raw diff is the canonical source of exact comparison evidence. Its
projection should keep two classifications distinct: scope (managed or
unmanaged) and remedy (candidate action or unresolvable). An unresolvable
difference can still be inside or outside the declaration's scope. Failures
remain the source of the rejection reason and recovery. Human renderers,
including `SyncFailedError`, should compose those two sources rather than copy
the same list of subjects into failure prose.

Text rendering therefore needs a projection for `ColumnRenameConflict`,
`PropertyUndeclared`, and `PartitioningChanged` as well as the existing action
projection; merely retaining `TableDiff` is not enough. The additive machine
projection should follow in its own change after that entry vocabulary and
classification have settled.

This is preferable to only lengthening the existing
`UnmanagedAspectDrift.message`: exact drift is useful for every planning
failure, and the DIFF view is its natural home.

`TableRunReport` and the concrete failure dataclasses are public. Retaining the
diff on the internal planning outcome avoids changing the public report
constructor. If implementation constraints force a report field instead, the
documented fallback is an additive optional field whose `None` value explicitly
means “comparison unavailable on a manually constructed legacy report”; it
must not be presented as the ideal domain invariant. JSON compatibility alone
is not the whole public contract.

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
belong at debug logging level; engine phase logs should not repeat failures
that the report is about to print. This work should not require adding a new
CLI verbosity option: callers that configure debug logging can retain the
traceback, while the default CLI owns one concise presentation.

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
the current `phase`, `type`, and `message` for compatibility. Prefer exact
subtype fields over a generic code whose meaning changes by failure kind:

```text
summary              concise cause-first text
raw_message          complete backend message, when applicable
details              subtype-specific plain JSON fields
```

The details should carry `rule_name` for validation, `reason` for foreign keys,
and distinct `step` (read only), `condition`, and `exception_type` fields for
backend failures. Do not fall back from a backend condition to its Python
exception class under one supposedly stable `code`: an implementation class
such as `ServerOperationError` is neither stable nor a semantic condition.

Adding fields is already documented as backwards-compatible. Do not silently
truncate `raw_message`; if a display is shortened, mark it as truncated and
tell the caller where the complete diagnostic remains.

### 4. Headlines lead with implementation vocabulary instead of the cause — cross-cutting

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
only explanation. Treat this as a presentation rule applied while improving
each failure subtype, not as an independent implementation workstream.

### 5. Some validation diagnostics omit subjects or recovery — medium

Most validation rules are already specific. Type changes include the column
and both types; partitioning includes current and requested layouts; property
rules include keys and values; rename conflicts and primary-key blockers name
their objects.

`UnmanagedAspectDrift` currently reports only the aspect, but finding 1 should
solve its missing subjects through the canonical comparison evidence rather
than duplicating them in failure prose. The notable remaining rule-local
exception is `ColumnMappingRequiredForDrop`, which reports that “dropping a
column” requires column mapping but does not name the column or columns.

`NonNullableColumnAdd` names the column but gives no recovery path, unlike the
more complete nullability-tightening rule.

#### Recommendation

Use the following checklist when adding or changing a validation diagnostic:

1. What exact subject differs?
2. What are the live and declared/requested states where relevant?
3. Why is the operation blocked?
4. What is the safest next action?

For rules that deliberately aggregate multiple actions, list the affected
subjects rather than returning an anonymous aggregate. In this review's scope,
the rule-local gaps are `ColumnMappingRequiredForDrop` and the recovery text
for `NonNullableColumnAdd`; a fresh rewrite of every validation message is not
required.

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
- the direct upstream table for dependency blocking.

The existing broad reason remains useful as the stable code. Model
reason-specific evidence so invalid combinations are difficult to construct;
do not turn `ForeignKeyFailure` into a bag of unrelated optional fields. The
upstream table's current status and cause belong to the aggregate report and
should be composed at presentation time rather than copied into the
foreign-key failure.

### 7. Execution diagnostics lack a stable condition and some presentation polish — low

Execution failures are the best of the four types: they show progress, backend
message, and exact SQL. Three details remain:

- the shared Databricks condition is not retained;
- `statement_index` is zero-based internally and displayed directly, producing
  “statement 0” for the first statement; and
- a message longer than five lines is truncated with no ellipsis or pointer to
  the complete value.

Retain the condition separately from the exception type. Keep the stored index
zero-based if that is the API contract, but display `statement_index + 1`.
Mark truncated diagnostics explicitly. A `1 of N` display would require
total-plan context that the failure value does not own and is not part of this
small correction.

## Scope and delivery

The breadth is appropriate for a review, but implementing all findings as one
change would be too large. It crosses the report state model, public
dataclasses, rendering, adapter error normalization, CLI logging, the
machine-readable schema, validation policy, and foreign-key resolution.

Keep the findings together as the diagnostic backlog, but deliver them in the
following order. Each step should be an independently reviewable change with
its own black-box tests; do not start by renaming every `Result` or `Summary`.

### Step 1: Make comparison states explicit

**Status:** implemented and verified.

This is a domain-model change only:

- add `TableInSync(desired, observed)` and include it in `TableDiff`;
- make `diff_table()` return it when no action or unresolvable difference was
  found;
- reject construction of an empty `TableDrift`;
- update every exhaustive match in validation and planning;
- run mandatory declaration/scope gates for `TableInSync`, especially
  `StreamingTableTagsOnly`;
- produce an empty, target-bearing plan only when an in-sync comparison passes
  those gates; and
- update the architecture explanation and domain/planning tests.

Do not change reports, renderers, or JSON in this step. Its purpose is to make
comparison not run, in sync, and different impossible to confuse.

Exit criteria:

- an ordinary in-sync table produces `TableInSync` and an accepted empty plan;
- an in-sync streaming table with invalid claimed scope produces
  `TableInSync` and `PlanningFailed`;
- an empty `TableDrift` cannot be constructed; and
- missing-table and non-empty-drift behaviour is unchanged.

### Step 2: Retain comparison evidence and fix human rendering

Make planning outcomes retain the `TableDiff` they judged and expose it as a
read-only `TableRunReport.diff` property. Validate that a successful outcome's
plan and diff target the same table, and that a failed outcome contains at
least one failure. This keeps the public `TableRunReport` constructor unchanged
while making comparison evidence available after either planning outcome.

Extend the shared diff-entry interpretation to cover all `Unresolvable`
variants as well as actions. It should be the canonical source for exact
subjects, with separate scope (managed/unmanaged) and remedy
(candidate-action/unresolvable) classifications.
Renderers then combine those entries with the failure's reason and recovery:

- accepted `TableInSync`: in sync/no changes;
- rejected `TableInSync`: in sync, but declaration rejected;
- rejected `TableMissing` or `TableDrift`: rejected differences and no DDL
  planned;
- accepted non-empty diff: the existing planned-change presentation; and
- read failure: comparison not run.

Use one shared interpretation in `render_diff`, report headlines/counts, and
`SyncFailedError`, but let each presentation place the full comparison block
once. In particular, the CLI already prints `render_diff` before the report
and must not repeat the same rejected entries in its failure section. Do not
add the JSON projection yet.

Exit criteria:

- multi-entry unmanaged drift names every exact subject;
- scope and remedy classifications remain distinct, and no candidate remedy is
  mistaken for a planned action;
- every unresolvable variant has a stable text projection;
- an in-sync declaration rejection never invents rejected differences;
- observed differences are never labelled “no changes”; and
- `has_changes` and the public `TableRunReport` constructor remain compatible.

### Step 3: Add read context and give the report presentation ownership

Add an application-owned stable read-step value and an optional backend
condition to `ReadError` and `ReadFailure`. At the shared Databricks boundary,
identify the step around each query and expose the existing condition
inspection safely. Preserve the backend exception type and complete raw
message. Because condition extraction is shared by both Databricks boundaries,
also retain the optional condition on `ExecutionError` and `ExecutionFailure`;
leave its presentation cleanup for step 4.

Move adapter tracebacks to debug logging and remove the engine-level repetition
of a failure the completed report or `SyncFailedError` will present. Apply the
cause-first headline rule as part of this change rather than as a separate
headline rewrite.

Exit criteria:

- every supported read step has a failure test;
- the condition is retained when supplied by either Databricks exception
  shape for read and execution failures;
- exception inspection cannot raise a second diagnostic exception;
- the default CLI shows one concise read failure and no traceback; and
- debug logging still makes the traceback available to configured callers.

### Step 4: Make the small validation and execution corrections

Keep this deliberately narrow:

- name every affected column in `ColumnMappingRequiredForDrop`;
- add a safe recovery to `NonNullableColumnAdd`;
- display execution statement indexes as one-based without changing the stored
  zero-based value;
- include the stable backend condition in execution headlines when available;
- mark truncated backend messages explicitly; and
- make the touched headlines cause-first while retaining searchable technical
  identifiers secondarily.

Do not rewrite otherwise adequate validation messages.

Exit criteria:

- the two validation rules answer subject, reason, and recovery;
- the first failed statement is displayed as statement 1 everywhere;
- an execution condition is shown separately from its exception type; and
- no shortened message can be mistaken for the complete raw diagnostic.

### Step 5: Model reason-specific foreign-key evidence

Add a small typed evidence union for cycle, unresolved reference, failed
dependency, referenced-key mismatch, and type mismatch. The resolver should
construct the reason-appropriate evidence from the complete constraint and
registered table definitions it already owns.

`ForeignKeyFailure` is public, so preserve its existing constructor fields.
An additive optional evidence field is the compatibility concession for
manually constructed legacy values; engine-produced failures should always
carry evidence, and `__post_init__` should reject evidence whose variant does
not match `reason`.

Exit criteria:

- type mismatches identify both columns and both types;
- key mismatches identify requested referenced columns and the registered key;
- dependency blocking identifies the direct upstream table, while aggregate
  presentation supplies that table's status/cause;
- constraint names and local-to-referenced mappings survive; and
- incompatible reason/evidence pairs cannot be constructed.

### Step 6: Add the machine-readable projections after the models settle

Add a table-level `comparison` record distinct from the existing planned
`changes`. It must distinguish comparison not run, in sync, table missing, and
existing-table drift, and use the same difference-entry interpretation as the
text renderers.

Make failure records subtype-aware while retaining `phase`, `type`, and
`message`. Add a concise `summary`, an untruncated `raw_message` where
applicable, and subtype-specific plain-JSON `details`. Keep backend `condition`
and `exception_type` separate rather than inventing one unstable generic code.

This is additive under the documented schema policy, but update the run-report
reference and pin the complete shape with deterministic serialization tests.

Exit criteria:

- machine consumers can distinguish not-run, in-sync, and rejected
  comparisons without parsing prose;
- rejected differences are structured independently of planned changes;
- every failure subtype retains the fields needed to reconstruct its
  diagnostic;
- `raw_message` is complete; and
- all existing version-2 fields retain their names, types, and meanings.

### Step 7: Consider internal naming cleanup separately

After the behaviour and projections are stable, decide whether the optional
`ReadResult`/`PlanningResult`/`ExecutionSummary` renames materially improve the
code. If so, make them as one mechanical internal cleanup with no diagnostic
or schema changes. If the rename would create churn without reducing
ambiguity, leave the names and rely on the canonical vocabulary in this
document.
