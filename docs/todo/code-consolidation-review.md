---
tags:
  - todo
  - architecture
---

# Code consolidation and containment review

**Status:** Fresh sweep complete; findings await agreement and implementation

**Review date:** 2026-07-21

**Reviewed revision:** `1dd37a4`

## Scope

This review follows the policy-visibility review, but asks a different
question: does each important fact have one obvious representation and owner,
or must several callers reconstruct it and keep parallel values synchronized?

The review follows declarations through normalization, domain construction,
diffing, validation, planning, dependency resolution, compilation, execution,
and reporting. It looks for:

- the same classification or identity rule computed in several places;
- context passed beside a value that should be part of the value;
- mutable or public records that permit contradictory states;
- compound behavior whose contents must be rediscovered by each consumer; and
- backend mechanics that have more than one entry point for the same physical
  operation.

This is not another Databricks correctness sweep. Platform behavior and known
correctness defects remain in
`business-logic-delta-databricks-correctness-review.md`. Some findings below
explain why one of those defects had no natural implementation home, but do not
reclassify it as a new defect.

## Overall assessment

The codebase already contains several appropriately deep boundaries:

- declaration input has an explicit normalize → validate → lower lifecycle;
- property ownership and transition semantics live behind `PropertyPolicy`;
- validation composition is visible in one module;
- dependency graph construction, cycle detection, ordering, and structural FK
  classification are hidden behind `resolve`;
- `ActionPlan` owns deterministic action ordering; and
- the shared Databricks reader and SQL compiler each have one public entry
  point.

The main remaining weakness is not module placement. It is parallel truth:
several important values are accompanied by another value that callers must
keep consistent, or are repeatedly projected into the same semantic shape by
different functions.

## Summary

| # | Priority | Finding | Main symptom |
| --- | --- | --- | --- |
| 1 | High | Column correspondence is rebuilt per aspect | Added, removed, and matched columns can be interpreted differently by structure, comment, and tag diffing |
| 2 | High | A table run stores the same outcome more than once | Public reports can represent a failed read or execution as `SUCCESS` |
| 3 | Medium | An action plan does not carry its execution target | A valid plan can be compiled against an unrelated table name |
| 4 | Medium | Table creation is a hidden compound transition | Scope, SQL, and reporting each rediscover a different subset of what `CreateTable` does |
| 5 | Medium | Primary-key identity is reconstructed by callers | API lowering, diffing, and dependency resolution each know that key order and physical name are not semantic identity |
| 6 | Medium | Backend SQL transport has multiple entry points | Spark session guards and warehouse cursor policy must be implemented in both readers and executors |
| 7 | Low | Tag actions discard transition context | Reporting cannot distinguish adding a tag from changing one |

## 1. Build column correspondence once

### Cause

After rename projection, three diff helpers independently align desired and
observed columns:

- `_diff_column_structure` builds both desired and observed name maps and
  determines added, removed, and matched columns;
- `_diff_column_comments` builds another observed name map and determines its
  own matched pairs; and
- `_diff_column_tags` builds another observed name map and iterates desired
  columns only.

The shared fact is column correspondence, but it has no value of its own. Each
aspect therefore chooses independently what a missing column means. The
governed-tagged column-drop defect in correctness-review item 10 is the concrete
failure mode: structure diffing knows a column is removed, while tag diffing
never sees the same removed-column classification and therefore emits no tag
cleanup.

Relevant code:

- `src/delta_engine/domain/plan/diff.py` (`_apply_renames`,
  `_diff_column_structure`, `_diff_column_comments`, and `_diff_column_tags`)

### Proposed consolidation

Build one immutable alignment immediately after rename projection:

```python
@dataclass(frozen=True, slots=True)
class _ColumnAlignment:
    added: tuple[DesiredColumn, ...]
    removed: tuple[ObservedColumn, ...]
    matched: tuple[tuple[DesiredColumn, ObservedColumn], ...]


alignment = _align_columns(desired.columns, rename_projection.columns)
```

Structure, comment, and tag diffing should consume this alignment rather than
raw column tuples. The alignment owns name-based identity and the three-way
classification; the aspect helpers own only the differences within their
aspect.

Keep this focused. A generic mapping-diff framework would obscure the column
specifics, especially rename projection and ordered column declarations.

### Implemented

Column correspondence is now built once after rename projection as an immutable
`_ColumnAlignment` of added, removed, and matched columns. One `_diff_columns`
boundary consumes that classification to produce structure, comment, and tag
actions without rebuilding name maps per aspect. Removed columns now retain
their observed tags long enough to plan the required tag cleanup before the
column drop, resolving correctness-review item 10.

## 2. Make phase outcomes the only source of run truth

### Cause

`_TableRun` is a mutable bag of optional phase fields plus a general
`failures` list. The same outcome is stored in both:

- a read failure is assigned to `run.read` and appended to `run.failures`; and
- an execution failure is retained in `run.execution.results` and copied into
  `run.failures`.

`TableRunReport` publishes the same parallel representation. Its `status` and
`has_failures` properties inspect only `failures`, while rendering also reads
`read` and `execution`. Nothing enforces agreement between them. Its table
identity is duplicated too: `qualified_name` is passed beside
`desired.qualified_name`.

`ExecutionSummary` is similarly more permissive than the engine contract. The
engine stops at the first failure, but the value accepts multiple failures,
success after failure, arbitrary indexes, and statements unrelated to the
compiled statements.

Read-only probes against the reviewed revision demonstrate the contradiction:

```python
TableRunReport(..., read=ReadFailure("IOError", "boom")).status
# TableRunStatus.SUCCESS

TableRunReport(
    ...,
    execution=ExecutionSummary((ExecutionFailure(...),)),
    failures=(),
).status
# TableRunStatus.SUCCESS
```

These states are not produced by `Engine`, but `TableRunReport` and its concrete
failure types are public. More importantly, the engine remains correct only
because every phase remembers the same copying convention.

Relevant code:

- `src/delta_engine/application/engine.py` (`_TableRun`, `_read`, and
  `_execute`)
- `src/delta_engine/application/ports.py` (`ExecutionSummary`)
- `src/delta_engine/application/report.py` (`TableRunReport`)

### Proposed consolidation

Retain phase outcomes once and derive the flat failure view when a report is
frozen. At minimum:

1. derive `qualified_name` from `desired`;
2. derive read and execution failures from their phase outcomes rather than
   accepting copies in an unrelated tuple;
3. retain planning and resolution failures in named phase outcomes, or funnel
   all phase recording through `_TableRun` methods that make duplication
   impossible; and
4. construct `ExecutionSummary` through the stop-on-first-failure executor, or
   validate its chronology and indexes on construction.

Do not introduce one public class per lifecycle phase merely to eliminate
`None`. The useful boundary is one canonical outcome per phase and one place
that derives report status, eligibility, and the flattened failure stream from
those outcomes.

### Implemented

`_TableRun` and `TableRunReport` now retain the read, planning, resolution, and
execution outcomes directly. The public plan, execution summary, failure list,
status, and table identity are derived projections, so they cannot disagree
with those outcomes. Completed reports reject impossible phase histories, and
`ExecutionSummary` enforces contiguous, stop-on-first-failure execution whose
statements must match the compiled statement prefix.

## 3. Put the table target on `ActionPlan`

### Cause

`plan_diff` has both the target table name and relation kind, but `ActionPlan`
retains only the kind. The name travels separately through `_TableRun`,
`PlanExecutor.compile`, and `compile_plan`.

That makes an accepted plan incomplete and permits contradictory inputs. In
particular, a `CreateTable` contains a `DesiredTable` with its own qualified
name, but the compiler ignores that name and emits `CREATE TABLE` against the
separate `qualified_name` argument. A plan containing `CreateTable(source)` can
therefore be compiled as `CREATE TABLE other (...)`.

Relevant code:

- `src/delta_engine/application/planning.py` (`plan_diff`)
- `src/delta_engine/domain/plan/actions.py` (`ActionPlan`)
- `src/delta_engine/application/ports.py` (`PlanExecutor.compile`)
- `src/delta_engine/adapters/databricks/sql/compile.py` (`compile_plan`)

### Proposed consolidation

Make the validated plan self-contained:

```python
@dataclass(frozen=True, slots=True)
class ActionPlan:
    target: QualifiedName
    actions: tuple[Action, ...]
    kind: TableKind = TableKind.TABLE
```

Then the port becomes `compile(plan)`, and the SQL compiler creates `_Target`
from the plan alone. `plan_diff` is the one constructor boundary because it has
the desired table name on both diff arms.

This pairs naturally with finding 2: a phase that was never planned should be
represented separately from a successfully planned no-op. A successful empty
plan still has a real target and kind.

### Implemented

`ActionPlan` now carries its required `QualifiedName` target and rejects a
`CreateTable` for a different name. `plan_diff` supplies that target on both
diff arms, and `PlanExecutor.compile` plus the Databricks SQL compiler accept
the plan alone. Reports use `plan=None` when reading or planning failed while a
successful no-op retains an empty, target-bearing plan.

## 4. Make the creation aggregate honest

### Cause

`CreateTable` carries a complete `DesiredTable`, but its declared
`aspect` is only `TABLE_EXISTENCE`. In practice the compiled statement also
establishes columns, column comments, the table comment, declared properties,
partitioning or clustering, and the primary key. Table and column tags plus
foreign keys are emitted as follow-up actions.

That division is currently reconstructed in three places:

- `TableMissing.actions` decides which state needs a follow-up action;
- the `CreateTable` compiler decides which state is embedded in the SQL; and
- `action_entries(CreateTable)` decides which embedded state appears in human
  and machine summaries.

Those views already disagree. A create carrying a table comment, a column
comment, properties, and partitioning currently produces only a column diff
entry; those declared changes are visible only in the SQL. The action's
single-aspect metadata also understates what the operation touches. Scope
validation remains safe only because a missing table is handled by a separate
whole-table gate.

Relevant code:

- `src/delta_engine/domain/plan/diff.py` (`TableMissing.actions`)
- `src/delta_engine/domain/plan/actions.py` (`CreateTable`)
- `src/delta_engine/adapters/databricks/sql/compile.py` (the `CreateTable` arm)
- `src/delta_engine/application/diff_entries.py` (the `CreateTable` arm)

### Proposed consolidation

Keep creation as a named aggregate—exploding it into ordinary alteration
actions and silently fusing them again in the SQL compiler would make the plan
less truthful. Instead:

1. let an action state all affected aspects, with ordinary actions exposing a
   singleton and `CreateTable` exposing the complete set it establishes;
2. move missing-table action construction behind one named creation-plan
   function, including the explicit post-create tag and foreign-key actions;
3. make the creation diff projection cover every declared fact embedded in the
   create, including comments, properties, and partitioning; and
4. add a coverage-style test that a populated creation declaration is fully
   represented in both the semantic diff and compiled SQL.

The domain action remains the owner of what a create means; SQL and rendering
remain separate consumers of that meaning.

### Implemented

Creation remains a single `CreateTable` aggregate. Its semantic diff projection
now reports every declared fact embedded in the create statement: columns and
nullability, primary key, partitioning or clustering, valued properties, column
comments, and the table comment. Tags and foreign keys remain visible through
their explicit follow-up actions. Coverage exercises a populated creation in
both semantic reporting and compiled SQL; property absence assertions are
correctly omitted because a new table already satisfies them.

## 5. Put key identity on the key values

### Cause

The code intentionally treats primary-key identity as its column set: physical
constraint name and column order do not determine whether the desired and
observed key are the same. `PrimaryKeyConstraint` does not expose that identity,
though. Three callers reconstruct it themselves:

- foreign-key API lowering compares a set of referenced columns with the
  parent's key columns;
- `_diff_primary_key` converts desired and observed columns to frozensets; and
- dependency resolution builds a set-valued primary-key index and compares
  foreign-key referenced columns against it.

The neighboring `ForeignKeyConstraint` already exposes `signature`, making the
asymmetry especially visible. Dataclass equality is not a safe substitute for
primary keys because it includes ordered columns and `constraint_name`.

Relevant code:

- `src/delta_engine/domain/model/constraints.py`
- `src/delta_engine/api/delta_table.py` (`ForeignKey._to_constraint`)
- `src/delta_engine/domain/plan/diff.py` (`_diff_primary_key`)
- `src/delta_engine/application/dependency_resolution.py`

### Proposed consolidation

Give `PrimaryKeyConstraint` a semantic `signature`, and give a foreign key a
named projection or predicate for its referenced key:

```python
type KeySignature = frozenset[str]

PrimaryKeyConstraint.signature -> KeySignature
ForeignKeyConstraint.referenced_key_signature -> KeySignature
```

Diffing and resolution should compare those values. API lowering can use the
same vocabulary even before it creates the final constraint. Ordered columns
remain available for deterministic SQL rendering; the signature states only
semantic identity.

### Implemented

Primary-key identity is now exposed as `PrimaryKeyConstraint.signature`, with
foreign keys exposing `referenced_key_signature`. API lowering, diffing, and
dependency resolution use these shared projections instead of reconstructing
set identity independently. Ordered columns remain available for SQL output.

## 6. Contain physical SQL invocation per backend

### Cause

The shared read and execution boundaries contain parsing and error translation,
but each physical backend still has two ways to invoke SQL:

- Spark reader and executor both call `spark.sql` directly; and
- warehouse reader and executor each implement cursor acquisition and silently
  suppressed cursor cleanup.

Any session-wide execution guard, cleanup logging, retry boundary, telemetry,
or transport quirk must therefore be changed twice per backend. The known Spark
`${...}` substitution issue in `todo.md` demonstrates the cost: protecting all
SQL requires finding both read and write calls, even though variable
substitution is a property of the one Spark session transport.

Relevant code:

- `src/delta_engine/adapters/databricks/spark/reader.py`
- `src/delta_engine/adapters/databricks/spark/executor.py`
- `src/delta_engine/adapters/databricks/warehouse/reader.py`
- `src/delta_engine/adapters/databricks/warehouse/executor.py`

### Proposed consolidation

Have each factory construct one backend-private SQL runner/session used by both
the reader and executor. It should own physical invocation and resource policy,
while the existing read and execution boundaries continue to own their distinct
application-error translations.

For Spark, the runner is the single place for session guards around every
`spark.sql` call. For the warehouse, use one cursor context implementation with
an explicit close-failure log; the reader may retain one cursor for its metadata
batch while execution uses one per statement, without duplicating cleanup
policy.

Do not merge `ReadError` and `ExecutionError`, or move their translation into a
generic transport. They signal different application operations. The shared
piece is only the physical SQL/session mechanism.

### Implemented

Each backend now has one private physical SQL runner shared by its reader and
executor. The Spark runner is the only caller of `spark.sql` and contains the
session guard for `${...}` variable substitution. The warehouse runner is the
only owner of cursor acquisition, execution, fetching, and cleanup: reads reuse
one lazily acquired cursor for their metadata batch, while each write statement
gets a fresh cursor scope. Cursor-close failures are logged at DEBUG without
replacing the read or execution outcome. The existing shared read and execution
boundaries continue to translate failures independently into `ReadError` and
`ExecutionError`.

## 7. Preserve tag transition context in the action

### Cause

`SetProperty`, comment, type, nullability, and clustering actions retain both
desired and observed state. `SetTableTag` and `SetColumnTag` retain only the new
value, even though the differ knows whether the tag was absent or carried a
different value.

`action_entries` must therefore label every tag set as `~` (change). A newly
added tag, including one added after table creation, is projected as a change
rather than an addition. This contradicts the action module's stated contract
that actions carry the semantic state reporting needs.

Relevant code:

- `src/delta_engine/domain/plan/diff.py` (`TableMissing.actions`,
  `_diff_table_tags`, and `_diff_column_tags`)
- `src/delta_engine/domain/plan/actions.py` (`SetTableTag` and
  `SetColumnTag`)
- `src/delta_engine/application/diff_entries.py`

### Proposed consolidation

Add `observed_value: str | None` to both set-tag actions. Use `None` for a new
tag and the previous value for a change, reject no-op payloads in the action,
and let the diff-entry projection select `+` or `~` from that retained fact.

This is deliberately not a generic `SetMappingEntry` action. Table tags,
column tags, and properties have different aspects, SQL, validation, and
ownership semantics; only their missing transition context needs fixing.

### Implemented

`SetTableTag` and `SetColumnTag` now carry required desired and observed
values, and reject transitions whose values are equal. Diffing records `None`
when a tag is absent and the previous string value when replacing one.
Diff-entry projection consequently reports new tags as additions and
replacements as changes with their previous value, while SQL compilation uses
only the desired value.

## Behavior that is already appropriately distributed

These should not be consolidated merely because a new feature touches more than
one module:

- **Action compilation and action reporting.** SQL and human/machine summaries
  are different consumers. Keep their dispatch arms separate and retain the
  exhaustiveness tests; fix incomplete action payloads rather than putting SQL
  or display strings on domain actions.
- **Desired and observed table types.** Their common fields are intentional,
  while declaration-only assertions and observed-only facts differ. The shared
  structural validator is preferable to an inheritance hierarchy or a nested
  common-state object with pass-through properties.
- **Individual validation-rule scans.** Each rule independently judging the
  immutable drift is readable. Build a change index only if more rules need the
  same non-trivial classification; do not replace the current loops with a
  generic rule engine.
- **Dependency resolution internals.** The module is complex because graph
  resolution is complex, but it already presents one total `resolve` boundary.
  The recursive traversal is a robustness defect tracked elsewhere, not a
  reason to distribute the graph algorithm.
- **API and domain validation.** Deployment policy belongs at declaration
  admission, while structural invariants belong on domain constructors. The
  explicit normalization/lowering boundary now makes that separation visible.
- **Identifier normalization in constructors.** Repeating a one-line lowercase
  normalization at the values that own identifiers is preferable to a generic
  string wrapper unless identifier behavior grows further. The important
  ordering rule—normalize before declaration validation—is already explicit.

## Recommended implementation order

1. Build the column alignment and use it for structure, comments, and tags.
   This gives correctness-review item 10 a natural implementation home.
2. Preserve observed tag values on tag actions; it is a small, local proof of
   the “actions carry their meaning” rule.
3. Add primary-key signatures and replace caller-created sets.
4. Redesign run outcome storage and put the target on successful action plans
   together; both remove parallel values from the phase chain.
5. Make creation's affected aspects and semantic projection complete.
6. Introduce backend-private SQL runners when addressing the Spark substitution
   and warehouse cleanup follow-ups, so the abstraction is justified by real
   contained behavior.

Each item should be its own reviewable change unless findings 2 and 3 prove
smaller together. None requires changing the public `DeltaTable` declaration
surface.

## Verification

The sweep was performed on `main` at `1dd37a4`. Read-only local probes confirmed
the contradictory report states, the out-of-band plan retargeting, the
incomplete create projection, and the lost tag-add context described above.
No production code was changed as part of this review.

After writing the review:

- `uv run pytest -q` passed: 977 passed, 63 credentialed/live tests deselected;
- `uv run ruff check .` and `uv run ruff format --check .` passed;
- `uv run mypy .` passed for 141 source files;
- `uv run lint-imports` kept all seven architecture contracts; and
- `uv run --group docs sphinx-build -W -b html docs docs/_build/html` passed.
