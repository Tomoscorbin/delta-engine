---
tags:
  - todo
  - architecture
  - review
---

# Ousterhoutian complexity review

**Status:** Review complete; resolved findings are marked below, the rest
await agreement and implementation

**Review date:** 2026-07-31

**Reviewed revision:** `8338495`

**Rechecked against:** `20f3fd0` (the PR base)

**Follow-up revision:** `db4f41ae` (current reporting branch)

**Rendering follow-up revision:** `a3ddbc4e` (`report-carries-the-diff` branch)

**Implementation recheck:** `50193acc` (`main`, 2026-08-05)

**Second review pass:** `50193acc` (`main`, 2026-08-05) — an independent
whole-codebase design pass, recorded in its own section at the end of this
document

**Finding 14 / run-report mirror resolved:** `86ba54ac` (`main`, 2026-08-05,
[PR #328](https://github.com/Tomoscorbin/delta-engine/pull/328))

## Scope

This review asks where the code makes callers understand or coordinate more
than they should. It follows declarations through normalization, lowering,
relationship resolution, catalog reads, diffing, validation, planning,
compilation, execution, and reporting. In particular, it looks for:

- policy reconstructed by several consumers;
- temporal rules that live in comments rather than interfaces;
- special cases caused by a representation that cannot express the real
  distinction;
- abstractions that pass complexity through instead of hiding it; and
- important decisions that are technically centralized but hard to discover.

This is a design review, not an implementation plan. Some recommendations
would change behavior and need their own design and compatibility work before
implementation.

## Overall assessment

The codebase already contains several appropriately deep boundaries:

- `DeltaTable` has an explicit normalize → validate → lower lifecycle;
- `PropertyPolicy` owns managed-property admission and transition policy;
- `ActionPhase` makes table-local operation ordering visible;
- the differ retains desired and observed state in self-contained differences;
- the shared Databricks reader hides the physical metadata query sequence; and
- reports derive their public status from canonical phase outcomes.

The first pass found the largest remaining complexity concentrated around two
concepts the system uses without representing directly:

1. a declaration set that has been admitted under one complete policy; and
2. an execution schedule for the whole sync, rather than one ordered plan per
   table.

A follow-up pass found a second, narrower cluster: boundaries named as frozen,
validated, complete, or versioned values whose construction does not establish
those claims. Those gaps make downstream correctness depend on caller
convention and couple machine-facing data to display implementation details.

A rendering-focused pass found the same ownership opportunity demonstrated by
the recent `DataType.__str__` work: several facts with one semantic answer are
still reconstructed by `report.py`, `diff_entries.py`, and `rendering.py`.
Moving those facts to the value or interpretation boundary that owns them
would simplify both text and JSON consumers. Alignment, headings, truncation,
and backend SQL spelling should remain at their presentation boundaries.

## Summary

| # | Priority | Finding | Main symptom |
| --- | --- | --- | --- |
| 1 | High | Declaration admission is not a total boundary | Alternate sources can bypass public declaration policy |
| 2 | High | Foreign-key references mix object and name identity | The API and resolver validate different parent objects |
| 3 | Strategic | Dependencies are action-level but scheduling is table-level | Cross-table ordering produces special cases and duplicate policy folds |
| 4 | Medium | One aspect set represents three reconciliation modes | Every consumer interprets an unmanaged aspect differently |
| 5 | Medium | The shared reader catches beyond the backend boundary | Programming defects become ordinary per-table read failures |
| 6 | Medium | Rename name frames are documented but not enforced | A valid `ActionPlan` can compile actions against stale column names |
| 7 ✅ | High | Compiled execution does not prove plan coverage | A changed table can report success without executing work |
| 8 ✅ | High | Frozen snapshots retain mutable caller aliases | Mutation after validation can invalidate trusted state |
| 9 ✅ | High | The machine report reuses lossy display rendering | Structured diagnostics are truncated and wire names follow Python symbols |
| 10 | High | The closed data-type vocabulary is operationally open | Invalid types fail late or bypass feature policy |
| 11 | Medium | Unresolvable differences are not classified exhaustively | A new blocker can be silently omitted from a successful plan |
| 12 | Medium | Other error translators also catch beyond their boundary | Adapter and import defects become expected operational failures |
| 13 | Medium | Declaration loading leaks process-wide import state | One invocation can affect backend imports and later invocations |
| 14 ✅ | High | Planning outcomes do not retain the diff they judged | Reports carry planning and diff as unrelated values that can disagree |
| 15 | High | Change interpretation is partial and repeated | Rejected creates and added-column comments disappear from reports |
| 16 | Medium | Consumers reconstruct one-answer semantic facts | Renderers and serializers know action polarity, plan kind, and wire identity |

## 1. Make declaration admission a total boundary

### Cause

`DeltaTable.__init__` owns a strong normalize → validate → lower sequence in
`src/delta_engine/api/delta_table.py`. That path applies property policy,
layout checks, name checks, tag checks, scope translation, and foreign-key
lowering before exposing a `DesiredTable`.

`Engine.sync`, however, accepts the broader `DesiredTableSource` protocol.
`lower_desired_tables` in `src/delta_engine/application/engine.py` calls
`to_desired_table()` and checks only duplicate qualified names. The returned
domain value enforces structural invariants, but it carries no proof that the
source-independent application policy was applied.

Read-only probes against the PR base demonstrate the gap:

- a custom source returning an arbitrary property named
  `arbitrary.engine.bypass` is accepted and plans `SetProperty`; and
- a missing declaration that manages only `TABLE_EXISTENCE` is accepted and
  plans `CreateTable`, although creation establishes the complete table.

Neither state is reachable through the normal `DeltaTable` API. Both are
reachable through the port described as the contract for a user-facing table
specification.

### Why this creates complexity

The system has two effective admission paths: one complete and one structural.
Every new source must know which private API rules to reproduce, and downstream
code must either trust that convention or add another defensive check. Policy
therefore spreads as soon as a second declaration producer appears.

### Recommended direction

First decide whether alternate declaration sources are a supported extension
point.

- If not, narrow the public engine input to `DeltaTable` and remove the false
  generality.
- If they are, introduce one deep declaration-set preparation boundary. It
  should apply every source-independent admission rule, reject unsupported
  aspect combinations and properties, validate creation completeness, resolve
  relationships, and return an opaque `PreparedDeclarationSet` (or equivalent)
  that the phase pipeline can trust.

Do not duplicate the public syntax checks from `DeltaTable`; iterable freezing
and argument-shape errors belong at that API. The preparation boundary should
own only the laws every producer must satisfy.

## 2. Give foreign-key references one identity

### Cause

The public `ForeignKey` accepts a `DeltaTable` object. During construction of
the child, `_to_constraint` validates the key mapping and column types against
that exact object. Lowering then discards object identity and the source of the
parent types, retaining only the parent's `QualifiedName` and referenced
column spelling in the domain constraint.

`resolve` later treats the qualified name as authoritative. It looks up the
table registered under that name and repeats key, spelling, and type checks
against that declaration. This is necessary because the registered object may
not be the object originally passed to `ForeignKey`.

The result is a hybrid rule:

- construction means “this particular object”; but
- synchronization means “whichever declaration has this name.”

`Self` adds another branch because the owner object does not yet exist while
its constraints are lowered.

### Why this creates complexity

The API and resolver both own part of the same validation policy, error timing
depends on which mismatch is present, and maintainers must reason about two
parents with the same identity. The duplication has already caused
documentation drift: `docs/how-to-configure-table.md` says the resolver does
not repeat the registered-parent type check, while the implementation does.

### Recommended direction

Choose one semantic and make the representation preserve it:

- **Object identity:** require the exact referenced object to be registered and
  retain an identity token until declaration-set preparation.
- **Name identity:** treat the object as convenient syntax for a symbolic
  reference and resolve it once against the prepared declaration set.

Name identity is the better fit for generated declarations and independent
modules. Under that design, declaration-set preparation selects one
authoritative parent and performs key, spelling, and type validation once.
The trade-off is that some FK errors move from child construction to set
preparation; that timing change should be intentional and documented.

## 3. Represent a sync-wide action schedule

### Cause

The domain expresses ordering at the action level through `ActionPhase`, but
the application schedules at the table level:

1. `resolve` returns tables in parent-first dependency order;
2. each `ActionPlan` sorts only that table's actions; and
3. `Engine._execute` completes one table plan before starting the next.

That mismatch leaks into policy:

- `PrimaryKeyReferencedByForeignKeys` contains a two-sync exception: dropping
  a child FK and changing its parent's PK cannot happen together because the
  parent plan executes first, even though `DROP_FOREIGN_KEY` precedes
  `DROP_PRIMARY_KEY` within a plan.
- Multi-table FK cycles fail during declaration resolution. The scheduler
  cannot distinguish an already-satisfied cycle from one that could be staged
  as “create all tables, then add constraints,” even though creation and
  `SetForeignKey` are already separate actions.
- Dependency failure blocks a whole table, including no-op or unrelated work,
  because the dependency unit is the table rather than the action that needs
  the parent.
- The same convergence fold is implemented in both `Engine._execute` and
  `SyncReport.assemble`: a table does not converge if it has its own failures
  or depends on another table that did not converge.

### Why this creates complexity

Table order is being asked to encode several kinds of prerequisite that do not
share one direction. Parent creation must precede child FK creation, while
child FK removal may need to precede parent key removal. No single topological
table order can express both, so special cases accumulate in validation and
reporting.

### Recommended direction

The long-term model should be a sync-level schedule whose nodes are accepted
actions and whose edges state real prerequisites. It should make rules such as
these explicit:

- create the referenced table and key before adding an inbound FK;
- remove inbound FKs before removing or changing their referenced key; and
- execute table-local name-frame and phase prerequisites in order.

This should not be implemented as a blind global sort by `ActionPhase`; the
schedule needs cross-table dependency edges as well as local phase order.

If that behavioral refactor is not yet justified, make the current table-level
policy deeper without changing it. Return a `ResolutionBatch` rather than a
semantically ordered tuple, let it own the convergence fold, and provide the
derived blocking result to both execution and reporting. That removes the
duplicated policy while preserving today's scheduling semantics.

## 4. Model reconciliation mode explicitly

### Cause

Named scopes are represented as `frozenset[TableAspect]`, but absence from the
set has several meanings:

- most unmanaged differences are still computed and must match, otherwise
  `UnmanagedAspectDrift` rejects the declaration;
- unmanaged properties are not compared at all;
- unmanaged foreign keys are compared locally but excluded from dependency
  construction and structural relationship checks; and
- table creation and streaming-table eligibility are interpreted by separate
  checks with additional rules.

The `DeltaTable` docstring has to call properties “the exception.” That is a
signal that set membership is not rich enough to state the policy consumers
need.

### Why this creates complexity

Every consumer asks a different question of the same set. A missing member can
mean “reject drift,” “ignore state,” “exclude graph edges,” or “cannot create.”
Adding a new scope or aspect therefore requires a coordinated audit of diffing,
validation, relationship resolution, relation-kind eligibility, and docs.

### Recommended direction

Introduce a `ReconciliationContract` with an explicit per-aspect mode, for
example:

- `MANAGE` — compare and plan changes;
- `REQUIRE_MATCH` — compare and reject drift; and
- `IGNORE` — do not compare for reconciliation.

The contract should also answer named questions about creation eligibility and
supported observed relation kinds. Named public scopes then become predefined
contracts rather than bare sets. This adds one richer value but removes policy
interpretation from several modules; callers ask the contract instead of
reconstructing its meaning.

## 5. Narrow the production read exception boundary

### Cause

`read_catalog_state` in `src/delta_engine/adapters/databricks/read.py` wraps the
complete query, parsing, provider admission, and domain-assembly path in
`except Exception`. A deliberately injected parser `AssertionError` is
therefore translated into a normal `ReadError` and retained as a per-table
failure.

That conflicts with the `CatalogStateReader` contract in
`src/delta_engine/application/ports.py`, which says expected adapter failures
cross as `ReadError` while unexpected adapter errors propagate.

### Why this creates complexity

Operational failures and defects have the same representation. The engine can
continue across table-specific backend errors, which is desirable, but tests,
logs, and callers cannot tell that pure parser or assembly code violated an
invariant. A defect may look like an ordinary inaccessible table and be
silently tolerated by a caller inspecting the report.

### Recommended direction

Keep broad catches only around the outbound client invocation, where the two
Databricks libraries and runtime versions genuinely expose unstable exception
families. Have the physical query runners translate those failures into a
typed internal query error. The shared reader should then translate only the
expected query, missing-relation, metadata-parse, and unsupported-relation
outcomes into `ReadError`.

Pure relation policy and domain assembly should run outside a broad catch so
assertions, indexing defects, and other programming errors remain visible.

## 6. Enforce the rename name frame

### Cause

The differ correctly projects observed columns through declared renames before
emitting later actions. The maintenance guide also explains the temporal rule:
actions before `RENAME_COLUMN` use observed names; actions after it use desired
names.

`ActionPlan.__post_init__`, however, validates only a `CreateTable` target and
sorts actions. It does not enforce the rename frame. A plan containing
`RenameColumn("old", "new")` followed by
`SetColumnComment(column_name="old", ...)` is accepted and compiles to:

```sql
ALTER TABLE ... RENAME COLUMN `old` TO `new`;
ALTER TABLE ... ALTER COLUMN `old` COMMENT '...';
```

The normal differ does not currently produce this sequence. The hole is in
the authoring invariant for future action types and alternate plan producers.

### Why this creates complexity

Correctness depends on a temporal convention spread between phase ordering,
rename projection, individual differ helpers, and a how-to guide. A maintainer
can construct a type-correct plan that becomes invalid only after the compiler
has faithfully lowered it.

### Recommended direction

Make plan construction own the name frame. Two reasonable shapes are:

- a private builder that receives rename projection once and exposes explicit
  pre-rename and post-rename action methods; or
- action metadata identifying which frame a column target uses, validated when
  the plan is constructed.

Prefer the smaller invariant-bearing mechanism over a general workflow
framework. The objective is simply to make a stale post-rename target
unrepresentable.

## 7. Make compiled execution prove plan coverage

### Cause

`PlanExecutor.compile` returns a flat `tuple[str, ...]`. The engine and report
then carry the accepted `ActionPlan`, compiled statements, and execution
results as parallel values. Nothing proves that the statements cover the plan
or that a failure-free execution completed all of them.

The built-in compiler currently emits one statement per action, but the port
does not retain that association. A read-only probe used a protocol-valid
executor whose `compile` returned `()` for a non-empty comment-change plan. The
real engine reported `has_changes=True` and `status=SUCCESS`, with no planned or
executed SQL.

The same representational gap appears in the phase outcomes:

- `PlanningFailed(failures=())` is constructible and derives a successful
  status despite having no accepted plan; and
- `TableRunReport` accepts a failure-free execution that is only a proper
  prefix of its planned statements.

### Why this creates complexity

The names "compiled," "failed," and "completed" promise more than their values
establish. Correctness depends on conventions shared by compiler adapters, the
engine loop, report assembly, and callers constructing public report values.
Each consumer has to understand which combinations the engine happens to
produce.

### Recommended direction

Introduce one value that owns compilation, such as `CompiledPlan`, retaining
the source plan and the statement group produced for each action. Require every
action to compile to at least one statement while allowing one-to-many lowering
where a backend needs it.

Construct execution outcomes against that compiled value. A terminal outcome
must then be either a complete success or a prefix ending in the recorded
failure. Also require `PlanningFailed` to contain at least one failure and move
`TableRunReport` construction behind an application-owned assembly boundary.

This should not merge SQL compilation with action-to-report interpretation.
Those remain separate consumers; the improvement is that compilation and
execution carry evidence for the claims their values already make.

## 8. Make frozen snapshots own every collection

### Cause

Several frozen dataclasses validate caller-provided collections without first
copying all of them:

- `DesiredTable` leaves `columns`, `foreign_keys`, and `managed_aspects`
  aliased;
- `ObservedTable` also leaves feature and inbound-reference collections
  aliased;
- `TableDrift` and `PartitioningChanged` retain their input sequences; and
- `ExecutionSummary` and public report values have the same pattern.

A read-only probe constructed a `DesiredTable` from a list of columns and a set
of managed aspects, then mutated both inputs. The frozen table changed from one
column and one managed aspect to two columns and no managed aspects. That
bypassed the non-empty-aspect and structural validation already completed.

This contradicts the architectural rule that frozen values copy their
collections so the object cannot change through an alias retained by its
caller.

### Why this creates complexity

Downstream phases are told they receive stable, validated snapshots, but their
correctness actually depends on an undocumented temporal obligation: no caller
may mutate an input collection later. Defensive copying or revalidation would
otherwise spread to every consumer.

### Recommended direction

Audit frozen aggregates systematically. Copy sequences to tuples, sets to
`frozenset`, and mappings to read-only copies before validating them. Store
exactly the values that were validated. The linear construction cost is the
appropriate price at these trust boundaries; valid public declarations and
adapter-produced values retain their current behavior.

### Resolved (2026-08-01)

[PR #320](https://github.com/Tomoscorbin/delta-engine/pull/320) and
[PR #321](https://github.com/Tomoscorbin/delta-engine/pull/321): frozen domain
and report values now copy their collection inputs at construction —
sequences to tuples behind the explicit `ListOrTuple` input type, aspect sets
to `frozenset` — so the stored collections are exactly the ones that were
validated and the alias-mutation probe no longer bypasses validation.

## 9. Give the machine report a lossless schema boundary

### Cause

Failure values retain complete backend messages and deliberately truncate them
only for display. `_failure_records`, however, builds the versioned machine
payload from `format_lines()`, so the structured report receives the same
truncated text. A probe with a nine-line `ReadFailure.message` confirmed that
the live failure retained line nine while `TableRunReport.to_dict()` discarded
everything after line five.

The payload also derives public wire identifiers from implementation symbols:

- change kinds use `entry.category.name.lower()`;
- phases use `failure.phase.name`; and
- failure types use `type(failure).__name__`.

Renaming a Python class or enum member can therefore change a documented public
format. The serializer returns `dict[str, Any]`, and the schema version is an
isolated integer literal, so type checking cannot help keep the versioned
contract coherent.

### Why this creates complexity

A human renderer and a machine interface have different information needs, but
the machine interface is implemented as another display consumer. Presentation
limits destroy diagnostic data, while internal refactors accidentally become
wire-format decisions.

### Recommended direction

Create one explicit versioned report-schema boundary:

- give changes, phases, and failure variants stable wire codes;
- serialize lossless fields such as raw backend message, exception type, rule
  code, FK reason and target, SQL, and statement index;
- have each failure own one headline plus lossless diagnostic facts, and derive
  its display lines from that headline rather than repeating the prefix;
- return physical supporting lines rather than one string containing embedded
  newlines; and
- keep truncation solely in text renderers and `SyncFailedError`; and
- describe the payload with typed schema definitions and a named version
  constant.

One application-level failure-block renderer can then own line splitting,
indentation, and display truncation for both `render_report` and
`SyncFailedError`. That is presentation policy worth sharing; the structured
serializer should continue to consume the underlying facts instead.

Existing version 2 fields can remain while additive structured fields are
introduced. Changing the current `message` semantics would justify version 3.

### Update (2026-08-05)

[PR #322](https://github.com/Tomoscorbin/delta-engine/pull/322) made failure
messages actionable but did not move the machine boundary: `_failure_records`
still serializes `" ".join(failure.format_lines())`, so the versioned payload
still receives head-truncated display text, and the wire identifiers still
derive from Python symbol names. The finding stands.

### Resolved at MVP scope (2026-08-05)

[PR #329](https://github.com/Tomoscorbin/delta-engine/pull/329) plugged the
data loss and guarded the renames, deliberately stopping short of the full
recommended direction.

Each failure record now carries its variant's lossless facts beside the
unchanged `phase` / `type` / `message` keys: `exception_type` and the full
untruncated `diagnostic` for read and execution failures, the 0-based
`statement_index` and `sql` for execution failures, `rule` / `subject` /
`details` (physical lines, not one embedded-newline string) for validation
failures, and `reason` / `columns` / `references` for foreign-key failures.
The projection lives in one `_failure_facts` match at the schema boundary in
`report.py`; `failures.py` and the renderers are untouched and display output
is byte-identical. The schema stays at version 2 — additive keys only — and
the bare literal became `_SCHEMA_VERSION`.

Rename fragility is guarded by freeze pins rather than wire codes: exact-set
tests make the current symbol-derived names the contract (phases, failure
type names, FK reason codes, entry kinds), and a `Failure.__subclasses__()`
coverage pin makes an unhandled variant fail two named tests instead of
silently emitting a lossy record. A rename now breaks a test naming the wire
contract instead of silently changing the documented format.

Deliberately deferred, not oversight: explicit wire codes owned by the
boundary (the pins give equivalent protection until a rename is actually
wanted), typed schema definitions, and the shared failure-block renderer
(`_message_head` stays inside `format_lines()`). The read-failure double-log
dedup this finding blocked is now unblocked, since the payload retains the
full traceback.

## 10. Close or explicitly extend the data-type vocabulary

### Cause

The Databricks type adapter describes `DataType` as a closed set, but the base
class is concrete and freely subclassable. `StructField`, `Array`, `Map`, and
columns do not establish that their children are supported concrete variants.
Consumers then apply inconsistent assumptions:

- `render_data_type` rejects some unsupported values only during compilation;
- feature derivation uses an exact runtime-type lookup; and
- nested traversal is implemented independently in API validation and domain
  feature policy.

Read-only probes demonstrate both failure modes:

- `Column("items", Array(Integer))`, a missing-parentheses mistake, passes
  declaration and planning before compilation raises `TypeError`; and
- a subclass of `TimestampNtz` renders as `TIMESTAMP_NTZ` but evades the exact
  type lookup, so planning omits its required `EnableTableFeature` action.

### Why this creates complexity

Construction does not establish the closed-world assumption that every
consumer relies on. A new or malformed value can fail far from its source, and
each consumer must independently decide whether subclassing is meaningful.
Recursive tree mechanics are also duplicated even though the naming and
feature policies using them should remain separate.

### Recommended direction

Choose one extension policy explicitly. The present design and documentation
point to a closed vocabulary: reject unknown concrete variants and malformed
children recursively at construction, make the base non-instantiable, and
express the supported variants as a closed type where practical.

Let the domain type module own one iterative tree traversal. API field-name
policy and application feature policy can consume that mechanism without
being merged. If custom types are intended instead, introduce an explicit
registration boundary that supplies rendering and feature requirements
together.

### Update (2026-08-05)

[PR #325](https://github.com/Tomoscorbin/delta-engine/pull/325) closed the
malformed-child probe: columns and the nested type constructors now require
`DataType` instances, so `Column("items", Array(Integer))` fails at
declaration. The base class deliberately remains constructible. The second
probe stands — a `TimestampNtz` subclass still renders while evading the
exact-type feature lookup — and the duplicated tree traversals remain, so
the extension-policy decision is still open.

## 11. Classify every unresolvable difference exhaustively

### Cause

`TableDrift` separates executable actions from `unresolvable` differences, but
eligibility and safety rules inspect those differences through independent
`isinstance` filters. `plan_diff` constructs an `ActionPlan` from
`drift.actions` whenever validation returns no failures and then discards the
unresolvable tuple.

All current variants are handled, but adding a new `Unresolvable` member and
forgetting a rule silently makes planning succeed without representing the
blocker. Neither the union nor type checking forces the policy update. The
follow-up revision already shows the maintenance burden: the union contains
four variants after adding `ColumnCaseDrift`, while architecture and
maintenance documentation still describe three.

### Why this creates complexity

The planning boundary is documented as total, yet its completeness depends on
maintainers finding every open filter that interprets a closed union. A missed
edit produces an apparently valid plan rather than a loud implementation
failure.

### Recommended direction

Add a mandatory exhaustive classification at the planning boundary. A
`match`/`assert_never` classifier can retain variant-specific policy, or a final
guard can reject any unresolvable difference not accounted for by validation.
Safety rules may continue to own their detailed user messages; successful
planning must prove that no blocker was silently dropped.

## 12. Narrow the remaining error-translation boundaries

### Cause

The read-side broad catch in finding 5 is not isolated.

`execute_statement` catches every `Exception` raised by a complete runner
method. The Spark and warehouse runners contain adapter-owned guard,
configuration, and cursor-lifecycle code as well as physical client calls. An
injected `AssertionError("adapter invariant")` therefore becomes an ordinary
`ExecutionError`, despite the `PlanExecutor` contract saying unexpected
programming errors propagate.

The CLI has the same classification problem for imports. Its console shim
turns every non-`delta_engine` `ImportError` raised while importing the app into
the `[cli]` installation hint, while backend loading maps every unrelated
`ImportError` to missing Databricks distributions. Probes using
`ImportError(name="surprise_dependency")` confirmed both translations.

### Why this creates complexity

Expected operational failures, incompatible installations, and implementation
defects collapse into the same user-facing outcomes. Reports and CLI handlers
can continue after a defect, while maintainers must inspect chained exceptions
to recover the distinction the boundary erased.

### Recommended direction

Wrap only physical client operations in a private typed transport failure and
translate only that type at the shared execution boundary. Runner invariants
and unexpected assertions should propagate. Configuration restoration and
cleanup failures need an explicit classification because they may leave the
backend context compromised.

For optional dependencies, classify absence of the directly declared package
roots. Narrow `ModuleNotFoundError` handling should preserve the friendly
installation hint while symbol errors, unrelated transitive imports, and
incompatible installed packages remain visible.

## 13. Scope declaration imports to one invocation

### Cause

`load_declarations` permanently moves the current working directory to the
front of `sys.path`. Repeated invocations from two working directories leave
both paths at the front, newest then stale. A long-lived process can therefore
resolve imports from a project it loaded previously.

The connection module explicitly knows this implementation detail so it can
diagnose a local `databricks.py` shadowing the installed SDK. The declaration
loader comment says the two policies must change together.

### Why this creates complexity

A local import convenience becomes process-wide mutable policy. Declaration
loading changes how an otherwise separate backend module resolves its own
dependencies, and one invocation affects later programmatic invocations. Tests
must snapshot global import state to contain the leak.

### Recommended direction

Use one invocation-scoped declaration import environment that restores the
exact previous `sys.path`. Resolve official backend dependencies outside that
scope where possible so project lookup precedence cannot affect them.

The scope boundary must account for declaration classes that perform delayed
project-local imports during lowering or sync. Scoping only initial module
execution is simpler but may break that behavior; scoping the whole invocation
retains in-invocation shadowing but prevents cross-invocation leakage. Choose
and document that contract explicitly.

## 14. Make a planning outcome own the diff it judged

### Cause

`PlanningSucceeded` is documented as an accepted diff but retains only its
`ActionPlan`; `PlanningFailed` is documented as a rejected diff but retains
only validation failures. `_TableRun` and `TableRunReport` consequently carry
`planning` and `diff` as independent, parallel fields.

The report rejects a diff after a failed read and checks an accepted plan's
target, but it does not establish that the planning result and diff came from
the same table or even the same planning call. A hand-constructed report can
therefore pair one table's rejected differences with another table's failures
and render a coherent-looking false account.

### Why this creates complexity

The facts are born together in `plan_diff` but separated immediately. Every
later consumer must coordinate them again: the engine transports both, the
report decides which one is authoritative, and rendering needs both to explain
a rejection. The association exists only in the phase ordering convention.

### Recommended direction

Put `diff: TableDiff` on both `PlanningSucceeded` and `PlanningFailed` and have
`plan_diff` construct the complete outcome. Derive `TableRunReport.diff` from
`planning` rather than accepting it as another constructor argument. The
outcome should validate that its plan and diff share a target, and the report
should validate that the outcome target matches its declaration.

This keeps the domain diff as the retained fact; it does not put report text or
`DiffEntry` values on the planning result. The improvement is the same one as
moving a canonical string to its data type: the value that makes the claim
also carries everything required to substantiate it.

### Resolved (2026-08-05)

[PR #328](https://github.com/Tomoscorbin/delta-engine/pull/328) implemented
this and went one step further: `plan_changes(desired, observed)` absorbed
`diff_table` into the planning boundary, so the association is constructed
where the facts are born and no caller can pair a diff with the wrong
outcome. `PlanningSucceeded` and `PlanningFailed` both retain `diff`;
`PlanningSucceeded` validates that its plan targets that diff; `TableRun`
(`TableRunReport` renamed in the same PR) validates that the planning
outcome targets the reported table and derives its `diff` from `planning`
rather than accepting it as a constructor argument.

## 15. Give every table diff one total change interpretation

### Cause

Text rendering and machine projection independently choose their source of
change entries. An accepted result uses `plan_entries`; a rejected result uses
`drift_entries`, but only when the retained diff is a `TableDrift`. A
`TableMissing` can legitimately be rejected by `MissingTableUnmanaged`; that
diff contains the proposed `CreateTable` action, yet the text view says “no
changes” and the JSON view emits an empty `rejected_changes` list.

Interpretation is also incomplete inside one action family.
`_column_add_entry` emits a column's name, type, and nullability. `CreateTable`
separately emits comments for its columns, while the `AddColumn` arm returns
only `_column_add_entry`. `AddColumn` retains the complete `DesiredColumn`, and
the SQL compiler applies its comment, so the operation executes a change that
neither report view shows.

Finally, the complete meaning of a `TableDrift` is repeatedly reconstructed as
`actions + unresolvable`: `drift_entries` does it for reports and
`UnmanagedAspectDrift` does it for validation.

### Why this creates complexity

Consumers are not merely laying out the same facts; they are independently
deciding which facts exist. Correctness now depends on remembering every
lifecycle variant and every one-to-many action expansion in multiple places.
The two omissions above are the resulting user-visible defects.

### Recommended direction

Make `diff_entries.py` the one total semantic interpreter:

- `table_diff_entries(TableDiff)` should exhaustively lower `TableMissing` to
  its creation entries and `TableDrift` to both action and unresolvable entries;
- `_column_add_entries(DesiredColumn)` should return the structural column
  entry plus its optional comment entry, and be reused by both `CreateTable`
  and `AddColumn`; and
- `TableDrift` may expose a derived `differences` stream so validation and
  reporting no longer reconstruct the concatenation, while retaining separate
  action and unresolvable storage for planning.

Then let `TableRunReport` expose the lifecycle-derived view — for example,
`planned_entries` and `rejected_entries`, or a small accepted/rejected change
disposition. `rendering.py` should decide only wording, grouping, alignment,
and headings; `to_dict()` should decide only schema shape. Neither should
reinterpret the run state or the diff.

Keep `DiffEntry` in the application layer. Moving it or its prose onto domain
actions would make the domain depend on one report vocabulary even though SQL,
human text, and machine output are legitimately separate projections.

## 16. Let semantic values answer their own one-answer questions

### Cause

Several smaller consumer-side branches have the same shape:

- `TableRunReport.creates_table` imports `CreateTable` and searches an
  `ActionPlan` even though the plan owns and validates its action aggregate;
- validation, report interpretation, and SQL compilation each decode
  `SetColumnNullability.desired_nullable` to decide whether the transition
  tightens or loosens nullability;
- validation rebuilds an unaligned diff line from `DiffEntry.symbol` and
  `DiffEntry.cells`, while the text renderer composes the same semantic line
  with alignment;
- `DiffCategory` has human names but its public wire name is manufactured by
  lowercasing its Python member name; and
- API layout errors use `type(data_type).__name__` rather than the canonical
  `DataType.__str__`, losing decimal parameters and nested structure.

The failure phase and concrete failure type have the same wire-identity issue,
covered by finding 9.

### Why this creates complexity

Each branch is small, but it makes the consumer know a representation detail
that should be private to the value. Boolean polarity is particularly easy to
invert, and deriving public identifiers from Python names turns ordinary
refactors into accidental schema changes.

### Recommended direction

Move only the canonical semantic query, not presentation policy:

- add `ActionPlan.creates_table` (or `is_creation`);
- add `SetColumnNullability.tightens` and derive the opposite case from it;
- add an explicit unaligned `DiffEntry.inline_text()` for diagnostic reuse;
- give `DiffCategory` an explicit stable wire value, using a `StrEnum` if
  declaration order remains its display order; and
- use `str(data_type)` anywhere a human-facing error names the actual type.

Prefer `inline_text()` to `DiffEntry.__str__`: omitting a category is a
deliberate context-specific representation, so calling it the universal string
would overstate its authority. Likewise, do not add `__str__` merely to make
`DesiredColumn` or constraint values satisfy one report layout; those values
have several valid SQL, generated-name, ordering, and human representations.
The useful test is whether the answer is determined solely by the value and is
canonical across consumers.

## Smaller, contained improvements

### Replace recursive graph traversal

`_strongly_connected_components` uses recursive Tarjan traversal. Dependency
depth therefore inherits Python's recursion limit even though the public sync
API declares no depth limit. The existing TODO identifies this accurately.
An iterative traversal would keep the graph limit inside the relationship
module instead of leaking an interpreter implementation detail.

### Validate decimal parameter types ✅

`Decimal.__post_init__` checks numeric ranges but not that `precision` and
`scale` are integers. `Decimal(10.5, 2.5)` is accepted and renders as invalid
`DECIMAL(10.5,2.5)`. Reject non-integers at construction (including deciding
explicitly whether `bool`, an `int` subclass, is acceptable) so the compiler
can trust the domain value.

### Collapse the backend executor pass-throughs

`SparkExecutor` and `WarehouseExecutor` both delegate `compile` to
`compile_plan` and `execute` to `execute_statement(self._runner.run, ...)`.
Their physical runners contain the real backend differences. One shared
Databricks executor parameterized by the runner would remove two shallow
classes without merging the runner policies that should remain separate.

### Put validation composition before implementation detail

`ELIGIBILITY_CHECKS` and `DEFAULT_SAFETY_RULES` are the important policy view,
but they appear after roughly 650 lines of rule implementations in
`application/validation.py`. Keep the rules together, but make the composition
discoverable near the module's front or expose it through one small
front-facing policy module. Do not split every rule into its own thin module.

### Normalize column-name collections consistently ✅

The public declaration boundary rejects a bare-string `primary_key` before it
can be consumed character by character. `partitioned_by` and `clustered_by`
are immediately iterated without the same guard: `partitioned_by="id"` becomes
`("i", "d")`, while a one-character column works accidentally. Use one
name-sequence normalizer for these three inputs that rejects bare strings,
validates entries, wraps `Identifier`, and freezes the result. Keep
`ForeignKey.columns` separate because its string form deliberately means one
column.

Resolved by [PR #321](https://github.com/Tomoscorbin/delta-engine/pull/321):
the three inputs now share one bare-string guard during normalization, so
`partitioned_by="id"` fails with a corrective message instead of becoming
`("i", "d")`. `ForeignKey.columns` keeps its deliberate single-column string
form.

### Make the CLI target own its validity

`Target.__post_init__` normalizes its host and warehouse ID but permits blank
values and IDs containing path fragments. Factory helpers currently carry some
of those checks, so direct construction can still produce an invalid connector
hostname or HTTP path. Put nonblank host, nonblank warehouse ID, and "ID, not
path" invariants on the CLI-private value; keep SDK- and environment-specific
error wording in the factories.

## Documentation drift found

These are documentation defects, not separate architectural findings:

- `docs/how-to-configure-table.md` says relationship resolution does not repeat
  the registered-parent type check; it does.
- `docs/reference-cli.md` says SQL compilation precedes cross-table dependency
  resolution; `Engine.sync` resolves first.
- `docs/how-to-add-action-type.md` omits `ENABLE_TABLE_FEATURE` from its phase
  example.
- The same guide constructs `DiffEntry` with the raw string `"~"`; the field is
  now `DiffOperation` and the example should use `DiffOperation.CHANGE`.
- At the follow-up revision, that guide and the architecture explanation say
  there are three unresolvable differences; the union contains four after
  `ColumnCaseDrift` was added.

All five were corrected on `main` by
[PR #327](https://github.com/Tomoscorbin/delta-engine/pull/327) (2026-08-05).

## Boundaries to retain

These separations hide real complexity and should not be collapsed merely to
reduce file or type count:

- SQL compilation and action-to-report interpretation are separate consumers
  with different responsibilities.
- Domain actions should not import `DiffEntry` or human report prose; the
  application interpreter is the natural home for that projection and its
  one-to-many expansions.
- Structured machine projection and human rendering also have different
  responsibilities; sharing semantic source values is healthy, sharing
  truncated display strings is not.
- `DataType.__str__` is a logical human spelling, not Databricks DDL. SQL type
  casing, quoting, and dialect rules should remain in the backend compiler.
- Desired and observed table types prevent catalog facts from carrying
  declaration-only syntax.
- Phase-specific public run types stay rejected: they would add pass-through
  union handling without removing the need to retain terminal runs. (This
  bullet originally also defended `_TableRun` as a useful private scratchpad;
  PR #328's table-major flip removed the scratchpad itself — each `TableRun`
  is born frozen and complete — while leaving the rejection of phase types
  intact.)
- Property policy, action phase ordering, and the shared read assembly are the
  right kinds of named policy homes. The recommendations above deepen those
  boundaries rather than moving their logic back into the engine.

## Verification

The focused core suite passed at the reviewed revision:

```text
uv run pytest tests/application tests/domain tests/api tests/adapters/databricks tests/cli --no-cov -q
1052 passed
```

The concrete admission, reader, decimal, and rename-plan probes were repeated
against the PR base before publication.

The follow-up pass used read-only probes for empty compilation, collection
alias mutation, report-message loss, malformed and subclassed data types,
execution exception translation, optional-import classification, and repeated
declaration imports. No production code changed; this review changes
documentation only.

## Second review: whole-codebase design pass (2026-08-05)

An independent design review of the complete source tree at `50193acc`
(`main`), read module by module — domain, application, api, adapters, cli,
and a sample of the tests — without reference to the findings above.
Overlaps discovered afterwards are cross-referenced. Where the findings
above largely track correctness obligations at boundaries, this pass asks
only the complexity question: where does the design make a maintainer
understand or coordinate more than the problem requires?

### Verdict

Mostly good, minor complexity. This is an unusually deep-module codebase:
interfaces are small, invariants live in constructors, platform knowledge
is buried at the edges, and comments record why. The remaining structural
complexity is concentrated in one seam — the engine's private run against
the public report — and the code itself already knows it: the open `TODO`s
in `application/engine.py` cluster at that seam.

### The run/report mirror

**Resolved (2026-08-05).**
[PR #328](https://github.com/Tomoscorbin/delta-engine/pull/328) adopted both
improvements below, in order: finding 14 first, then the table-major flip.
`_TableRun` and `to_report()` are gone; `Engine._plan_execution(resolution)`
builds one frozen `TableRun` per table (`TableRunReport` renamed, with
`table_reports` becoming `table_runs`), execution attaches its summary via
`replace`, and the engine's `TODO` cluster at this seam went with it. The
analysis below is retained as the reasoning of record.

`_TableRun` (`application/engine.py`) and `TableRunReport`
(`application/report.py`) are near-mirror types. They carry the same six
outcomes (`resolution`, `read`, `diff`, `planning`, `compiled`,
`execution`), the same derived properties (`desired`, `qualified_name`,
`plan`), and — the load-bearing part — two independent definitions of
failure: `_TableRun.has_failures` and `TableRunReport.failures`. The
engine's skip logic folds one; the report's status derivation folds the
other. They agree today because both enumerate the same failure sources by
hand; nothing structural keeps them agreeing. Every new phase outcome pays
a triple tax: a run field, a report field, and another coherence guard in
`TableRunReport.__post_init__`'s stack — guards that exist precisely
because the report cannot trust a scratch pad whose fields are all optional
and mutable.

The root cause is the phase-major loop shape: `_read`, `_diff`, `_plan`,
and `_compile` each iterate every run and re-narrow the previous phase's
optional field (`case ReadFailure() | None`, `if run.diff is None`,
`case PlanningFailed() | None`). That conflates two meanings of `None` —
"not yet" and "not applicable" — which is the confusion behind the engine's
own `TODO: why can this be None?`.

Improvements, in increasing size; the first stands alone:

- **Adopt finding 14.** This pass independently reached the same first
  step: planning outcomes should own the diff they judged. That deletes the
  `diff` field from both records, the "A failed read produces no diff"
  guard, and the `run.diff is None` narrowing.
- **Flip phase-major to table-major.** One `_prepare(resolution) →
  TableRunReport` per table builds the frozen report directly, each
  intermediate a plain non-optional local. Execution attaches its summary
  via `replace(report, execution=...)` — the pattern `SyncReport.assemble`
  already uses for `blocked_failures`. `_TableRun` and `to_report()`
  disappear; "None because not yet" leaves the types, and only "None
  because not applicable" remains. Semantics are unchanged — all reads
  still precede all execution, because execution is already a separate
  dependency-ordered walk. The observable cost is per-table rather than
  per-phase log grouping.

This is not the phase-typed proposal the boundaries-to-retain section
rejects: it introduces no phase-specific public types and no pass-through
union handling. It removes the private scratchpad rather than typing it.

Sketch:

```python
def sync(self, *tables, dry_run=False):
    reports = [self._prepare(res) for res in resolve(lower_desired_tables(*tables))]
    if not dry_run:
        reports = self._execute(reports)  # replace(report, execution=...) per attempted table
    return SyncReport.assemble(..., table_reports=reports, dry_run=dry_run)
```

### Minor findings

- `PropertyPolicy.permits_transition` silently permits unknown keys
  (`application/properties.py`, an acknowledged `TODO`). Unknown keys are
  unreachable by construction — the API validates declared keys and the
  reader projects observed keys through the policy — so the lenient
  `return True` can only mask a programming error. Raise instead, matching
  the fail-loud convention the compiler's `AddColumn` guard already sets.
- `validate_diff(rules=...)` is configurability nothing reaches
  (`application/validation.py`). No production path passes it; only tests
  do. Either rule customization is a feature — then it should reach the
  surface — or the parameter and the docstring space spent on its semantics
  are interface complexity no user can touch. The eligibility
  short-circuit that tests pin with `rules=()` can be pinned through public
  behaviour instead.
- Housekeeping `TODO`s in `Engine._compile` (log ownership, `__len__` on
  `CompiledPlan`): trivial, and the why-can-this-be-`None` one dissolves
  with the moves above. (Confirmed after PR #328: `_compile` and its `TODO`s
  are gone, and the structural-verdict log moved into
  `relationships.resolve`.)

### Examined and deliberately not flagged

- The convergence rule "folded twice" (`Engine._execute` and
  `SyncReport.assemble`) is acceptable as is: the kernel lives once in
  `TableResolution.blocked_by`, and the two folds answer different
  questions — what to skip now versus what to say afterwards. If
  consolidation is ever wanted, finding 3's `ResolutionBatch` is the right
  shape.
- The open action vocabulary (define → diff → validate → compile → render)
  is the expression problem, but both singledispatch registries carry
  exhaustiveness pins, so a missing arm fails tests rather than production.
- The `scope` docstring's weight reflects the problem, not the design:
  mirror-the-pipeline semantics are what make unmanaged drift detectable,
  and the eligibility checks carry that complexity below the API.

### What carries the design

Named so the review does not read as a list of debts:

- `RunQuery = Callable[[str], Rows]` is the entire read-side backend
  contract; `read_catalog_state` hides the describe, five
  information_schema queries, relation-kind admission, property projection,
  and the typed error boundary behind one function. Both backends collapse
  to ~20-line wirings.
- Invalid states are designed out consistently: actions refuse
  no-difference construction, `ActionPlan` sorts itself and validates its
  target, `ExecutionSummary` rejects impossible statement histories, and
  `CompiledPlan` requires exact action-to-statement pairing.
- `diff_entries.py` as the one meaning layer keeps the human and machine
  views of a change from drifting, and validation reuses it so rejection
  messages quote drift in the diff's own words.
- The eligibility/safety split means no rule does scope filtering of its
  own — the special cases are handled once, behind the boundary.
- `relationships.resolve` hides Tarjan entirely; `blocked_by` is the single
  rule kernel both execution and accounting fold.
- Tests are classical: recording fakes at the ports, real domain values,
  behaviour-named tests, mocks only at true boundaries.
- Comments record platform reasoning no code could express — the plain
  `CREATE` versus `DROP ... IF EXISTS` convergence asymmetry pair being the
  standout.

### Scope note

This pass read every module in `src/` and sampled the test suite; it ran no
probes and changed no production code. This section changes documentation
only.
