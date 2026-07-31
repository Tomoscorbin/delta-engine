---
tags:
  - todo
  - architecture
  - review
---

# Ousterhoutian complexity review

**Status:** Review complete; findings await agreement and implementation

**Review date:** 2026-07-31

**Reviewed revision:** `8338495`

**Rechecked against:** `20f3fd0` (the PR base)

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

The main remaining complexity is not broad disorder. It is concentrated around
two concepts the system uses without representing directly:

1. a declaration set that has been admitted under one complete policy; and
2. an execution schedule for the whole sync, rather than one ordered plan per
   table.

## Summary

| # | Priority | Finding | Main symptom |
| --- | --- | --- | --- |
| 1 | High | Declaration admission is not a total boundary | Alternate sources can bypass public declaration policy |
| 2 | High | Foreign-key references mix object and name identity | The API and resolver validate different parent objects |
| 3 | Strategic | Dependencies are action-level but scheduling is table-level | Cross-table ordering produces special cases and duplicate policy folds |
| 4 | Medium | One aspect set represents three reconciliation modes | Every consumer interprets an unmanaged aspect differently |
| 5 | Medium | The shared reader catches beyond the backend boundary | Programming defects become ordinary per-table read failures |
| 6 | Medium | Rename name frames are documented but not enforced | A valid `ActionPlan` can compile actions against stale column names |

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

## Smaller, contained improvements

### Replace recursive graph traversal

`_strongly_connected_components` uses recursive Tarjan traversal. Dependency
depth therefore inherits Python's recursion limit even though the public sync
API declares no depth limit. The existing TODO identifies this accurately.
An iterative traversal would keep the graph limit inside the relationship
module instead of leaking an interpreter implementation detail.

### Validate decimal parameter types

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

## Boundaries to retain

These separations hide real complexity and should not be collapsed merely to
reduce file or type count:

- SQL compilation and action-to-report interpretation are separate consumers
  with different responsibilities.
- Desired and observed table types prevent catalog facts from carrying
  declaration-only syntax.
- `_TableRun` is a useful private mutable phase scratchpad; phase-specific
  public types would add pass-through union handling without removing the need
  to retain terminal runs.
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
against the PR base before publication. This review changes documentation only.
