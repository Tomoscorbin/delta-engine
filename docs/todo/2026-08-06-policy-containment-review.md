---
tags:
  - todo
  - architecture
  - review
---

# Policy containment and information-leakage review

**Status:** Fresh sweep complete; findings await agreement and implementation

**Review date:** 2026-08-06

**Reviewed revision:** `84bcab17`

## Context

This review follows three changes that simplified the planning and compilation
boundaries:

- streaming-table policy moved out of individual action compilers;
- column-addition compilation began trusting the validated plan; and
- table scope became a closed, ordered domain model rather than a permission
  bitmap reconstructed by callers.

The common improvement was not merely removing conditionals. Each change gave
one decision an authoritative owner and let downstream code consume the result
without repeating the decision. In Ousterhout's terms, the old designs leaked
information: several modules needed to understand the same policy or infer the
same fact from a lower-level representation.

This sweep asks where that pattern remains. It follows declarations through
normalisation, domain lowering, relationship resolution, diffing, validation,
planning, compilation, execution, and reporting.

## Review heuristic

Distributed policy is a particularly dangerous form of information leakage,
but repeated code alone is not enough to establish a design problem. For each
candidate, the review asks:

1. Are several callers making the same semantic decision, or are they merely
   translating one decision for different consumers?
2. Has an earlier boundary already established a fact that later code could
   trust?
3. Is a raw representation leaking, forcing callers to know how to interpret
   it?
4. Did an early lowering step discard information that a later boundary must
   reconstruct or revalidate?
5. Can a deeper value or module make contradictory states unrepresentable
   without introducing a pass-through abstraction?

The aim is to pull complexity downward into the module that owns the decision,
not to gather unrelated behaviour into a generic policy object.

## Overall assessment

The codebase already has several strong containment boundaries:

- declaration input follows an explicit normalise → validate → lower lifecycle;
- `PropertyPolicy` owns managed-property admission and transition semantics;
- `resolve` hides dependency graph construction, cycle detection, ordering, and
  structural relationship verdicts;
- `ActionPlan` owns the target, relation kind, and deterministic action order;
- the compiler lowers an accepted plan without re-running validation policy;
  and
- action compilation and action reporting remain appropriately separate
  consumers of the domain action vocabulary.

The strongest remaining opportunities are where the representation reaching a
boundary is not yet authoritative. Foreign-key intent is lowered before the
complete declaration set is known, raw property strings still expose feature
semantics to callers, desired constraints carry observed-only physical
identity, and dependency convergence is folded independently by execution and
report assembly.

## Summary

| # | Priority | Finding | Main symptom |
| --- | --- | --- | --- |
| 1 | High | Foreign keys are resolved before the registered declaration set is known | The API validates one parent object and relationship resolution repeats the judgment against the registered parent |
| 2 | Medium | Raw property values leak property-dependent capabilities | API admission and safety rules independently interpret `"name"` and `"true"` |
| 3 | Medium | Desired and observed constraints share one physical-name requirement | The API fabricates names that semantic identity excludes, creating collision and validity policy |
| 4 | Low | Dependency convergence has two stateful folds | Execution and report assembly independently maintain and propagate the not-converged set |

## 1. Resolve foreign keys at the declaration-set boundary

### Cause

`ForeignKey._to_constraint` lowers each public foreign-key declaration while
its owning `DeltaTable` is constructed. It resolves the referenced
`DeltaTable`, checks that it declares a primary key, infers or validates the
column pairing, and compares local and referenced column types.

At that point, however, the engine does not know which declaration will be
registered under the referenced qualified name. A caller can construct a
foreign key against one `DeltaTable` object and pass a different object with
the same name to `Engine.sync`. Consequently, `relationships.resolve` must
rebuild primary-key signatures, exact key spellings, and column-type mappings
for the registered declarations and judge the constraint again.

The duplication is deliberate and currently necessary, but it exposes the
underlying design problem: per-table lowering runs before the authoritative
set-level context exists. `DeltaTable` also retains both its lowered
`DesiredTable` and the original `ForeignKey` declarations because lowering
cannot preserve the public reference object.

Relevant code:

- `src/delta_engine/api/delta_table.py` (`ForeignKey._to_constraint`,
  `_resolve_reference`, `_lower_declaration`, and `DeltaTable.__init__`)
- `src/delta_engine/application/engine.py` (`lower_desired_tables`)
- `src/delta_engine/application/relationships.py`
  (`_classify_structural_failures`)

### Recommendation

Introduce one declaration-set admission boundary that owns the complete
operation:

1. freeze and deduplicate the registered declarations;
2. bind every non-self foreign-key reference to the declaration actually
   registered under its qualified name;
3. resolve column pairs and validate referenced-key identity, exact spelling,
   and types once;
4. retain per-table structural failures rather than aborting the whole sync;
5. construct the resolved constraints and dependency edges; and
6. return the tables in dependency order with their structural verdicts.

The output should be an authoritative resolved declaration set, or the existing
`TableResolution` vocabulary deepened to serve that role. Downstream graph
ordering, diffing, and planning should consume resolved constraints rather than
re-checking how a public reference was interpreted.

This is the closest analogue to trusting a validated plan: relationship
resolution would trust a declaration-set boundary that has already bound and
validated every relationship.

### Trade-offs

This is a large design change. The current `DesiredTableSource` contract lowers
one table independently, and unregistered references are represented as
per-table failures rather than construction exceptions. A replacement must
preserve those behaviours and support self-references without making the
engine accept only the concrete public `DeltaTable` class.

Treat this as a design PR before implementation. A partial change that merely
moves the same checks between `api` and `relationships` would relocate the
complexity without removing it.

## 2. Hide property-dependent capabilities behind property policy

### Cause

`PropertyPolicy` is already the authoritative owner of managed keys, accepted
values, observed projection, and permitted transitions. Callers nevertheless
interpret the raw declared values themselves:

- rename admission checks `delta.columnMapping.mode == "name"`;
- column-name admission repeats the same check;
- change-data-feed naming checks `delta.enableChangeDataFeed == "true"`;
- type-change validation checks `delta.enableTypeWidening == "true"`; and
- column-drop validation interprets column mapping again.

The callers own the consequences of those features, but they should not each
know the raw property spelling that activates them. Adding another
property-dependent feature would repeat the pattern.

Relevant code:

- `src/delta_engine/application/properties.py` (`PropertyPolicy`)
- `src/delta_engine/api/delta_table.py` (`_validate_renames` and
  `_validate_column_names`)
- `src/delta_engine/application/validation.py`
  (`TypeWideningRequiredForTypeChange` and `ColumnMappingRequiredForDrop`)

### Recommendation

Deepen `PropertyPolicy` with a small immutable semantic view derived from a
property mapping. For example, `state_for(properties)` could expose questions
such as:

- `uses_name_column_mapping`;
- `change_data_feed_enabled`; and
- `type_widening_enabled`.

API admission and safety rules would ask those questions instead of indexing
keys and comparing raw strings. The property module would become the only code
that knows which declared value means a capability is active, while each
consumer would continue to own what that capability permits or forbids in its
context.

The view must remain derived. Do not store capabilities beside
`DesiredTable.properties`: that would recreate the parallel desired feature
state removed by the table-feature refactor and allow the two representations
to disagree.

### Trade-offs

Avoid a generic backend policy interface here. Delta Engine has one backend
policy today, and an injected abstraction with one implementation would be
shallow. The useful change is to deepen the existing property module around a
decision it already owns.

This is the best next implementation candidate: it is bounded,
behaviour-preserving, and removes policy knowledge from several callers.

## 3. Separate desired constraint intent from observed physical identity

### Cause

`PrimaryKeyConstraint` and `ForeignKeyConstraint` represent both desired and
observed constraints. Both require `constraint_name`, even though their
semantic signatures deliberately exclude the name so an existing catalog
constraint can match the same declared key under a different physical name.

Because the desired model requires an observed-style physical handle, API
lowering synthesises `{table}_pk` and `{table}_{columns}_fk`. That creates
additional policy:

- generated names must be legal platform identifiers;
- generated foreign-key names must not collide within a table;
- different valid table and column spellings can generate the same name; and
- declaration authors cannot adopt or override an existing physical name.

The compiler then uses a name that was fabricated to satisfy the shared value
shape rather than supplied as part of the declaration's semantic intent.

Relevant code:

- `src/delta_engine/domain/model/constraints.py`
- `src/delta_engine/domain/model/table.py` (`DesiredTable.__post_init__`)
- `src/delta_engine/api/delta_table.py` (`_foreign_key_constraint_name` and
  `_lower_declaration`)
- `src/delta_engine/domain/plan/actions.py` (`Set*` and `Drop*` constraint
  actions)
- `src/delta_engine/adapters/databricks/sql/compile.py`

### Recommendation

Represent the two lifecycle meanings explicitly:

- desired primary and foreign keys carry semantic content plus, if supported,
  an optional explicitly declared physical name;
- observed primary and foreign keys always carry the physical name required to
  drop them;
- `SetPrimaryKey` and `SetForeignKey` carry desired constraints; and
- `DropPrimaryKey` and `DropForeignKey` carry observed constraints.

If live Databricks verification confirms that unnamed primary- and foreign-key
creation is stable and observable, the compiler should omit names when none
were declared and let the platform allocate them. If explicit names remain
necessary, their validation and generation should still have one dedicated
owner rather than being incidental string concatenation in API lowering.

This makes it impossible to construct an observed constraint without the
handle needed to drop it, while no longer forcing desired intent to pretend it
already has observed identity.

### Trade-offs

The SQL behaviour must be verified live before removing generated names. This
finding also intersects adoption tooling and the existing request for explicit
constraint names, so its public API shape should be decided once rather than
through separate incremental parameters.

Coordinate this work with finding 1, but it can be implemented independently:
one concerns desired-versus-observed state, while the other concerns the
boundary at which cross-table intent becomes authoritative.

## 4. Give dependency convergence one owner

### Cause

`Engine._execute` and `SyncReport.assemble` independently fold the same rule
over dependency-ordered table runs:

1. maintain a set of table names that will not converge;
2. add a table when it has failures of its own;
3. ask `TableResolution.blocked_by` whether a sound table depends on that set;
   and
4. add a blocked table so failure propagates through longer dependency chains.

Execution needs the fold to decide whether SQL may run. Report assembly repeats
it so dry runs and real runs expose derived blocking consistently. The
edge-level decision lives on `TableResolution`, but the state transition over a
sequence has two owners.

Relevant code:

- `src/delta_engine/application/engine.py` (`Engine._execute`)
- `src/delta_engine/application/report.py` (`SyncReport.assemble`)
- `src/delta_engine/application/relationships.py`
  (`TableResolution.blocked_by`)

### Recommendation

Add a small stateful convergence tracker beside `TableResolution`. Given one
resolution and whether the table has failed independently, it should return the
blocking failures and update the not-converged set exactly once.

Both execution and report assembly can use that state machine:

- execution still owns whether and when to execute compiled SQL; and
- report assembly still owns attaching derived failures to immutable run
  records.

The tracker should hide the propagation rule, not the surrounding loop. A
generic graph-processing framework would be larger than the complexity being
contained.

### Trade-offs

The duplicated fold is short and stable, so the payoff is smaller than the
first three findings. Implement this only if the helper remains a genuine
state owner; a stateless pass-through function would add indirection without
removing policy.

## Behaviour that should remain distributed

The following are not consolidation targets:

- **Action compilation and action reporting.** SQL statements and semantic
  report entries are distinct translations for distinct consumers. Keep their
  dispatch functions separate and make action payloads complete enough for
  each.
- **Relation recognition, relation eligibility, and SQL dialect.** The reader's
  mapping from catalog relation types, validation's authority checks, and the
  compiler's Databricks spelling are different facts. Do not turn `TableKind`
  into a general policy container merely because all three mention relation
  kind.
- **Individual safety-rule scans.** Each rule independently judging an
  immutable drift remains direct and readable. Build an index only when several
  rules reconstruct the same non-trivial classification.
- **A generic backend policy interface.** The application layer contains
  Delta-specific semantics, but injecting a one-implementation policy object
  now would redistribute the same code behind pass-through methods. Revisit the
  boundary when another backend creates a concrete second policy.
- **Data-type rendering and domain type traversal.** The SQL adapter must know
  how external Databricks types are spelled. A shared domain traversal may
  become useful if another composite type or another recursive domain rule is
  added, but the present duplication does not yet justify a visitor framework.

## Recommended sequence

1. Implement the derived property semantic view. It is the smallest
   high-confidence policy-containment change.
2. Verify unnamed constraint creation and observation live, then split desired
   and observed constraint values together with the explicit-name decision.
3. Write a focused design for declaration-set relationship admission before
   changing the `DesiredTableSource` or `DeltaTable` lowering contracts.
4. Introduce a convergence tracker opportunistically when dependency execution
   or reporting next changes.

Each item should be its own reviewable PR. Findings 1 and 3 should share an
agreed direction, but combining their implementations would make the first
change unnecessarily difficult to review.

## Verification

The sweep was performed against the merged `origin/main` tree at `84bcab17`.
The primary worktree's unrelated local commit was not modified. This review
changed documentation only; no runtime behaviour was changed.
