# Constraint lifecycle redesign

## Status

Proposed implementation plan. This document replaces the implementation shape
of draft PR #350 with a sequence of smaller, independently coherent pull
requests. PR #350 remains useful as a behavioral prototype and source of tests,
but should not be merged as one change.

## Goal

Model primary- and foreign-key constraints according to their lifecycle while
keeping reconciliation simple:

```python
desired_key == observed_key
```

That expression means the two values describe the same relational constraint.
Requested creation names and physical catalog names are lifecycle metadata, not
part of relational identity.

The redesign should make these distinctions explicit without turning every
concept into a class:

- a **definition** is the relational meaning of a constraint;
- a **desired constraint** is a declaration plus an optional requested creation
  name;
- an **observed constraint** is a catalog occurrence with a required physical
  name;
- reconciliation pairs desired and observed values by definition;
- actions add desired constraints or drop observed occurrences;
- the SQL adapter alone handles Databricks naming grammar.

## Locked design decisions

The pull-request series should preserve these decisions unless new platform
evidence requires an explicit design revision.

### Vocabulary

Use the following internal terms consistently:

| Term | Meaning |
| --- | --- |
| Definition | Relational meaning of a constraint, excluding its physical name |
| Desired constraint | Constraint the user wants present |
| Requested name | Optional name to request if the constraint is created |
| Observed constraint | Physical constraint occurrence read from the catalog |
| Catalog name | Actual name assigned to an observed occurrence |
| Match | Desired and observed constraints have the same definition |
| Adopt | Accept a matching observed occurrence regardless of its catalog name |
| Add | Create a missing desired constraint |
| Drop | Remove an observed occurrence |
| Replace | A derived drop followed by an add, not a primitive action |
| Converged | Every desired definition is matched and no managed observation is unmatched |

Avoid an unqualified internal field named `name`. Use `requested_name` and
`catalog_name` so creation intent cannot be confused with a physical handle.

### Domain types

Use symmetric lifecycle names:

```text
DesiredPrimaryKey
ObservedPrimaryKey
DesiredForeignKey
ObservedForeignKey
ObservedReferencingForeignKey
```

`ObservedReferencingForeignKey` is the small catalog projection used to explain
why a primary-key drop is blocked. It is not another complete constraint type.

The table remains the durable entity. Constraint values are owned by
`DesiredTable` and `ObservedTable`:

```python
class DesiredTable:
    primary_key: DesiredPrimaryKey | None
    foreign_keys: tuple[DesiredForeignKey, ...]


class ObservedTable:
    primary_key: ObservedPrimaryKey | None
    foreign_keys: tuple[ObservedForeignKey, ...]
```

Private helpers or mixins may share normalization and equality behavior. They
are implementation mechanisms, not domain entities, and should not appear in
the public vocabulary.

### Equality and identity

Constraint equality means relational equivalence.

Primary-key equality is based on the case-insensitive set of key columns.
Column order and lifecycle names are excluded.

Foreign-key equality is based on:

- the referenced table; and
- the canonical set of local-to-referenced column pairs.

Pair declaration order and lifecycle names are excluded.

Equality must be symmetric, transitive, and hash-compatible across desired and
observed variants of the same constraint kind. Primary and foreign keys must
never compare equal to one another.

The implementation may expose a private or read-only `definition_key` to make
the contract explicit:

```text
PK definition key = frozenset(columns)
FK definition key = referenced table + canonical column pairs
```

Other identities remain distinct:

```text
creation signature   = definition + requested_name
occurrence signature = definition + catalog_name
```

Operational action equality should use the appropriate complete signature, so
two actions that compile differently do not compare equal even though their
constraint payloads are relationally equal.

### Naming policy

- An omitted requested name remains absent through desired state and SQL.
- Databricks generates the physical name.
- An explicit requested name is used only when an add is compiled.
- A matching observed definition satisfies the declaration under any catalog
  name.
- Changing only a requested name is a no-op.
- A structural change becomes a drop followed by an add; the add uses the
  current requested name.
- Generated names are opaque. The engine observes them but never predicts,
  persists, or reproduces them.
- `DeltaTable.primary_key_name` exposes only the explicit request, not a future
  or observed catalog name.

### Actions

Use Databricks' operation vocabulary:

```text
AddPrimaryKey
DropPrimaryKey
AddForeignKey
DropForeignKey
```

Do not use `SetPrimaryKey` or `SetForeignKey`: these operations compile to
`ADD`, not assignment or upsert.

Actions should carry lifecycle-correct values:

```python
class AddPrimaryKey:
    primary_key: DesiredPrimaryKey


class DropPrimaryKey:
    primary_key: ObservedPrimaryKey


class AddForeignKey:
    foreign_key: DesiredForeignKey


class DropForeignKey:
    foreign_key: ObservedForeignKey
```

The compiler uses the subset needed by each Databricks operation:

- a primary-key add uses the definition and optional requested name;
- a foreign-key add uses the definition and optional requested name;
- a primary-key drop uses name-independent `DROP PRIMARY KEY` syntax;
- a foreign-key drop uses the exact observed `catalog_name`.

Carrying the complete lifecycle value keeps reporting and validation useful
without copying name fields into the action vocabulary.

### Process ownership

Each rule should have one owner:

| Process | Responsibilities |
| --- | --- |
| Declaration normalization | Freeze public input, resolve conveniences, preserve requested names |
| Domain construction | Enforce timeless table and constraint invariants |
| Relationship resolution | Validate registered FK targets, types, dependencies, and cycles |
| Catalog observation | Read definitions and exact catalog names into observed values |
| Reconciliation | Pair by definition and produce add/drop actions |
| Plan validation | Judge whether a correct transition is safe to execute |
| SQL compilation | Render Databricks syntax, quoting, and optional names |
| Execution | Apply already accepted operations and surface platform failures |
| Reporting | Use requested names, catalog names, or definitions according to lifecycle |

The table differ should be the deep reconciliation module. Callers provide
desired and observed tables; they should not need to understand matching,
adoption, generated names, or replacement sequencing.

## Pull-request sequence

Build the series from `main`. Each PR must be independently coherent, include
its own focused tests, and pass the complete local validation suite. PR #350
should be used as a reference rather than preserved commit-for-commit.

### PR 1: Model constraint lifecycle and equivalence

Suggested title:

```text
refactor: model desired and observed constraints
```

#### Scope

- Introduce the final desired and observed constraint type names.
- Rename lifecycle fields to `requested_name` and `catalog_name`.
- Keep desired names required temporarily because the public API still
  generates its existing defaults in this PR.
- Store catalog names with exact string identity rather than as
  case-insensitive identifiers.
- Introduce structural equality and hashing across desired and observed
  variants.
- Update `DesiredTable` and `ObservedTable` field types and invariants.
- Make the catalog reader construct observed constraints exclusively.
- Rename the inbound-reference projection to `ObservedReferencingForeignKey`.
- Reconcile primary keys with direct equality.
- Reconcile foreign keys with deterministic, one-to-one structural matching.
- Keep SQL for newly created constraints named exactly as it is on `main`.

This PR deliberately changes one behavior: a name-only difference no longer
causes replacement. Omitted public names still use the existing engine-generated
creation requests until PR 3.

#### Primary files

- `src/delta_engine/domain/model/constraints.py`
- `src/delta_engine/domain/model/table.py`
- `src/delta_engine/domain/model/__init__.py`
- `src/delta_engine/adapters/databricks/sql/rows.py`
- `src/delta_engine/domain/plan/diff.py`
- focused model, reader, table, and differ tests

#### Acceptance criteria

- Desired and observed PKs with the same definition compare equal under
  different lifecycle names.
- Desired and observed FKs with the same definition compare equal under
  different lifecycle names.
- Different definitions compare unequal.
- PK and FK variants never compare equal.
- Equal constraints have equal hashes.
- Observed values reject absent or blank catalog names.
- Exact catalog spelling is retained.
- Name-only differences produce no actions.
- One-to-one FK matching is deterministic.

### PR 2: Make actions lifecycle-explicit

Suggested title:

```text
refactor: use add and drop constraint actions
```

#### Scope

- Rename `SetPrimaryKey` and `SetForeignKey` to `AddPrimaryKey` and
  `AddForeignKey`.
- Rename their execution phases from `SET_*` to `ADD_*`.
- Make add actions carry desired constraints.
- Make drop actions carry observed constraints.
- Compile PK drops without a name and FK drops with the exact catalog name.
- Give action equality operational semantics through creation and occurrence
  signatures.
- Update reporting to use requested names for additions and catalog names for
  removals.
- Update validation, ordering, compiler dispatch, exports, and the action
  extension guide.

SQL behavior should otherwise remain unchanged.

#### Primary files

- `src/delta_engine/domain/plan/actions.py`
- `src/delta_engine/domain/plan/diff.py`
- `src/delta_engine/application/diff_entries.py`
- `src/delta_engine/application/validation.py`
- `src/delta_engine/adapters/databricks/sql/compile.py`
- action, planning, rendering, validation, and compiler tests

#### Acceptance criteria

- Drops still phase before additions.
- Add actions differing by requested name are operationally unequal.
- Drop actions differing by catalog name are operationally unequal.
- Constraint payloads inside those actions retain structural equality.
- PK drop SQL is name-independent.
- FK drop SQL uses exact observed spelling.
- Reports identify each lifecycle value correctly.

### PR 3: Delegate omitted names to Databricks

Suggested title:

```text
feat: delegate constraint naming to Databricks
```

#### Scope

- Make desired `requested_name` optional.
- Remove primary- and foreign-key name generators.
- Preserve omitted names through public lowering.
- Render named or unnamed PK and FK additions from the same compiler helpers.
- Make `DeltaTable.primary_key_name` return only the explicit request.
- Report unnamed additions by their structural columns rather than `None`.
- Document requested names as creation preferences.
- Preserve compatibility by adopting existing engine-generated and manually
  named occurrences by definition.

#### Behavior matrix

| Desired state | Observed state | Result |
| --- | --- | --- |
| No key | No key | Nothing |
| Unnamed key | No matching key | Add without `CONSTRAINT name` |
| Named key | No matching key | Add with requested name |
| Any key request | Matching definition under any name | Nothing |
| No key | Existing key | Drop observed occurrence |
| Different definition | Existing key | Drop observed, then add desired |
| Requested name changes only | Matching definition | Nothing |

#### Primary files

- `src/delta_engine/api/delta_table.py`
- `src/delta_engine/domain/model/constraints.py`
- `src/delta_engine/adapters/databricks/sql/compile.py`
- `src/delta_engine/application/diff_entries.py`
- public API, compiler, reporting, and live constraint tests
- user-facing configuration, architecture, and limitations documentation

#### Acceptance criteria

- Omitted names produce unnamed Databricks SQL.
- Explicit requests retain their supplied spelling in SQL.
- Databricks assigns non-empty catalog names to unnamed PKs and FKs.
- A second sync is a no-op.
- Legacy explicitly named constraints are adopted.
- Changing only a requested name is a no-op.
- Catalog-generated name shape and stability are not asserted.
- Schema-wide explicit-name collisions remain clear platform execution errors.

### PR 4: Consolidate documentation and remove obsolete vocabulary

Suggested title:

```text
docs: document the constraint lifecycle model
```

#### Scope

- Add one authoritative constraint-lifecycle section to the architecture
  documentation.
- Remove descriptions of names as managed relational identity.
- Remove claims that the engine generates default names.
- Update historical TODO and roadmap references that describe current
  behavior incorrectly.
- Remove obsolete generators, compatibility scaffolding, or transitional
  aliases left by the preceding PRs.
- Ensure internal production fields use `requested_name` or `catalog_name`,
  never ambiguous `name`.
- Reassess the private normalization/equality mixins after the final types
  settle; retain them only if they reduce repetition without exposing another
  concept.
- Note internal import renames in release documentation if necessary.

`PrimaryKeyConstraint` and `ForeignKeyConstraint` are not part of the curated
`delta_engine.schema` API, so prefer a clean internal rename over permanent
aliases. The public `DeltaTable`, `ForeignKey`, `primary_key_name`, and
`ForeignKey(name=...)` surface remains compatible.

#### Acceptance criteria

- Architecture, how-to, limitations, roadmap, and code use the same vocabulary.
- No active documentation describes engine-generated default names.
- Documentation explains the difference between equality, creation signature,
  and catalog identity.
- Documentation builds with warnings treated as errors.

### PR 5: Validate planned creation-name collisions

Suggested title:

```text
feat: validate planned constraint name collisions
```

This is a follow-up hardening PR. It should not block the core lifecycle and
platform-naming redesign because it introduces a new cross-table planning
boundary.

#### Scope

- Remove unconditional duplicate requested-name validation from individual
  desired-table construction.
- After all tables are planned, collect `AddPrimaryKey` and `AddForeignKey`
  actions with explicit requested names.
- Group requests by catalog, schema, and case-folded name.
- Reject every group containing multiple planned additions before execution.
- Surface failures in dry runs.
- Prevent partial execution in real runs.
- Propagate blocking through the existing dependency mechanism.

Only planned additions should participate. Two declarations may request the
same name harmlessly when both are already satisfied and neither request will
be used.

The validator must not claim complete schema-wide knowledge. Information
schema visibility is permission-filtered and the engine does not read every
constraint in a schema. Collisions with unseen or externally managed catalog
objects remain Databricks execution failures.

#### Architectural work

This likely requires an explicit plan-set validation step between per-table
planning and compilation/execution:

```text
read and plan each table
    -> validate the set of accepted plans
    -> compile accepted plans
    -> execute
```

Keep that orchestration change isolated in this PR.

#### Acceptance criteria

- Case variants of one requested name collide.
- PK and FK requests share the same validation namespace.
- Requests collide across tables in one schema.
- The same spelling in different schemas does not collide.
- Unnamed additions never collide locally.
- Already-satisfied requests do not collide.
- Dry runs report collisions without execution.
- Real runs perform no affected addition before reporting the failure.

## Out of scope

Do not expand the redesign to include:

- `UNIQUE` constraints;
- `RELY` or `NORELY` management;
- `TIMESERIES` key components;
- `MATCH FULL`, `ON UPDATE`, or `ON DELETE` options;
- constraint comments;
- direct constraint rename support;
- cross-catalog foreign keys;
- prediction or persistence of Databricks-generated names; or
- complete discovery of the schema-wide constraint-name namespace.

Those features require separate behavioral decisions and observation support.

## Validation strategy

Every PR should validate narrowly first and then run the complete local suite:

```bash
uv run pytest <focused tests> -q
uv run mypy src
uv run ruff check .
uv run ruff format --check .
uv run lint-imports
uv run pytest -q
git diff --check
```

Documentation-changing PRs must also run:

```bash
uv run --group docs sphinx-build -W -b html docs docs/_build/html
```

The platform-naming PR should run the credentialed live constraint suite when
credentials are available. Without credentials, collect the live suite and
record that execution was not performed.

## Final acceptance checklist

The redesign is complete when:

- `desired_key == observed_key` directly expresses relational equivalence;
- equality is symmetric, transitive, hash-compatible, and name-independent;
- operational actions still distinguish creation requests and catalog handles;
- desired and observed constraint values cannot be confused by type;
- no internal field ambiguously represents both naming concepts;
- no engine-generated constraint-name policy remains;
- existing constraints are adopted by definition;
- name-only changes create no drift;
- actions use lifecycle-correct `Add` and `Drop` vocabulary;
- optional naming grammar is confined to the SQL compiler;
- only readers construct observed constraint occurrences;
- exact catalog names are available for physical operations and diagnostics;
- documentation explains requested names as creation preferences; and
- unit, static, documentation, and applicable live validation are green.
