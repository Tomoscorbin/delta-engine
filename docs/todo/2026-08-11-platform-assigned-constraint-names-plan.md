# Platform-assigned constraint names implementation plan

**Status:** Task 1 implemented and locally validated on 2026-08-11; Tasks 2–9
remain.

**Goal:** When a primary-key or foreign-key name is omitted, manage the
constraint definition but not its physical name. Let Databricks allocate the
name, adopt any matching observed occurrence, and continue to manage an
explicitly supplied name exactly.

**Architecture:** Preserve name omission through declaration lowering instead
of converting it into an engine-generated default. Desired constraints may
therefore carry `constraint_name=None`; catalog-observed constraints must
always carry a concrete name. The differ owns the one asymmetric rule: an
unnamed desired constraint is satisfied by any observed name when the
definitions match, while a named desired constraint requires both definition
and name. Set actions carry desired intent and compile the optional
`CONSTRAINT <name>` clause; drop actions carry concrete observed identity.

This plan resolves finding 13, “Stop synthesizing unsafe physical constraint
names,” in `docs/todo/business-logic-delta-databricks-correctness-review.md`.

## Why this change

The current public API accepts omitted names, but lowering immediately invents
physical names:

- primary key: `{table}_pk`;
- foreign key: `{table}_{local_columns}_fk`.

Those generated values are then treated exactly like explicit desired state.
Consequences:

1. A matching live constraint under another name is dropped and recreated even
   though the caller did not request a name.
2. The generator is not closed over valid declarations. Long table names can
   produce names over Unity Catalog's 255-character limit, and column
   characters valid under column mapping can be invalid in an object name.
3. Concatenation is ambiguous: distinct table/column combinations can produce
   the same string.
4. Constraint names occupy one schema-wide, case-insensitive namespace across
   tables and constraint kinds. A table-local generator cannot ensure that its
   output is available.
5. Databricks already has the deeper abstraction: omit the name and it
   allocates a valid schema-unique one.

The current apparent simplicity is therefore false economy. It erases a real
piece of user intent at the API boundary, then creates naming, validation,
migration, and collision work in lower layers.

## Verified Databricks contract

The implementation may rely on the following platform behaviour:

- The table constraint grammar makes `CONSTRAINT name` optional for primary,
  foreign, and unique keys created with `CREATE TABLE`. If omitted,
  Databricks generates a name that is unique within the schema.
  [CONSTRAINT clause](https://docs.databricks.com/aws/en/sql/language-manual/sql-ref-syntax-ddl-create-table-constraint)
- The same clause is optional for key constraints added with `ALTER TABLE ...
  ADD`; Databricks again generates a schema-unique name.
  [ADD CONSTRAINT clause](https://docs.databricks.com/aws/en/sql/language-manual/sql-ref-syntax-ddl-alter-table-add-constraint)
- `information_schema.table_constraints.constraint_name` is non-null. The
  relation's primary key is `(constraint_catalog, constraint_schema,
  constraint_name)`, confirming schema-wide physical identity.
  [TABLE_CONSTRAINTS](https://docs.databricks.com/aws/en/sql/language-manual/information-schema/table_constraints)
- A primary key can be dropped without naming it. A foreign key can be dropped
  by its ordered local columns or by its concrete observed name. Keep the
  existing name-based FK drop because the reader already provides that exact
  catalog spelling.
  [DROP CONSTRAINT clause](https://docs.databricks.com/aws/en/sql/language-manual/sql-ref-syntax-ddl-alter-table-drop-constraint)
- Databricks does not allow two foreign keys to share an identical set of
  local columns, including permutations. That makes an unnamed desired FK's
  structural match unique in valid catalog state.
- Unity Catalog object names are at most 255 characters, omit several special
  characters, and are stored lowercase.
  [Names](https://docs.databricks.com/aws/en/sql/language-manual/sql-ref-names)

Existing live coverage already proves that:

- raw `CREATE TABLE` accepts unnamed PKs and FKs and exposes non-empty generated
  names;
- the namespace spans tables and constraint kinds and is case-insensitive;
- catalog names are normalized lowercase, while named FK drops require the
  observed spelling;
- no direct constraint-rename clause exists.

The implementation must additionally pin the engine's unnamed `ALTER TABLE ...
ADD` path and its convergent second sync.

## Behavioural contract

`None` has one meaning throughout desired state: **the physical name is not a
managed property**. It does not mean “resolve a default later.”

| Desired name | Observed state | Result |
| --- | --- | --- |
| Omitted | No constraint | Add without `CONSTRAINT name` |
| Omitted | Same definition, any name | No change; retain the observed name |
| Omitted | Conflicting definition | Drop the conflict and add unnamed |
| Explicit | No constraint | Add with the explicit name |
| Explicit | Same definition and identifier-equivalent name | No change |
| Explicit | Same definition, different name | Drop and recreate with the explicit name |
| Explicit | Same name, different definition | Drop and recreate with the explicit definition |

Other consequences are intentional:

- Two environments can use different physical names for the same unnamed
  declaration. Their managed schema is still equivalent.
- A caller that needs a predictable physical name must supply it explicitly.
- `DeltaTable.primary_key_name` returns the explicitly authored name, or
  `None` when the key is unnamed or absent. `DeltaTable.primary_key`
  distinguishes an unnamed key from no key.
- `ForeignKey.name` already exposes authored intent and remains unchanged.
- Existing engine-generated names are adopted without churn when the
  declaration continues to omit the name.
- Dry runs report an unnamed addition by its definition. They must not invent
  or predict the future catalog name.

## Chosen domain design

Keep the existing two constraint value types. Make only their name optional:

```python
@dataclass(frozen=True, slots=True, eq=False)
class PrimaryKeyConstraint:
    columns: ListOrTuple[str]
    constraint_name: str | None = None


@dataclass(frozen=True, slots=True)
class ForeignKeyConstraint:
    local_columns: ListOrTuple[str]
    referenced_table: QualifiedName
    referenced_columns: ListOrTuple[str]
    constraint_name: str | None = None
```

This deliberately does not add definition/specification/occurrence wrappers,
generic name parameters, or a naming-policy hierarchy. Desired and observed
tables already provide the lifecycle context. The aggregate constructors
enforce the contextual invariant:

- `DesiredTable` permits omitted names.
- `ObservedTable` rejects a PK or owned FK without a concrete name.
- `ForeignKeyReference` remains concretely named because it is always observed.
- `DropForeignKey` rejects an unnamed constraint because a drop carries an
  observed physical occurrence, and exposes that checked name as a concrete
  property for validation, reporting, and compilation.
- `SetPrimaryKey` and `SetForeignKey` permit omitted names because SQL supports
  that complete instruction directly.

Constraint constructors continue to reject blank and non-string explicit
names. They normalize a supplied name to `Identifier`; `None` passes through
unchanged.

Python's type checker does not narrow a constraint field merely because an
`ObservedTable` validated it. Narrow a concrete observed name exactly once at
the observed-to-drop boundary. `DropPrimaryKey` already stores a concrete name;
`DropForeignKey` should provide a concrete-name property after its constructor
check. Downstream code uses those action fields/properties instead of repeating
casts, assertions, or `None` branches. This is the small cost of retaining one
constraint type without letting optionality leak through executable drops.

Keep ordinary value equality exact: definition plus optional name. Add one
meaningful operation to each constraint value:

```python
desired.is_satisfied_by(observed)
```

It compares the canonical definition and then requires the name only when the
desired name is explicit. This is intentionally asymmetric because desired
state now expresses a requirement while observed state expresses a physical
occurrence. Do not restore `resolved_name(owner)`: no lower layer resolves or
generates a missing name.

For `DesiredTable`'s duplicate-name invariant, compare only explicit FK names.
Multiple unnamed FKs are valid as long as their local-column sets remain
distinct. Keep the existing local-column-set invariant, which agrees with the
Databricks rule and guarantees unique structural matching for valid desired
state.

## Reconciliation design

### Primary key

There is at most one PK per table, so `_diff_primary_key` remains direct:

```text
desired absent, observed absent -> nothing
desired absent, observed present -> drop observed
desired present, observed absent -> set desired
desired.is_satisfied_by(observed) -> nothing
otherwise -> drop observed, set desired
```

The drop retains the concrete observed name for reporting even though its SQL
uses the name-independent `DROP PRIMARY KEY IF EXISTS` form.

### Foreign keys

Foreign keys need deterministic matching because a table may have several and
name ownership is per constraint. Use two simple passes over small per-table
collections rather than introducing a generic reconciliation framework.

1. Partition desired FKs into explicitly named and unnamed declarations,
   preserving declaration order.
2. Reconcile explicitly named declarations first:
   - find and remove the observed constraint with that name;
   - if none exists, set the desired constraint;
   - if it satisfies the desired declaration, retain it;
   - otherwise drop it and set the desired constraint.
3. Reconcile unnamed declarations against the remaining observations:
   - find and remove the first observation satisfying the desired definition;
   - if found, retain it regardless of its physical name;
   - otherwise set the unnamed desired constraint.
4. Drop every remaining observed constraint.
5. Return drops before sets, preserving stable order inside each collection.

Explicit-first matching is a correctness requirement. Consider:

- desired unnamed `FK(a -> parent.id)`;
- desired named `reserved` for `FK(b -> parent.id)`;
- observed name `reserved` currently describes `FK(a -> parent.id)`.

The unnamed declaration must not adopt `reserved`. The observed constraint is
dropped, the unnamed `a` constraint is added without a name, and the explicit
`b` constraint is added as `reserved`.

The scan is intentionally quadratic in the number of FKs on one table. Those
collections are small, and the direct algorithm is easier to verify than
parallel indexes, signature wrapper types, or a generic matcher. If evidence
later shows this is material, optimize behind the same behaviour.

Malformed synthetic observed state containing duplicate structural FKs remains
representable. One occurrence can satisfy an unnamed declaration and every
extra occurrence is dropped. No ambiguity failure is required: their names are
unmanaged and either surviving occurrence has the same desired meaning.

## Actions and deterministic ordering

`CreateTable` continues carrying the complete `DesiredTable`. A missing table's
PK is rendered inside `CREATE TABLE`; its FKs remain follow-up
`SetForeignKey` actions after the parent tables exist.

Set actions carry desired constraint intent, including an optional name. Drop
actions carry observed state and therefore always have a concrete physical
name.

`Action.subject` must remain a string:

- named `SetPrimaryKey`: use the name as today;
- unnamed `SetPrimaryKey`: use the canonical comma-joined key columns;
- `SetForeignKey`: continue using canonical local columns, independent of its
  optional name;
- drop actions: continue using observed physical names.

Only one PK set can exist in a table plan, so the unnamed PK fallback does not
create ordering ambiguity. FK local-column sets are unique by desired-table
invariant.

## SQL compilation

Hide the optional syntax in one compiler-local helper:

```python
def _constraint_prefix(name: str | None) -> str:
    return f"CONSTRAINT {backtick(name)} " if name is not None else ""
```

Use it in all three creation paths:

```sql
CREATE TABLE ... (... PRIMARY KEY (...))
ALTER TABLE ... ADD PRIMARY KEY (...)
ALTER TABLE ... ADD FOREIGN KEY (...) REFERENCES ...
```

Explicit names continue producing:

```sql
... CONSTRAINT `name` PRIMARY KEY (...)
... ADD CONSTRAINT `name` FOREIGN KEY (...) REFERENCES ...
```

Do not change drop SQL:

- PK: `DROP PRIMARY KEY IF EXISTS`;
- FK: `DROP CONSTRAINT IF EXISTS <observed-name>`.

Backticks remain mandatory for supplied names. No code may call `backtick(None)`
or stringify `None` into DDL.

## Reporting and machine projection

An unnamed addition has no physical name to report. Interpret it by its stable
table-local definition:

- unnamed PK: render `primary key (id, tenant_id)`;
- unnamed FK: render `foreign key (customer_id) -> catalog.schema.customers`;
- named additions: retain the existing `primary key <name>` / `foreign key
  <name>` subjects and column/reference detail;
- drops: retain the observed physical name.

Keep the implementation inside `application/diff_entries.py`, shared by text
rendering and `to_dict()`. Do not teach either consumer about optional names.
The machine report already emits an ordered list of change records, so several
unnamed FK records cannot overwrite one another. Each record must nevertheless
retain its local-column signature so it is independently intelligible.

The `schema_version` remains 2: the record keys and types do not change, and
change records are documented as human-oriented summaries rather than a
lossless action serialization. Update `reference-run-report.md` to include
the unnamed-key subject forms.

## Reader, relationship, validation, and execution boundaries

The reader does not change. It already obtains non-null physical names from
`information_schema`, preserves their catalog spelling, and groups FK rows by
that name.

Relationship resolution does not depend on desired constraint names. It uses
local/referenced columns and the referenced table's key columns, so it should
continue accepting the now-optional field without a new abstraction.

Inbound `ForeignKeyReference` values and the
`PrimaryKeyReferencedByForeignKeys` safety rule remain concrete-name paths.
They describe observed blockers and drops, not desired naming policy.
`application/validation.py` should consume the checked concrete name exposed by
`DropForeignKey`, not reach back through its optional constraint field.

Execution remains authoritative. An explicit name can still collide with a
constraint outside the engine's registered/observed set, and a read-to-write
race can invalidate any preflight. The resulting DDL failure must continue to
surface as an `ExecutionFailure`; it must never be swallowed or reported as
success.

## Constraint-name collision scope

Delegating omitted names removes the correctness problem for automatically
named constraints. Explicit names remain user-owned and Databricks requires
them to be schema-unique.

Do not add a schema-wide catalog scan in this change. It would:

- expand a local naming correction into new read scope and permissions;
- still be unable to rule out a concurrent collision;
- duplicate the authoritative DDL check;
- complicate handling of constraints owned outside registered tables.

Retain or add only cheap declaration invariants already available without I/O,
such as rejecting duplicate explicit FK names within one `DesiredTable`.
Reframe the open TODO from generated-or-explicit collision validation to the
narrow question of whether duplicate **explicit** names across registered
declarations deserve an earlier usability error. That is separate from this
correctness fix.

## Migration and compatibility

### Existing tables

No physical migration is required:

- old `{table}_pk` and `{table}_{columns}_fk` constraints structurally match
  unnamed declarations and remain untouched;
- manually created or legacy names also remain untouched when definitions
  match;
- explicitly named declarations retain today's exact reconciliation;
- changing a declaration from explicit to omitted relinquishes name ownership
  without recreating the constraint;
- changing from omitted to explicit begins managing the name and may require a
  drop/recreate.

### Fresh tables

Fresh environments receive Databricks-generated names for omitted declarations.
Tests and documentation must not depend on their spelling or stability. They
may assert only that observation returns a non-empty concrete name and that the
next sync is a no-op.

### Public compatibility

This is a deliberate behavioural change:

- `DeltaTable.primary_key_name` no longer returns a synthetic default;
- generated default strings disappear from planned SQL and reports;
- callers querying information schema by the old naming convention must supply
  explicit names instead;
- `to_desired_table()` exposes `constraint_name=None` for omitted names.

Use a conventional commit with a `BREAKING CHANGE:` footer so the next
Commitizen release records the accessor and DDL behaviour. Do not edit released
changelog history manually.

## Rejected alternatives

### Generate a bounded digest name

A prefix plus stable hash could satisfy length and character limits, but it
still claims ownership the caller did not request, duplicates Databricks name
allocation, requires schema-wide collision reasoning, and introduces algorithm
migration whenever its format changes.

### Separate desired-specification and observed-occurrence classes

`PrimaryKeySpec`/`ObservedPrimaryKey` and equivalent FK types would make the
name invariant static. They also introduce four constraint types and broad
conversion/import churn through tables, relationships, actions, readers, and
tests. The existing `DesiredTable`/`ObservedTable` aggregates can enforce the
same runtime boundary with much lower total complexity.

Revisit this only if optional-name mistakes recur after the aggregate and drop
action invariants are in place.

### A `PlatformNamed | ExactName` variant hierarchy

This spells the two states without `None` but adds small variants and match
arms without hiding additional complexity. There is no third naming strategy
to justify the hierarchy.

### Resolve a default during compilation

The first version of the named-PK work used `None` for adoption and then
`resolved_name(owner)` for creation. That overloaded omission with two
meanings and spread table-owner naming policy into compilation. This plan does
not revive that design: omission stays omission all the way into valid SQL.

### Always ignore names

Removing explicit name management would be simpler internally but would remove
a shipped adoption/control feature. Explicit names are useful when external
automation depends on stable physical identity.

## Non-goals

- No general schema-wide constraint inventory or collision preflight.
- No constraint rename action; Databricks has no such DDL.
- No support for `UNIQUE`, `RELY`, check constraints, or constraint comments.
- No change to key enforcement semantics; PK/FK constraints remain
  informational.
- No change to table scope, dependency ordering, safety rules, retries, or
  concurrency guarantees.
- No persistence of Databricks-generated names in declaration files.
- No attempt to infer or reproduce Databricks' generated-name format.

## Implementation tasks

Each task starts with focused behavioural tests, makes the narrow production
change, and runs that focused suite before moving on. Preserve unrelated user
changes and use conventional commits; never work on `main`.

### Task 1: Express naming intent in domain values

**Production files:**

- `src/delta_engine/domain/model/constraints.py`
- `src/delta_engine/domain/model/table.py`
- `src/delta_engine/domain/plan/actions.py`
- `src/delta_engine/application/validation.py`

**Test files:**

- `tests/domain/model/test_primary_key.py`
- `tests/domain/model/test_foreign_key.py`
- `tests/domain/model/test_table.py`
- `tests/domain/plan/test_actions.py`
- `tests/application/test_validation.py`

- [x] Allow `constraint_name=None` on PK/FK constraint construction while
      preserving all explicit-name validation and normalization.
- [x] Add `is_satisfied_by` behaviour for matching definitions with optional
      name ownership; preserve exact ordinary equality and hashing.
- [x] Make `ObservedTable` reject unnamed PKs and owned FKs.
- [x] Keep `DesiredTable`'s local-column-set invariant and ignore `None` when
      checking duplicate FK names.
- [x] Make `DropForeignKey` require a concrete observed name.
- [x] Expose the checked drop name so validation, reporting, and compilation do
      not repeat optional-name narrowing.
- [x] Give unnamed `SetPrimaryKey` a deterministic column-based subject.
- [x] Test mixed-case explicit identity, omitted-name satisfaction, explicit
      name drift, observed invariants, multiple unnamed FKs, and duplicate
      explicit FK names.

Focused validation:

```bash
uv run pytest \
  tests/domain/model/test_primary_key.py \
  tests/domain/model/test_foreign_key.py \
  tests/domain/model/test_table.py \
  tests/domain/plan/test_actions.py \
  tests/application/test_validation.py -q --no-cov
uv run mypy \
  src/delta_engine/domain/model/constraints.py \
  src/delta_engine/domain/model/table.py \
  src/delta_engine/domain/plan/actions.py \
  src/delta_engine/application/validation.py \
  tests/domain/model/test_primary_key.py \
  tests/domain/model/test_foreign_key.py \
  tests/domain/model/test_table.py \
  tests/domain/plan/test_actions.py \
  tests/application/test_validation.py
```

Task 1 checkpoint: the focused suite passes (256 tests), the full non-live
suite passes (1,239 tests, 78 deselected, 97.20% coverage), and the changed
boundary passes focused mypy and Ruff checks. Whole-repository mypy is
deliberately deferred until the new optional type reaches Tasks 3 and 4: after
Task 1 it identifies only the two old named-PK assumptions in `diff.py` and the
three old named-create/set assumptions in `compile.py`. Do not hide those with
casts; the later tasks replace them with the intended reconciliation and
optional-DDL behaviour.

### Task 2: Preserve omission at the public lowering boundary

**Production file:**

- `src/delta_engine/api/delta_table.py`

**Test file:**

- `tests/api/test_delta_table.py`

- [ ] Delete `_foreign_key_constraint_name` and both default-generation paths.
- [ ] Lower omitted `primary_key_name` and `ForeignKey.name` as `None`.
- [ ] Preserve explicit spelling and existing column/reference canonicalization.
- [ ] Change `DeltaTable.primary_key_name` to return authored explicit intent.
- [ ] Replace generated-name collision tests with behaviour proving distinct
      unnamed FKs are accepted.
- [ ] Retain tests that explicit duplicate names and invalid explicit values are
      rejected.
- [ ] Add long table-name, special-column-name, and formerly ambiguous
      concatenation declarations to prove lowering no longer invents an invalid
      object name.

Focused validation:

```bash
uv run pytest tests/api/test_delta_table.py -q
```

### Task 3: Reconcile names and definitions correctly

**Production file:**

- `src/delta_engine/domain/plan/diff.py`

**Test file:**

- `tests/domain/plan/test_diff.py`

- [ ] Implement the direct PK state table above.
- [ ] Implement explicit-first FK reconciliation without a generic matching
      framework or new wrapper values.
- [ ] Preserve drop-before-set output and deterministic ordering.
- [ ] Cover unnamed adoption of arbitrary old/default/platform names.
- [ ] Cover absent unnamed constraints, definition drift, explicit name drift,
      case-insensitive explicit identity, extra observed constraints, and
      multiple simultaneous FKs.
- [ ] Add the reserved-name precedence regression described above.
- [ ] Cover malformed duplicate observed definitions: retain one satisfying
      occurrence and drop extras.
- [ ] Confirm column rename/case-drift behaviour is unchanged; matching still
      uses raw constraint columns because Databricks drops keys during relevant
      column mutations.

Focused validation:

```bash
uv run pytest tests/domain/plan/test_diff.py -q
```

### Task 4: Compile named and unnamed creation paths

**Production file:**

- `src/delta_engine/adapters/databricks/sql/compile.py`

**Test file:**

- `tests/adapters/databricks/sql/test_compile.py`

- [ ] Add one `_constraint_prefix` helper.
- [ ] Render named and unnamed inline PKs in `CREATE TABLE`.
- [ ] Render named and unnamed `SetPrimaryKey` actions.
- [ ] Render named and unnamed `SetForeignKey` actions, including composites.
- [ ] Keep drop statements unchanged and based on concrete observed identity.
- [ ] Assert exact SQL for all six named/unnamed create/add combinations.
- [ ] Retain backtick and declared-column-spelling regressions.
- [ ] Ensure no emitted statement contains a synthetic default or `None`.

Focused validation:

```bash
uv run pytest tests/adapters/databricks/sql/test_compile.py -q
```

### Task 5: Report unnamed constraints honestly

**Production file:**

- `src/delta_engine/application/diff_entries.py`

**Test files:**

- `tests/application/test_rendering.py`
- `tests/application/test_report.py`

- [ ] Interpret named additions exactly as today.
- [ ] Interpret unnamed additions using their columns/reference, never the text
      `None` or an engine-generated guess.
- [ ] Keep drops named by the observed physical constraint.
- [ ] Cover both `CreateTable` PK expansion and standalone set actions.
- [ ] Pin text rendering and `to_dict()` records for one unnamed PK and multiple
      unnamed FKs.
- [ ] Confirm change counts and report schema version remain unchanged.

Focused validation:

```bash
uv run pytest \
  tests/application/test_rendering.py \
  tests/application/test_report.py -q
```

### Task 6: Prove end-to-end engine behaviour

**Likely test files:**

- `tests/application/test_engine.py`
- any focused planning/validation tests whose fixtures currently assume
  generated names

- [ ] Update fixtures to distinguish desired optional names from observed
      concrete names.
- [ ] Prove a missing table compiles an unnamed inline PK and follow-up unnamed
      FKs in dependency order.
- [ ] Prove an existing matching constraint under a legacy name produces no
      action or SQL.
- [ ] Prove explicit-name drift still produces drop/recreate SQL.
- [ ] Prove foreign-key failure and PK-drop safety paths continue using concrete
      observed names.
- [ ] Change only expectations belonging to the new contract; do not loosen
      unrelated ordering, casing, or failure assertions.

Focused validation:

```bash
uv run pytest \
  tests/application/test_engine.py \
  tests/application/test_planning.py \
  tests/application/test_validation.py -q
```

### Task 7: Pin the live platform and migration path

**Test file:**

- `tests/live/test_sql_warehouse_live_constraints.py`

- [ ] Convert the engine PK lifecycle test into the live unnamed
      `ALTER TABLE ... ADD PRIMARY KEY` proof: assert a non-empty observed name,
      not its shape, then assert a second sync has no changes.
- [ ] Pin an engine-created unnamed FK added through `ALTER TABLE`, read its
      generated name, and assert a convergent second sync.
- [ ] Add or adapt a legacy-adoption case where raw SQL creates named matching
      PK/FK constraints and declarations omit both names; assert zero SQL and
      preservation of the observed names.
- [ ] Retain a separate explicit-name case proving managed name drift and
      lowercase catalog identity.
- [ ] Stop asserting generated-name spelling in lifecycle, self-reference, and
      camel-case tests. Where a test is specifically about exact named SQL,
      supply an explicit name instead of weakening the assertion.
- [ ] Keep the schema-wide collision and named-drop asymmetry platform probes.

Credentialed validation:

```bash
uv run pytest tests/live/test_sql_warehouse_live_constraints.py \
  -m databricks_e2e --no-cov -q
```

If credentials are unavailable locally, the implementation PR is not complete
until the existing credentialed workflow passes these cases.

### Task 8: Update public and architecture documentation

**Files:**

- `docs/how-to-configure-table.md`
- `docs/how-to-add-action-type.md`
- `docs/explanation-architecture.md`
- `docs/reference-limitations.md`
- `docs/reference-run-report.md`
- `docs/todo/business-logic-delta-databricks-correctness-review.md`
- `docs/todo/roadmap.md`
- `docs/todo/todo.md`

- [ ] Document omission as unmanaged physical naming and explicit values as
      managed naming.
- [ ] Explain existing-name adoption, fresh-environment name variance, and the
      explicit-name escape hatch for consumers that query information schema by
      name.
- [ ] Replace the architecture section that currently claims every lowered
      constraint has a complete physical name.
- [ ] Document that desired set actions may be unnamed while observed/drop
      values are concrete.
- [ ] Correct the action-authoring guide so it no longer claims every set
      constraint exposes a physical name directly.
- [ ] Update report subject examples for unnamed keys.
- [ ] Rephrase the roadmap's generated-name collision rationale while retaining
      explicit names as an adoption/code-generation requirement.
- [ ] Mark correctness finding 13 resolved with implementation and live
      evidence.
- [ ] Reframe the collision TODO around explicit names only; do not claim a
      schema-wide preflight exists.
- [ ] Add a `BREAKING CHANGE:` commit footer rather than editing released
      changelog entries.

Documentation validation:

```bash
uv run --group docs sphinx-build -W --keep-going -b html docs docs/_build/html
git diff --check
```

### Task 9: Full verification and PR handoff

- [ ] Run the complete non-live suite.
- [ ] Run formatting, linting, typing, and import-boundary checks.
- [ ] Run the focused credentialed live constraint suite.
- [ ] Review the final diff for generated-name remnants in production code,
      current docs, and behavioural tests.
- [ ] Ensure any retained `{table}_pk` or `{table}_{columns}_fk` strings are
      deliberate explicit-name fixtures or released changelog history.
- [ ] Open the PR with the behaviour table, migration guarantee, live evidence,
      and breaking public accessor/report implications in the body.

Full validation:

```bash
uv run ruff format --check .
uv run ruff check .
uv run mypy .
uv run lint-imports
uv run pytest -q
uv run --group docs sphinx-build -W --keep-going -b html docs docs/_build/html
git diff --check
```

## Acceptance criteria

The work is complete only when all of the following hold:

- No production function synthesizes default PK or FK names.
- Omitted names reach both `CREATE TABLE` and `ALTER TABLE ... ADD` as omitted
  SQL clauses.
- Every observed constraint and every drop action has a concrete catalog name.
- A matching existing definition under any name is a no-op for an unnamed
  declaration.
- Explicit names remain case-insensitively managed and drift causes
  drop/recreate.
- Mixed named/unnamed FKs obey explicit-name precedence.
- Existing engine-generated constraints upgrade without DDL churn.
- Fresh unnamed constraints expose a non-empty Databricks-generated name and
  converge on the next sync.
- Reports never display `None` or a predicted generated name.
- Long, special-character, and concatenation-ambiguous declarations no longer
  fail because the engine invented an invalid name.
- No schema-wide read or speculative collision subsystem is introduced.
- Focused, full, documentation, architecture, and credentialed live checks are
  green.

## Implementation review checklist

Review the eventual PR against these failure-prone edges rather than only its
happy path:

- Does any API accessor or doc still promise a generated default?
- Can an unnamed value reach an observed table, inbound reference, or drop
  compiler?
- Can an unnamed FK steal an observed name reserved by an explicit desired FK?
- Are extra observed FKs still dropped under full-state management?
- Are identifier-equivalent explicit names still no-ops after catalog
  lowercasing?
- Do column-case drift and constraint replacement remain separate stated
  differences?
- Is every SQL space correct with and without the optional prefix?
- Are reports useful for several unnamed FKs on one table?
- Do existing tables retain their names without persisting adoption state?
- Are external collisions and read/write races still surfaced honestly as
  execution failures?
