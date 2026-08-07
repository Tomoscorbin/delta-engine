---
tags:
  - todo
  - architecture
  - review
  - plan
---

# Constraint value depth review and implementation plan

**Status:** Review complete; implementation awaits agreement

**Review date:** 2026-08-07

**Reviewed revision:** `ae9d1fbb` (`feat: allow named foreign keys (#340)`)

## Scope

This review asks whether the primary- and foreign-key classes form deep
boundaries: callers should supply natural declarations, while the values hide
normalization, identity, pairing, and validity rules behind small interfaces.
It covers:

- the public `DeltaTable`, `ForeignKey`, and `Self` declaration surface;
- `PrimaryKeyConstraint`, `ForeignKeyConstraint`, and
  `ForeignKeyReference` in the domain model;
- constraint-related invariants on `DesiredTable` and `ObservedTable`;
- the private declaration-lowering values in `api/delta_table.py`; and
- the diff and relationship consumers that reveal which domain knowledge has
  escaped the values.

This is a design review rather than a new platform-correctness audit. The
recent live constraint-name verification and the PK/FK naming PRs are treated
as established behaviour.

## Evaluation criteria

The review uses four questions:

1. Does each class represent one coherent value with a clear lifecycle role?
2. Does construction establish every invariant knowable from that value alone?
3. Does ordinary equality express the identity most callers need?
4. Do callers ask the value meaningful questions, or reconstruct its internal
   representation themselves?

Validation in more than one module is not automatically leakage. A rule should
live at the narrowest boundary that has all the information needed to judge it.

## Inventory: what exists and why

| Category | Concepts | Responsibility |
| --- | --- | --- |
| Public declaration | `DeltaTable`, `ForeignKey`, `Self` | Ergonomic user input, shorthand forms, and optional explicit names |
| Private lowering | `_NormalizedDeclaration`, `_ReferencedSide` | Freeze public inputs and carry temporary FK-resolution context |
| Complete domain state | `PrimaryKeyConstraint`, `ForeignKeyConstraint` | Fully named, normalized constraint values used by desired and observed tables |
| Observed safety projection | `ForeignKeyReference` | The minimal inbound-FK occurrence needed to protect primary-key changes |
| Table aggregate | `DesiredTable`, `ObservedTable` | Ensure constraints agree with the table that owns them |
| Declaration set | `relationships.resolve` | Judge registered targets, dependencies, and cycles |
| Transition messages | `Set*` and `Drop*` actions | Carry already-decided physical changes to compilation and reporting |

These are mostly immutable values, not entities with independent identity and
lifecycle. The number of classes is therefore not itself evidence of excess
complexity. The important question is whether two classes represent the same
fact or whether callers need to understand two representations at once.

### Public `ForeignKey` versus domain `ForeignKeyConstraint`

This is a useful distinction and should remain.

`ForeignKey` is unresolved, ergonomic input. It accepts a single local column,
a same-name sequence, or an explicit local-to-parent mapping, and it retains
the referenced `DeltaTable` object required by the public API.

`ForeignKeyConstraint` is a complete physical constraint. It has explicit
local and referenced columns, a qualified target, and a physical name. Diffing,
actions, SQL, and catalog rows need this complete form and should not understand
the public shorthand.

Removing either class would transfer its complexity to every caller. Adding a
generic declaration/spec/occurrence hierarchy would add vocabulary without
hiding more work than these two values already hide.

### Desired versus observed constraint values

The same `PrimaryKeyConstraint` and `ForeignKeyConstraint` types are used in
`DesiredTable` and `ObservedTable`. That reuse is now appropriate: default
names are generated once during `DeltaTable` construction, so both desired and
observed constraints are complete named physical values with the same
invariants.

Separate desired and observed constraint classes would repeat fields and
validation while forcing diffing to translate between otherwise identical
values. Origin is carried by the containing table and action, where it matters.

### `ForeignKeyReference`

`ForeignKeyReference` is not a second representation of an outbound FK. The
catalog query for an inbound FK exposes only the referencing table and physical
constraint name, and primary-key safety needs only those fields. A small
projection is preferable to manufacturing a partial `ForeignKeyConstraint` or
making its column fields optional.

### Private lowering records

`_NormalizedDeclaration` and `_ReferencedSide` are private, short-lived
records. `_NormalizedDeclaration` prevents generators and mutable mappings from
being consumed differently by validation and lowering. `_ReferencedSide`
keeps the target facts passed between the private FK-lowering helpers together.

They are data carriers rather than deep modules, but their scope is local and
they avoid large parallel argument lists. They do not justify public lifecycle
types.

## Validation ownership

The current validation is spread across several locations because the rules
require different context:

| Boundary | Information available | Rules that belong there |
| --- | --- | --- |
| Individual value | Its own fields | Input shape, non-empty columns, duplicate columns, valid physical name, aligned FK columns |
| Public declaration | User syntax and the referenced object | Shorthand normalization, default naming, pair inference, declaration-time target checks |
| Owning table | All owner columns and all owned constraints | Referenced owner columns exist, desired PK columns are non-nullable, owned FK sets and names are coherent |
| Registered declaration set | The table actually registered under every qualified name | Target availability, registered key and type agreement, cycles, dependency ordering |
| Desired/observed transition | Both snapshots and inbound catalog references | Drift actions and safety of dropping a referenced primary key |

This distribution is fundamentally sound. In particular:

- an individual constraint cannot judge whether its local columns exist
  without being given its owner;
- nullable primary-key rejection is desired-only because an observed legacy
  table must remain representable;
- duplicate local-column sets and duplicate names are collection invariants,
  so they belong to `DesiredTable`, not one `ForeignKeyConstraint`; and
- a constraint cannot judge an unregistered target or graph cycle by itself.

Moving these rules into a `ConstraintSet`, `ConstraintManager`, or generic
validation service would mostly relocate conditionals and add pass-through
interfaces.

## Findings

| # | Priority | Finding | Main symptom |
| --- | --- | --- | --- |
| 1 | Medium | Primary-key identity escapes the value | Callers must choose `.signature` instead of ordinary equality |
| 2 | Medium | Constraint constructors do not establish all intrinsic invariants | Invalid values can be constructed or fail later through incidental `AttributeError`s |
| 3 | Low | FK pair traversal remains a caller convention | Relationship code must know to zip two positionally aligned attributes |
| 4 | Deferred | FK authority is split between eager lowering and set resolution | The referenced object is judged first and the registered table is judged again |

## 1. Make `PrimaryKeyConstraint` own its identity

### Escaped primary-key identity

`PrimaryKeyConstraint` preserves declared column order because SQL and public
accessors render that order. Its generated dataclass equality consequently
treats the tuple order as identity. The documented and implemented semantic
rule is different: primary-key column order is irrelevant, identifier identity
is case-insensitive, and the physical constraint name is managed state.

That mismatch is repaired by an alternate identity path:

- `key_signature(columns)` creates an order-independent set;
- `PrimaryKeyConstraint.signature` exposes that set;
- `_diff_primary_key` separately compares `constraint_name` and `signature`;
- `ForeignKeyConstraint.referenced_key_signature` exposes another projection;
- relationship resolution builds a map of primary-key signatures; and
- public FK lowering calls `key_signature` directly for target validation and
  same-name pair inference.

The class therefore stores the data, but its callers own the meaning. This is
the main depth problem in the current constraint design.

`ForeignKeyConstraint` demonstrates the desired result: it canonicalizes pair
order internally, so ordinary equality already means same case-insensitive
name and same complete relationship definition. `_diff_foreign_keys` can use
normal equality without a caller-selected identity projection.

### Deeper primary-key interface

Keep the rendering order, but make `PrimaryKeyConstraint` define managed
constraint equality explicitly:

```python
@dataclass(frozen=True, slots=True, eq=False)
class PrimaryKeyConstraint:
    columns: ListOrTuple[str]
    constraint_name: str

    def matches_columns(self, columns: Iterable[str]) -> bool:
        """Whether `columns` name this key, ignoring order and case."""
        ...

    def __eq__(self, other: object) -> bool:
        if not isinstance(other, PrimaryKeyConstraint):
            return NotImplemented
        return (
            self.constraint_name == other.constraint_name
            and self.matches_columns(other.columns)
        )

    def __hash__(self) -> int:
        ...
```

The custom hash must use the same case-insensitive name and column-set
identity. The stored `columns` remain ordered and spelling-preserving; only
identity changes.

Callers then read directly:

```python
if desired_key == observed_key:
    return ()
```

and:

```python
if parent_key is None or not parent_key.matches_columns(
    foreign_key.referenced_columns
):
    ...
```

`matches_columns` is deliberately narrow. A generic `matches`, `satisfies`, or
`is_satisfied_by` method would hide which part of identity is intentionally
being ignored and would recreate the ambiguity removed from diffing.

`_ReferencedSide` should carry `PrimaryKeyConstraint | None` rather than a
second bare key-column tuple. Public FK lowering can then use the same
`matches_columns` operation for explicit mapping validation and same-name
inference.

After migration, remove:

- `KeySignature`;
- `key_signature` and its domain-model export;
- `PrimaryKeyConstraint.signature`; and
- `ForeignKeyConstraint.referenced_key_signature`.

### Alternatives rejected for PK identity

- **Sort stored PK columns.** This would simplify equality but change declared
  order in accessors and generated SQL.
- **Compare only the physical name.** A changed definition under an existing
  name must still become drop-then-set.
- **Add definition/spec/occurrence wrappers.** Both sides already carry complete
  physical constraints, so wrappers would add types without removing a state.
- **Keep `signature` as a convenience.** It continues to invite each caller to
  choose which notion of identity it needs instead of asking a meaningful
  question of the value.

### PK acceptance criteria

- Two PK constraints with the same identifier name and column set compare
  equal across name case, column case, and column order.
- A different physical name or different column set compares unequal.
- Declared column spelling and order remain unchanged for rendering.
- PK diffing uses ordinary equality and preserves all current actions.
- FK target validation uses `matches_columns` and preserves exact-spelling
  failure classification.
- `rg` finds no production use or export of `key_signature`, `.signature`, or
  `referenced_key_signature`.

## 2. Make constructors establish intrinsic validity

### Incomplete construction boundaries

The normal public `DeltaTable` path produces well-formed constraint values, but
the individual constructors accept or mishandle inputs outside their annotated
types:

```python
PrimaryKeyConstraint(columns="id", constraint_name="t_pk")
# accepted as columns ("i", "d")

ForeignKeyConstraint(
    local_columns="id",
    referenced_table=target,
    referenced_columns="id",
    constraint_name="t_fk",
)
# accepted as a two-column key, canonicalized to ("d", "i")
```

Non-string column entries and a non-string
`ForeignKeyReference.constraint_name` reach `Identifier` or `.strip()` and
raise `AttributeError` rather than a boundary-specific `TypeError`.

The public `ForeignKey` validates sequence entries but not mapping keys and
values. For example, `ForeignKey(columns={"id": 1}, references=parent)` is a
constructible declaration and fails only when an owner later lowers it. An
unsupported `references` value is likewise retained until a `DeltaTable` is
constructed.

These are primarily depth problems: a successfully constructed frozen value
should be safe for every later method to consume without repeating defensive
shape checks.

### Stronger construction boundary

Strengthen the existing constructors; do not add validation classes.

1. Make `Identifier.__new__` explicitly require `str`, raising `TypeError`
   before calling string methods. This gives the primitive identifier value one
   reliable boundary for element and physical-name types.
2. In `PrimaryKeyConstraint` and `ForeignKeyConstraint`, explicitly reject a
   bare string where a list/tuple is required. Normalize and validate every
   element once before setting frozen fields.
3. In `ForeignKey.__post_init__`, validate mapping keys and values as strings,
   reject an empty sequence or mapping, and validate that `references` is a
   `DeltaTable` or `Self` immediately.
4. Keep `ForeignKeyConstraint` validation even though the public declaration
   validates similar inputs. Catalog adapters and internal/custom desired-table
   sources construct domain constraints directly; each trust boundary must
   establish its own output invariant.
5. Do not move owner-column, target-key, type, or registered-set checks into
   these constructors. Those facts require larger context.

A small private tuple-normalization helper is acceptable if it removes the
same bare-string, element-type, non-empty, and duplicate loop from both domain
constraint classes. It should return the normalized tuple and remain specific
to identifier sequences; a generic validation framework would be shallower
than the explicit constructors.

### Constructor acceptance criteria

- Domain PK/FK column collections reject bare strings.
- Non-string constraint names and column entries raise `TypeError`, not an
  incidental `AttributeError`.
- Blank identifiers remain `ValueError`s.
- Public `ForeignKey` rejects invalid mapping entries, empty columns, and an
  invalid reference when it is constructed.
- Valid string, sequence, mapping, `Self`, explicit-name, and inferred-name
  declarations retain their current frozen values and lowering behaviour.
- Catalog row translation continues to construct the same domain values.

## 3. Expose FK column pairs as one operation

### Positional pairing contract

`ForeignKeyConstraint` accepts parallel local and referenced sequences,
validates equal length, and canonicalizes them together. The resulting frozen
value cannot be misaligned. Most consumers naturally need one projection or
the other:

- SQL renders a local column clause and a referenced column clause;
- reports identify an FK by its local columns; and
- generated names use local columns.

Relationship type validation is the notable pair-oriented consumer. It must
know that the two attributes are positionally aligned and zip them with
`strict=True`.

### Pair projection

Add a read-only projection:

```python
@property
def column_pairs(
    self,
) -> tuple[tuple[Identifier, Identifier], ...]:
    """Canonical local-to-referenced column pairs."""
    return tuple(zip(self.local_columns, self.referenced_columns, strict=True))
```

Use it wherever a consumer needs the relationship rather than two clauses.
This lets the class retain responsibility for positional alignment.

Do not change stored fields or constructor input in this series. Replacing
them with `column_pairs` would touch every adapter, action fixture, and direct
domain construction site, while only one current production consumer needs
pair iteration. Do not introduce a `ForeignKeyColumnPair` class for two
identifiers.

### Pair-projection acceptance criteria

- `column_pairs` preserves local-to-referenced association and canonical order.
- Pair identity remains insensitive to declaration order and identifier case.
- No production code outside `ForeignKeyConstraint` zips its two column
  projections.
- SQL and report interfaces remain unchanged.

## 4. Keep FK admission as separate architectural debt

Public FK lowering initially judges the referenced `DeltaTable` object so it
can infer pairs and construct a complete `DesiredTable`. The sync may register
a different table object under the same qualified name, so
`relationships.resolve` correctly rechecks the completed FK against the
registered table.

This is genuine parallel authority, but it cannot be removed by moving a few
methods between the existing constraint classes:

- trusting only the referenced object makes the registered sync declaration
  non-authoritative;
- trusting only the registered table requires unresolved FK intent to survive
  until the complete declaration set exists; and
- weakening eager checks lets `DeltaTable.to_desired_table()` return a value
  whose relationship has not actually been resolved.

The identity and constructor work above will reduce duplicated mechanics — in
particular both checks can ask `PrimaryKeyConstraint.matches_columns` — but the
two judgment times remain.

Do not smuggle an admission redesign into these small PRs. Revisit it only as a
separate design with evidence that the resulting boundary hides more concepts
than it adds. The previously considered `TableDeclaration`,
`ForeignKeyIntent`, accepted/rejected resolution variants, and source-protocol
migration are not prerequisites for deepening the constraint values.

## Changes deliberately not proposed

- Do not add a public `PrimaryKey` declaration solely for symmetry with
  `ForeignKey`; `primary_key` plus `primary_key_name` keeps the common API
  direct and rejects a name without a key during normalization.
- Do not split desired and observed constraint classes.
- Do not add `Constraint`, `ConstraintSpec`, `NamedConstraint`,
  `ConstraintSet`, or `ConstraintManager` abstractions.
- Do not move desired-only aggregate checks out of `DesiredTable` merely to
  shorten its `__post_init__`.
- Do not make cross-table or transition validation methods on an individual
  constraint.
- Do not remove `_NormalizedDeclaration`; freezing all public iterables once is
  a useful private boundary.
- Do not remove `DeltaTable.to_desired_table`; it is the established
  `DesiredTableSource` port and is unrelated to the local value-depth changes.
- Do not combine schema-wide physical-name collision admission with this work;
  it remains a separately tracked declaration-set concern.
- Do not extract the constraint helpers from `delta_table.py` solely because
  the file is long. File movement without a smaller interface does not deepen
  the module.

## Implementation plan

Two small PRs keep semantic refactoring separate from invalid-input behaviour.
They may be combined only if the final diff remains easier to review than the
two boundaries below.

### PR 1: Make constraint values own semantic operations

#### Semantic-operation scope

- Give `PrimaryKeyConstraint` order-independent, case-insensitive, name-aware
  equality and a matching hash while preserving stored column order.
- Add `PrimaryKeyConstraint.matches_columns` for comparisons that deliberately
  exclude the physical PK name.
- Make `_ReferencedSide` carry the referenced primary-key value rather than a
  duplicate tuple of its columns.
- Change primary-key diffing to use ordinary equality.
- Change public FK lowering and registered relationship checks to call
  `matches_columns`.
- Remove `KeySignature`, `key_signature`, `PrimaryKeyConstraint.signature`,
  and `ForeignKeyConstraint.referenced_key_signature`.
- Add `ForeignKeyConstraint.column_pairs` and use it for pair-oriented
  relationship checks.

#### Semantic-operation files

- `src/delta_engine/domain/model/constraints.py`
- `src/delta_engine/domain/model/__init__.py`
- `src/delta_engine/api/delta_table.py`
- `src/delta_engine/domain/plan/diff.py`
- `src/delta_engine/application/relationships.py`
- `tests/domain/model/test_primary_key.py`
- `tests/domain/model/test_foreign_key.py`
- `tests/domain/plan/test_diff.py`
- `tests/application/test_relationships.py`
- `tests/api/test_delta_table.py`

#### Semantic-operation tests

- PK equality includes the physical name and semantic column set.
- PK equality ignores declared column order and identifier case.
- PK equality does not change stored/rendered column order or spelling.
- `matches_columns` ignores only name and order, not differing membership.
- Reordered desired/observed keys converge without actions.
- Same-name changed definitions and renamed definitions still drop then set.
- FK inference, registered-parent key mismatch, and parent case-drift outcomes
  remain unchanged after signature removal.
- `column_pairs` preserves canonical association across declaration order and
  case variants.

#### Semantic-operation validation

```bash
uv run pytest -q --no-cov tests/domain/model/test_primary_key.py
uv run pytest -q --no-cov tests/domain/model/test_foreign_key.py
uv run pytest -q --no-cov tests/domain/plan/test_diff.py
uv run pytest -q --no-cov tests/application/test_relationships.py
uv run pytest -q --no-cov tests/api/test_delta_table.py
```

#### Semantic-operation review checkpoint

The PR should make the primary-key and foreign-key diff code equally direct.
If custom PK equality needs callers to remember another qualifier beyond
`matches_columns`, stop and reassess rather than adding another identity API.

### PR 2: Harden declaration and domain construction

#### Construction-hardening scope

- Make `Identifier` reject non-string inputs explicitly.
- Make the domain constraint constructors reject bare-string collections and
  non-string entries before normalization.
- Make `ForeignKeyReference` use the same reliable physical-name boundary.
- Validate every public FK mapping key/value, reject empty input, and validate
  `references` in `ForeignKey.__post_init__`.
- Remove later defensive arms made unreachable by the strengthened public
  constructor, while retaining domain validation at adapter boundaries.

#### Construction-hardening files

- `src/delta_engine/domain/model/identifier.py`
- `src/delta_engine/domain/model/constraints.py`
- `src/delta_engine/api/delta_table.py`
- `tests/domain/model/test_identifier.py`
- `tests/domain/model/test_primary_key.py`
- `tests/domain/model/test_foreign_key.py`
- `tests/domain/model/test_table.py`
- `tests/api/test_delta_table.py`

#### Construction-hardening tests

- A bare string cannot become a character-by-character PK or FK.
- Non-string identifier values fail with `TypeError` at the value boundary.
- Empty and ill-typed public FK forms fail when `ForeignKey` is constructed,
  before an owning table exists.
- An invalid FK reference fails when `ForeignKey` is constructed.
- Input lists and mappings remain defensively copied.
- Every valid public FK shorthand still lowers to the same complete constraint.
- Direct adapter/domain construction remains possible for valid catalog data.

#### Construction-hardening validation

```bash
uv run pytest -q --no-cov tests/domain/model/test_identifier.py
uv run pytest -q --no-cov tests/domain/model/test_primary_key.py
uv run pytest -q --no-cov tests/domain/model/test_foreign_key.py
uv run pytest -q --no-cov tests/domain/model/test_table.py
uv run pytest -q --no-cov tests/api/test_delta_table.py
uv run pytest -q --no-cov tests/adapters/databricks/sql/test_rows.py
```

#### Construction-hardening review checkpoint

Every new check must be intrinsic to the object being constructed. If a check
needs owner columns, the registered declaration set, or observed catalog state,
leave it at the existing wider boundary.

## Full validation for each implementation PR

Run focused tests first, followed by the configured repository gates:

```bash
uv run ruff check .
uv run ruff format --check .
uv run mypy .
uv run pytest -q
uv run lint-imports
uv run --group docs sphinx-build -W --keep-going -b html docs docs/_build/html
git diff --check
```

No live Databricks suite is required for these internal refactors because they
do not change generated SQL, catalog queries, physical names, or relationship
policy. If implementation changes any of those behaviours, the relevant live
constraint suite becomes mandatory before merge.

## Completion criteria

The review is implemented when:

1. ordinary PK equality expresses complete managed constraint identity;
2. callers use one precisely named operation when comparing only PK columns;
3. no signature vocabulary remains in production code;
4. pair-oriented FK consumers use the constraint's pair projection;
5. constructed constraint and public FK values establish every intrinsic
   invariant and fail invalid types deliberately;
6. validation requiring owner, declaration-set, or transition context remains
   at those wider boundaries;
7. no new public constraint concepts are introduced; and
8. focused and full repository validation pass without SQL or report changes.
