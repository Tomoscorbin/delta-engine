# Identifier Value Type Implementation Plan

> Historical plan. The `Identifier` value type remains current; its references
> to a resulting-schema planning pass were superseded by the 2026-07-27
> action-construction simplification recorded in the spelling design document.

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Replace the call-site `identifier_key()` convention with an `Identifier` domain value type whose equality and hash are case-insensitive by construction, deleting `identifier_key`, `index_by_identifier`, and `canonical_data_type` and reverting ~84 keyed call sites to plain comparisons.

**Architecture:** `Identifier` subclasses `str` with case-insensitive `__eq__`/`__ne__`/`__hash__` and preserved spelling. Domain constructors wrap every identifier-carrying field in `__post_init__`, so the domain interior only ever compares Identifiers and plain `==`/`in`/dict/set code is correct automatically. Public constructors (`schema.Column`, `StructField`) keep accepting plain `str` — the wrap is invisible because an `Identifier` *is* a `str`. The SQL compiler, validation messages, and reports are untouched: f-strings and `backtick()` render the preserved spelling. The planner's binding pass (resulting schemas → exact physical spelling for constraint DDL) is **kept** — it solves spelling *choice*, not identity, and is pinned by the live suite.

**Tech Stack:** Python ≥3.12, dataclasses (`frozen=True, slots=True`), uv, pytest, mypy (`strict_equality`), ruff, import-linter.

## Why a `str` subclass (decision record)

- `schema.Column` **is** `DesiredColumn` and `StructField` is public (`src/delta_engine/schema.py`). An opaque wrapper would force `Column(Identifier("x"), ...)` on users or hand-written `__init__` overloads on every domain dataclass. A `str` subclass keeps every public signature, every f-string error message, and the entire compiler byte-identical.
- Reflected-operator priority guarantees symmetry: for `"raw" == Identifier(...)` Python calls the subclass `__eq__` first, so mixed comparisons are case-insensitive in both directions. `__ne__` must be overridden explicitly (an inherited method does not get subclass priority).
- Trade-off accepted: enforcement is by construction at runtime (constructors wrap), not by mypy. The residual discipline shrinks from ~84 comparison sites to ~5 *boundary probe* sites (rule below).

**Boundary probe rule (the one convention that remains):** any dict/set that is *built from* domain values but *probed with* a string that never passed a domain constructor (or vice versa) must wrap the raw side in `Identifier(...)`. All such sites live in `api/delta_table.py` (user-input validation helpers) and `adapters/databricks/sql/rows.py` (catalog tag rows). Task 9 and Task 10 enumerate every one.

## Global Constraints

- Run all commands with `uv run ...`; never install packages globally.
- Conventional commit messages, `refactor:`/`test:`/`docs:` prefixes; no `Co-authored-by` trailers.
- Work on the existing branch `fix/preserve-column-identifier-case` (this folds into PR #287). Never commit to `main`.
- Public API is frozen: `schema.Column`, `StructField`, `DeltaTable`, `ForeignKey` signatures and accepted input types must not change; accessors keep returning values usable as `str`.
- Error message text is frozen: every `ValueError` message that exists today must stay byte-identical (existing tests pin them). Keep local blank-name checks; `Identifier`'s own blank check is backstop armor only.
- Emitted SQL is frozen: matched-column actions keep **observed** spelling; bound constraint/clustering references keep **post-sync physical** spelling. The live suite pins this.
- Gates that must pass at every commit: `uv run pytest -q` (the narrow file first, then full), and at wrap-up `uv run ruff format . && uv run ruff check . && uv run mypy . && uv run lint-imports` plus the docs build. Coverage must not drop below the configured gate.
- Python 3.12 idioms; `dataclass(frozen=True, slots=True)`; `object.__setattr__` for `__post_init__` writes (existing pattern).

## File Structure

| File | Change |
| --- | --- |
| `src/delta_engine/domain/model/identifier.py` | Add `Identifier(str)`; keep `identifier_key`/`index_by_identifier` until Task 11, then delete them |
| `src/delta_engine/domain/model/column.py` | Wrap `name`/`renamed_from`; revert keyed self-rename check |
| `src/delta_engine/domain/model/data_type.py` | Wrap `StructField.name`; plain duplicate check; delete `canonical_data_type` (Task 3) |
| `src/delta_engine/domain/model/constraints.py` | Wrap columns + `constraint_name`; plain dup checks/signature; sort FK pairs by `.key` |
| `src/delta_engine/domain/model/table.py` | Wrap layout tuples; revert validation to plain membership |
| `src/delta_engine/domain/plan/actions.py` | Wrap name fields; plain no-difference checks; `_execution_order` uses `.lower()` |
| `src/delta_engine/domain/plan/diff.py` | Revert alignment/rename/layout to plain collections (keep observed-spelling emission) |
| `src/delta_engine/domain/plan/resulting_schema.py` | `Mapping[Identifier, Identifier]`; drop explicit keying |
| `src/delta_engine/application/planning.py` | Probe resulting schemas directly with Identifiers |
| `src/delta_engine/application/dependency_resolution.py` | Plain type-equality and dict probes |
| `src/delta_engine/api/delta_table.py` | Revert keyed sites; wrap-at-probe for raw user input; Identifier CDF reserved set |
| `src/delta_engine/adapters/databricks/read.py`, `.../sql/rows.py` | Tag dict keyed by `Identifier` |
| `src/delta_engine/domain/model/__init__.py` | Export `Identifier`; drop deleted helpers (Task 11) |
| `tests/domain/model/test_identifier.py` | Rewrite around `Identifier` |
| Other test files | Strengthen exact-spelling pins via `.spelling` (Task 12) |

---

### Task 1: The `Identifier` type

**Files:**
- Modify: `src/delta_engine/domain/model/identifier.py` (add class; keep existing helpers for now)
- Modify: `src/delta_engine/domain/model/__init__.py` (export `Identifier`)
- Test: `tests/domain/model/test_identifier.py` (append new class tests; keep existing helper tests until Task 11)

**Interfaces:**
- Consumes: nothing.
- Produces: `class Identifier(str)` with `key: str` (property), `spelling: str` (property, plain `str`), case-insensitive `__eq__`/`__ne__`/`__hash__`, blank-rejecting `__new__`. Constructor: `Identifier(spelling: str)`; idempotent for `Identifier` input. All later tasks rely on exactly these names.

- [ ] **Step 1: Write the failing tests**

Append to `tests/domain/model/test_identifier.py`:

```python
import pytest

from delta_engine.domain.model import Identifier


class TestIdentifierIdentity:
    def test_case_variant_spellings_are_equal(self) -> None:
        assert Identifier("requestId") == Identifier("REQUESTID")

    def test_equality_is_case_insensitive_against_plain_strings_both_ways(self) -> None:
        assert Identifier("requestId") == "requestid"
        assert "requestid" == Identifier("requestId")
        assert not (Identifier("requestId") != "REQUESTID")
        assert not ("REQUESTID" != Identifier("requestId"))

    def test_different_identifiers_are_unequal(self) -> None:
        assert Identifier("request_id") != Identifier("requestId")
        assert Identifier("requestId") != 5

    def test_hash_follows_identity_so_sets_and_dicts_deduplicate(self) -> None:
        assert hash(Identifier("ID")) == hash(Identifier("id"))
        assert len({Identifier("ID"), Identifier("id")}) == 1
        assert {Identifier("ID"): 1}[Identifier("id")] == 1

    def test_lowercase_keyed_dict_is_probed_by_identifier(self) -> None:
        # Adapter dicts keyed by plain lowercase strings stay probe-able.
        assert {"requestid": 1}[Identifier("RequestId")] == 1


class TestIdentifierSpelling:
    def test_spelling_is_preserved_verbatim(self) -> None:
        assert str(Identifier("requestId")) == "requestId"
        assert f"{Identifier('requestId')}" == "requestId"
        assert repr(Identifier("requestId")) == "'requestId'"

    def test_spelling_property_is_a_plain_case_sensitive_str(self) -> None:
        spelling = Identifier("requestId").spelling
        assert type(spelling) is str
        assert spelling == "requestId"
        assert spelling != "REQUESTID"

    def test_key_is_the_lowercase_identity(self) -> None:
        assert Identifier("RequestId").key == "requestid"
        assert type(Identifier("RequestId").key) is str


class TestIdentifierConstruction:
    def test_blank_spelling_is_rejected(self) -> None:
        with pytest.raises(ValueError, match="must not be blank"):
            Identifier("   ")

    def test_wrapping_an_identifier_is_idempotent(self) -> None:
        original = Identifier("requestId")
        rewrapped = Identifier(original)
        assert isinstance(rewrapped, Identifier)
        assert rewrapped.spelling == "requestId"
```

- [ ] **Step 2: Run tests to verify they fail**

Run: `uv run pytest tests/domain/model/test_identifier.py -q`
Expected: FAIL — `ImportError: cannot import name 'Identifier'`

- [ ] **Step 3: Implement `Identifier`**

Add to `src/delta_engine/domain/model/identifier.py` (above `identifier_key`, keeping the module docstring's platform rationale):

```python
class Identifier(str):
    """
    A column-like identifier: preserved spelling, case-insensitive identity.

    ``Identifier`` is a ``str`` whose equality and hash follow Databricks
    identifier resolution: two spellings differing only in case are the same
    identifier. The construction spelling is preserved, so rendering, SQL
    compilation, and error messages show it verbatim. Comparisons against
    plain strings are case-insensitive in both directions (the subclass's
    reflected operator takes priority).

    Identity uses ``str.lower``, deliberately not ``str.casefold``: the live
    object-name pin distinguishes Python lowercasing from casefolding, and
    identifier identity must not silently adopt new Unicode semantics.
    """

    __slots__ = ()

    def __new__(cls, spelling: str) -> "Identifier":
        if not spelling.strip():
            raise ValueError(f"Identifier must not be blank: {spelling!r}")
        return super().__new__(cls, spelling)

    @property
    def key(self) -> str:
        """The lowercase identity key shared by every spelling of this identifier."""
        return str.lower(self)

    @property
    def spelling(self) -> str:
        """The exact spelling as a plain ``str``, for case-sensitive comparison."""
        # Slicing a str subclass returns a plain str copy.
        return self[:]

    def __eq__(self, other: object) -> bool:
        if isinstance(other, str):
            return str.lower(self) == other.lower()
        return NotImplemented

    def __ne__(self, other: object) -> bool:
        equal = self.__eq__(other)
        if equal is NotImplemented:
            return equal
        return not equal

    def __hash__(self) -> int:
        return hash(str.lower(self))
```

In `src/delta_engine/domain/model/__init__.py`, change the identifier import line and `__all__`:

```python
from delta_engine.domain.model.identifier import Identifier, identifier_key, index_by_identifier
```

and add `"Identifier",` to `__all__` (alphabetical position, before `"Integer"`).

- [ ] **Step 4: Run tests to verify they pass**

Run: `uv run pytest tests/domain/model/test_identifier.py -q`
Expected: PASS

- [ ] **Step 5: Run the full fast suite**

Run: `uv run pytest -q`
Expected: PASS (nothing consumes the class yet)

- [ ] **Step 6: Commit**

```bash
git add src/delta_engine/domain/model/identifier.py src/delta_engine/domain/model/__init__.py tests/domain/model/test_identifier.py
git commit -m "refactor: add case-insensitive Identifier str type"
```

---

### Task 2: Columns and struct fields store Identifiers

**Files:**
- Modify: `src/delta_engine/domain/model/column.py`
- Modify: `src/delta_engine/domain/model/data_type.py` (StructField/Struct only; `canonical_data_type` untouched until Task 3)
- Test: `tests/domain/model/test_column.py`, `tests/domain/model/test_data_type.py`

**Interfaces:**
- Consumes: `Identifier` from Task 1.
- Produces: `DesiredColumn.name`, `DesiredColumn.renamed_from`, `ObservedColumn.name`, `StructField.name` are `Identifier` instances at runtime (annotations stay `str` — the public contract). Every later task relies on this wrapping.

- [ ] **Step 1: Write the failing tests**

Append to `tests/domain/model/test_column.py`:

```python
def test_column_names_compare_case_insensitively() -> None:
    desired = DesiredColumn(name="RequestId", data_type=String())
    observed = ObservedColumn(name="requestid", data_type=String())
    assert desired.name == observed.name
    assert desired.name in {observed.name}


def test_column_name_spelling_is_preserved_verbatim() -> None:
    column = DesiredColumn(name="RequestId", data_type=String())
    assert column.name.spelling == "RequestId"


def test_case_only_self_rename_is_rejected() -> None:
    with pytest.raises(ValueError, match="cannot be renamed_from itself"):
        DesiredColumn(name="RequestId", data_type=String(), renamed_from="requestid")
```

(Adjust imports at the top of the file if `pytest`/`String` are not already imported; the branch's existing tests import them.)

Append to `tests/domain/model/test_data_type.py`:

```python
def test_struct_types_differing_only_in_field_case_are_equal() -> None:
    assert Struct((StructField("Payload", String()),)) == Struct((StructField("payload", String()),))


def test_struct_field_spelling_is_preserved_verbatim() -> None:
    [field] = Struct((StructField("Payload", String()),)).fields
    assert field.name.spelling == "Payload"


def test_struct_rejects_case_variant_duplicate_fields() -> None:
    with pytest.raises(ValueError, match="Duplicate struct field name"):
        Struct((StructField("id", String()), StructField("ID", String())))
```

- [ ] **Step 2: Run tests to verify the new ones fail**

Run: `uv run pytest tests/domain/model/test_column.py tests/domain/model/test_data_type.py -q`
Expected: FAIL — `.spelling` does not exist on plain `str`; struct equality is case-sensitive.

- [ ] **Step 3: Implement the wrapping**

In `src/delta_engine/domain/model/column.py`, replace the import of `identifier_key` with `Identifier` and change both `__post_init__` methods:

```python
from delta_engine.domain.model.identifier import Identifier
```

`DesiredColumn.__post_init__` becomes:

```python
    def __post_init__(self) -> None:
        object.__setattr__(self, "tags", MappingProxyType(dict(self.tags)))
        _validate_column_fields(self.name, self.tags)
        object.__setattr__(self, "name", Identifier(self.name))
        if self.renamed_from is not None:
            if not self.renamed_from.strip():
                raise ValueError(f"renamed_from must not be blank: {self.renamed_from!r}")
            object.__setattr__(self, "renamed_from", Identifier(self.renamed_from))
            if self.renamed_from == self.name:
                raise ValueError(f"Column {self.name!r} cannot be renamed_from itself")
```

(Blank validation runs first so existing messages stay byte-identical; `self.renamed_from == self.name` is now the case-insensitive Identifier comparison.)

`ObservedColumn.__post_init__` becomes:

```python
    def __post_init__(self) -> None:
        object.__setattr__(self, "tags", MappingProxyType(dict(self.tags)))
        _validate_column_fields(self.name, self.tags)
        object.__setattr__(self, "name", Identifier(self.name))
```

Update both class docstrings' `name:` attribute line to: `Column name, stored verbatim as a case-insensitive :class:`Identifier`.`

In `src/delta_engine/domain/model/data_type.py`, import `Identifier` alongside `identifier_key` (which `canonical_data_type` still uses until Task 3):

```python
from delta_engine.domain.model.identifier import Identifier, identifier_key
```

`StructField.__post_init__` becomes:

```python
    def __post_init__(self) -> None:
        if not self.name.strip():
            raise ValueError(f"Struct field name must not be blank: {self.name!r}")
        object.__setattr__(self, "name", Identifier(self.name))
```

`Struct.__post_init__`'s duplicate check reverts to plain membership:

```python
        seen: set[str] = set()
        for field in self.fields:
            if field.name in seen:
                raise ValueError(f"Duplicate struct field name: {field.name}")
            seen.add(field.name)
```

- [ ] **Step 4: Run the module tests, then the full suite**

Run: `uv run pytest tests/domain/model/ -q` then `uv run pytest -q`
Expected: PASS. (Struct equality becoming case-insensitive makes `canonical_data_type` a no-op semantically but breaks none of its call sites.)

- [ ] **Step 5: Commit**

```bash
git add src/delta_engine/domain/model/column.py src/delta_engine/domain/model/data_type.py tests/domain/model/test_column.py tests/domain/model/test_data_type.py
git commit -m "refactor: store column and struct-field names as Identifiers"
```

---

### Task 3: Delete `canonical_data_type`

**Files:**
- Modify: `src/delta_engine/domain/model/data_type.py` (delete function), `src/delta_engine/domain/model/__init__.py` (drop export)
- Modify: `src/delta_engine/domain/plan/diff.py:383` (`_diff_existing_column` type guard), `src/delta_engine/domain/plan/actions.py:420` (`AlterColumnType.__post_init__`), `src/delta_engine/api/delta_table.py` (FK type check in `ForeignKey._validate`), `src/delta_engine/application/dependency_resolution.py` (`_foreign_key_types_match`)
- Test: `tests/domain/model/test_data_type.py`, `tests/domain/plan/test_diff.py`

**Interfaces:**
- Consumes: Identifier-carrying `StructField` from Task 2 (dataclass equality on `DataType` is now case-insensitive for field names).
- Produces: plain `==`/`!=` on `DataType` everywhere; `canonical_data_type` no longer exists anywhere in `src/` or `tests/`.

- [ ] **Step 1: Confirm existing behaviour tests cover the guard**

The branch's `tests/domain/plan/test_diff.py` already pins "case-only struct field difference produces no `AlterColumnType`" (added by PR #287). Run them now to establish green:

Run: `uv run pytest tests/domain/plan/test_diff.py tests/domain/model/test_data_type.py -q`
Expected: PASS

- [ ] **Step 2: Replace every `canonical_data_type` call with plain equality**

- `diff.py` `_diff_existing_column`: `if canonical_data_type(desired.data_type) != canonical_data_type(observed.data_type):` → `if desired.data_type != observed.data_type:`
- `actions.py` `AlterColumnType.__post_init__`: `if canonical_data_type(self.desired_type) == canonical_data_type(self.observed_type):` → `if self.desired_type == self.observed_type:`
- `api/delta_table.py` `ForeignKey._validate`: `if canonical_data_type(local_type) != canonical_data_type(referenced_type):` → `if local_type != referenced_type:`
- `application/dependency_resolution.py` `_foreign_key_types_match`: drop both `canonical_data_type(...)` wrappers, comparing the looked-up types directly (keep the `identifier_key` dict probes for now — Task 9 removes them).
- Delete `canonical_data_type` from `data_type.py`, remove its import from all four files above, and remove it from `domain/model/__init__.py` imports and `__all__`.
- In `tests/domain/model/test_data_type.py`, delete tests that call `canonical_data_type` directly — Task 2's equality tests are their behavioural replacement. Keep any test asserting *behaviour* (e.g. case-variant structs compare equal) by rewriting it to plain `==` if it isn't already.

- [ ] **Step 3: Verify nothing references it**

Run: `rg -n "canonical_data_type" src/ tests/`
Expected: no matches.

- [ ] **Step 4: Run the full suite**

Run: `uv run pytest -q`
Expected: PASS

- [ ] **Step 5: Commit**

```bash
git add -A src/ tests/
git commit -m "refactor: replace canonical_data_type with Identifier-aware type equality"
```

---

### Task 4: Constraints store Identifiers

**Files:**
- Modify: `src/delta_engine/domain/model/constraints.py`
- Test: `tests/domain/model/test_primary_key.py`, `tests/domain/model/test_foreign_key.py`

**Interfaces:**
- Consumes: `Identifier` from Task 1.
- Produces: `PrimaryKeyConstraint.columns`, `ForeignKeyConstraint.local_columns`/`referenced_columns` are tuples of `Identifier`; `constraint_name` on all three constraint classes is an `Identifier`; `key_signature(columns)` returns `frozenset` of the given names (Identifiers hash case-insensitively); `ForeignKeyConstraint.signature` returns the plain field tuple.

- [ ] **Step 1: Write the failing test**

Append to `tests/domain/model/test_foreign_key.py`:

```python
def test_signatures_match_across_case_variant_spellings() -> None:
    declared = ForeignKeyConstraint(
        local_columns=("orderref",),
        referenced_table=QualifiedName("cat", "sch", "parent"),
        referenced_columns=("orderid",),
        constraint_name="child_orderref_fk",
    )
    observed = ForeignKeyConstraint(
        local_columns=("OrderRef",),
        referenced_table=QualifiedName("cat", "sch", "parent"),
        referenced_columns=("OrderId",),
        constraint_name="fk_from_catalog",
    )
    assert declared.signature == observed.signature
    assert declared.local_columns[0].spelling == "orderref"
    assert observed.local_columns[0].spelling == "OrderRef"
```

(The branch already has equivalent-behaviour tests via `identifier_key`-based signatures; this one additionally pins `.spelling` and must fail before the change because `signature` currently returns lowercased key tuples while `.spelling` doesn't exist.)

- [ ] **Step 2: Run test to verify it fails**

Run: `uv run pytest tests/domain/model/test_foreign_key.py -q`
Expected: FAIL — `'str' object has no attribute 'spelling'`

- [ ] **Step 3: Implement**

In `constraints.py`, replace the `identifier_key` import with `Identifier` and:

`key_signature` reverts to:

```python
def key_signature(columns: Iterable[str]) -> KeySignature:
    """Return the order-independent, case-insensitive identity of a key's columns."""
    return frozenset(Identifier(column) for column in columns)
```

`PrimaryKeyConstraint.__post_init__` becomes:

```python
    def __post_init__(self) -> None:
        if not self.columns:
            raise ValueError("columns must not be empty")
        object.__setattr__(self, "columns", tuple(Identifier(column) for column in self.columns))

        seen: set[str] = set()
        for column in self.columns:
            if column in seen:
                raise ValueError(f"Duplicate primary key column: {column}")
            seen.add(column)

        if not self.constraint_name.strip():
            raise ValueError("constraint_name must not be blank")
        object.__setattr__(self, "constraint_name", Identifier(self.constraint_name))
```

`ForeignKeyConstraint.__post_init__`: wrap before sorting, sort by `.key`, plain duplicate checks:

```python
        pairs = sorted(
            zip(
                (Identifier(column) for column in self.local_columns),
                (Identifier(column) for column in self.referenced_columns),
                strict=True,
            ),
            key=lambda pair: pair[0].key,
        )
        object.__setattr__(self, "local_columns", tuple(pair[0] for pair in pairs))
        object.__setattr__(self, "referenced_columns", tuple(pair[1] for pair in pairs))

        seen_local: set[str] = set()
        for column in self.local_columns:
            if column in seen_local:
                raise ValueError(f"Duplicate foreign key local column: {column}")
            seen_local.add(column)

        seen_referenced: set[str] = set()
        for column in self.referenced_columns:
            if column in seen_referenced:
                raise ValueError(f"Duplicate foreign key referenced column: {column}")
            seen_referenced.add(column)

        if not self.constraint_name.strip():
            raise ValueError("constraint_name must not be blank")
        object.__setattr__(self, "constraint_name", Identifier(self.constraint_name))
```

(Keep the length-mismatch check that precedes this code unchanged. The explicit `key=lambda pair: pair[0].key` matters: inherited `str` ordering is spelling-sensitive, and the canonical pair order must be identity-based.)

`ForeignKeyConstraint.signature` reverts to the plain tuple (Identifier equality makes it case-insensitive):

```python
    @property
    def signature(self) -> tuple[tuple[str, ...], QualifiedName, tuple[str, ...]]:
        """
        Content identity: local columns, referenced table, referenced columns.

        Column entries are Identifiers, so a desired constraint and a
        catalog-observed one compare equal across display casing. Excludes
        ``constraint_name`` so generated and catalog names still match by
        content.
        """
        return (self.local_columns, self.referenced_table, self.referenced_columns)
```

`ForeignKeyReference.__post_init__`: after the blank check, add `object.__setattr__(self, "constraint_name", Identifier(self.constraint_name))`.

- [ ] **Step 4: Run constraint and table tests, then the full suite**

Run: `uv run pytest tests/domain/model/ -q` then `uv run pytest -q`
Expected: PASS

- [ ] **Step 5: Commit**

```bash
git add src/delta_engine/domain/model/constraints.py tests/domain/model/test_foreign_key.py
git commit -m "refactor: store constraint columns and names as Identifiers"
```

---

### Task 5: Tables wrap layout tuples; validation reverts to plain membership

**Files:**
- Modify: `src/delta_engine/domain/model/table.py`
- Test: `tests/domain/model/test_table.py`

**Interfaces:**
- Consumes: Identifier-carrying columns (Task 2) and constraints (Task 4).
- Produces: `DesiredTable.partitioned_by`/`clustered_by` and `ObservedTable.partitioned_by`/`clustered_by` are tuples of `Identifier`. `_validate_key_column_list(kind, names, column_names: set[str])` takes the plain set of column-name Identifiers.

- [ ] **Step 1: Write the failing test**

Append to `tests/domain/model/test_table.py` (build helpers as the existing tests in that file do):

```python
def test_layout_references_preserve_spelling_and_resolve_case_insensitively() -> None:
    table = DesiredTable(
        qualified_name=QualifiedName("cat", "sch", "tbl"),
        columns=(DesiredColumn(name="RequestId", data_type=String()),),
        clustered_by=("REQUESTID",),
    )
    assert table.clustered_by[0].spelling == "REQUESTID"
```

- [ ] **Step 2: Run test to verify it fails**

Run: `uv run pytest tests/domain/model/test_table.py -q`
Expected: FAIL — `'str' object has no attribute 'spelling'`

- [ ] **Step 3: Implement**

In `table.py`, replace the `identifier_key` import with `Identifier`, then revert every keyed site to main's plain shape while wrapping at construction:

- Both `__post_init__` methods: `object.__setattr__(self, "partitioned_by", tuple(Identifier(n) for n in self.partitioned_by))` and the same for `clustered_by` (replacing the branch's bare `tuple(...)` copies).
- `_validate_key_column_list` reverts to plain membership (rename the parameter back to `column_names`):

```python
def _validate_key_column_list(kind: str, names: tuple[str, ...], column_names: set[str]) -> None:
    """Rules shared by partition and clustering key lists: existing and unique."""
    missing = [name for name in names if name not in column_names]
    if missing:
        raise ValueError(f"{kind} column not found: {', '.join(missing)}")

    seen: set[str] = set()
    for name in names:
        if name in seen:
            raise ValueError(f"Duplicate {kind.lower()} column: {name}")
        seen.add(name)
```

- `_validate_table_structure`: build `column_names = {column.name for column in columns}` (a set of Identifiers), plain `if column.name in seen_names` duplicate loop, plain `name not in column_names` for the primary-key and foreign-key missing checks. All `identifier_key(...)` wrapping disappears; the code returns to main's original form.
- `DesiredTable.__post_init__` duplicate-FK/nullable-PK/rename checks: `frozenset(foreign_key.local_columns)`, `set(self.primary_key.columns)`, `{column.name for column in self.columns}`, plain `source in declared_names` / `source in rename_sources` — again main's original form (Identifier equality does the keying).

Note: `_validate_key_column_list` is called with `names` that were wrapped by `__post_init__` *before* validation runs — keep the wrap statements above the `_validate_table_structure(...)` call.

- [ ] **Step 4: Run the module tests, then the full suite**

Run: `uv run pytest tests/domain/model/test_table.py -q` then `uv run pytest -q`
Expected: PASS

- [ ] **Step 5: Commit**

```bash
git add src/delta_engine/domain/model/table.py tests/domain/model/test_table.py
git commit -m "refactor: wrap table layout references as Identifiers"
```

---

### Task 6: Actions wrap their name fields

**Files:**
- Modify: `src/delta_engine/domain/plan/actions.py`
- Test: `tests/domain/plan/test_actions.py`

**Interfaces:**
- Consumes: `Identifier` from Task 1.
- Produces: `RenameColumn.old_name`/`new_name`, `column_name` on `SetColumnComment`/`SetColumnTag`/`UnsetColumnTag`/`SetColumnNullability`/`AlterColumnType`, and `AlterClustering.desired_clustering`/`observed_clustering` elements are `Identifier`s. `_execution_order` returns `(action.phase, action.subject.lower(), action.subject)`.

- [ ] **Step 1: Write the failing test**

Append to `tests/domain/plan/test_actions.py`:

```python
def test_case_only_rename_carries_no_difference_even_from_raw_strings() -> None:
    with pytest.raises(ValueError, match="carries no difference"):
        RenameColumn(old_name="requestid", new_name="REQUESTID")


def test_case_variant_clustering_carries_no_difference_even_from_raw_strings() -> None:
    with pytest.raises(ValueError, match="carries no difference"):
        AlterClustering(desired_clustering=("A", "b"), observed_clustering=("a", "B"))
```

(These pass on the branch via `identifier_key`; they must **stay** green through the refactor — they pin that wrapping in `__post_init__`, not caller discipline, provides the guarantee. If the branch already has equivalent tests, skip adding duplicates and just identify them.)

- [ ] **Step 2: Establish green, then implement**

Run: `uv run pytest tests/domain/plan/test_actions.py -q` (expected PASS — this task is a behaviour-preserving mechanism swap; the tests guard the swap).

In `actions.py`, replace the `identifier_key` import with `Identifier` (and drop `canonical_data_type` if Task 3 left the import) then:

- `RenameColumn.__post_init__`:

```python
    def __post_init__(self) -> None:
        object.__setattr__(self, "old_name", Identifier(self.old_name))
        object.__setattr__(self, "new_name", Identifier(self.new_name))
        if self.old_name == self.new_name:
            raise ValueError(f"RenameColumn carries no difference: {self.old_name!r}")
```

- Add a one-line `__post_init__` wrap `object.__setattr__(self, "column_name", Identifier(self.column_name))` at the **top** of the existing `__post_init__` of `SetColumnComment`, `SetColumnTag`, `SetColumnNullability`, and `AlterColumnType`; add a new `__post_init__` doing only that wrap to `UnsetColumnTag`.
- `AlterClustering.__post_init__`:

```python
    def __post_init__(self) -> None:
        object.__setattr__(
            self, "desired_clustering", tuple(Identifier(n) for n in self.desired_clustering)
        )
        object.__setattr__(
            self, "observed_clustering", tuple(Identifier(n) for n in self.observed_clustering)
        )
        if set(self.desired_clustering) == set(self.observed_clustering):
            raise ValueError(f"AlterClustering carries no difference: {self.desired_clustering!r}")
```

- `_execution_order`:

```python
def _execution_order(action: Action) -> tuple[int, str, str]:
    """Deterministic ordering key: phase, lowercased subject, then exact subject."""
    return (action.phase, action.subject.lower(), action.subject)
```

(Subjects mix column Identifiers with case-sensitive property/tag names; plain `.lower()` gives both a stable primary sort without importing identifier policy.)

- [ ] **Step 3: Run the plan tests, then the full suite**

Run: `uv run pytest tests/domain/plan/ -q` then `uv run pytest -q`
Expected: PASS

- [ ] **Step 4: Commit**

```bash
git add src/delta_engine/domain/plan/actions.py tests/domain/plan/test_actions.py
git commit -m "refactor: wrap action name fields as Identifiers"
```

---

### Task 7: Diff reverts to plain collections

**Files:**
- Modify: `src/delta_engine/domain/plan/diff.py`
- Test: `tests/domain/plan/test_diff.py` (existing mixed-case tests are the net; no new tests)

**Interfaces:**
- Consumes: Identifier-carrying domain objects (Tasks 2, 4, 5, 6).
- Produces: `_align_columns`, `_resolve_column_renames`, `_project_names`, `_diff_layout` in main's original plain-collection shape; matched-column actions still emit `observed.name`. Drops the `identifier_key`/`index_by_identifier` imports.

- [ ] **Step 1: Establish green**

Run: `uv run pytest tests/domain/plan/test_diff.py -q`
Expected: PASS — this task must not change any observable diff behaviour; the branch's mixed-case diff tests (case-only no-op, observed-spelling emission, keyed renames, layout identity) are the safety net.

- [ ] **Step 2: Revert the keyed machinery**

In `diff.py`, drop `identifier_key`/`index_by_identifier` (and `canonical_data_type`, removed in Task 3) from the `domain.model` import, then:

`_resolve_column_renames` becomes (plain dicts of Identifiers; observed spelling still flows into `RenameColumn` and conflicts):

```python
def _resolve_column_renames(desired: DesiredTable, observed: ObservedTable) -> _RenameResolution:
    """Resolve applicable rename hints and project rename-preserved observed state."""
    rename_targets_by_source = {
        column.renamed_from: column
        for column in desired.columns
        if column.renamed_from is not None
    }
    observed_by_name = {column.name: column for column in observed.columns}
    new_names_by_old: dict[str, str] = {}
    conflicted_sources: set[str] = set()
    actions: list[RenameColumn] = []
    conflicts: list[ColumnRenameConflict] = []

    for old_name, target in rename_targets_by_source.items():
        observed_column = observed_by_name.get(old_name)
        if observed_column is None:
            continue

        if target.name in observed_by_name:
            conflicted_sources.add(old_name)
            conflicts.append(
                ColumnRenameConflict(old_name=observed_column.name, new_name=target.name)
            )
            continue

        new_names_by_old[old_name] = target.name
        actions.append(RenameColumn(old_name=observed_column.name, new_name=target.name))

    projected_columns = tuple(
        replace(column, name=new_names_by_old[column.name])
        if column.name in new_names_by_old
        else column
        for column in observed.columns
        if column.name not in conflicted_sources
    )
    return _RenameResolution(
        columns=projected_columns,
        partitioned_by=_project_names(observed.partitioned_by, new_names_by_old),
        clustered_by=_project_names(observed.clustered_by, new_names_by_old),
        actions=tuple(actions),
        conflicts=tuple(conflicts),
    )
```

`_project_names` reverts to:

```python
def _project_names(names: tuple[str, ...], renames: Mapping[str, str]) -> tuple[str, ...]:
    """Project column names through the applied rename mapping."""
    return tuple(renames.get(name, name) for name in names)
```

`_align_columns` reverts to plain dicts (Identifier hashing does the keying; both tables pre-validate duplicate names, so plain comprehensions cannot silently collide):

```python
    desired_by_name = {column.name: column for column in desired_columns}
    observed_by_name = {column.name: column for column in observed_columns}

    added = tuple(column for column in desired_columns if column.name not in observed_by_name)
    removed = tuple(column for column in observed_columns if column.name not in desired_by_name)
    matched = tuple(
        (column, observed_by_name[column.name])
        for column in desired_columns
        if column.name in observed_by_name
    )
```

`_diff_layout` reverts to plain set/tuple comparison:

```python
    actions: tuple[AlterClustering, ...] = ()
    if set(desired.clustered_by) != set(observed.clustered_by):
        ...
    unresolvable: tuple[PartitioningChanged, ...] = ()
    if desired.partitioned_by != observed.partitioned_by:
        ...
```

(keeping the action constructions inside each branch exactly as they are). `_diff_existing_column` and `_diff_column_tags` keep `column_name=observed.name` — do not touch the emission side.

- [ ] **Step 3: Run diff tests, then the full suite**

Run: `uv run pytest tests/domain/plan/test_diff.py -q` then `uv run pytest -q`
Expected: PASS

- [ ] **Step 4: Commit**

```bash
git add src/delta_engine/domain/plan/diff.py
git commit -m "refactor: diff through plain Identifier collections"
```

---

### Task 8: Resulting schema and planning probe with Identifiers

**Files:**
- Modify: `src/delta_engine/domain/plan/resulting_schema.py`
- Modify: `src/delta_engine/application/planning.py`
- Test: `tests/domain/plan/test_resulting_schema.py`, `tests/application/test_planning.py` (existing tests are the net)

**Interfaces:**
- Consumes: Identifier-carrying diffs and actions.
- Produces: `resulting_column_spellings(diff: TableDiff) -> dict[Identifier, Identifier]`; `type ResultingSchemas = Mapping[QualifiedName, Mapping[Identifier, Identifier]]`; `plan_diff(diff, resulting_schemas)` signature otherwise unchanged; `_own_spelling(own, name)` and `_parent_spelling(parent, name)` probe directly with the constraint's Identifier.

- [ ] **Step 1: Establish green**

Run: `uv run pytest tests/domain/plan/test_resulting_schema.py tests/application/test_planning.py -q`
Expected: PASS — behaviour-preserving mechanism swap; the branch's binding tests are the net.

- [ ] **Step 2: Implement**

`resulting_schema.py` — replace `identifier_key` usage with direct Identifier keys and update the annotation:

```python
from delta_engine.domain.model import Identifier
from delta_engine.domain.plan.actions import RenameColumn
from delta_engine.domain.plan.diff import TableDiff, TableDrift, TableMissing


def resulting_column_spellings(diff: TableDiff) -> dict[Identifier, Identifier]:
    """Map each column's identity to its exact post-sync spelling."""
    match diff:
        case TableMissing(desired=desired):
            return {column.name: column.name for column in desired.columns}
        case TableDrift() as drift:
            return _drift_spellings(drift)
        case _ as unreachable:
            assert_never(unreachable)


def _drift_spellings(drift: TableDrift) -> dict[Identifier, Identifier]:
    """Resolve matched columns to observed spelling, renames and adds to desired."""
    observed_by_name = {column.name: column.name for column in drift.observed.columns}
    rename_targets = {
        action.new_name for action in drift.actions if isinstance(action, RenameColumn)
    }

    spellings: dict[Identifier, Identifier] = {}
    for column in drift.desired.columns:
        if column.name in rename_targets or column.name not in observed_by_name:
            spellings[column.name] = column.name
        else:
            spellings[column.name] = observed_by_name[column.name]
    return spellings
```

(The `observed_by_name` dict maps each name to itself so that probing with an equal-but-differently-spelled Identifier returns the stored **value**, which carries the observed spelling — that is the whole trick, and `test_resulting_schema.py` already pins it.)

`planning.py` — update the type alias and helpers; everything else stays:

```python
type ResultingSchemas = Mapping[QualifiedName, Mapping[Identifier, Identifier]]
```

```python
def _own_spelling(own: Mapping[Identifier, Identifier], name: str) -> Identifier:
    """Resolve an own-table reference; a miss is an engine invariant violation."""
    spelling = own.get(Identifier(name))
    if spelling is None:
        raise RuntimeError(
            f"Accepted action references no resulting column: {name!r}."
            " Declaration validation makes this unreachable short of an engine defect."
        )
    return spelling


def _parent_spelling(parent: Mapping[Identifier, Identifier] | None, name: str) -> str:
    """
    Resolve a foreign key's referenced column to the parent's post-sync spelling.

    An unregistered, read-failed, or divergent parent legitimately cannot
    bind, so any miss falls back to the declared spelling: the child still
    compiles preview SQL, and dependency resolution owns classifying the
    failure and blocking execution.
    """
    if parent is None:
        return name
    return parent.get(Identifier(name), name)
```

Import `Identifier` in `planning.py` in place of `identifier_key`. The `Identifier(name)` wraps are the boundary-probe rule applied: constraint fields are already Identifiers (Task 4), so the wrap is an idempotent no-op that also keeps mypy's `Mapping[Identifier, ...]` probe types exact. `_bind_actions`, `_bind_action`, and `_bind_created_table` are unchanged.

- [ ] **Step 3: Run the tests, then the full suite**

Run: `uv run pytest tests/domain/plan/test_resulting_schema.py tests/application/ -q` then `uv run pytest -q`
Expected: PASS

- [ ] **Step 4: Commit**

```bash
git add src/delta_engine/domain/plan/resulting_schema.py src/delta_engine/application/planning.py
git commit -m "refactor: bind plans through Identifier-keyed resulting schemas"
```

---

### Task 9: API layer and dependency resolution — wrap at the raw-string boundary

**Files:**
- Modify: `src/delta_engine/api/delta_table.py`
- Modify: `src/delta_engine/application/dependency_resolution.py`
- Test: `tests/api/test_delta_table.py` (existing mixed-case tests are the net)

**Interfaces:**
- Consumes: `Identifier`; Identifier-carrying domain objects.
- Produces: no `identifier_key`/`index_by_identifier` usage outside `identifier.py`. Every site where **raw user input** probes a domain-valued collection wraps the raw side in `Identifier(...)`. This task is the complete enumeration of those sites.

- [ ] **Step 1: Establish green**

Run: `uv run pytest tests/api/test_delta_table.py tests/application/ -q`
Expected: PASS — behaviour-preserving; the branch's mixed-case API tests are the net.

- [ ] **Step 2: Implement `delta_table.py`**

Replace the `canonical_data_type, identifier_key, index_by_identifier` imports with `Identifier`, then:

- `_foreign_key_constraint_name`: `columns = "_".join(sorted(column.lower() for column in local_columns))` (raw user strings; the generated physical name stays deterministic lowercase — behaviour identical).
- `_validate_layout`: `columns_by_name = {column.name: column for column in columns}` (Identifier keys) and probe with wrapped raw input: `column = columns_by_name.get(Identifier(name))` in both loops. The partition-by-every-column guard wraps its probe side:

```python
    partition_names = {Identifier(name) for name in partitioned_by}
    if (
        partitioned_by
        and partition_names <= columns_by_name.keys()
        and len(partition_names) == len(columns)
    ):
```

- `_validate_column_names` CDF check: make the reserved set Identifier-valued at module scope — `_CDF_RESERVED_COLUMN_NAMES = frozenset(Identifier(name) for name in (...))` with the existing literal names — and revert the probe to `if column.name in _CDF_RESERVED_COLUMN_NAMES`.
- `ForeignKey._validate`: `local_types = {column.name: column.data_type for column in owner_columns}` probed with `local_types.get(Identifier(local_name))`; `referenced.column_types[Identifier(referenced_name)]` (raw user pair names probing Identifier-keyed dicts).
- `ForeignKey._referenced_side`: `types = {column.name: column.data_type for column in ...}` in both arms (plain comprehensions; keys are already Identifiers).
- `ForeignKey._pair_columns` same-name check: `if {Identifier(c) for c in local_columns} == {Identifier(c) for c in parent_columns}:` (both sides are raw user strings here).
- `_normalize_declaration` and `ForeignKey.__post_init__` are already verbatim on this branch — no change.

- [ ] **Step 3: Implement `dependency_resolution.py`**

Replace the `canonical_data_type, identifier_key` import with nothing extra (no `Identifier` needed):

- `_foreign_key_types_match`: `local_types[local_column] == referenced_types[referenced_column]` — plain probes; both the dict keys (from `column.name`) and the probe values (constraint columns, Task 4) are Identifiers.
- `_classify_failures`: `{column.name: column.data_type for column in table.columns}` — plain comprehension.

- [ ] **Step 4: Run API and application tests, then the full suite**

Run: `uv run pytest tests/api/ tests/application/ -q` then `uv run pytest -q`
Expected: PASS

- [ ] **Step 5: Commit**

```bash
git add src/delta_engine/api/delta_table.py src/delta_engine/application/dependency_resolution.py
git commit -m "refactor: wrap raw declaration input at the Identifier boundary"
```

---

### Task 10: Adapters key catalog rows by Identifier

**Files:**
- Modify: `src/delta_engine/adapters/databricks/sql/rows.py`
- Modify: `src/delta_engine/adapters/databricks/read.py`
- Test: `tests/adapters/databricks/sql/test_rows.py`, `tests/adapters/databricks/test_read.py` (existing mixed-case tag tests are the net)

**Interfaces:**
- Consumes: `Identifier`.
- Produces: `read_column_tags` returns a mapping keyed by `Identifier(row.column_name)`; `_read_observed_table` probes it with `column.name` directly.

- [ ] **Step 1: Establish green**

Run: `uv run pytest tests/adapters/ -q`
Expected: PASS

- [ ] **Step 2: Implement**

`rows.py` — import `Identifier` instead of `identifier_key`; in `read_column_tags`:

```python
    grouped: dict[str, dict[str, str]] = {}
    for row in run_query(column_tags_query(qualified_name)):
        grouped.setdefault(Identifier(row.column_name), {})[row.tag_name] = row.tag_value
```

(Keys are Identifiers built from raw catalog strings — the boundary-probe rule from the adapter side. Update the function and module docstrings: the dict is keyed by `Identifier`, so any spelling of the column probes it.)

`read.py` — drop the `identifier_key` import and revert the probe:

```python
    tagged_columns = tuple(
        replace(column, tags=column_tags.get(column.name, MappingProxyType({})))
        for column in description.columns
    )
```

(`column.name` is an Identifier; the dict keys are Identifiers; hashing matches by identity.)

- [ ] **Step 3: Run adapter tests, then the full suite**

Run: `uv run pytest tests/adapters/ -q` then `uv run pytest -q`
Expected: PASS

- [ ] **Step 4: Commit**

```bash
git add src/delta_engine/adapters/databricks/sql/rows.py src/delta_engine/adapters/databricks/read.py
git commit -m "refactor: key catalog column tags by Identifier"
```

---

### Task 11: Delete `identifier_key` and `index_by_identifier`

**Files:**
- Modify: `src/delta_engine/domain/model/identifier.py` (delete both functions; `Identifier` remains)
- Modify: `src/delta_engine/domain/model/__init__.py` (drop both from import and `__all__`)
- Test: `tests/domain/model/test_identifier.py` (delete their tests)

**Interfaces:**
- Consumes: Tasks 2–10 having removed every consumer.
- Produces: `identifier.py` contains only `Identifier`. `domain.model` exports `Identifier` and no longer exports `identifier_key`, `index_by_identifier`, or `canonical_data_type`.

- [ ] **Step 1: Verify no consumers remain**

Run: `rg -n "identifier_key|index_by_identifier" src/ tests/ --glob '!src/delta_engine/domain/model/identifier.py' --glob '!tests/domain/model/test_identifier.py'`
Expected: no matches. If any match appears, fix that site first using the pattern of its task above (plain comparison, or `Identifier(...)` at a raw boundary) — do not proceed with dangling consumers.

- [ ] **Step 2: Delete**

Remove both functions from `identifier.py` (keep the module docstring, rewritten around the type: "The engine stores spelling verbatim; `Identifier` carries case-insensitive identity. This module is the only place that canonicalization lives."). Remove their tests from `test_identifier.py`. Update `domain/model/__init__.py` imports and `__all__`.

- [ ] **Step 3: Run the full suite**

Run: `uv run pytest -q`
Expected: PASS

- [ ] **Step 4: Commit**

```bash
git add src/delta_engine/domain/model/identifier.py src/delta_engine/domain/model/__init__.py tests/domain/model/test_identifier.py
git commit -m "refactor: delete identifier_key helpers superseded by Identifier"
```

---

### Task 12: Restore exact-spelling pins in tests

Case-insensitive equality means an assertion like `assert table.primary_key == ("requestId",)` now passes for *any* casing — spelling pins written on this branch have silently weakened. Restore their strength with `.spelling`.

**Files:**
- Modify: `tests/api/test_delta_table.py`, `tests/application/test_planning.py`, `tests/domain/plan/test_resulting_schema.py`, `tests/domain/plan/test_diff.py`, `tests/domain/model/*` — wherever a test's *purpose* is pinning exact case.
- SQL-text assertions (`tests/adapters/databricks/sql/test_compile.py`, `tests/live/*`) need **no** change: compiled statements are plain strings and already compare case-sensitively.

- [ ] **Step 1: Enumerate candidate assertions**

Run: `rg -n '== \(?"[^"]*[A-Z]' tests/ --glob '!tests/live/*' --glob '!tests/adapters/databricks/sql/test_compile.py'` and review each hit, plus every test whose name mentions case, spelling, or preserved (`rg -ln "case|spelling|preserv" tests/`).

- [ ] **Step 2: Strengthen each spelling pin**

Conversion rule, applied only where the test exists to pin case:

```python
# Weak since Identifier (passes for any casing):
assert action.primary_key.columns == ("requestId",)
# Strong:
assert tuple(column.spelling for column in action.primary_key.columns) == ("requestId",)
```

and for scalars: `assert column.name.spelling == "requestId"`. Membership/identity assertions (`in`, dict lookups, no-op convergence) stay as they are — insensitive is exactly what they assert.

- [ ] **Step 3: Prove the pins bite**

Temporarily break spelling preservation (e.g. in `DesiredColumn.__post_init__`, wrap with `Identifier(self.name.lower())`), run `uv run pytest tests/ -q -x`, and confirm at least one strengthened test **fails**. Revert the sabotage. This guards against a sweep that converted nothing.

- [ ] **Step 4: Run the full suite**

Run: `uv run pytest -q`
Expected: PASS

- [ ] **Step 5: Commit**

```bash
git add tests/
git commit -m "test: pin exact spelling through Identifier.spelling"
```

---

### Task 13: Docs, gates, and live verification

**Files:**
- Modify: `docs/explanation-architecture.md`, `docs/reference-limitations.md` (rewrite `identifier_key` mentions around `Identifier`; the identity *semantics* described there are unchanged)
- Modify: `docs/todo/2026-07-24-column-identifier-spelling-design.md` — append a short "Amendment (2026-07-26)" note: the implementation triggered the design's own escape hatch ("unless implementation shows raw strings cannot maintain the invariants"); `Identifier` replaces `identifier_key`/`index_by_identifier`/`canonical_data_type`.

- [ ] **Step 1: Update the docs**

Run `rg -n "identifier_key|index_by_identifier|canonical_data_type" docs/ --glob '!docs/todo/2026-07-2*'` and rewrite each live-doc mention. Add the design-doc amendment note.

- [ ] **Step 2: Run every gate**

```bash
uv run ruff format . && uv run ruff check . && uv run mypy . && uv run lint-imports
uv run pytest -q
uv run --group docs sphinx-build -W -b html docs docs/_build/html
git diff --check
```

Expected: all pass; coverage at or above the configured gate. Fix anything that fails before continuing.

- [ ] **Step 3: Commit and push**

```bash
git add -A
git commit -m "docs: describe Identifier as the single identity mechanism"
git push
```

- [ ] **Step 4: Re-run the live Databricks suite**

```bash
gh workflow run live.yaml --ref fix/preserve-column-identifier-case
gh run watch $(gh run list --workflow=live.yaml --branch fix/preserve-column-identifier-case --limit 1 --json databaseId --jq '.[0].databaseId')
```

Expected: green, including the camelCase primary-key, exact-spelling foreign-key, and convergence pins.

- [ ] **Step 5: Update the PR description**

Amend the PR #287 body (`gh pr edit 287 --body-file ...`): replace the `identifier_key` mechanism description with the `Identifier` type, and add the new live-run link under Validation.

---

## Self-review notes

- **Spec coverage:** every `identifier_key`/`index_by_identifier` consumer found by `rg` on the branch (diff, delta_table, table, constraints, resulting_schema, actions, dependency_resolution, data_type, planning, rows, column, read) has a named task; `canonical_data_type`'s four consumers are enumerated in Task 3; deletion is gated by the Task 11 grep.
- **Ordering:** consumers migrate before helpers are deleted; the suite is green at every commit boundary; behaviour-preserving tasks (6–10) lean on the branch's own mixed-case tests as their net, which is why Task 12's sabotage step exists — it proves the net still has teeth after the equality semantics changed.
- **Type consistency:** `resulting_column_spellings -> dict[Identifier, Identifier]` (Task 8) matches `ResultingSchemas = Mapping[QualifiedName, Mapping[Identifier, Identifier]]` consumed by `plan_diff`/`_own_spelling`/`_parent_spelling`; `Identifier.key`/`.spelling` (Task 1) are the only accessors later tasks use.
- **Known risk, accepted:** mixed collections of raw `str` and `Identifier` mis-bucket (`hash("A") != hash(Identifier("A"))`). The wrap-at-construction rule plus the enumerated boundary probes (Tasks 9, 10) keep collections homogeneous; no site outside those two files builds a name collection from raw strings.
