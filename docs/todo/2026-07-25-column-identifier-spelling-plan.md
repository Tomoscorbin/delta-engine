# Preserve column identifier spelling — implementation plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Column-like identifiers keep their declared/catalog spelling end to end; lowercase survives only as an explicit identity key; executable plans carry exact post-sync physical spelling, so `ADD CONSTRAINT … PRIMARY KEY (\`requestId\`)` compiles with the catalog's spelling and the live PK reproduction passes without manual SQL.

**Architecture:** Three phases keep every commit green. First, add the identity helpers (`identifier_key`, `index_by_identifier`, `canonical_data_type`) and convert every lookup, signature, comparison, and generated name to explicit identity keys **while constructors still lowercase** — pure no-op refactors. Second, build the resulting-schema index and the planning-boundary binder for symbolic references (`SetPrimaryKey`, `SetForeignKey`, `AlterClustering`, `CreateTable`-internal) — also a no-op while everything is lowercase. Third, remove constructor lowercasing in one task and land the mixed-case behavior matrix; because every consumer is already identity-keyed, the flip is contained. Live-test inversion, docs, and the full validation ladder close it out.

**Tech Stack:** Python 3.12, uv, pytest (+hypothesis), ruff, mypy, import-linter, Sphinx/MyST. No new dependencies.

**Spec:** `docs/todo/2026-07-24-column-identifier-spelling-design.md` (accepted 2026-07-25, including the dated decision notes on binding placement and failure scoping). Where this plan states a Databricks fact, the spec and the live reproduction module are its source.

## Global Constraints

- Branch: `fix/preserve-column-identifier-case` (PR #287 grows into the implementation PR). Never commit to `main`. Force-push is hook-blocked; refresh by merging from `main` if needed.
- Use `uv run …` for every command. Line length 100. Absolute imports only. Type hints on all function signatures. Ruff pydocstyle (`D`) is enabled for `src`: every new public module/class/function needs a docstring; docstrings describe what the code does — design rationale lives in the spec.
- A PostToolUse autofix hook strips unused imports after every edit: always add an import in the same edit as the code that uses it.
- Coverage runs by default (`fail_under = 70`); focused runs use `-q --no-cov`. The full local suite runs without Databricks credentials (live tests are deselected by the default `-m "not databricks_e2e"` filter).
- **Do not dispatch the Live workflow between Task 10 and Task 12.** In that window the carried live pin (engine PK addition fails) contradicts the new engine behavior. Dispatch only after Task 12 lands the inverted test.
- Conventional commit messages (commitizen generates the changelog from them at release — there is no manual CHANGELOG edit in this plan). No `Co-authored-by` trailers.
- Sequencing invariant from the spec: never land a state that removes constructor `.lower()` while any exact-string lookup still depends on it. Tasks 1–9 convert lookups first; only Task 10 removes the lowering.

## File structure

| File | Responsibility in this change |
| --- | --- |
| `src/delta_engine/domain/model/identifier.py` | New: `identifier_key`, `index_by_identifier` — the only canonicalization site |
| `src/delta_engine/domain/model/data_type.py` | `canonical_data_type`; `StructField` preserves spelling; `Struct` duplicate check by identity |
| `src/delta_engine/domain/model/constraints.py` | Identity-keyed `key_signature` and FK signature/sort/duplicates; spelling preserved |
| `src/delta_engine/domain/model/column.py` | Names and `renamed_from` preserved; identity-keyed self-rename check |
| `src/delta_engine/domain/model/table.py` | Identity-keyed structural validation; layout tuples preserved |
| `src/delta_engine/domain/model/__init__.py` | Export `identifier_key`, `index_by_identifier`, `canonical_data_type` |
| `src/delta_engine/domain/plan/diff.py` | Identity-keyed alignment/renames/layout/type comparison; matched actions carry projected observed spelling; `RenameColumn` carries observed source |
| `src/delta_engine/domain/plan/actions.py` | Identity-keyed ordering and no-difference invariants |
| `src/delta_engine/domain/plan/resulting_schema.py` | New: `resulting_column_spellings(diff)` — post-sync spelling index |
| `src/delta_engine/domain/plan/__init__.py` | Export `resulting_column_spellings` |
| `src/delta_engine/application/planning.py` | `plan_diff(diff, resulting_schemas)`; binder for symbolic references with scoped failure |
| `src/delta_engine/application/engine.py` | Build the sync-wide resulting-schema index between diff and plan phases |
| `src/delta_engine/application/dependency_resolution.py` | Identity-keyed type maps; canonical type comparison |
| `src/delta_engine/api/delta_table.py` | Identity-keyed lookups; generated FK names canonical; declarations preserved |
| `src/delta_engine/adapters/databricks/sql/rows.py` | Column-tag grouping keyed by `identifier_key` |
| `src/delta_engine/adapters/databricks/read.py` | Tag attachment probes by identity key |
| `tests/live/test_sql_warehouse_live_column_case_repro.py` | PK repro inverted to engine success; new FK repro |
| `tests/live/job_summary.py` | Updated area blurb |
| `docs/reference-limitations.md` | Identifier-handling section rewritten (spelling/identity/execution) |
| Unit test files | `tests/domain/model/test_identifier.py` (new), `test_data_type.py`, `test_primary_key.py`, `test_foreign_key.py`, `test_column.py`, `test_table.py`, `tests/domain/plan/test_resulting_schema.py` (new), `test_diff.py`, `test_actions.py`, `tests/application/test_planning.py`, `test_engine.py`, `tests/api/test_delta_table.py`, `tests/adapters/databricks/sql/test_compile.py` |

---

### Task 1: Identifier policy module

**Files:**

- Create: `src/delta_engine/domain/model/identifier.py`
- Modify: `src/delta_engine/domain/model/__init__.py`
- Test: `tests/domain/model/test_identifier.py` (new)

**Interfaces:**

- Produces: `identifier_key(name: str) -> str`; `index_by_identifier[T](items: Iterable[T], name_of: Callable[[T], str]) -> dict[str, T]` (raises `ValueError` on a case-insensitive duplicate). Both importable as `from delta_engine.domain.model import identifier_key, index_by_identifier`. Every later task consumes these names.

- [ ] **Step 1: Write the failing tests**

Create `tests/domain/model/test_identifier.py`:

```python
import pytest

from delta_engine.domain.model import identifier_key, index_by_identifier


def test_identifier_key_lowercases_ascii():
    assert identifier_key("RequestId") == "requestid"


def test_identifier_key_preserves_already_lowercase_unicode():
    # 'straße' is already lowercase; casefold would rewrite it to 'strasse',
    # a different identifier from the one Unity Catalog stores.
    assert identifier_key("straße") == "straße"


def test_identifier_key_uses_lower_not_casefold():
    # lower() keeps 'ß'; casefold() would expand it to 'ss' and silently
    # change identity semantics.
    assert identifier_key("GRÖßE") == "größe"


def test_index_by_identifier_keys_items_by_identity_and_keeps_them():
    index = index_by_identifier(["RequestId", "amount"], name_of=lambda item: item)

    assert index == {"requestid": "RequestId", "amount": "amount"}


def test_index_by_identifier_rejects_case_insensitive_duplicates():
    with pytest.raises(ValueError, match="Duplicate identifier"):
        index_by_identifier(["requestId", "REQUESTID"], name_of=lambda item: item)
```

- [ ] **Step 2: Run them to verify they fail**

Run: `uv run pytest tests/domain/model/test_identifier.py -q --no-cov`
Expected: FAIL — `ImportError: cannot import name 'identifier_key'`.

- [ ] **Step 3: Implement**

Create `src/delta_engine/domain/model/identifier.py`:

```python
"""Identifier identity policy for column-like names.

Databricks resolves column-like identifiers case-insensitively while
preserving their display spelling. The engine stores spelling verbatim and
derives an explicit lowercase identity key wherever two identifiers must be
judged the same column. This module is the only place that canonicalization
lives.
"""

from collections.abc import Callable, Iterable


def identifier_key(name: str) -> str:
    """
    Return the Databricks identity key without changing stored spelling.

    Uses ``str.lower``, deliberately not ``str.casefold``: the live
    object-name pin distinguishes Python lowercasing from casefolding, and
    identifier identity must not silently adopt new Unicode semantics.
    """
    return name.lower()


def index_by_identifier[T](items: Iterable[T], name_of: Callable[[T], str]) -> dict[str, T]:
    """
    Index ``items`` by identifier key, rejecting case-insensitive duplicates.

    A silent duplicate would let the later value win and hide a real
    identity collision, so a collision raises ``ValueError`` naming both
    spellings.
    """
    index: dict[str, T] = {}
    for item in items:
        key = identifier_key(name_of(item))
        if key in index:
            raise ValueError(
                f"Duplicate identifier: {name_of(item)!r} collides with"
                f" {name_of(index[key])!r}"
            )
        index[key] = item
    return index
```

In `src/delta_engine/domain/model/__init__.py`, add the import (alphabetical between the `data_type` and `qualified_name` blocks):

```python
from delta_engine.domain.model.identifier import identifier_key, index_by_identifier
```

and add `"identifier_key",` and `"index_by_identifier",` to `__all__` (alphabetical: after `"__..."` entries ending with `"key_signature"` — exact positions: `"identifier_key"` after `"ForeignKeyReference"`… place both alphabetically; `ruff` will flag misordering).

- [ ] **Step 4: Run the tests**

Run: `uv run pytest tests/domain/model/test_identifier.py -q --no-cov`
Expected: PASS (5 tests).

- [ ] **Step 5: Commit**

```bash
git add src/delta_engine/domain/model/identifier.py src/delta_engine/domain/model/__init__.py tests/domain/model/test_identifier.py
git commit -m "feat: add the identifier identity-key policy module"
```

---

### Task 2: Semantic data-type identity

**Files:**

- Modify: `src/delta_engine/domain/model/data_type.py` (Struct duplicate check ~line 136; new function at end of file)
- Modify: `src/delta_engine/domain/model/__init__.py`
- Test: `tests/domain/model/test_data_type.py`

**Interfaces:**

- Consumes: `identifier_key` (Task 1).
- Produces: `canonical_data_type(data_type: DataType) -> DataType`, importable from `delta_engine.domain.model`. Tasks 5, 6, 7 compare types via `canonical_data_type(a) == canonical_data_type(b)`.

- [ ] **Step 1: Write the tests**

Append to `tests/domain/model/test_data_type.py` (add `canonical_data_type` to its imports from `delta_engine.domain.model`; reuse existing imports of `Array`, `Integer`, `Map`, `String`, `Struct`, `StructField`, adding any that are missing):

```python
def test_struct_types_differing_only_in_field_case_share_canonical_identity():
    camel = Struct((StructField("requestId", String()),))
    lower = Struct((StructField("requestid", String()),))

    assert canonical_data_type(camel) == canonical_data_type(lower)


def test_canonical_identity_recurses_through_arrays_and_maps():
    nested_camel = Map(String(), Array(Struct((StructField("Amount", Integer()),))))
    nested_lower = Map(String(), Array(Struct((StructField("amount", Integer()),))))

    assert canonical_data_type(nested_camel) == canonical_data_type(nested_lower)


def test_genuinely_different_field_names_stay_semantically_different():
    underscore = Struct((StructField("request_id", String()),))
    camel = Struct((StructField("requestId", String()),))

    assert canonical_data_type(underscore) != canonical_data_type(camel)


def test_primitive_types_are_their_own_canonical_identity():
    assert canonical_data_type(Integer()) == Integer()


def test_struct_rejects_fields_differing_only_by_case():
    with pytest.raises(ValueError, match="[Dd]uplicate struct field"):
        Struct((StructField("id", Integer()), StructField("ID", Integer())))
```

These pin invariants across the Task 10 flip: today they pass because constructors lowercase; afterwards they pass because canonicalization does the work. They are invariant pins, not fail-first TDD — the fail-first step for this task is the missing function import.

- [ ] **Step 2: Run to verify the import fails**

Run: `uv run pytest tests/domain/model/test_data_type.py -q --no-cov -k "canonical"`
Expected: FAIL — `ImportError: cannot import name 'canonical_data_type'`.

- [ ] **Step 3: Implement**

In `src/delta_engine/domain/model/data_type.py`:

1. Add the import at the top (with the existing imports):

```python
from delta_engine.domain.model.identifier import identifier_key
```

2. In `Struct.__post_init__`, replace the duplicate check body:

```python
        seen: set[str] = set()
        for field in self.fields:
            key = identifier_key(field.name)
            if key in seen:
                raise ValueError(f"Duplicate struct field name: {field.name}")
            seen.add(key)
```

3. Append at the end of the file:

```python
def canonical_data_type(data_type: DataType) -> DataType:
    """
    Return the semantic identity form of ``data_type``.

    Struct field names are identifier-keyed so two types differing only in
    field-name case are the same managed type. Every other variant is its
    own identity. The original value's spelling is untouched: render and
    report from the original, compare through this.
    """
    match data_type:
        case Struct(fields=fields):
            return Struct(
                tuple(
                    StructField(identifier_key(field.name), canonical_data_type(field.data_type))
                    for field in fields
                )
            )
        case Array(element=element):
            return Array(canonical_data_type(element))
        case Map(key=key, value=value):
            return Map(canonical_data_type(key), canonical_data_type(value))
        case _:
            return data_type
```

4. In `src/delta_engine/domain/model/__init__.py`, add `canonical_data_type` to the `data_type` import block and `"canonical_data_type",` to `__all__` (alphabetical).

- [ ] **Step 4: Run the tests**

Run: `uv run pytest tests/domain/model/test_data_type.py -q --no-cov`
Expected: PASS.

- [ ] **Step 5: Commit**

```bash
git add src/delta_engine/domain/model/data_type.py src/delta_engine/domain/model/__init__.py tests/domain/model/test_data_type.py
git commit -m "feat: add recursive semantic data-type identity"
```

---

### Task 3: Identity-keyed signatures and generated constraint names

Convert every signature, duplicate check, and generated name in the constraint path to explicit identity keys, while constructors still lowercase — a behavior-preserving refactor pinned by tests that must stay true across the Task 10 flip.

**Files:**

- Modify: `src/delta_engine/domain/model/constraints.py`
- Modify: `src/delta_engine/api/delta_table.py` (`_foreign_key_constraint_name`, ~line 90)
- Test: `tests/domain/model/test_primary_key.py`, `tests/domain/model/test_foreign_key.py`, `tests/api/test_delta_table.py`

**Interfaces:**

- Consumes: `identifier_key` (Task 1).
- Produces: `key_signature(columns)` returns `frozenset(identifier_key(c) for c in columns)`; `ForeignKeyConstraint.signature` returns identity-keyed column tuples. Task 5's diff and Task 7's dependency resolution rely on both being case-insensitive.

- [ ] **Step 1: Write the pinning tests**

Append to `tests/domain/model/test_primary_key.py` (add `from delta_engine.domain.model import key_signature` if not imported):

```python
def test_signature_is_identical_across_declaration_casing():
    # Signatures judge identity; case is never identity on Databricks.
    camel = PrimaryKeyConstraint(columns=("RequestId",), constraint_name="t_pk")
    lower = PrimaryKeyConstraint(columns=("requestid",), constraint_name="t_pk")

    assert camel.signature == lower.signature


def test_rejects_columns_differing_only_by_case_as_duplicates():
    with pytest.raises(ValueError, match=r"[Dd]uplicate"):
        PrimaryKeyConstraint(columns=("id", "ID"), constraint_name="t_pk")
```

Append to `tests/domain/model/test_foreign_key.py` (match the file's existing constructor helper style — it constructs `ForeignKeyConstraint(local_columns=…, referenced_table=QualifiedName(…), referenced_columns=…, constraint_name=…)`):

```python
def test_signature_is_identical_across_declaration_casing():
    referenced = QualifiedName("cat", "sch", "parent")
    camel = ForeignKeyConstraint(
        local_columns=("OrderId",),
        referenced_table=referenced,
        referenced_columns=("Id",),
        constraint_name="t_orderid_fk",
    )
    lower = ForeignKeyConstraint(
        local_columns=("orderid",),
        referenced_table=referenced,
        referenced_columns=("id",),
        constraint_name="t_orderid_fk",
    )

    assert camel.signature == lower.signature


def test_rejects_local_columns_differing_only_by_case_as_duplicates():
    with pytest.raises(ValueError, match=r"[Dd]uplicate"):
        ForeignKeyConstraint(
            local_columns=("id", "ID"),
            referenced_table=QualifiedName("cat", "sch", "parent"),
            referenced_columns=("a", "b"),
            constraint_name="t_fk",
        )
```

Append to `tests/api/test_delta_table.py`:

```python
def test_generated_foreign_key_name_is_identical_across_declaration_casing():
    def declare(local_spelling: str) -> DeltaTable:
        parent = DeltaTable(
            catalog="main",
            schema="sales",
            name="customers",
            columns=[Column("id", Integer())],
            primary_key=["id"],
        )
        return DeltaTable(
            catalog="main",
            schema="sales",
            name="orders",
            columns=[Column(local_spelling, Integer())],
            foreign_keys=[ForeignKey(columns={local_spelling: "id"}, references=parent)],
        )

    camel = declare("CustomerId").to_desired_table().foreign_keys[0].constraint_name
    lower = declare("customerid").to_desired_table().foreign_keys[0].constraint_name

    assert camel == lower == "orders_customerid_fk"
```

- [ ] **Step 2: Run them (they pass today — record that)**

Run: `uv run pytest tests/domain/model/test_primary_key.py tests/domain/model/test_foreign_key.py tests/api/test_delta_table.py -q --no-cov`
Expected: PASS. These pin behavior the refactor and the Task 10 flip must preserve.

- [ ] **Step 3: Implement the identity-keyed derivations**

In `src/delta_engine/domain/model/constraints.py`:

1. Add the import:

```python
from delta_engine.domain.model.identifier import identifier_key
```

2. Replace `key_signature`:

```python
def key_signature(columns: Iterable[str]) -> KeySignature:
    """Return the order-independent, case-insensitive identity of a key's columns."""
    return frozenset(identifier_key(column) for column in columns)
```

3. In `PrimaryKeyConstraint.__post_init__`, replace the duplicate loop (keep the `.lower()` storage line above it for now — it is removed in Task 10):

```python
        seen: set[str] = set()
        for column in self.columns:
            key = identifier_key(column)
            if key in seen:
                raise ValueError(f"Duplicate primary key column: {column}")
            seen.add(key)
```

4. In `ForeignKeyConstraint.__post_init__`, replace both duplicate loops the same way (`seen_local` and `seen_referenced` hold identity keys; error messages keep the original `column` spelling).

5. Replace `ForeignKeyConstraint.signature`:

```python
    @property
    def signature(self) -> tuple[tuple[str, ...], QualifiedName, tuple[str, ...]]:
        """
        Content identity: local columns, referenced table, referenced columns.

        Column entries are identity keys, so a desired constraint and a
        catalog-observed one compare equal across display casing. Excludes
        ``constraint_name`` so generated and catalog names still match by
        content.
        """
        return (
            tuple(identifier_key(column) for column in self.local_columns),
            self.referenced_table,
            tuple(identifier_key(column) for column in self.referenced_columns),
        )
```

In `src/delta_engine/api/delta_table.py`, replace `_foreign_key_constraint_name` (add `identifier_key` to the file's `delta_engine.domain.model` import in the same edit):

```python
def _foreign_key_constraint_name(
    *,
    owner_table_name: str,
    local_columns: tuple[str, ...],
) -> str:
    """
    Return the physical name used for a generated foreign key.

    Joins the sorted identity keys of the local columns so the generated
    name is identical across declaration casing and column order.
    """
    columns = "_".join(sorted(identifier_key(column) for column in local_columns))
    return f"{owner_table_name}_{columns}_fk"
```

- [ ] **Step 4: Run the focused suites, then the full suite**

Run: `uv run pytest tests/domain/model tests/api -q --no-cov`
Expected: PASS.
Run: `uv run pytest -q`
Expected: PASS (1030+ tests) — behavior unchanged.

- [ ] **Step 5: Commit**

```bash
git add src/delta_engine/domain/model/constraints.py src/delta_engine/api/delta_table.py tests/domain/model/test_primary_key.py tests/domain/model/test_foreign_key.py tests/api/test_delta_table.py
git commit -m "refactor: judge key signatures and generated names by identity key"
```

---

### Task 4: Identity-keyed table and column validation

**Files:**

- Modify: `src/delta_engine/domain/model/table.py` (`_validate_key_column_list`, `_validate_table_structure`, `DesiredTable.__post_init__`)
- Modify: `src/delta_engine/domain/model/column.py` (self-rename check, ~line 60)
- Test: `tests/domain/model/test_table.py`, `tests/domain/model/test_column.py`

**Interfaces:**

- Consumes: `identifier_key` (Task 1).
- Produces: structural validation that resolves and deduplicates by identity while stored values keep whatever spelling constructors give them. Task 10 depends on every check here being spelling-independent.

- [ ] **Step 1: Write the pinning tests**

Append to `tests/domain/model/test_table.py` (the file defines `_QUALIFIED_NAME` and the `@_EACH_TABLE_AND_COLUMN_TYPE` parametrization over desired/observed table+column types — reuse both):

```python
@_EACH_TABLE_AND_COLUMN_TYPE
def test_rejects_columns_differing_only_by_case_as_duplicates(table_type, column_type):
    cols = (column_type("Id", Integer()), column_type("ID", Integer()))

    with pytest.raises(ValueError, match="Duplicate column name"):
        table_type(_QUALIFIED_NAME, cols)


def test_primary_key_reference_resolves_across_casing():
    # The declared key spelling and the column spelling are the same identifier.
    table = DesiredTable(
        qualified_name=_QUALIFIED_NAME,
        columns=(DesiredColumn("request_id", Integer(), nullable=False),),
        primary_key=PrimaryKeyConstraint(columns=("REQUEST_ID",), constraint_name="t_pk"),
    )

    assert table.primary_key is not None


def test_nullable_primary_key_column_is_rejected_across_casing():
    with pytest.raises(ValueError, match="NOT NULL"):
        DesiredTable(
            qualified_name=_QUALIFIED_NAME,
            columns=(DesiredColumn("request_id", Integer(), nullable=True),),
            primary_key=PrimaryKeyConstraint(columns=("REQUEST_ID",), constraint_name="t_pk"),
        )
```

(Add `PrimaryKeyConstraint` to the file's imports if missing.) These pass today; they pin the invariant across Task 10.

- [ ] **Step 2: Run them**

Run: `uv run pytest tests/domain/model/test_table.py -q --no-cov`
Expected: PASS.

- [ ] **Step 3: Implement**

In `src/delta_engine/domain/model/table.py`, add `identifier_key` to the imports:

```python
from delta_engine.domain.model.identifier import identifier_key
```

1. Replace `_validate_key_column_list` (parameter renamed to say what it now holds):

```python
def _validate_key_column_list(kind: str, names: tuple[str, ...], column_keys: set[str]) -> None:
    """Rules shared by partition and clustering key lists: existing and unique."""
    missing = [name for name in names if identifier_key(name) not in column_keys]
    if missing:
        raise ValueError(f"{kind} column not found: {', '.join(missing)}")

    seen: set[str] = set()
    for name in names:
        key = identifier_key(name)
        if key in seen:
            raise ValueError(f"Duplicate {kind.lower()} column: {name}")
        seen.add(key)
```

2. In `_validate_table_structure`, replace the name bookkeeping (`seen_names` becomes a set of identity keys named `column_keys`; downstream checks use it):

```python
    column_keys: set[str] = set()
    for column in columns:
        key = identifier_key(column.name)
        if key in column_keys:
            raise ValueError(f"Duplicate column name: {column.name}")
        column_keys.add(key)

    _validate_key_column_list("Partition", partitioned_by, column_keys)
    _validate_key_column_list("Clustering", clustered_by, column_keys)

    if primary_key is not None:
        missing_pk = [
            name for name in primary_key.columns if identifier_key(name) not in column_keys
        ]
        if missing_pk:
            raise ValueError(f"Primary key column not found in columns: {missing_pk[0]}")

    for foreign_key in foreign_keys:
        missing_fk_columns = [
            name
            for name in foreign_key.local_columns
            if identifier_key(name) not in column_keys
        ]
        if missing_fk_columns:
            raise ValueError(
                f"Foreign key local column not found in columns: {missing_fk_columns[0]}"
            )
```

3. In `DesiredTable.__post_init__`:

- FK same-local-columns check: `local_column_set = frozenset(identifier_key(c) for c in foreign_key.local_columns)`.
- Nullable-PK check:

```python
        if self.primary_key is not None:
            key_columns = {identifier_key(column) for column in self.primary_key.columns}
            nullable_key_columns = [
                column.name
                for column in self.columns
                if identifier_key(column.name) in key_columns and column.nullable
            ]
```

- Rename checks:

```python
        declared_keys = {identifier_key(column.name) for column in self.columns}
        rename_source_keys: set[str] = set()
        for column in self.columns:
            source = column.renamed_from
            if source is None:
                continue
            if TableAspect.COLUMN_STRUCTURE not in self.managed_aspects:
                raise ValueError(
                    f"Column {column.name!r} declares renamed_from, but this"
                    " declaration does not manage column structure"
                )
            source_key = identifier_key(source)
            if source_key in declared_keys:
                raise ValueError(
                    f"Column {column.name!r} declares renamed_from {source!r},"
                    f" but {source!r} is still declared. Remove the old column,"
                    " or apply the rename and the reuse of the name in separate"
                    " syncs."
                )
            if source_key in rename_source_keys:
                raise ValueError(
                    f"Two columns declare renamed_from {source!r}; a rename source must be unique"
                )
            rename_source_keys.add(source_key)
```

In `src/delta_engine/domain/model/column.py`, add the same `identifier_key` import and change only the self-rename comparison (the `.lower()` storage lines stay until Task 10):

```python
            if identifier_key(self.renamed_from) == identifier_key(self.name):
                raise ValueError(f"Column {self.name!r} cannot be renamed_from itself")
```

- [ ] **Step 4: Run the model suite, then the full suite**

Run: `uv run pytest tests/domain/model -q --no-cov` → PASS.
Run: `uv run pytest -q` → PASS.

- [ ] **Step 5: Commit**

```bash
git add src/delta_engine/domain/model/table.py src/delta_engine/domain/model/column.py tests/domain/model/test_table.py
git commit -m "refactor: validate table structure through identifier keys"
```

---

### Task 5: Diff alignment, matched-action spelling, and action ordering

The diff aligns by identity and emits matched-column actions with the rename-projected observed name — the spec's "resulting spelling" for every matched column. `RenameColumn` carries the observed source. Action ordering and no-difference invariants become identity-keyed. All of it is a no-op while constructors still lowercase.

**Files:**

- Modify: `src/delta_engine/domain/plan/diff.py`
- Modify: `src/delta_engine/domain/plan/actions.py`
- Test: `tests/domain/plan/test_diff.py`, `tests/domain/plan/test_actions.py`

**Interfaces:**

- Consumes: `identifier_key`, `index_by_identifier`, `canonical_data_type` (Tasks 1–2).
- Produces: matched-column actions (`AlterColumnType`, `SetColumnNullability`, `SetColumnComment`, `SetColumnTag`, `UnsetColumnTag`) whose `column_name` is the projected observed column's name; `RenameColumn(old_name=<observed spelling>, new_name=<desired spelling>)`. Task 8 relies on `RenameColumn.new_name` being the desired spelling; Task 11 asserts the emissions with mixed-case fixtures.

- [ ] **Step 1: Write the pinning tests**

Append to `tests/domain/plan/test_diff.py` (reuse the file's imports; construct tables inline as the file does):

```python
def test_matched_column_actions_carry_the_observed_column_name():
    qualified_name = QualifiedName("cat", "sch", "t")
    desired = DesiredTable(
        qualified_name=qualified_name,
        columns=(DesiredColumn("amount", Integer(), comment="net"),),
    )
    observed = ObservedTable(
        qualified_name=qualified_name,
        columns=(ObservedColumn("amount", Integer(), comment=""),),
    )

    diff = diff_table(desired, observed)

    [action] = diff.actions
    assert isinstance(action, SetColumnComment)
    assert action.column_name == observed.columns[0].name


def test_rename_action_carries_the_observed_source_name():
    qualified_name = QualifiedName("cat", "sch", "t")
    desired = DesiredTable(
        qualified_name=qualified_name,
        columns=(DesiredColumn("customer_name", String(), renamed_from="customer_nm"),),
    )
    observed = ObservedTable(
        qualified_name=qualified_name,
        columns=(ObservedColumn("customer_nm", String()),),
    )

    diff = diff_table(desired, observed)

    [action] = diff.actions
    assert isinstance(action, RenameColumn)
    assert action.old_name == observed.columns[0].name
    assert action.new_name == "customer_name"
```

Append to `tests/domain/plan/test_actions.py`:

```python
def test_plan_orders_subjects_by_identity_key_with_exact_subject_tiebreak():
    # Deterministic ordering must not depend on subject casing: 'B' sorts with
    # 'b', not before 'a'. (ASCII order would put 'B' < 'a'.)
    plan = ActionPlan(
        target=QualifiedName("cat", "sch", "t"),
        actions=(
            SetColumnComment(column_name="beta", desired_comment="x", observed_comment=""),
            SetColumnComment(column_name="Alpha", desired_comment="x", observed_comment=""),
        ),
    )

    assert [action.subject for action in plan] == ["Alpha", "beta"]
```

(Add `SetColumnComment`, `ActionPlan`, `QualifiedName` to that file's imports if missing.)

- [ ] **Step 2: Run them to check current state**

Run: `uv run pytest tests/domain/plan -q --no-cov -k "observed_column_name or observed_source or identity_key"`
Expected: the two diff tests PASS today (both sides lowercase); the ordering test FAILS (`['Alpha', 'beta']` vs current ASCII order `['Alpha', 'beta']`… note: `'A' < 'b'` in ASCII, so if it passes, it still pins the invariant — the meaningful failing case is `'B'` vs `'a'`; the test uses `beta`/`Alpha` which sorts the same either way, so strengthen it):

Use exactly this assertion set instead if the above passes trivially — subjects `"Beta"` and `"alpha"`:

```python
        actions=(
            SetColumnComment(column_name="Beta", desired_comment="x", observed_comment=""),
            SetColumnComment(column_name="alpha", desired_comment="x", observed_comment=""),
        ),
    )

    assert [action.subject for action in plan] == ["alpha", "Beta"]
```

Expected with this version: FAIL today (ASCII sorts `"Beta"` before `"alpha"`).

- [ ] **Step 3: Implement `actions.py`**

In `src/delta_engine/domain/plan/actions.py`:

1. Extend the file's existing `from delta_engine.domain.model import (...)` block with `canonical_data_type` and `identifier_key` (both are re-exported by Task 1/2; no import cycle — `domain.model` does not import `domain.plan`).

2. Replace `_execution_order`:

```python
def _execution_order(action: Action) -> tuple[int, str, str]:
    """Deterministic ordering key: phase, identity-keyed subject, then exact subject."""
    return (action.phase, identifier_key(action.subject), action.subject)
```

3. `RenameColumn.__post_init__`:

```python
    def __post_init__(self) -> None:
        if identifier_key(self.old_name) == identifier_key(self.new_name):
            raise ValueError(f"RenameColumn carries no difference: {self.old_name!r}")
```

4. `AlterClustering.__post_init__`:

```python
    def __post_init__(self) -> None:
        desired_keys = {identifier_key(name) for name in self.desired_clustering}
        observed_keys = {identifier_key(name) for name in self.observed_clustering}
        if desired_keys == observed_keys:
            raise ValueError(f"AlterClustering carries no difference: {self.desired_clustering!r}")
```

5. `AlterColumnType.__post_init__`:

```python
    def __post_init__(self) -> None:
        if canonical_data_type(self.desired_type) == canonical_data_type(self.observed_type):
            raise ValueError(f"AlterColumnType carries no difference: {self.desired_type!r}")
```

- [ ] **Step 4: Implement `diff.py`**

In `src/delta_engine/domain/plan/diff.py`, extend the domain-model import with `canonical_data_type`, `identifier_key`, `index_by_identifier`, then:

1. Replace `_resolve_column_renames`:

```python
def _resolve_column_renames(desired: DesiredTable, observed: ObservedTable) -> _RenameResolution:
    """Resolve applicable rename hints and project rename-preserved observed state."""
    rename_targets_by_source_key = {
        identifier_key(column.renamed_from): column
        for column in desired.columns
        if column.renamed_from is not None
    }
    observed_by_key = index_by_identifier(observed.columns, lambda column: column.name)
    new_spelling_by_old_key: dict[str, str] = {}
    conflicted_source_keys: set[str] = set()
    actions: list[RenameColumn] = []
    conflicts: list[ColumnRenameConflict] = []

    for old_key, target in rename_targets_by_source_key.items():
        observed_column = observed_by_key.get(old_key)
        if observed_column is None:
            continue

        if identifier_key(target.name) in observed_by_key:
            conflicted_source_keys.add(old_key)
            conflicts.append(
                ColumnRenameConflict(old_name=observed_column.name, new_name=target.name)
            )
            continue

        new_spelling_by_old_key[old_key] = target.name
        actions.append(RenameColumn(old_name=observed_column.name, new_name=target.name))

    projected_columns: list[ObservedColumn] = []
    for column in observed.columns:
        key = identifier_key(column.name)
        if key in conflicted_source_keys:
            continue
        new_spelling = new_spelling_by_old_key.get(key)
        projected_columns.append(
            replace(column, name=new_spelling) if new_spelling is not None else column
        )

    return _RenameResolution(
        columns=tuple(projected_columns),
        partitioned_by=_project_names(observed.partitioned_by, new_spelling_by_old_key),
        clustered_by=_project_names(observed.clustered_by, new_spelling_by_old_key),
        actions=tuple(actions),
        conflicts=tuple(conflicts),
    )
```

2. Replace `_project_names`:

```python
def _project_names(names: tuple[str, ...], renames: Mapping[str, str]) -> tuple[str, ...]:
    """Project column names through the applied rename mapping, keyed by identity."""
    return tuple(renames.get(identifier_key(name), name) for name in names)
```

3. Replace `_align_columns`'s body:

```python
    desired_by_key = index_by_identifier(desired_columns, lambda column: column.name)
    observed_by_key = index_by_identifier(observed_columns, lambda column: column.name)

    added = tuple(
        column
        for column in desired_columns
        if identifier_key(column.name) not in observed_by_key
    )
    removed = tuple(
        column
        for column in observed_columns
        if identifier_key(column.name) not in desired_by_key
    )
    matched = tuple(
        (column, observed_by_key[identifier_key(column.name)])
        for column in desired_columns
        if identifier_key(column.name) in observed_by_key
    )
```

4. In `_diff_existing_column`, compare types semantically and emit every action with `observed.name`:

```python
    if canonical_data_type(desired.data_type) != canonical_data_type(observed.data_type):
        actions.append(
            AlterColumnType(
                column_name=observed.name,
                desired_type=desired.data_type,
                observed_type=observed.data_type,
            )
        )
    if desired.nullable != observed.nullable:
        actions.append(
            SetColumnNullability(
                column_name=observed.name,
                desired_nullable=desired.nullable,
                observed_nullable=observed.nullable,
            )
        )
    if desired.comment != observed.comment:
        actions.append(
            SetColumnComment(
                column_name=observed.name,
                desired_comment=desired.comment,
                observed_comment=observed.comment,
            )
        )
```

5. In `_diff_column_tags`, emit `column_name=observed.name` in both the `SetColumnTag` and `UnsetColumnTag` branches (the matched column addresses its physical self).

6. Replace `_diff_layout`'s comparisons (clustering by identity set, partitioning by identity sequence):

```python
    actions: tuple[AlterClustering, ...] = ()
    desired_cluster_keys = {identifier_key(name) for name in desired.clustered_by}
    observed_cluster_keys = {identifier_key(name) for name in observed.clustered_by}
    if desired_cluster_keys != observed_cluster_keys:
        actions = (
            AlterClustering(
                desired_clustering=desired.clustered_by,
                observed_clustering=observed.clustered_by,
            ),
        )

    unresolvable: tuple[PartitioningChanged, ...] = ()
    desired_partition_keys = tuple(identifier_key(name) for name in desired.partitioned_by)
    observed_partition_keys = tuple(identifier_key(name) for name in observed.partitioned_by)
    if desired_partition_keys != observed_partition_keys:
        unresolvable = (
            PartitioningChanged(
                desired_partitioning=desired.partitioned_by,
                observed_partitioning=observed.partitioned_by,
            ),
        )
```

- [ ] **Step 5: Run the plan suites, then the full suite**

Run: `uv run pytest tests/domain/plan -q --no-cov` → PASS (including the strengthened ordering test).
Run: `uv run pytest -q` → PASS.

- [ ] **Step 6: Commit**

```bash
git add src/delta_engine/domain/plan/diff.py src/delta_engine/domain/plan/actions.py tests/domain/plan/test_diff.py tests/domain/plan/test_actions.py
git commit -m "refactor: align and order the diff through identifier keys"
```

---

### Task 6: Identity-keyed API lowering

**Files:**

- Modify: `src/delta_engine/api/delta_table.py`
- Test: `tests/api/test_delta_table.py`

**Interfaces:**

- Consumes: `identifier_key`, `index_by_identifier`, `canonical_data_type` (Tasks 1–2; `identifier_key` already imported in Task 3).
- Produces: every declaration-time lookup resolves by identity. Task 10 depends on this file having no exact-string identity assumptions left outside `_normalize_declaration`'s (temporary) lowering.

- [ ] **Step 1: Write the pinning test**

Append to `tests/api/test_delta_table.py`:

```python
def test_layout_and_key_references_resolve_across_casing():
    table = DeltaTable(
        catalog="main",
        schema="sales",
        name="orders",
        columns=[Column("region", String()), Column("order_id", Integer(), nullable=False)],
        clustered_by=["REGION"],
        primary_key=["ORDER_ID"],
    )

    assert table.to_desired_table().primary_key is not None
```

- [ ] **Step 2: Run it**

Run: `uv run pytest tests/api/test_delta_table.py -q --no-cov -k "resolve_across_casing"`
Expected: PASS (pins the invariant across Task 10).

- [ ] **Step 3: Implement**

In `src/delta_engine/api/delta_table.py` (extend the domain-model import with `canonical_data_type` and `index_by_identifier` in the same edit as their first use):

1. `_validate_layout`: replace the `columns_by_name` mapping and its three uses:

```python
    columns_by_key = index_by_identifier(columns, lambda column: column.name)
    for name in partitioned_by:
        column = columns_by_key.get(identifier_key(name))
        if column is not None and isinstance(column.data_type, _TYPES_UNUSABLE_AS_PARTITION_KEYS):
            raise ValueError(
                f"Partition column {name!r} has type"
                f" {type(column.data_type).__name__}, which Delta cannot partition by"
            )
    for name in clustered_by:
        column = columns_by_key.get(identifier_key(name))
        if column is not None and isinstance(column.data_type, _TYPES_UNUSABLE_AS_CLUSTERING_KEYS):
            raise ValueError(
                f"Clustering column {name!r} has type"
                f" {type(column.data_type).__name__}, which cannot be a clustering key"
            )

    partition_keys = {identifier_key(name) for name in partitioned_by}
    if (
        partitioned_by
        and partition_keys <= columns_by_key.keys()
        and len(partition_keys) == len(columns)
    ):
        raise ValueError(
            "Cannot partition by every column: at least one non-partition column is required"
        )
```

2. `_validate_column_names`: the CDF reserved check compares by identity (`_CDF_RESERVED_COLUMN_NAMES` entries are lowercase):

```python
        reserved = [
            column.name
            for column in columns
            if identifier_key(column.name) in _CDF_RESERVED_COLUMN_NAMES
        ]
```

3. `ForeignKey._to_constraint`: identity-key the type maps and compare canonically:

```python
        local_types = {identifier_key(column.name): column.data_type for column in owner_columns}
        for local_name, referenced_name in pairs:
            local_type = local_types.get(identifier_key(local_name))
            if local_type is None:
                continue  # local column existence is enforced when the DesiredTable is built
            referenced_type = referenced.column_types[identifier_key(referenced_name)]
            if canonical_data_type(local_type) != canonical_data_type(referenced_type):
                raise ValueError(
                    f"foreign key column type mismatch: {owner_name}.{local_name}"
                    f" is {local_type} but {referenced.table}.{referenced_name}"
                    f" is {referenced_type}"
                )
```

4. `ForeignKey._resolve_reference`: both `types` dict comprehensions key by `identifier_key(column.name)`.

5. `ForeignKey._resolve_column_pairs`: the same-name comparison becomes identity-keyed:

```python
        if {identifier_key(c) for c in local_columns} == {identifier_key(c) for c in parent_columns}:
            return tuple((column, column) for column in local_columns)
```

(The `key_signature` comparison of referenced columns against the parent key is already canonical from Task 3.)

- [ ] **Step 4: Run the API suite, then the full suite**

Run: `uv run pytest tests/api -q --no-cov` → PASS.
Run: `uv run pytest -q` → PASS.

- [ ] **Step 5: Commit**

```bash
git add src/delta_engine/api/delta_table.py tests/api/test_delta_table.py
git commit -m "refactor: resolve declaration references through identifier keys"
```

---

### Task 7: Identity-keyed dependency resolution and reader tag join

**Files:**

- Modify: `src/delta_engine/application/dependency_resolution.py`
- Modify: `src/delta_engine/adapters/databricks/sql/rows.py`
- Modify: `src/delta_engine/adapters/databricks/read.py`
- Test: `tests/application/test_dependency_resolution.py`, `tests/adapters/databricks/sql/test_rows.py`

**Interfaces:**

- Consumes: `identifier_key`, `canonical_data_type`.
- Produces: FK type checks and tag joins that survive mixed-case observed state. The reader keeps `ObservedColumn.name` verbatim (it already passes catalog values through; the constructors stop rewriting them in Task 10).

- [ ] **Step 1: Implement `dependency_resolution.py`**

Add `canonical_data_type` and `identifier_key` to the `delta_engine.domain.model` import, then:

1. `_foreign_key_types_match`:

```python
    return all(
        canonical_data_type(local_types[identifier_key(local_column)])
        == canonical_data_type(referenced_types[identifier_key(referenced_column)])
        for local_column, referenced_column in zip(
            foreign_key.local_columns, foreign_key.referenced_columns, strict=True
        )
    )
```

2. `column_types_by_name` in `_classify_failures`:

```python
    column_types_by_name = {
        table.qualified_name: {
            identifier_key(column.name): column.data_type for column in table.columns
        }
        for table in tables
    }
```

(`primary_key_by_name` already compares canonical signatures via Task 3.)

- [ ] **Step 2: Implement the reader join**

In `src/delta_engine/adapters/databricks/sql/rows.py`:

1. Add the import: `from delta_engine.domain.model import identifier_key` (extend the existing `delta_engine.domain.model` import block).
2. In `read_column_tags`, key the grouping by identity:

```python
        grouped.setdefault(identifier_key(row.column_name), {})[row.tag_name] = row.tag_value
```

3. Rewrite the module docstring's normalization paragraph (last paragraph) to state the new contract:

```text
Identifier spelling is preserved end to end — mapped rows carry catalog
values verbatim and the domain stores them verbatim. The one derived key is
``read_column_tags``'s lookup dict: it is probed by identifier identity, so
its keys go through ``identifier_key``. Tag keys and values are
case-sensitive and preserved verbatim.
```

In `src/delta_engine/adapters/databricks/read.py`, add `identifier_key` to the `delta_engine.domain.model` import and change the tag probe in `_read_observed_table`:

```python
    tagged_columns = tuple(
        replace(column, tags=column_tags.get(identifier_key(column.name), MappingProxyType({})))
        for column in description.columns
    )
```

- [ ] **Step 3: Update the rows test name to the new contract**

In `tests/adapters/databricks/sql/test_rows.py`, the test `test_column_tags_read_lowercases_column_names_but_preserves_tag_case` keeps its body (the lookup dict is still keyed lowercase — that is now the identity key) but rename it to say why:

```python
def test_column_tags_read_keys_by_identifier_identity_and_preserves_tag_case():
```

- [ ] **Step 4: Run the affected suites, then the full suite**

Run: `uv run pytest tests/application/test_dependency_resolution.py tests/adapters/databricks -q --no-cov` → PASS.
Run: `uv run pytest -q` → PASS.

- [ ] **Step 5: Commit**

```bash
git add src/delta_engine/application/dependency_resolution.py src/delta_engine/adapters/databricks/sql/rows.py src/delta_engine/adapters/databricks/read.py tests/adapters/databricks/sql/test_rows.py
git commit -m "refactor: key dependency types and tag joins by identity"
```

---

### Task 8: Resulting-schema index

**Files:**

- Create: `src/delta_engine/domain/plan/resulting_schema.py`
- Modify: `src/delta_engine/domain/plan/__init__.py`
- Test: `tests/domain/plan/test_resulting_schema.py` (new)

**Interfaces:**

- Consumes: `identifier_key` (Task 1); `TableDiff`/`TableDrift`/`TableMissing`, `RenameColumn` with desired-spelling `new_name` (Task 5).
- Produces: `resulting_column_spellings(diff: TableDiff) -> dict[str, str]` mapping identity key → exact post-sync spelling, importable from `delta_engine.domain.plan`. Task 9's binder and the engine consume it.

- [ ] **Step 1: Write the failing tests**

Create `tests/domain/plan/test_resulting_schema.py`:

```python
from delta_engine.domain.model import (
    DesiredColumn,
    DesiredTable,
    ObservedColumn,
    ObservedTable,
    QualifiedName,
    String,
)
from delta_engine.domain.plan import diff_table, resulting_column_spellings

_NAME = QualifiedName("cat", "sch", "t")


def _desired(*columns: DesiredColumn) -> DesiredTable:
    return DesiredTable(qualified_name=_NAME, columns=columns)


def _observed(*columns: ObservedColumn) -> ObservedTable:
    return ObservedTable(qualified_name=_NAME, columns=columns)


def test_a_missing_table_resolves_every_column_to_its_desired_spelling():
    diff = diff_table(_desired(DesiredColumn("request_id", String())), None)

    assert resulting_column_spellings(diff) == {"request_id": "request_id"}


def test_a_matched_column_resolves_to_the_observed_spelling():
    diff = diff_table(
        _desired(DesiredColumn("request_id", String())),
        _observed(ObservedColumn("request_id", String())),
    )

    assert resulting_column_spellings(diff) == {"request_id": "request_id"}


def test_an_added_column_resolves_to_the_desired_spelling():
    diff = diff_table(
        _desired(DesiredColumn("request_id", String()), DesiredColumn("extra", String())),
        _observed(ObservedColumn("request_id", String())),
    )

    assert resulting_column_spellings(diff)["extra"] == "extra"


def test_a_renamed_column_resolves_to_the_rename_target_spelling():
    diff = diff_table(
        _desired(DesiredColumn("customer_name", String(), renamed_from="customer_nm")),
        _observed(ObservedColumn("customer_nm", String())),
    )

    spellings = resulting_column_spellings(diff)
    assert spellings == {"customer_name": "customer_name"}
    assert "customer_nm" not in spellings


def test_a_removed_column_does_not_appear():
    diff = diff_table(
        _desired(DesiredColumn("keep", String())),
        _observed(ObservedColumn("keep", String()), ObservedColumn("drop_me", String())),
    )

    assert "drop_me" not in resulting_column_spellings(diff)
```

(Mixed-case spelling assertions arrive with Task 10/11, once constructors preserve. These prove the shape and the rename/removal rules.)

- [ ] **Step 2: Run them to verify they fail**

Run: `uv run pytest tests/domain/plan/test_resulting_schema.py -q --no-cov`
Expected: FAIL — `ImportError: cannot import name 'resulting_column_spellings'`.

- [ ] **Step 3: Implement**

Create `src/delta_engine/domain/plan/resulting_schema.py`:

```python
"""
Post-sync column spellings derived from a table's diff.

The resulting schema of a table maps each column's identity key to the
exact spelling the column will have after the table's plan executes:
observed spelling for a matched column, the declared spelling for an added
column or a rename target, and the desired spelling for every column of a
table being created. Removed columns do not appear — drop-path actions
carry their observed column verbatim and never resolve through this index.
"""

from typing import assert_never

from delta_engine.domain.model import identifier_key
from delta_engine.domain.plan.actions import RenameColumn
from delta_engine.domain.plan.diff import TableDiff, TableDrift, TableMissing


def resulting_column_spellings(diff: TableDiff) -> dict[str, str]:
    """Map each column's identity key to its exact post-sync spelling."""
    match diff:
        case TableMissing(desired=desired):
            return {identifier_key(column.name): column.name for column in desired.columns}
        case TableDrift() as drift:
            return _drift_spellings(drift)
        case _ as unreachable:
            assert_never(unreachable)


def _drift_spellings(drift: TableDrift) -> dict[str, str]:
    """Resolve matched columns to observed spelling, renames and adds to desired."""
    observed_by_key = {
        identifier_key(column.name): column.name for column in drift.observed.columns
    }
    rename_target_keys = {
        identifier_key(action.new_name)
        for action in drift.actions
        if isinstance(action, RenameColumn)
    }

    spellings: dict[str, str] = {}
    for column in drift.desired.columns:
        key = identifier_key(column.name)
        if key in rename_target_keys or key not in observed_by_key:
            spellings[key] = column.name
        else:
            spellings[key] = observed_by_key[key]
    return spellings
```

In `src/delta_engine/domain/plan/__init__.py`, add:

```python
from delta_engine.domain.plan.resulting_schema import resulting_column_spellings
```

and `"resulting_column_spellings",` to `__all__` (alphabetical).

- [ ] **Step 4: Run the tests**

Run: `uv run pytest tests/domain/plan/test_resulting_schema.py -q --no-cov`
Expected: PASS (5 tests).

- [ ] **Step 5: Commit**

```bash
git add src/delta_engine/domain/plan/resulting_schema.py src/delta_engine/domain/plan/__init__.py tests/domain/plan/test_resulting_schema.py
git commit -m "feat: derive post-sync column spellings from a table diff"
```

---

### Task 9: Planner binding and engine wiring

`plan_diff` gains the sync-wide resulting-schema index and binds symbolic references on accepted actions. Failure scoping per the spec: own-table miss = engine invariant violation (`RuntimeError`); FK referenced-side miss = fall back to declared spelling so the child still compiles preview SQL and dependency resolution owns the failure.

**Files:**

- Modify: `src/delta_engine/application/planning.py`
- Modify: `src/delta_engine/application/engine.py` (`_plan`, ~line 298)
- Test: `tests/application/test_planning.py`

**Interfaces:**

- Consumes: `resulting_column_spellings` (Task 8), `identifier_key`.
- Produces: `plan_diff(diff: TableDiff, resulting_schemas: Mapping[QualifiedName, Mapping[str, str]]) -> PlanningResult`. The engine builds `resulting_schemas` for every diffed run and passes the same map to every `plan_diff` call. Tests use the `_plan(diff)` helper defined below.

- [ ] **Step 1: Adapt existing planning tests and write the new ones**

In `tests/application/test_planning.py`:

1. Add to the imports:

```python
from delta_engine.domain.plan import resulting_column_spellings
```

2. Add a helper next to `_desired`/`_observed`:

```python
def _plan(diff):
    """plan_diff with the diff's own resulting schema — the engine's per-table contract."""
    return plan_diff(diff, {diff.target: resulting_column_spellings(diff)})
```

3. Mechanically replace every existing direct call `plan_diff(<expr>)` with `_plan(<expr>)`. Find them: `rg -n "plan_diff\(" tests/application/test_planning.py`.

4. Append the new tests:

```python
def test_plan_diff_requires_the_resulting_schema_index():
    diff = diff_table(_desired(), _observed())

    with pytest.raises(TypeError):
        plan_diff(diff)  # type: ignore[call-arg]


def test_planning_a_diffed_table_without_its_own_schema_entry_is_an_engine_error():
    desired = DesiredTable(
        qualified_name=_NAME,
        columns=(DesiredColumn("id", Integer(), nullable=False),),
        primary_key=PrimaryKeyConstraint(columns=("id",), constraint_name="t_pk"),
    )
    diff = diff_table(desired, _observed())

    with pytest.raises(RuntimeError, match="resulting schema"):
        plan_diff(diff, {})


def test_a_rejected_diff_fails_before_binding_is_reached():
    # Validation runs first: a rejected diff returns PlanningFailed even with
    # an empty schema index, proving binding never sees rejected actions.
    desired = DesiredTable(
        qualified_name=_NAME,
        columns=(DesiredColumn("id", Integer()), DesiredColumn("extra", Integer())),
        managed_aspects=METADATA_ASPECTS,
    )
    diff = diff_table(desired, _observed())

    result = plan_diff(diff, {})

    assert isinstance(result, PlanningFailed)


def test_foreign_key_to_an_unregistered_parent_keeps_its_declared_referenced_spelling():
    # The parent is absent from the index: planning still succeeds and the
    # referenced side falls back to the declaration. Dependency resolution —
    # not planning — classifies and blocks this constraint.
    constraint = ForeignKeyConstraint(
        local_columns=("id",),
        referenced_table=QualifiedName("dev", "silver", "unregistered_parent"),
        referenced_columns=("parent_id",),
        constraint_name="test_id_fk",
    )
    desired = DesiredTable(
        qualified_name=_NAME,
        columns=(DesiredColumn("id", Integer()),),
        foreign_keys=(constraint,),
    )
    diff = diff_table(desired, _observed())

    result = plan_diff(diff, {diff.target: resulting_column_spellings(diff)})

    assert isinstance(result, PlanningSucceeded)
    [action] = [a for a in result.plan if isinstance(a, SetForeignKey)]
    assert action.constraint.referenced_columns == ("parent_id",)
```

(`METADATA_ASPECTS` is already imported in this file; the third test relies on structural drift being out of scope for a metadata declaration, which fails validation. Adjust the drift trigger to whatever the file's existing scope-failure fixtures use if this shape does not reject — the assertion that matters is `PlanningFailed` from `plan_diff(diff, {})` without a `RuntimeError`.)

- [ ] **Step 2: Run them to verify they fail**

Run: `uv run pytest tests/application/test_planning.py -q --no-cov`
Expected: FAIL — `TypeError: plan_diff() takes 1 positional argument but 2 were given` (and the helper-based tests fail the same way).

- [ ] **Step 3: Implement `planning.py`**

Replace the module body below the result dataclasses with:

```python
type ResultingSchemas = Mapping[QualifiedName, Mapping[str, str]]


def plan_diff(diff: TableDiff, resulting_schemas: ResultingSchemas) -> PlanningResult:
    """
    Validate ``diff`` and return an accepted (bound) or rejected result.

    This is the only boundary that constructs an :class:`ActionPlan` from a
    complete diff. A rejected result carries validation failures and
    deliberately has no plan, making execution of unvalidated drift
    unrepresentable. Accepted actions are bound before the plan is built:
    symbolic column references (primary keys, foreign keys, clustering, and
    a created table's internal references) are resolved through
    ``resulting_schemas`` to the exact post-sync spelling, so the plan is
    self-contained and compilation stays mechanical. The plan carries the
    relation kind its actions lower against: the observed kind for drift,
    and the default ordinary kind for a creation.
    """
    validation = validate_diff(diff)
    if validation.failed:
        return PlanningFailed(failures=validation.failures)
    bound_actions = _bind_actions(diff, resulting_schemas)
    match diff:
        case TableDrift() as drift:
            plan = ActionPlan(
                target=drift.target,
                actions=bound_actions,
                kind=drift.observed.kind,
            )
        case TableMissing() as missing:
            plan = ActionPlan(
                target=missing.target,
                actions=bound_actions,
            )
        case _ as unreachable:
            assert_never(unreachable)
    return PlanningSucceeded(plan=plan)


def _bind_actions(diff: TableDiff, resulting_schemas: ResultingSchemas) -> tuple[Action, ...]:
    """Bind every accepted action's symbolic references to post-sync spelling."""
    own = resulting_schemas.get(diff.target)
    if own is None:
        raise RuntimeError(
            f"No resulting schema for planned table {diff.target}; the engine"
            " derives one for every diffed table"
        )
    return tuple(_bind_action(action, own, resulting_schemas) for action in diff.actions)


def _bind_action(
    action: Action,
    own: Mapping[str, str],
    resulting_schemas: ResultingSchemas,
) -> Action:
    """Return ``action`` with symbolic column references bound, or unchanged."""
    match action:
        case SetPrimaryKey(primary_key=primary_key):
            return SetPrimaryKey(
                primary_key=replace(
                    primary_key,
                    columns=tuple(_own_spelling(own, name) for name in primary_key.columns),
                )
            )
        case AlterClustering():
            return replace(
                action,
                desired_clustering=tuple(
                    _own_spelling(own, name) for name in action.desired_clustering
                ),
            )
        case SetForeignKey(constraint=constraint):
            parent = resulting_schemas.get(constraint.referenced_table)
            return SetForeignKey(
                constraint=replace(
                    constraint,
                    local_columns=tuple(
                        _own_spelling(own, name) for name in constraint.local_columns
                    ),
                    referenced_columns=tuple(
                        _parent_spelling(parent, name)
                        for name in constraint.referenced_columns
                    ),
                )
            )
        case CreateTable(table=table):
            return CreateTable(table=_bind_created_table(table, own))
        case _:
            return action


def _bind_created_table(table: DesiredTable, own: Mapping[str, str]) -> DesiredTable:
    """
    Bind a created table's internal primary-key and layout references.

    The table's ``foreign_keys`` are deliberately untouched: CREATE TABLE
    renders no foreign keys — the separate ``SetForeignKey`` actions carry
    the bound, executable constraints.
    """
    bound_primary_key = (
        replace(
            table.primary_key,
            columns=tuple(_own_spelling(own, name) for name in table.primary_key.columns),
        )
        if table.primary_key is not None
        else None
    )
    return replace(
        table,
        primary_key=bound_primary_key,
        partitioned_by=tuple(_own_spelling(own, name) for name in table.partitioned_by),
        clustered_by=tuple(_own_spelling(own, name) for name in table.clustered_by),
    )


def _own_spelling(own: Mapping[str, str], name: str) -> str:
    """Resolve an own-table reference; a miss is an engine invariant violation."""
    spelling = own.get(identifier_key(name))
    if spelling is None:
        raise RuntimeError(
            f"Accepted action references no resulting column: {name!r}."
            " Declaration validation makes this unreachable short of an engine defect."
        )
    return spelling


def _parent_spelling(parent: Mapping[str, str] | None, name: str) -> str:
    """
    Resolve a foreign key's referenced column to the parent's post-sync spelling.

    An unregistered, read-failed, or divergent parent legitimately cannot
    bind, so any miss falls back to the declared spelling: the child still
    compiles preview SQL, and dependency resolution owns classifying the
    failure and blocking execution.
    """
    if parent is None:
        return name
    return parent.get(identifier_key(name), name)
```

Imports for the module become:

```python
from collections.abc import Mapping
from dataclasses import dataclass, replace
from typing import assert_never

from delta_engine.application.failures import ValidationFailure
from delta_engine.application.validation import validate_diff
from delta_engine.domain.model import DesiredTable, QualifiedName, identifier_key
from delta_engine.domain.plan import (
    Action,
    ActionPlan,
    AlterClustering,
    CreateTable,
    SetForeignKey,
    SetPrimaryKey,
    TableDiff,
    TableDrift,
    TableMissing,
)
```

- [ ] **Step 4: Wire the engine**

In `src/delta_engine/application/engine.py`, extend the `delta_engine.domain.plan` import with `resulting_column_spellings`, then replace `_plan`'s loop head:

```python
    def _plan(self, runs: tuple[_TableRun, ...]) -> None:
        """
        Accept or reject each diff according to the default planning policy.

        Builds the sync-wide resulting-schema index from every diffed run
        first — planning binds symbolic references (including cross-table
        foreign-key spellings) through it. Rejected runs retain
        ``PlanningFailed``; accepted runs retain ``PlanningSucceeded`` with
        the validated, bound action plan.
        """
        resulting_schemas = {
            run.diff.target: resulting_column_spellings(run.diff)
            for run in runs
            if run.diff is not None
        }
        for run in runs:
            if run.diff is None:
                continue

            planning = plan_diff(run.diff, resulting_schemas)
            run.planning = planning
```

(The `match planning` logging block below is unchanged.)

- [ ] **Step 5: Run planning and engine suites, then the full suite**

Run: `uv run pytest tests/application -q --no-cov` → PASS.
Run: `uv run pytest -q` → PASS (binding is a spelling no-op while constructors still lowercase; structure and invariants are what these tests prove).

- [ ] **Step 6: Commit**

```bash
git add src/delta_engine/application/planning.py src/delta_engine/application/engine.py tests/application/test_planning.py
git commit -m "feat: bind symbolic plan references to post-sync spelling"
```

---

### Task 10: Preserve spelling — remove constructor lowercasing

The flip. Every consumer is identity-keyed (Tasks 3–9), so removing the destructive normalization changes stored spelling only. Invert the normalization-pinning tests to preservation pins and update the docstrings that promise lowercase.

**Files:**

- Modify: `src/delta_engine/domain/model/column.py`, `data_type.py`, `constraints.py`, `table.py`
- Modify: `src/delta_engine/api/delta_table.py` (`_normalize_declaration`, `ForeignKey.__post_init__`, docstrings)
- Test: `tests/domain/model/test_column.py`, `test_data_type.py`, `test_primary_key.py`, `test_foreign_key.py`, `test_table.py`, `tests/api/test_delta_table.py`

**Interfaces:**

- Produces: `DesiredColumn.name/renamed_from`, `ObservedColumn.name`, `StructField.name`, PK/FK constraint columns and names, `partitioned_by`/`clustered_by`, and the public `DeltaTable`/`ForeignKey` accessors all preserve their input spelling. Task 11's mixed-case matrix depends on this.

- [ ] **Step 1: Invert the normalization pins to preservation pins**

Find every remaining lowercase assertion: `rg -n "lower" tests/domain/model tests/api`. Apply these inversions (same test intent, corrected expectation):

`tests/domain/model/test_column.py`:

```python
def test_mixed_case_name_is_preserved_verbatim() -> None:
    # Case is never identity on Databricks, but display spelling is real
    # catalog state: the engine stores it verbatim and compares by identity.
    col = DesiredColumn("UserId", Integer())
    assert col.name == "UserId"


def test_observed_column_preserves_catalog_spelling() -> None:
    assert ObservedColumn("requestId", Integer()).name == "requestId"


@given(st.text(min_size=1).filter(str.strip))
def test_construction_preserves_any_name_verbatim(name: str) -> None:
    # Preservation is identity on every name — normalizing was the special case.
    assert DesiredColumn(name, Integer()).name == name


def test_renamed_from_is_preserved_verbatim() -> None:
    column = DesiredColumn("customer_name", String(), renamed_from="Customer_NM")
    assert column.renamed_from == "Customer_NM"
```

(These replace `test_mixed_case_name_normalizes_to_lowercase`, `test_already_lowercase_unicode_name_is_preserved_verbatim` — keep that one, it still passes — `test_normalization_is_identity_on_already_canonical_names`, and `test_renamed_from_normalizes_to_lowercase`. `test_case_only_rename_collapses_to_renamed_from_itself` stays: a case-only rename still raises, now via identity keys.)

`tests/domain/model/test_primary_key.py` — replace `test_mixed_case_columns_and_name_normalize_to_lowercase`:

```python
def test_mixed_case_columns_and_name_are_preserved():
    pk = PrimaryKeyConstraint(columns=("OrderId",), constraint_name="Orders_PK")
    assert pk.columns == ("OrderId",)
    assert pk.constraint_name == "Orders_PK"
```

`tests/domain/model/test_foreign_key.py` — invert its normalization test(s) the same way (spelling preserved; pairs still sorted by the local column's identity key — assert both on a two-column constraint):

```python
def test_mixed_case_columns_are_preserved_and_sorted_by_identity():
    constraint = ForeignKeyConstraint(
        local_columns=("Zebra", "Apple"),
        referenced_table=QualifiedName("cat", "sch", "parent"),
        referenced_columns=("z_id", "a_id"),
        constraint_name="t_fk",
    )
    assert constraint.local_columns == ("Apple", "Zebra")
    assert constraint.referenced_columns == ("a_id", "z_id")
```

`tests/domain/model/test_table.py` — the two `*_normalizes_to_lowercase` layout tests become preservation + resolution pins:

```python
@_EACH_TABLE_AND_COLUMN_TYPE
def test_mixed_case_partition_reference_is_preserved_and_resolves(table_type, column_type):
    cols = (column_type("visit_date", Date()), column_type("id", Integer()))
    table = table_type(_QUALIFIED_NAME, cols, partitioned_by=("VISIT_DATE",))
    assert table.partitioned_by == ("VISIT_DATE",)


@_EACH_TABLE_AND_COLUMN_TYPE
def test_mixed_case_clustering_reference_is_preserved_and_resolves(table_type, column_type):
    columns = (column_type("id", Integer()), column_type("region", String()))
    table = table_type(_QUALIFIED_NAME, columns, clustered_by=("REGION",))
    assert table.clustered_by == ("REGION",)
```

`tests/api/test_delta_table.py` — split `test_mixed_case_declaration_normalizes_to_lowercase`: object-name parts stay lowercase (Unity Catalog stores them lowercase — unchanged, live-pinned), column spelling is preserved:

```python
def test_mixed_case_declaration_preserves_columns_and_lowercases_object_names():
    # Unity Catalog stores catalog/schema/table names lowercase (live-pinned);
    # column spelling is catalog display state and is preserved.
    table = DeltaTable(
        catalog="Main",
        schema="Sales",
        name="Orders",
        columns=[Column("Id", Integer())],
    )

    assert (table.catalog, table.schema, table.name) == ("main", "sales", "orders")
    assert [column.name for column in table.columns] == ["Id"]
```

Also add public-surface preservation pins:

```python
def test_public_accessors_return_declared_spelling():
    table = DeltaTable(
        catalog="main",
        schema="sales",
        name="orders",
        columns=[Column("OrderId", Integer(), nullable=False), Column("Region", String())],
        clustered_by=["Region"],
        primary_key=["OrderId"],
    )

    assert table.primary_key == ("OrderId",)
    assert table.clustered_by == ("Region",)
```

- [ ] **Step 2: Run to verify the new pins fail**

Run: `uv run pytest tests/domain/model tests/api -q --no-cov`
Expected: FAIL — every new preservation assertion sees lowercase values.

- [ ] **Step 3: Remove the lowering**

1. `src/delta_engine/domain/model/column.py`:
   - Delete `object.__setattr__(self, "name", self.name.lower())` from both `DesiredColumn.__post_init__` and `ObservedColumn.__post_init__`.
   - Delete `object.__setattr__(self, "renamed_from", self.renamed_from.lower())`.
   - Update the `DesiredColumn` docstring's `name` attribute line to:

```text
        name: Column name, stored verbatim. Identifiers differing only in
            case are the same column on the platform; identity is judged
            through explicit identifier keys, never by rewriting spelling.
```

2. `src/delta_engine/domain/model/data_type.py`: in `StructField.__post_init__`, delete `object.__setattr__(self, "name", self.name.lower())`.

3. `src/delta_engine/domain/model/constraints.py`:
   - `PrimaryKeyConstraint.__post_init__`: delete the `columns` lowering line and the `constraint_name` lowering line.
   - `ForeignKeyConstraint.__post_init__`: replace the canonicalization block with a sort by identity key that keeps spelling:

```python
        # Pairs are stored sorted by the local column's identity key. Column
        # order is not part of a foreign key's meaning (mirroring the primary
        # key's set identity), so one canonical order makes identity,
        # generated names, and rendered DDL independent of declaration order
        # and case — while both original spellings are retained.
        pairs = sorted(
            zip(self.local_columns, self.referenced_columns, strict=True),
            key=lambda pair: identifier_key(pair[0]),
        )
        object.__setattr__(self, "local_columns", tuple(pair[0] for pair in pairs))
        object.__setattr__(self, "referenced_columns", tuple(pair[1] for pair in pairs))
```

   - Delete the `constraint_name` lowering in `ForeignKeyConstraint.__post_init__` and `ForeignKeyReference.__post_init__` (observed constraint names must stay exact so `DROP CONSTRAINT` sends the catalog spelling back).
   - Update the `PrimaryKeyConstraint`/`ForeignKeyConstraint` docstrings: columns and names are stored verbatim; identity, duplicates, sorting, and signatures go through identifier keys.

4. `src/delta_engine/domain/model/table.py`: in both `__post_init__` methods, replace the two lowering lines with plain freezing:

```python
        object.__setattr__(self, "partitioned_by", tuple(self.partitioned_by))
        object.__setattr__(self, "clustered_by", tuple(self.clustered_by))
```

5. `src/delta_engine/api/delta_table.py`:
   - `_normalize_declaration`: `partitioned_by=tuple(partitioned_by)`, `clustered_by=tuple(clustered_by)`, `primary_key=(tuple(primary_key) if primary_key is not None else None)`.
   - `ForeignKey.__post_init__`: freeze without lowering:

```python
        frozen: tuple[str, ...] | Mapping[str, str]
        match self.columns:
            case str():
                frozen = (self.columns,)
            case Mapping():
                frozen = MappingProxyType(dict(self.columns))
            case Sequence():
                if not all(isinstance(column, str) for column in self.columns):
                    raise TypeError("foreign key columns must be strings")
                frozen = tuple(self.columns)
            case _:
                raise TypeError(
                    "foreign key columns must be a column name,"
                    " a sequence of same-name columns, or"
                    " a {local: referenced} mapping"
                )
```

   - `ForeignKey` docstring: replace "Identifiers are normalized to lowercase when the declaration is constructed." with "Identifier spelling is preserved; identifiers differing only in case name the same column."
   - `_normalize_declaration` docstring: "Freeze public inputs before judging them" (it no longer canonicalizes).

- [ ] **Step 4: Run the full suite and repair stragglers**

Run: `uv run pytest -q`
Expected: PASS after the Step 1 inversions. If any other test fails:

- A test asserting a lowercase value from a mixed-case fixture pins the old normalization — invert it to preservation, as in Step 1.
- A *production* failure (KeyError, false drift, missing column) means a missed exact-string lookup. Fix it with `identifier_key`/`index_by_identifier` at the lookup site. **Never fix by re-adding `.lower()` to a constructor.**

- [ ] **Step 5: Verify no destructive lowering remains**

Run: `rg -n "\.lower\(\)" src/delta_engine`
Expected — exactly these survivors, none of which stores a column-like identifier:

- `domain/model/identifier.py` (the policy itself)
- `domain/model/qualified_name.py` (object names, live-pinned lowercase)
- `domain/model/table.py`: `kind.lower()` in an error message and `self.name.lower()` in `TableAspect.label` (display text)
- `application/report.py`: enum label
- `adapters/databricks/sql/types.py`: type-name parsing
- `adapters/databricks/spark/_runner.py`: config parsing (`casefold`)

- [ ] **Step 6: Commit**

```bash
git add -A src tests
git commit -m "fix: preserve column identifier spelling in the model and API"
```

---

### Task 11: Mixed-case behavior matrix

Now that spelling survives, prove the case behavior end to end: case-only differences are no-ops, existing-column DDL uses catalog spelling, keys bind to post-sync spelling on every side, and dry-run SQL is byte-for-byte the executed SQL.

**Files:**

- Test: `tests/domain/plan/test_diff.py`, `tests/domain/plan/test_resulting_schema.py`, `tests/application/test_planning.py`, `tests/adapters/databricks/sql/test_compile.py`, `tests/application/test_engine.py`

**Interfaces:**

- Consumes: everything from Tasks 1–10. Any failure here is an implementation gap in an earlier task, not a reason to weaken an assertion.

- [ ] **Step 1: Diff-level cases**

Append to `tests/domain/plan/test_diff.py`:

```python
def test_case_only_column_difference_is_not_drift():
    qualified_name = QualifiedName("cat", "sch", "t")
    desired = DesiredTable(
        qualified_name=qualified_name,
        columns=(DesiredColumn("requestid", String()),),
    )
    observed = ObservedTable(
        qualified_name=qualified_name,
        columns=(ObservedColumn("requestId", String()),),
    )

    diff = diff_table(desired, observed)

    assert diff.actions == ()
    assert diff.unresolvable == ()


def test_case_only_layout_and_key_differences_are_not_drift():
    qualified_name = QualifiedName("cat", "sch", "t")
    desired = DesiredTable(
        qualified_name=qualified_name,
        columns=(DesiredColumn("requestId", String(), nullable=False),),
        clustered_by=("requestId",),
        primary_key=PrimaryKeyConstraint(columns=("requestId",), constraint_name="t_pk"),
    )
    observed = ObservedTable(
        qualified_name=qualified_name,
        columns=(ObservedColumn("requestid", String(), nullable=False),),
        clustered_by=("requestid",),
        primary_key=PrimaryKeyConstraint(columns=("REQUESTID",), constraint_name="t_pk"),
    )

    diff = diff_table(desired, observed)

    assert diff.actions == ()


def test_case_only_struct_field_difference_is_not_drift():
    qualified_name = QualifiedName("cat", "sch", "t")
    desired = DesiredTable(
        qualified_name=qualified_name,
        columns=(DesiredColumn("payload", Struct((StructField("requestId", String()),))),),
    )
    observed = ObservedTable(
        qualified_name=qualified_name,
        columns=(ObservedColumn("payload", Struct((StructField("requestid", String()),))),),
    )

    assert diff_table(desired, observed).actions == ()


def test_genuinely_different_name_still_reports_structural_drift():
    # The control: request_id vs requestId is a real difference, not casing.
    qualified_name = QualifiedName("cat", "sch", "t")
    desired = DesiredTable(
        qualified_name=qualified_name,
        columns=(DesiredColumn("requestId", String()),),
    )
    observed = ObservedTable(
        qualified_name=qualified_name,
        columns=(ObservedColumn("request_id", String()),),
    )

    diff = diff_table(desired, observed)

    action_types = {type(action) for action in diff.actions}
    assert action_types == {AddColumn, DropColumn}


def test_matched_column_action_uses_observed_spelling_across_casing():
    qualified_name = QualifiedName("cat", "sch", "t")
    desired = DesiredTable(
        qualified_name=qualified_name,
        columns=(DesiredColumn("requestid", String(), comment="AWS request id"),),
    )
    observed = ObservedTable(
        qualified_name=qualified_name,
        columns=(ObservedColumn("requestId", String(), comment=""),),
    )

    [action] = diff_table(desired, observed).actions

    assert isinstance(action, SetColumnComment)
    assert action.column_name == "requestId"


def test_rename_source_uses_observed_spelling_when_hint_casing_differs():
    qualified_name = QualifiedName("cat", "sch", "t")
    desired = DesiredTable(
        qualified_name=qualified_name,
        columns=(DesiredColumn("newName", String(), renamed_from="oldname"),),
    )
    observed = ObservedTable(
        qualified_name=qualified_name,
        columns=(ObservedColumn("OldName", String()),),
    )

    [action] = diff_table(desired, observed).actions

    assert isinstance(action, RenameColumn)
    assert action.old_name == "OldName"
    assert action.new_name == "newName"
```

Append to `tests/domain/plan/test_resulting_schema.py`:

```python
def test_mixed_case_matched_column_resolves_to_the_observed_spelling():
    diff = diff_table(
        _desired(DesiredColumn("requestid", String())),
        _observed(ObservedColumn("requestId", String())),
    )

    assert resulting_column_spellings(diff) == {"requestid": "requestId"}
```

- [ ] **Step 2: Planning-level cases**

Append to `tests/application/test_planning.py` (reuse `_NAME`, `_desired`, `_observed`, `_plan`):

```python
def test_primary_key_binds_to_the_observed_column_spelling():
    # The headline defect: declared lowercase, catalog camelCase — the bound
    # plan must carry the catalog spelling into ADD CONSTRAINT.
    desired = _desired(
        columns=(DesiredColumn("requestid", String(), nullable=False),),
        primary_key=PrimaryKeyConstraint(columns=("requestid",), constraint_name="test_pk"),
    )
    observed = _observed(columns=(ObservedColumn("requestId", String(), nullable=False),))

    result = _plan(diff_table(desired, observed))

    assert isinstance(result, PlanningSucceeded)
    [action] = [a for a in result.plan if isinstance(a, SetPrimaryKey)]
    assert action.primary_key.columns == ("requestId",)


def test_created_table_binds_internal_references_to_declared_column_spelling():
    desired = _desired(
        columns=(DesiredColumn("requestId", String(), nullable=False),),
        primary_key=PrimaryKeyConstraint(columns=("REQUESTID",), constraint_name="test_pk"),
        clustered_by=("REQUESTID",),
    )

    result = _plan(diff_table(desired, None))

    assert isinstance(result, PlanningSucceeded)
    [create] = [a for a in result.plan if isinstance(a, CreateTable)]
    assert create.table.primary_key is not None
    assert create.table.primary_key.columns == ("requestId",)
    assert create.table.clustered_by == ("requestId",)


def test_foreign_key_binds_both_sides_to_post_sync_spelling():
    parent_name = QualifiedName("dev", "silver", "parent")
    child_constraint = ForeignKeyConstraint(
        local_columns=("orderref",),
        referenced_table=parent_name,
        referenced_columns=("orderid",),
        constraint_name="test_orderref_fk",
    )
    child_desired = _desired(
        columns=(DesiredColumn("orderref", Integer()),),
        foreign_keys=(child_constraint,),
    )
    child_observed = _observed(columns=(ObservedColumn("orderRef", Integer()),))
    child_diff = diff_table(child_desired, child_observed)

    parent_desired = DesiredTable(
        qualified_name=parent_name,
        columns=(DesiredColumn("orderid", Integer(), nullable=False),),
        primary_key=PrimaryKeyConstraint(columns=("orderid",), constraint_name="parent_pk"),
    )
    parent_observed = ObservedTable(
        qualified_name=parent_name,
        columns=(ObservedColumn("OrderId", Integer(), nullable=False),),
        primary_key=PrimaryKeyConstraint(columns=("OrderId",), constraint_name="parent_pk"),
    )
    parent_diff = diff_table(parent_desired, parent_observed)

    schemas = {
        child_diff.target: resulting_column_spellings(child_diff),
        parent_diff.target: resulting_column_spellings(parent_diff),
    }
    result = plan_diff(child_diff, schemas)

    assert isinstance(result, PlanningSucceeded)
    [action] = [a for a in result.plan if isinstance(a, SetForeignKey)]
    assert action.constraint.local_columns == ("orderRef",)
    assert action.constraint.referenced_columns == ("OrderId",)


def test_foreign_key_to_a_renamed_parent_key_binds_to_the_new_spelling():
    parent_name = QualifiedName("dev", "silver", "parent")
    parent_desired = DesiredTable(
        qualified_name=parent_name,
        columns=(
            DesiredColumn("orderNumber", Integer(), nullable=False, renamed_from="orderid"),
        ),
        primary_key=PrimaryKeyConstraint(columns=("orderNumber",), constraint_name="parent_pk"),
    )
    parent_observed = ObservedTable(
        qualified_name=parent_name,
        columns=(ObservedColumn("OrderId", Integer(), nullable=False),),
        primary_key=PrimaryKeyConstraint(columns=("OrderId",), constraint_name="parent_pk"),
    )
    parent_diff = diff_table(parent_desired, parent_observed)

    child_constraint = ForeignKeyConstraint(
        local_columns=("ref",),
        referenced_table=parent_name,
        referenced_columns=("ordernumber",),
        constraint_name="test_ref_fk",
    )
    child_desired = _desired(
        columns=(DesiredColumn("ref", Integer()),),
        foreign_keys=(child_constraint,),
    )
    child_diff = diff_table(child_desired, _observed(columns=(ObservedColumn("ref", Integer()),)))

    schemas = {
        child_diff.target: resulting_column_spellings(child_diff),
        parent_diff.target: resulting_column_spellings(parent_diff),
    }
    result = plan_diff(child_diff, schemas)

    assert isinstance(result, PlanningSucceeded)
    [action] = [a for a in result.plan if isinstance(a, SetForeignKey)]
    assert action.constraint.referenced_columns == ("orderNumber",)


def test_self_referencing_foreign_key_binds_through_the_tables_own_schema():
    constraint = ForeignKeyConstraint(
        local_columns=("parentref",),
        referenced_table=_NAME,
        referenced_columns=("id",),
        constraint_name="test_parentref_fk",
    )
    desired = _desired(
        columns=(
            DesiredColumn("id", Integer(), nullable=False),
            DesiredColumn("parentref", Integer()),
        ),
        primary_key=PrimaryKeyConstraint(columns=("id",), constraint_name="test_pk"),
        foreign_keys=(constraint,),
    )
    observed = _observed(
        columns=(
            ObservedColumn("Id", Integer(), nullable=False),
            ObservedColumn("ParentRef", Integer()),
        ),
        primary_key=PrimaryKeyConstraint(columns=("Id",), constraint_name="test_pk"),
    )
    diff = diff_table(desired, observed)

    result = _plan(diff)

    assert isinstance(result, PlanningSucceeded)
    [action] = [a for a in result.plan if isinstance(a, SetForeignKey)]
    assert action.constraint.local_columns == ("ParentRef",)
    assert action.constraint.referenced_columns == ("Id",)
```

- [ ] **Step 3: Compiler-level cases**

Append to `tests/adapters/databricks/sql/test_compile.py` (reuse `_TARGET` and the file's helpers; add imports it lacks in the same edit):

```python
def test_set_primary_key_emits_the_exact_bound_spelling():
    # The compiler quotes what the plan carries — the bound camelCase name —
    # with no normalization of its own.
    action = SetPrimaryKey(
        primary_key=PrimaryKeyConstraint(columns=("requestId",), constraint_name="tbl_pk")
    )
    plan = ActionPlan(target=_TARGET, actions=(action,))

    [statement] = compile_plan(plan)

    assert statement == (
        "ALTER TABLE `cat`.`sch`.`tbl` ADD CONSTRAINT `tbl_pk` PRIMARY KEY (`requestId`)"
    )


def test_create_table_emits_declared_spelling_for_columns_and_inline_key():
    table = DesiredTable(
        qualified_name=_TARGET,
        columns=(DesiredColumn("requestId", String(), nullable=False),),
        primary_key=PrimaryKeyConstraint(columns=("requestId",), constraint_name="tbl_pk"),
    )
    plan = ActionPlan(target=_TARGET, actions=(CreateTable(table),))

    [statement] = compile_plan(plan)

    assert "`requestId` STRING NOT NULL" in statement
    assert "PRIMARY KEY (`requestId`)" in statement


def test_foreign_key_emits_exact_spelling_on_both_sides():
    constraint = ForeignKeyConstraint(
        local_columns=("orderRef",),
        referenced_table=_REFERENCED_TABLE,
        referenced_columns=("OrderId",),
        constraint_name="tbl_orderref_fk",
    )
    plan = ActionPlan(target=_TARGET, actions=(SetForeignKey(constraint=constraint),))

    [statement] = compile_plan(plan)

    assert "FOREIGN KEY (`orderRef`)" in statement
    assert "(`OrderId`)" in statement


def test_drop_foreign_key_emits_the_exact_observed_constraint_name():
    constraint = ForeignKeyConstraint(
        local_columns=("a",),
        referenced_table=_REFERENCED_TABLE,
        referenced_columns=("b",),
        constraint_name="Legacy_FK_Name",
    )
    plan = ActionPlan(target=_TARGET, actions=(DropForeignKey(constraint=constraint),))

    [statement] = compile_plan(plan)

    assert "DROP CONSTRAINT IF EXISTS `Legacy_FK_Name`" in statement
```

- [ ] **Step 4: Engine-level byte-equality**

Append to `tests/application/test_engine.py`, following the file's `_RecordingReader`/executor fixture patterns (a fake executor that records `execute(statement)` calls; reuse the existing fakes if they already record — check `rg -n "class _RecordingExecutor" tests/application/test_engine.py` and follow that shape):

```python
def test_executed_sql_is_byte_for_byte_the_planned_sql_for_mixed_case_tables():
    # The spec's invariant: ActionPlan -> exact SQL preview -> the same SQL
    # is executed. A camelCase catalog column must not change that.
    fqn = "c.s.mixed_case"
    catalog, schema, table_name = fqn.split(".")
    observed = TablePresent(
        table=ObservedTable(
            qualified_name=QualifiedName(catalog, schema, table_name),
            columns=(ObservedColumn("requestId", String(), nullable=False),),
        )
    )
    spec = _spec_with_primary_key(fqn)  # declared all-lowercase

    dry_report = Engine(
        reader=_RecordingReader({fqn: observed}),
        executor=_RecordingExecutor(per_call_results=[]),
    ).sync(spec, dry_run=True)

    executor = _RecordingExecutor(per_call_results=[])
    Engine(reader=_RecordingReader({fqn: observed}), executor=executor).sync(spec)

    [table_report] = list(dry_report)
    assert tuple(executor.executed_statements) == table_report.planned_sql_statements
    assert any(
        "PRIMARY KEY (`requestId`)" in statement for statement in executor.executed_statements
    )
```

Adapt the fixture names to the file's real fakes: `_RecordingReader`/`_RecordingExecutor` exist (check `rg -n "class _Recording" tests/application/test_engine.py` for the recorded-statements attribute name and the `per_call_results` shape), and `_spec_with_primary_key` may need defining beside the existing `_spec` helpers as a source whose `to_desired_table()` declares `DesiredColumn("requestid", String(), nullable=False)` with `PrimaryKeyConstraint(columns=("requestid",), constraint_name=f"{table_name}_pk")`. The two assertions are the contract and must not be weakened: the executed tuple equals the dry-run planned tuple for identical declaration and observed state, and the PK statement carries `` `requestId` ``.

- [ ] **Step 5: Reader-assembly case (the spec's catalog-assembly test)**

Append to `tests/adapters/databricks/test_read.py`, reusing the file's `_describe_responses`/`_router` helpers and its `SimpleNamespace` row style:

```python
def test_tags_returned_in_different_case_attach_without_rewriting_the_observed_name():
    # information_schema can display the same identifier with different
    # casing than the describe; the join is by identity, and the observed
    # spelling stays the describe's.
    responses = _describe_responses(
        **{
            describe_json_query(QN): [
                (
                    _describe_doc(
                        columns=[
                            {"name": "requestId", "type": {"name": "string"}, "nullable": True}
                        ]
                    ),
                )
            ],
            column_tags_query(QN): [
                SimpleNamespace(column_name="requestid", tag_name="pii", tag_value="low"),
            ],
        }
    )

    state = read_catalog_state(_router(responses), QN)

    assert isinstance(state, TablePresent)
    [column] = state.table.columns
    assert column.name == "requestId"
    assert dict(column.tags) == {"pii": "low"}
```

(The name-preservation assertion only holds after Task 10; the tag-attachment half already holds from Task 7 — the whole test belongs here in the post-flip matrix.)

- [ ] **Step 6: Run everything**

Run: `uv run pytest tests/domain tests/application tests/adapters tests/api -q --no-cov` → PASS.
Run: `uv run pytest -q` → PASS.

- [ ] **Step 7: Commit**

```bash
git add tests
git commit -m "test: prove mixed-case identifiers converge and bind to physical spelling"
```

---

### Task 12: Live suite — invert the PK reproduction, add the FK reproduction

**Files:**

- Modify: `tests/live/test_sql_warehouse_live_column_case_repro.py`
- Modify: `tests/live/job_summary.py`

**Interfaces:**

- Consumes: the fixed engine (Tasks 1–11); `tests/live` fixtures `live_connection`/`live_tables` and helpers `execute_sql`, `live_catalog`, `live_schema`, `qualified_table`, `read_live_table`.
- Produces: live proof for the spec's acceptance criteria — PK succeeds directly and converges; FK binds exact case on both sides; the raw `ALTER COLUMN` platform fact and the `request_id` control stay pinned.

- [ ] **Step 1: Rewrite the module docstring and invert the PK test**

Replace the module docstring (it is no longer throwaway):

```python
"""
Live pins for camelCase column identifier handling.

The platform facts: Unity Catalog preserves a column's display spelling,
ordinary ALTER COLUMN resolves lowercase references against camelCase
columns, but the managed-constraint path does not. The engine therefore
binds constraint references to the catalog's exact spelling; these tests
pin both the platform behaviour and the engine's convergence.
"""
```

Replace `test_column_identifier_case_repro_metadata_sync_adds_primary_key` with:

```python
def test_column_identifier_case_metadata_sync_adds_primary_key_with_exact_spelling(
    live_connection, live_tables
):
    """Metadata sync adds a primary key using the catalog's camelCase spelling."""
    table_name = live_tables("column_case_add_primary_key")
    execute_sql(
        live_connection,
        f"CREATE TABLE {qualified_table(table_name)} "
        "(`requestId` STRING NOT NULL) USING DELTA",
    )
    # Declared all-lowercase on purpose: the emitted spelling must come from
    # the catalog, not from the declaration echoing it.
    declaration = DeltaTable(
        catalog=live_catalog(),
        schema=live_schema(),
        name=table_name,
        columns=(Column("requestid", String(), nullable=False),),
        primary_key=("requestid",),
        scope="metadata",
    )
    engine = build_sql_engine(live_connection)

    report = engine.sync(declaration)

    assert report.has_failures is False
    statements = next(iter(report.planned_sql_statements.values()))
    assert statements == (
        f"ALTER TABLE {qualified_table(table_name)} "
        f"ADD CONSTRAINT `{table_name}_pk` PRIMARY KEY (`requestId`)",
    )
    state = read_live_table(live_connection, table_name)
    assert state["primary_key"] == ("requestId",)
    assert state["primary_key_name"] == f"{table_name}_pk"
    assert engine.sync(declaration).has_changes is False
```

Keep `test_column_identifier_case_repro_raw_alter_uses_lowercase_reference` (platform fact) and `test_column_identifier_case_repro_real_name_mismatch_reports_structural_drift` (control) unchanged. Keep `test_column_identifier_case_repro_metadata_sync_matches_contract_schema` but update its two statement assertions — the engine now addresses existing columns by catalog spelling:

```python
    assert any("ALTER COLUMN `requestId` COMMENT" in statement for statement in statements)
    assert any("ALTER COLUMN `modelId` COMMENT" in statement for statement in statements)
```

- [ ] **Step 2: Add the FK reproduction**

Append (child and parent casing varied independently — parent PascalCase, child camelCase, declarations all-lowercase):

```python
def test_column_identifier_case_foreign_key_binds_exact_spelling_on_both_sides(
    live_connection, live_tables
):
    """A foreign key compiles with each table's exact catalog spelling and converges."""
    parent_name = live_tables("column_case_fk_parent")
    child_name = live_tables("column_case_fk_child")
    execute_sql(
        live_connection,
        f"CREATE TABLE {qualified_table(parent_name)} "
        "(`OrderId` STRING NOT NULL) USING DELTA",
    )
    execute_sql(
        live_connection,
        f"ALTER TABLE {qualified_table(parent_name)} "
        f"ADD CONSTRAINT `{parent_name}_pk` PRIMARY KEY (`OrderId`)",
    )
    execute_sql(
        live_connection,
        f"CREATE TABLE {qualified_table(child_name)} (`orderRef` STRING) USING DELTA",
    )

    parent = DeltaTable(
        catalog=live_catalog(),
        schema=live_schema(),
        name=parent_name,
        columns=(Column("orderid", String(), nullable=False),),
        primary_key=("orderid",),
        scope="metadata",
    )
    child = DeltaTable(
        catalog=live_catalog(),
        schema=live_schema(),
        name=child_name,
        columns=(Column("orderref", String()),),
        foreign_keys=(ForeignKey(columns={"orderref": "orderid"}, references=parent),),
        scope="metadata",
    )
    engine = build_sql_engine(live_connection)

    report = engine.sync(parent, child)

    assert report.has_failures is False
    child_statements = report.planned_sql_statements[
        f"{live_catalog()}.{live_schema()}.{child_name}"
    ]
    assert any(
        "FOREIGN KEY (`orderRef`)" in statement and "(`OrderId`)" in statement
        for statement in child_statements
    )
    assert engine.sync(parent, child).has_changes is False
```

Add `ForeignKey` to the module's `delta_engine.schema` import. Check the exact key format of `report.planned_sql_statements` against the existing test (`next(iter(...))` there) — use whatever key shape `SyncReport` exposes (`rg -n "planned_sql_statements" src/delta_engine/application/report.py`) and adjust the lookup accordingly.

- [ ] **Step 3: Update the job summary area blurb**

In `tests/live/job_summary.py`, replace the entry added by PR #287:

```python
    "test_sql_warehouse_live_column_case_repro.py": (
        "Column identifier case",
        "case-only references converge; constraints bind the catalog's exact spelling",
    ),
```

- [ ] **Step 4: Verify collection and lint (live tests cannot run locally)**

Run: `uv run pytest tests/live/test_sql_warehouse_live_column_case_repro.py -m databricks_e2e --collect-only --no-cov -q`
Expected: 5 tests collected.
Run: `uv run ruff check tests/live/ && uv run ruff format tests/live/`
Expected: clean.

- [ ] **Step 5: Commit**

```bash
git add tests/live/test_sql_warehouse_live_column_case_repro.py tests/live/job_summary.py
git commit -m "test: pin exact-case constraint binding in the live suite"
```

---

### Task 13: Docs, validation ladder, Live dispatch, PR update

**Files:**

- Modify: `docs/reference-limitations.md` (Identifier handling, lines ~20–30)
- Modify: any straggler found by the doc sweep below
- No CHANGELOG edit (commitizen generates it from the conventional commits at release)

- [ ] **Step 1: Rewrite the identifier-handling documentation**

In `docs/reference-limitations.md`, replace the "Identifier handling" paragraph (currently promising normalization to lowercase) with:

```markdown
## Identifier handling

Identifiers are case-insensitive, matching the platform: Databricks resolves
all identifiers case-insensitively (backticked or not), and Unity Catalog
stores catalog, schema, and table names in lowercase. Object name parts are
therefore normalized to lowercase, exactly as the catalog stores them.

Column-like identifiers — column names, nested struct field names,
partition and clustering references, and constraint columns and names —
keep their declared or observed spelling. Case never distinguishes two
identifiers: names differing only in case are the same column, collide as
duplicates within one schema, and a case-only difference between a
declaration and the catalog is never drift. When the engine changes an
existing column or adds a constraint over one, it emits the catalog's
exact spelling (some Databricks DDL paths require it); newly created
columns are spelled as declared. Public accessors such as ``Column.name``
and ``DeltaTable.primary_key`` return preserved spelling — callers that
relied on lowercase values should apply their own presentation policy.
```

- [ ] **Step 2: Sweep for stale lowercase claims**

Run: `rg -n -i "lowercase|lower-case" docs/ src/delta_engine README.md`
Fix every hit that claims column-like identifiers are lowercased (known candidates: `adapters/databricks/sql/types.py`'s `data_type_from_json` docstring phrase "struct fields colliding after lowercasing" → "struct fields colliding by identity"; any API docstring missed in Task 10). Claims about object names, tag semantics, property values, and enum labels stay.

- [ ] **Step 3: Full validation ladder**

Run in order; every step must pass before the next:

```bash
uv run ruff format . && uv run ruff check .
uv run mypy .
uv run lint-imports
uv run pytest -q                       # full local suite, coverage on
uv run sphinx-build -W -b html docs docs/_build/html
git diff --check
```

Expected: all clean. Commit any fixes with a conventional message scoped to what they fix, then re-run the ladder from the top.

- [ ] **Step 4: Push and dispatch the Live workflow**

```bash
git push origin fix/preserve-column-identifier-case
gh workflow run live.yaml --ref fix/preserve-column-identifier-case
sleep 20
gh run list --workflow live.yaml --limit 1 --json databaseId,status --jq '.[0].databaseId'
gh run watch <run-id> --exit-status
```

Expected: the whole live suite passes (~15 min), including the inverted PK test and the new FK test. On failure: `gh run view <run-id> --log-failed`.

| Outcome | Action |
| --- | --- |
| All pass | Acceptance criteria met. Proceed to Step 5. |
| Inverted PK or FK test fails with `COLUMN_NOT_FOUND_IN_TABLE` | Binding did not reach the compiler for that path — read the failing SQL in the log, trace which action type carried the wrong spelling, fix in `planning.py`/`diff.py` with a unit test reproducing it. Do not patch the compiler. |
| An unrelated live pin fails | Read the log; if the failure predates this branch (check `main`'s last Live run), report it to Tom rather than folding a fix into this PR. |
| The catalog reports a lowercased PK/FK column spelling | Platform behaviour contradicts the spec's premise — **stop and report to Tom**; the design needs revisiting, not this plan. |

- [ ] **Step 5: Update PR #287**

```bash
gh pr edit 287 --title "fix: preserve Databricks column identifier spelling"
```

Update the body (via `gh pr edit 287 --body-file -`) to cover: the defect (lowercase constraint references fail on camelCase physical columns), the shape (spelling preserved; identity via explicit `identifier_key`; symbolic references bound to post-sync spelling at planning; mechanical compiler), the behavior change (public accessors return preserved spelling), and the validation evidence (full local suite, docs build, Live workflow run URL from Step 4).

- [ ] **Step 6: Final commit**

```bash
git add docs src
git commit -m "docs: describe preserved identifier spelling and identity keys"
git push origin fix/preserve-column-identifier-case
```

---

## Acceptance criteria traceability

| Spec criterion | Proven by |
| --- | --- |
| No column-like constructor destructively lowercases | Task 10 Step 5 survivor audit |
| All case-insensitive identity via one helper | Tasks 1–7 (`identifier_key` the only canonicalization; Task 10 audit) |
| Qualified object-name normalization unchanged | Task 10 (qualified_name untouched; API test keeps lowercase object names) |
| Case-only desired/observed differences are no-ops | Task 11 diff cases; live contract test resync |
| Existing-column actions emit observed spelling | Task 5 emission + Task 11 matched-action and live `ALTER COLUMN \`requestId\`` assertions |
| New/renamed columns emit desired spelling | Task 5 + Task 11 rename/create cases |
| PK/FK actions emit post-sync spelling on every side | Task 9 binder + Task 11 planning/compile cases |
| Live PK repro succeeds without manual SQL and converges | Task 12 inverted test |
| Live FK repro proves both sides | Task 12 FK test |
| Real-name mismatch control still reports drift | Task 11 control + Task 12 retained live control |
| Full local suite and Live workflow pass | Task 13 ladder + dispatch |
