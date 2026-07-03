# Always-named constraints — implementation plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Eliminate the `constraint_name: str | None` two-lifecycle-state problem (cluster A of the [special-case audit](2026-07-03-special-case-audit.md)) by making `constraint_name` a required `str` on both `PrimaryKeyConstraint` and `ForeignKeyConstraint`, so the four downstream guards that re-prove "it is named by now" become statically dead and are deleted.

**Architecture:** Names are generated at construction time via a `generate()` classmethod factory on each constraint (the naming formula lives there, once). The public API layer (`DeltaTable`) calls the factory when it lowers a user declaration; the Databricks reader supplies the catalog's own name for observed constraints (it already does this for foreign keys — this plan extends it to primary keys). `DesiredTable.__post_init__` stops generating names (it keeps its validation role). With every constraint named at birth, `constraint_name` becomes a required `str`, and the asserts in the differ and compiler plus the drop-path `is not None` filter are deleted — mypy proves them unreachable.

**Tech Stack:** Python 3.12, frozen/slotted dataclasses, pytest + hypothesis, mypy, ruff.

**Migration strategy (why the task order matters):** This is a behaviour-preserving refactor, not a feature. A field cannot flip from optional to required in one step without turning the whole suite red, because dozens of call sites construct constraints without a name and rely on `DesiredTable.__post_init__` to name them. The plan therefore keeps the suite green at every task boundary:

1. Add the `generate()` factory and make `__post_init__` *tolerate* an already-named constraint (name only if unnamed). Now named and unnamed constructions coexist.
2. Migrate the production paths (API layer, reader) to construct named constraints.
3. Migrate the remaining unnamed constructions (all in tests) to named.
4. Only once nothing relies on auto-naming: make the field required, delete `with_generated_name`, delete the `__post_init__` naming block, delete the four guards.

## Global Constraints

- Python signatures use snake_case and full type hints (no abbreviations).
- Tests follow the classical/Detroit school: real domain objects, mocks only for outgoing I/O (Spark). Given/When/Then comments throughout.
- Comments explain *why*, never restate *what*.
- Never commit to `main`; work on a branch. Conventional commit messages (`refactor:`, `test:`, `docs:`). No `Co-Authored-By` trailers.
- Verification gate for every task: `uv run pytest`, `uv run ruff check src tests`, `uv run ruff format --check src tests`, `uv run mypy src`. A scoped subset run needs `--no-cov` (coverage `fail_under = 90` fails a partial run). ~16 JVM/Spark e2e cases error locally without Spark — that is the known baseline, not a regression.
- **Behaviour must not change.** The FK content `signature` (local columns, referenced table, referenced columns) stays the identity used for matching — names are never part of FK identity, so idempotency over a catalog with externally-chosen names is preserved. Primary keys are still diffed by column *set*; the observed PK's name is not used by the differ or compiler today (`DropPrimaryKey` carries no name), so fetching it in Task 2 is for type-honesty and symmetry with FKs, not to change planning.

---

## File structure

Production:

- `src/delta_engine/domain/model/primary_key.py` — add `generate()`; later make `constraint_name` required, delete `with_generated_name`, tighten the blank-check.
- `src/delta_engine/domain/model/foreign_key.py` — same shape as primary key.
- `src/delta_engine/domain/model/table.py` — `DesiredTable.__post_init__`: tolerate-named shim (Task 1), then delete the naming block (Task 5). Validation stays.
- `src/delta_engine/api/table.py` — build constraints via `generate()` at lowering time.
- `src/delta_engine/adapters/databricks/reader.py` — `_fetch_primary_key` selects and sets the constraint name.
- `src/delta_engine/domain/plan/differ.py` — delete the two asserts (`_diff_primary_key`, `_diff_foreign_keys`) and the drop-path `is not None` filter.
- `src/delta_engine/adapters/databricks/sql/compile.py` — delete the `CreateTable` PK-name assert.
- `docs/explanation-architecture.md` — update the constraint-naming paragraph.

Tests (migrate unnamed → named constructions, add coverage):

- `tests/domain/model/test_primary_key.py`, `tests/domain/model/test_foreign_key.py`
- `tests/domain/model/test_table.py`
- `tests/domain/plan/test_differ.py`
- `tests/adapters/databricks/test_reader.py`
- `tests/adapters/databricks/sql/test_compile.py`
- `tests/application/test_engine.py`, `tests/application/test_dependency_resolution.py`
- `tests/api/test_table.py`

---

### Task 1: Constraint factories and a tolerate-named shim

**Files:**
- Modify: `src/delta_engine/domain/model/primary_key.py`
- Modify: `src/delta_engine/domain/model/foreign_key.py`
- Modify: `src/delta_engine/domain/model/table.py:146-155` (naming block in `DesiredTable.__post_init__`)
- Test: `tests/domain/model/test_primary_key.py`, `tests/domain/model/test_foreign_key.py`, `tests/domain/model/test_table.py`

**Interfaces:**
- Produces:
  - `PrimaryKeyConstraint.generate(*, table_name: str, columns: tuple[str, ...]) -> Self` — returns a constraint named `{table_name}_pk`.
  - `ForeignKeyConstraint.generate(*, owner_table_name: str, local_columns: tuple[str, ...], referenced_table: QualifiedName, referenced_columns: tuple[str, ...]) -> Self` — returns a constraint named `{owner_table_name}_{'_'.join(local_columns)}_fk`.
  - `with_generated_name` still exists and now delegates to `generate()` (removed in Task 5).
  - `DesiredTable.__post_init__` now names a constraint only when its `constraint_name is None`, instead of rejecting an already-named one.

This task is additive plus one behaviour relaxation: a `DesiredTable` built directly with an already-named constraint is now accepted (previously rejected). This is safe — the public `ForeignKey`/`DeltaTable` API exposes no `constraint_name` field, so users still cannot supply one. The now-obsolete rejection test is removed here.

- [ ] **Step 1: Add `generate()` to `PrimaryKeyConstraint` and delegate `with_generated_name`**

In `src/delta_engine/domain/model/primary_key.py`, add a classmethod and rewrite `with_generated_name` to delegate (keeps the formula in one place):

```python
    @classmethod
    def generate(cls, *, table_name: str, columns: tuple[str, ...]) -> Self:
        """Return a constraint over ``columns`` named ``{table_name}_pk``."""
        return cls(columns=columns, constraint_name=f"{table_name}_pk")

    def with_generated_name(self, table_name: str) -> Self:
        """
        Return a copy carrying the engine-generated constraint name ``{table}_pk``.

        Delegates to :meth:`generate`. Rejects an already-named constraint so a
        user-supplied name fails loudly rather than being silently overwritten.
        """
        if self.constraint_name is not None:
            raise ValueError(
                "primary key constraint_name is generated by the engine, not user-defined;"
                f" leave it unset (got {self.constraint_name!r})"
            )
        return type(self).generate(table_name=table_name, columns=self.columns)
```

- [ ] **Step 2: Add `generate()` to `ForeignKeyConstraint` and delegate `with_generated_name`**

In `src/delta_engine/domain/model/foreign_key.py`:

```python
    @classmethod
    def generate(
        cls,
        *,
        owner_table_name: str,
        local_columns: tuple[str, ...],
        referenced_table: QualifiedName,
        referenced_columns: tuple[str, ...],
    ) -> Self:
        """Return a constraint named ``{owner_table_name}_{local_columns}_fk``."""
        columns = "_".join(local_columns)
        return cls(
            local_columns=local_columns,
            referenced_table=referenced_table,
            referenced_columns=referenced_columns,
            constraint_name=f"{owner_table_name}_{columns}_fk",
        )

    def with_generated_name(self, table_name: str) -> Self:
        """
        Return a copy carrying the engine-generated name ``{table}_{local_cols}_fk``.

        Delegates to :meth:`generate`. Rejects an already-named constraint so a
        user-supplied name fails loudly rather than being silently overwritten.
        """
        if self.constraint_name is not None:
            raise ValueError(
                "foreign key constraint_name is generated by the engine, not user-defined;"
                f" leave it unset (got {self.constraint_name!r})"
            )
        return type(self).generate(
            owner_table_name=table_name,
            local_columns=self.local_columns,
            referenced_table=self.referenced_table,
            referenced_columns=self.referenced_columns,
        )
```

- [ ] **Step 3: Make `DesiredTable.__post_init__` tolerate an already-named constraint**

In `src/delta_engine/domain/model/table.py`, replace the naming block (currently lines 146-155) so it names only when unset. This lets named (API/reader-style) and unnamed (direct test) constructions coexist during the migration:

```python
        table_name = self.qualified_name.name
        if self.primary_key is not None and self.primary_key.constraint_name is None:
            object.__setattr__(
                self, "primary_key", self.primary_key.with_generated_name(table_name)
            )
        object.__setattr__(
            self,
            "foreign_keys",
            tuple(
                fk if fk.constraint_name is not None else fk.with_generated_name(table_name)
                for fk in self.foreign_keys
            ),
        )
```

- [ ] **Step 4: Add factory unit tests**

Append to `tests/domain/model/test_primary_key.py`:

```python
def test_generate_names_constraint_from_table():
    # Given a table name and key columns
    # When the engine generates the constraint
    constraint = PrimaryKeyConstraint.generate(table_name="orders", columns=("id",))

    # Then the name follows {table}_pk and the columns are carried through
    assert constraint.constraint_name == "orders_pk"
    assert constraint.columns == ("id",)
```

Append to `tests/domain/model/test_foreign_key.py`:

```python
def test_generate_names_constraint_from_table_and_local_columns():
    # Given a table name and foreign key content
    # When the engine generates the constraint
    constraint = ForeignKeyConstraint.generate(
        owner_table_name="orders",
        local_columns=("customer_id",),
        referenced_table=_customers(),
        referenced_columns=("id",),
    )

    # Then the name follows {table}_{local_cols}_fk
    assert constraint.constraint_name == "orders_customer_id_fk"
```

- [ ] **Step 5: Update the tolerate-named behaviour test and remove the obsolete rejection test**

In `tests/domain/model/test_table.py`, add a test that a directly-constructed, already-named FK is accepted (the shim no longer rejects it):

```python
def test_desired_table_accepts_an_already_named_foreign_key():
    # Given a FK that already carries a name (as the API layer will produce it)
    fk = ForeignKeyConstraint(
        local_columns=("customer_id",),
        referenced_table=QualifiedName("cat", "sch", "customers"),
        referenced_columns=("id",),
        constraint_name="orders_customer_id_fk",
    )

    # When building a DesiredTable with it
    table = DesiredTable(
        qualified_name=QualifiedName("cat", "sch", "orders"),
        columns=(Column("id", Integer()), Column("customer_id", Integer())),
        foreign_keys=(fk,),
    )

    # Then the name is preserved (not regenerated, not rejected)
    assert table.foreign_keys[0].constraint_name == "orders_customer_id_fk"
```

In `tests/domain/plan/test_differ.py`, delete `test_desired_fk_with_user_supplied_constraint_name_is_rejected` (lines ~795-800) and the `_FK_WITH_EXPLICIT_NAME` module constant (lines ~714-719) it is the only user of. The invariant it guarded — "a name cannot be smuggled into a DesiredTable" — is being deliberately removed; the public API has no name field, so this is unreachable from user code.

- [ ] **Step 6: Run the affected tests, then the gate**

Run: `uv run pytest tests/domain --no-cov -q`
Expected: PASS (factories named correctly; shim accepts both named and unnamed; obsolete test gone).
Then the full gate (`pytest`, `ruff check`, `ruff format --check`, `mypy src`).

- [ ] **Step 7: Commit**

```bash
git add src/delta_engine/domain/model tests/domain
git commit -m "refactor: add constraint generate() factory and tolerate-named shim"
```

---

### Task 2: Reader supplies the primary key's catalog name

**Files:**
- Modify: `src/delta_engine/adapters/databricks/reader.py:162-193` (`_fetch_primary_key`)
- Test: `tests/adapters/databricks/test_reader.py`

**Interfaces:**
- Consumes: `PrimaryKeyConstraint` (still `str | None` at this point, so passing a name is valid).
- Produces: observed `PrimaryKeyConstraint`s now carry the catalog's constraint name (foreign keys already do, via `_fetch_foreign_keys`). This mirrors the FK reader and makes every observed constraint named ahead of the Task 5 tightening.

The observed PK name is not consumed by the differ or compiler today; this change is for type-honesty and symmetry, not behaviour. Do not add any planning behaviour that reads it.

- [ ] **Step 1: Update the failing test first**

In `tests/adapters/databricks/test_reader.py`, the PK fixtures pass `pk_column_rows=[{"column_name": "..."}]`. Add the constraint name to each row and assert it on the result. Update `test_fetch_state_includes_primary_key_in_observed_table`:

```python
    spark = FakeSparkWithPrimaryKey(
        catalog=catalog,
        describe_rows=[{"properties": {}}],
        pk_column_rows=[{"column_name": "id", "constraint_name": "t_pk"}],
    )

    # When
    result = DatabricksReader(spark).fetch_state(qn)

    # Then: primary_key is populated on the ObservedTable, carrying the catalog name
    assert isinstance(result, TablePresent)
    assert result.table.primary_key == PrimaryKeyConstraint(
        columns=("id",), constraint_name="t_pk"
    )
```

Update `test_fetch_state_lowercases_primary_key_column_names_from_catalog` similarly — give the row `"constraint_name": "T_PK"` and assert the name is casefolded to `"t_pk"` on the result (consistent with how the reader casefolds FK names and column names):

```python
        pk_column_rows=[{"column_name": "OrderID", "constraint_name": "T_PK"}],
    ...
    assert result.table.primary_key == PrimaryKeyConstraint(
        columns=("orderid",), constraint_name="t_pk"
    )
```

Leave `test_fetch_state_primary_key_is_empty_when_none_defined` (empty rows → `None`) and `test_fetch_primary_key_returns_empty_when_information_schema_unavailable` unchanged — both still expect `None`.

- [ ] **Step 2: Run the test to see it fail**

Run: `uv run pytest tests/adapters/databricks/test_reader.py -k primary_key --no-cov -q`
Expected: FAIL — the reader does not yet select or set the constraint name, so the observed PK's name is `None`.

- [ ] **Step 3: Fetch and set the constraint name in `_fetch_primary_key`**

In `src/delta_engine/adapters/databricks/reader.py`, extend the SELECT to include the constraint name and construct a named `PrimaryKeyConstraint`. Replace the body of `_fetch_primary_key`:

```python
        catalog = backtick(qualified_name.catalog)
        query = (
            f"SELECT table_constraints_info.constraint_name,"
            f" constraint_columns.column_name"
            f" FROM {catalog}.information_schema.constraint_column_usage"
            f" AS constraint_columns"
            f" JOIN {catalog}.information_schema.table_constraints"
            f" AS table_constraints_info"
            f" USING (constraint_catalog, constraint_schema, constraint_name)"
            f" WHERE constraint_columns.table_schema ="
            f" {quote_literal(qualified_name.schema)}"
            f" AND constraint_columns.table_name ="
            f" {quote_literal(qualified_name.name)}"
            f" AND table_constraints_info.constraint_type = 'PRIMARY KEY'"
        )
        try:
            rows = self.spark.sql(query).collect()
        except AnalysisException:
            # information_schema is only available in Unity Catalog. On plain
            # Spark (e.g. local tests), the table does not exist and there are
            # no PK constraints to observe.
            return None
        columns = tuple(row["column_name"].casefold() for row in rows)
        if not columns:
            return None
        constraint_name = rows[0]["constraint_name"].casefold()
        return PrimaryKeyConstraint(columns=columns, constraint_name=constraint_name)
```

- [ ] **Step 4: Run the test to see it pass, then the gate**

Run: `uv run pytest tests/adapters/databricks/test_reader.py --no-cov -q`
Expected: PASS.
Then the full gate.

- [ ] **Step 5: Commit**

```bash
git add src/delta_engine/adapters/databricks/reader.py tests/adapters/databricks/test_reader.py
git commit -m "refactor: read the primary key's catalog constraint name"
```

---

### Task 3: API layer generates constraint names at lowering time

**Files:**
- Modify: `src/delta_engine/api/table.py:47-86` (`ForeignKey._to_constraint`), `:131-134` (primary key build)
- Test: `tests/api/test_table.py` (assertions already expect named constraints — verify, do not weaken)

**Interfaces:**
- Consumes: `PrimaryKeyConstraint.generate`, `ForeignKeyConstraint.generate` (Task 1).
- Produces: `DeltaTable.to_desired_table()` returns a `DesiredTable` whose constraints are already named by the API layer. The `__post_init__` shim (Task 1) sees them named and leaves them untouched — so with this task the production path no longer relies on auto-naming.

- [ ] **Step 1: Generate the primary key name in `DeltaTable.__init__`**

In `src/delta_engine/api/table.py`, replace the primary key construction (currently lines 131-134). The table name is `name` (the `__init__` parameter):

```python
        primary_key_columns = tuple(column.name for column in columns if column.primary_key)
        primary_key = (
            PrimaryKeyConstraint.generate(table_name=name, columns=primary_key_columns)
            if primary_key_columns
            else None
        )
```

- [ ] **Step 2: Generate the foreign key name in `ForeignKey._to_constraint`**

In the same file, change the `return` of `_to_constraint` (currently lines 82-86) to build a named constraint via the factory. `owner_name` is the enclosing table's `QualifiedName`:

```python
        return ForeignKeyConstraint.generate(
            owner_table_name=owner_name.name,
            local_columns=tuple(self.local_columns),
            referenced_table=referenced_table,
            referenced_columns=tuple(referenced_columns),
        )
```

- [ ] **Step 3: Confirm the API tests still assert names**

`tests/api/test_table.py:261` already asserts `desired.primary_key == PrimaryKeyConstraint(columns=("id",), constraint_name="orders_pk")`. Run it — it must still pass, now because the API generated the name rather than `__post_init__`:

Run: `uv run pytest tests/api/test_table.py --no-cov -q`
Expected: PASS. If any assertion there was relying on `__post_init__` to add the name, it is unaffected — the name is identical.

- [ ] **Step 4: Run the gate**

Full gate. The e2e tests exercise the API → engine path and must stay green (subject to the known no-Spark baseline).

- [ ] **Step 5: Commit**

```bash
git add src/delta_engine/api/table.py
git commit -m "refactor: generate constraint names when lowering the public API"
```

---

### Task 4: Migrate direct constraint constructions in tests to named form

**Files:**
- Test: `tests/domain/model/test_table.py`, `tests/domain/plan/test_differ.py`, `tests/application/test_engine.py`, `tests/application/test_dependency_resolution.py`, `tests/adapters/databricks/sql/test_compile.py`

After this task, no test relies on `DesiredTable.__post_init__` to name a constraint — every construction supplies a name, either via `generate()` (modelling the desired/API path) or an explicit `constraint_name=` (modelling an observed/catalog constraint). The `__post_init__` shim becomes inert, ready for deletion in Task 5.

This is mechanical. For each site listed, replace the unnamed construction. Do not change any assertion about *behaviour* (action counts, ordering, failure reasons) — only the construction of the input constraints. Run the affected file after each file's edits.

- [ ] **Step 1: `tests/domain/model/test_table.py`**

- `test_table_snapshot_rejects_pk_column_not_in_columns` (line ~83), `test_desired_table_rejects_nullable_primary_key_column` (~106), `test_desired_table_reports_the_offending_nullable_primary_key_column` (~120): these assert a `ValueError` is raised by `DesiredTable`. The PK column-existence and NOT-NULL checks run in `TableSnapshot.__post_init__` / `DesiredTable.__post_init__` *before* naming, so they still fire with an unnamed PK — but to keep every construction named, pass a generated PK, e.g. `primary_key=PrimaryKeyConstraint.generate(table_name="orders", columns=("missing_col",))`.
- `test_observed_table_has_primary_key_field` (~89) and `test_observed_table_allows_a_nullable_primary_key_column` (~124): these build `ObservedTable`. Give the PK an explicit catalog-style name (`constraint_name="t_pk"`) and update the equality assertion to match.
- `test_table_snapshot_stores_foreign_keys` (~148): replace the `fk.with_generated_name("orders")` comparison with an explicit expected named constraint:

```python
    assert table.foreign_keys == (
        ForeignKeyConstraint(
            local_columns=("customer_id",),
            referenced_table=QualifiedName("cat", "sch", "customers"),
            referenced_columns=("id",),
            constraint_name="orders_customer_id_fk",
        ),
    )
    assert table.foreign_keys[0].constraint_name == "orders_customer_id_fk"
```

- The duplicate-FK tests (`test_table_snapshot_rejects_foreign_keys_with_duplicate_derived_names`, `test_desired_table_rejects_two_foreign_keys_over_the_same_local_columns`, `test_desired_table_rejects_foreign_keys_that_differ_only_in_local_column_order`): these still expect a `ValueError` from the same-local-columns check, which runs before naming. Leave the FK inputs unnamed — the shim still names them, and Task 5 keeps this working because the duplicate check is independent of naming. No change needed unless mypy/red after Task 5; revisit in Task 5 if so.

Run: `uv run pytest tests/domain/model/test_table.py --no-cov -q` → PASS.

- [ ] **Step 2: `tests/domain/plan/test_differ.py`**

- Hypothesis helper (line ~122): `primary_key = PrimaryKeyConstraint.generate(table_name=_QUALIFIED_NAME.name, columns=primary_key_cols) if primary_key_cols else None`.
- PK-diff test fixtures at lines ~545, ~556, ~617, ~625, ~645, ~664 that build `PrimaryKeyConstraint(columns=...)` for a *desired* table: switch to `PrimaryKeyConstraint.generate(table_name=<that table's name>, columns=...)`. For any that represent an *observed* PK, give an explicit `constraint_name=`.
- `_FK` module constant (line ~708): leave unnamed only if it flows solely through `_orders_with_fk` (which builds a `DesiredTable`, so the shim names it). To make it explicit and Task-5-safe, prefer `ForeignKeyConstraint.generate(owner_table_name="orders", local_columns=("customer_id",), referenced_table=QualifiedName("cat","sch","customers"), referenced_columns=("id",))`. Update `test_fk_same_on_both_sides_produces_no_fk_actions` — it uses `_FK` on both the desired and observed side; the observed side needs a name too (any name; signature matching ignores it), so build the observed side from a named constraint.
- Observed-side FK fixtures already pass `constraint_name=` (lines ~759, ~806, ~856, ~882) — leave them.

Run: `uv run pytest tests/domain/plan/test_differ.py --no-cov -q` → PASS.

- [ ] **Step 3: `tests/application/test_engine.py` and `tests/application/test_dependency_resolution.py`**

- `test_engine.py:91` `primary_key=PrimaryKeyConstraint(columns=("id",))` inside a desired-table helper → `PrimaryKeyConstraint.generate(table_name=<helper's table name>, columns=("id",))`.
- `test_dependency_resolution.py:398` `primary_key=PrimaryKeyConstraint(columns=("id",))` and the FK at `:400` are inside a directly-built `DesiredTable` (`orders`) — name them via `generate()` (`table_name="orders"` / `owner_table_name="orders"`).

Run: `uv run pytest tests/application --no-cov -q` → PASS.

- [ ] **Step 4: `tests/adapters/databricks/sql/test_compile.py`**

- Line ~324 `primary_key=PrimaryKeyConstraint(columns=("id",))` feeds a `CreateTable` compile test. The compiler reads `table.primary_key.constraint_name`, so the PK must be named: `PrimaryKeyConstraint.generate(table_name=<the table name used in that test>, columns=("id",))`. Confirm the expected SQL string in that test already contains the generated constraint name (e.g. `CONSTRAINT \`orders_pk\` PRIMARY KEY (...)`); if the test built the PK unnamed and relied on `__post_init__`, the emitted name is unchanged.

Run: `uv run pytest tests/adapters/databricks/sql/test_compile.py --no-cov -q` → PASS.

- [ ] **Step 5: Run the full gate**

Full gate green. Nothing now depends on `__post_init__` naming.

- [ ] **Step 6: Commit**

```bash
git add tests
git commit -m "test: construct constraints with generated names ahead of the type tightening"
```

---

### Task 5: Make `constraint_name` required and delete the dead guards

**Files:**
- Modify: `src/delta_engine/domain/model/primary_key.py`, `src/delta_engine/domain/model/foreign_key.py`
- Modify: `src/delta_engine/domain/model/table.py` (delete `DesiredTable.__post_init__` naming block)
- Modify: `src/delta_engine/domain/plan/differ.py:282` (delete assert), `:318` (delete assert), `:327-333` (delete the drop-path `is not None` filter)
- Modify: `src/delta_engine/adapters/databricks/sql/compile.py:63-65` (delete the PK-name assert)
- Modify: `docs/explanation-architecture.md` (constraint-naming paragraph)
- Test: `tests/domain/model/test_primary_key.py`, `tests/domain/model/test_foreign_key.py`

**Interfaces:**
- Produces: `constraint_name: str` (required, no default) on both constraints. `with_generated_name` is removed; `generate()` is the only naming path. `DesiredTable.__post_init__` performs validation only.

- [ ] **Step 1: Require `constraint_name` and tighten the blank-check on `PrimaryKeyConstraint`**

In `src/delta_engine/domain/model/primary_key.py`: change the field to `constraint_name: str` (remove ` | None = None`), delete `with_generated_name`, and simplify `__post_init__` (the name is always present now):

```python
    columns: tuple[str, ...]
    constraint_name: str

    def __post_init__(self) -> None:
        if not self.columns:
            raise ValueError("columns must not be empty")
        seen: set[str] = set()
        for column in self.columns:
            if column in seen:
                raise ValueError(f"Duplicate primary key column: {column}")
            seen.add(column)
        if not self.constraint_name.strip():
            raise ValueError("constraint_name must not be blank")
```

Update the class docstring to drop the `| None` / `with_generated_name` wording.

- [ ] **Step 2: Require `constraint_name` and tighten the blank-check on `ForeignKeyConstraint`**

In `src/delta_engine/domain/model/foreign_key.py`: change the field to `constraint_name: str`, delete `with_generated_name`, and simplify the blank-check line in `__post_init__` to `if not self.constraint_name.strip():`. Update the docstring. `signature` is unchanged (it never included the name).

- [ ] **Step 3: Delete the `__post_init__` naming block in `DesiredTable`**

In `src/delta_engine/domain/model/table.py`, remove the entire naming block added/kept in Task 1 (the `table_name = ...` line and both `object.__setattr__` calls). Keep the FK-duplicate-local-columns check and the nullable-PK check. Update the `DesiredTable` class docstring: constraint names now arrive already generated (from the API layer or the reader), not resolved in `__post_init__`.

- [ ] **Step 4: Delete the four guards**

- `src/delta_engine/domain/plan/differ.py:282` — delete `assert desired_pk.constraint_name is not None  # ...`. The `SetPrimaryKey(...)` call remains; `desired_pk.constraint_name` is now typed `str`.
- `src/delta_engine/domain/plan/differ.py:318` — delete `assert foreign_key.constraint_name is not None  # ...` inside the `for foreign_key in matched.added:` loop.
- `src/delta_engine/domain/plan/differ.py:327-333` — remove the `if foreign_key.constraint_name is not None` filter and its comment; the drop-actions generator becomes:

```python
    drop_actions = tuple(
        DropForeignKey(constraint_name=foreign_key.constraint_name)
        for foreign_key in matched.dropped
    )
```

- `src/delta_engine/adapters/databricks/sql/compile.py:63-65` — delete `assert constraint_name is not None  # ...`; read `constraint_name = table.primary_key.constraint_name` (now `str`) and use it directly.

- [ ] **Step 5: Update the constraint unit tests for the required field**

In `tests/domain/model/test_primary_key.py`:
- `test_rejects_empty_columns` and `test_rejects_duplicate_columns`: add a name so the dataclass constructs and `__post_init__` runs, e.g. `PrimaryKeyConstraint(columns=(), constraint_name="t_pk")`.
- `test_rejects_blank_explicit_constraint_name`: keep — `PrimaryKeyConstraint(columns=("id",), constraint_name="  ")` still raises, now via the unconditional blank-check.
- `test_equal_by_value`: both sides need a name — `PrimaryKeyConstraint(columns=("a","b"), constraint_name="t_pk")` on each side.
- Keep `test_generate_names_constraint_from_table` (Task 1) — it now also demonstrates the only naming path.

In `tests/domain/model/test_foreign_key.py`:
- `test_signature_ignores_constraint_name`: both FKs now need names; give them *different* names and assert signatures are still equal.
- The empty/mismatched-columns rejection tests: add `constraint_name="x_fk"` so construction reaches `__post_init__`.
- `test_rejects_blank_explicit_constraint_name`: keep.
- Delete `test_generated_name_follows_table_and_local_columns` (it tests the removed `with_generated_name`); `test_generate_names_constraint_from_table_and_local_columns` (Task 1) replaces it.
- `test_foreign_key_constraint_is_frozen`: add a `constraint_name=`.

- [ ] **Step 6: Update the architecture doc**

In `docs/explanation-architecture.md`, rewrite the constraint-naming paragraph (the one mentioning `DesiredTable.__post_init__ calls each constraint's with_generated_name`) to state: a key constraint's name is a pure function of the table name and columns (`{table}_pk`, `{table}_{columns}_fk`), generated by the API layer when a `DeltaTable` is lowered (and read from the catalog for observed constraints), and then carried as data — the differ and compiler read the name off the constraint. Drop the `with_generated_name` reference.

- [ ] **Step 7: Run the full gate — mypy proves the deletions safe**

Run `uv run mypy src` first: it must pass with the asserts and filter gone, confirming `constraint_name` is statically `str` at every deleted site. Then `uv run pytest`, `ruff check`, `ruff format --check`.
Expected: all green (known no-Spark e2e baseline aside).

- [ ] **Step 8: Commit**

```bash
git add src/delta_engine tests/domain/model docs/explanation-architecture.md
git commit -m "refactor: require a named constraint and delete the is-named guards"
```

---

## Self-review checklist (run before dispatching Task 1)

- **Spec coverage:** cluster A's five guard sites — differ.py:282, differ.py:318, differ.py:332, compile.py:64, and the `with_generated_name` reject-path — are all deleted or subsumed (Task 5, plus the reject test removed in Task 1). ✅
- **Decision 4 (observed PK naming):** resolved as "fetch it" (Task 2), matching the chosen always-named approach. ✅
- **Green at every boundary:** Tasks 1-4 keep the field optional and the shim active; only Task 5 flips it required, after every construction is named. ✅
- **Behaviour preserved:** FK matching stays signature-based; PK diffing stays column-set-based; the generated names are byte-identical to today's (`{table}_pk`, `{table}_{cols}_fk`). ✅
- **Type consistency:** `generate()` signatures in the Interfaces blocks match their call sites in Tasks 2-3. ✅

## Follow-ups (out of scope)

- The "allow existing constraint names to be passed to `DeltaTable`" todo item becomes straightforward under this design: add an optional `constraint_name` to the public `ForeignKey`/primary-key API and pass it to `generate()` (or bypass generation). Not part of this plan.
