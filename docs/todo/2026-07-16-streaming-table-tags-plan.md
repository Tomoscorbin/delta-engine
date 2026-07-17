# Tags scope on streaming tables — implementation plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Tags-scope syncs (`DeltaTable(scope="tags")`) work against Databricks streaming tables: the reader admits them with a discovered kind, validation restricts them to tag aspects, and the compiler emits `ALTER STREAMING TABLE` for their DDL.

**Architecture:** The observed relation kind becomes a domain fact (`TableKind` on `ObservedTable`, copied onto `TableDrift`), judged by an unsuppressable validation gate (`StreamingTableTagsOnly`), and threaded to the SQL compiler by widening the `PlanExecutor.compile` port to `compile(qualified_name, plan, kind)`. Backend facts are pinned in the opt-in `tests/live` suite before the reader gate is written.

> **Superseded in part (2026-07-16, post-review):** the kind-threading above and the file-map rows for Tasks 4, 6, and 7 describe the shape as first executed. The shipped design differs: `TableDrift` carries the `observed` table, `ActionPlan` carries `kind`, and the `compile(qualified_name, plan)` port is unchanged. This document is a historical execution record — see the third adjustment under Task 2 and the design doc's dated revision notes for the final shape.

**Tech Stack:** Python 3.12, uv, pytest, ruff, mypy, import-linter, Sphinx/MyST. No new dependencies.

**Spec:** `docs/todo/2026-07-16-streaming-table-tags-design.md` (approved). Where this plan states a Databricks fact, the spec is its source; where a live pin (Task 2) disagrees with the spec's expectation, the pinned value wins.

## Global Constraints

- Branch: `claude/streaming-table-alter-scope-354849`. Never commit to `main`. Force-push is hook-blocked; if the branch needs refreshing, merge from `main`.
- The working tree is shared: run `git status` and confirm the branch before every commit.
- Use `uv run ...` for every command. Line length 100. Absolute imports only. Type hints on all function signatures. Ruff pydocstyle (`D`) is enabled for `src`: every new public module/class/function needs a docstring; docstrings describe what the code does — design rationale lives in the spec, not in docstrings.
- A PostToolUse autofix hook strips unused imports after every edit: always add an import in the same edit as the code that uses it.
- Coverage runs by default with `fail_under = 70`; the full local suite runs without Databricks credentials.
- The live suite (`tests/live/`, marker `databricks_e2e`) never runs locally or in default CI. It runs only via the manual GitHub workflow: `gh workflow run live.yaml --ref claude/streaming-table-alter-scope-354849` (~15 min). **Do not dispatch the Live workflow between Tasks 8 and 10** — in that window the old live pin (streaming table fails its read) contradicts the new reader.
- Conventional commit messages. No `Co-authored-by` trailers.

## File structure

| File                                                                                  | Responsibility in this change                                                                                                                                                                                                                                                                                                                                                    |
| ------------------------------------------------------------------------------------- | -------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------- |
| `src/delta_engine/domain/model/table.py`                                              | New `TableKind` enum; `ObservedTable.kind` field                                                                                                                                                                                                                                                                                                                                 |
| `src/delta_engine/domain/model/__init__.py`                                           | Export `TableKind`                                                                                                                                                                                                                                                                                                                                                               |
| `src/delta_engine/domain/plan/diff.py`                                                | `TableDrift.kind`; `diff_table` copies it from the observed table                                                                                                                                                                                                                                                                                                                |
| `src/delta_engine/application/validation.py`                                          | `StreamingTableTagsOnly` scope gate, wired into `_scope_failures`                                                                                                                                                                                                                                                                                                                |
| `src/delta_engine/adapters/databricks/sql/compile.py`                                 | Kind-selected `ALTER TABLE` / `ALTER STREAMING TABLE` clause via one `_SqlTarget` value                                                                                                                                                                                                                                                                                          |
| `src/delta_engine/application/ports.py`                                               | `PlanExecutor.compile` widens to `(qualified_name, plan, kind)`                                                                                                                                                                                                                                                                                                                  |
| `src/delta_engine/application/engine.py`                                              | `_TableRun.observed_kind`; the one compile call site passes it                                                                                                                                                                                                                                                                                                                   |
| `src/delta_engine/adapters/databricks/spark/executor.py`, `.../warehouse/executor.py` | Forward `kind` to `compile_plan`                                                                                                                                                                                                                                                                                                                                                 |
| `src/delta_engine/adapters/databricks/read.py`                                        | Admit-gate becomes a relation-type → `TableKind` mapping; streaming tables admitted, materialized views still fail closed                                                                                                                                                                                                                                                        |
| `src/delta_engine/api/delta_table.py`                                                 | `scope` docstring update only (no API change)                                                                                                                                                                                                                                                                                                                                    |
| `tests/live/test_sql_warehouse_live_streaming_tables.py`                              | New: five live pins + provisioning helper                                                                                                                                                                                                                                                                                                                                        |
| `tests/live/test_sql_warehouse_live_supported_relations.py`                           | The streaming-table fail-closed pin is removed (superseded)                                                                                                                                                                                                                                                                                                                      |
| `tests/adapters/databricks/warehouse/test_streaming_table_dry_run.py`                 | New: describe-JSON-to-SQL dry-run integration test                                                                                                                                                                                                                                                                                                                               |
| Unit test files                                                                       | `tests/domain/model/test_table.py`, `tests/domain/plan/test_diff.py`, `tests/application/test_validation.py`, `tests/adapters/databricks/sql/test_compile.py`, `tests/application/test_engine.py`, `tests/cli/conftest.py`, `tests/adapters/databricks/spark/test_executor.py`, `tests/adapters/databricks/warehouse/test_executor.py`, `tests/adapters/databricks/test_read.py` |
| Docs                                                                                  | `how-to-deploy-metadata-only.md`, `how-to-configure-table.md`, `reference-safe-change-rules.md`, `reference-limitations.md`, `explanation-safety-model.md`, `how-to-implement-adapter.md`, `how-to-add-action-type.md`, `explanation-architecture.md`                                                                                                                            |

---

### Task 1: Live pins for the streaming-table platform facts

Write the four backend-fact pins (spec live pins 1–4). They exercise no engine code, so they are written and dispatched before any implementation: the reader gate (Task 8) is written against what pin 1 reports.

**Files:**

- Create: `tests/live/test_sql_warehouse_live_streaming_tables.py`

**Interfaces:**

- Consumes: `tests/live/conftest.py` fixtures `live_connection` / `live_tables` (every test in `tests/live/` is auto-marked `databricks_e2e`; `live_tables` allocates uuid-suffixed names and drops them with `DROP TABLE IF EXISTS` in reverse order).
- Produces: `_create_streaming_table(live_connection, live_tables) -> str` — the provisioning helper Tasks 10's tests also call.

- [ ] **Step 1: Write the pin file**

Create `tests/live/test_sql_warehouse_live_streaming_tables.py`:

```python
"""
Live pins for the streaming-table facts the tags scope is built on.

A streaming table's definition — schema, comments, properties — is owned by
its pipeline; Unity Catalog tags are the one aspect durably manageable from
outside it, and only through the ALTER STREAMING TABLE dialect. Each pin
states one platform fact the engine's reader gate, validation gate, or SQL
dialect dispatch assumes.
"""

import json

import pytest

pytest.importorskip("databricks.sql")

from databricks.sql.exc import ServerOperationError

from delta_engine.adapters.databricks.sql.dialect import backtick, quote_literal
from tests.live.sql_warehouse_live_helpers import (
    execute_sql,
    fetch_rows,
    live_catalog,
    live_schema,
    qualified_table,
)


def _create_streaming_table(live_connection, live_tables) -> str:
    """Create a streaming table over a one-column Delta source; skip if the workspace cannot."""
    source_name = live_tables("st_source")
    table_name = live_tables("st")
    execute_sql(
        live_connection,
        f"CREATE TABLE {qualified_table(source_name)} (id INT) USING DELTA",
    )
    try:
        execute_sql(
            live_connection,
            f"CREATE STREAMING TABLE {qualified_table(table_name)} "
            f"AS SELECT id FROM STREAM({qualified_table(source_name)})",
        )
    except Exception as exc:  # intentional broad except: environment capability probe
        pytest.skip(f"workspace cannot create a streaming table here: {exc}")
    # Plain DROP TABLE drops a streaming table (verified live), so the
    # live_tables teardown owns the cleanup; the source is dropped after it.
    return table_name


def _table_tags(live_connection, table_name: str) -> dict[str, str]:
    rows = fetch_rows(
        live_connection,
        f"SELECT tag_name, tag_value "
        f"FROM {backtick(live_catalog())}.information_schema.table_tags "
        f"WHERE schema_name = {quote_literal(live_schema())} "
        f"AND table_name = {quote_literal(table_name)}",
    )
    return {row["tag_name"]: row["tag_value"] for row in rows}


def _column_tags(live_connection, table_name: str) -> dict[tuple[str, str], str]:
    rows = fetch_rows(
        live_connection,
        f"SELECT column_name, tag_name, tag_value "
        f"FROM {backtick(live_catalog())}.information_schema.column_tags "
        f"WHERE schema_name = {quote_literal(live_schema())} "
        f"AND table_name = {quote_literal(table_name)}",
    )
    return {(row["column_name"], row["tag_name"]): row["tag_value"] for row in rows}


def test_describe_as_json_reports_the_streaming_table_kind_and_provider(
    live_connection, live_tables
):
    """DESCRIBE AS JSON reports type=STREAMING_TABLE, provider=delta for a streaming table."""
    # The admit gate in adapters/databricks/read.py is written against
    # exactly these two values. If this pin fails, the gate and the unit
    # fixtures are wrong, not this test: the assertion output carries the
    # whole document — update them to the observed values.
    table_name = _create_streaming_table(live_connection, live_tables)

    [row] = fetch_rows(
        live_connection,
        f"DESCRIBE TABLE EXTENDED {qualified_table(table_name)} AS JSON",
    )
    document = json.loads(row["json_metadata"])

    assert document.get("type") == "STREAMING_TABLE", document
    assert document.get("provider") == "delta", document


def test_alter_streaming_table_manages_table_and_column_tags(live_connection, live_tables):
    """ALTER STREAMING TABLE supports SET TAGS and UNSET TAGS at table and column level."""
    # The four tag statements are the entire surface the engine compiles
    # against a streaming table; each raises ServerOperationError on failure.
    table_name = _create_streaming_table(live_connection, live_tables)
    target = qualified_table(table_name)

    execute_sql(live_connection, f"ALTER STREAMING TABLE {target} SET TAGS ('owner'='governance')")
    execute_sql(live_connection, f"ALTER STREAMING TABLE {target} UNSET TAGS ('owner')")
    execute_sql(
        live_connection,
        f"ALTER STREAMING TABLE {target} ALTER COLUMN id SET TAGS ('pii'='low')",
    )
    execute_sql(
        live_connection,
        f"ALTER STREAMING TABLE {target} ALTER COLUMN id UNSET TAGS ('pii')",
    )


def test_plain_alter_table_cannot_tag_a_streaming_table(live_connection, live_tables):
    """ALTER TABLE ... SET TAGS is rejected on a streaming table."""
    # The premise of the dialect dispatch (_ALTER_CLAUSES in sql/compile.py).
    # If Databricks ever starts accepting plain ALTER TABLE here, the dispatch
    # is obsolete and this pin says so.
    table_name = _create_streaming_table(live_connection, live_tables)

    with pytest.raises(ServerOperationError):
        execute_sql(
            live_connection,
            f"ALTER TABLE {qualified_table(table_name)} SET TAGS ('owner'='governance')",
        )


def test_information_schema_reports_streaming_table_tags(live_connection, live_tables):
    """Tags set through ALTER STREAMING TABLE are readable from information_schema."""
    # The engine's reader observes tags via information_schema.table_tags and
    # column_tags; streaming-table tags must appear there or a tag sync could
    # never converge.
    table_name = _create_streaming_table(live_connection, live_tables)
    target = qualified_table(table_name)
    execute_sql(live_connection, f"ALTER STREAMING TABLE {target} SET TAGS ('owner'='governance')")
    execute_sql(
        live_connection,
        f"ALTER STREAMING TABLE {target} ALTER COLUMN id SET TAGS ('pii'='low')",
    )

    assert _table_tags(live_connection, table_name) == {"owner": "governance"}
    assert _column_tags(live_connection, table_name) == {("id", "pii"): "low"}
```

- [ ] **Step 2: Verify the file collects (it cannot run locally)**

Run: `uv run pytest tests/live/test_sql_warehouse_live_streaming_tables.py -m databricks_e2e --collect-only --no-cov -q`
Expected: 4 tests collected (they would skip at runtime without credentials; collection proves imports and syntax).

- [ ] **Step 3: Lint the new file**

Run: `uv run ruff check tests/live/test_sql_warehouse_live_streaming_tables.py && uv run ruff format tests/live/test_sql_warehouse_live_streaming_tables.py`
Expected: no findings; format makes no changes (or reformats — rerun check after).

- [ ] **Step 4: Commit**

```bash
git add tests/live/test_sql_warehouse_live_streaming_tables.py
git commit -m "test: pin streaming-table tag facts in the live suite"
```

---

### Task 2: Dispatch the Live workflow and confirm the pinned values

The reader gate (Task 8) is written against pin 1's values, not guessed. This task runs the pins for real and locks the values in.

**Files:**

- Modify (only if a pin's expected value is wrong): `tests/live/test_sql_warehouse_live_streaming_tables.py`

**Interfaces:**

- Produces: the confirmed `type` / `provider` values for a streaming table. Every later task that writes `"STREAMING_TABLE"` / `"delta"` into a gate, fixture, or test uses these confirmed values.

- [ ] **Step 1: Push the branch and dispatch the workflow**

```bash
git push -u origin claude/streaming-table-alter-scope-354849
gh workflow run live.yaml --ref claude/streaming-table-alter-scope-354849
```

- [ ] **Step 2: Watch the run to completion (~15 min)**

```bash
sleep 20
gh run list --workflow live.yaml --limit 1 --json databaseId,status --jq '.[0].databaseId'
gh run watch <run-id> --exit-status
```

Expected: the whole live suite passes, including the four new pins. On failure, read the failing test output: `gh run view <run-id> --log-failed`.

- [ ] **Step 3: Interpret the result**

| Outcome                                                                                 | Action                                                                                                                                                                                                                                                                                                                                           |
| --------------------------------------------------------------------------------------- | ------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------ |
| All four pins pass                                                                      | Values confirmed as `type="STREAMING_TABLE"`, `provider="delta"`. Proceed to Task 3.                                                                                                                                                                                                                                                             |
| Pin 1 fails on `type` or `provider`                                                     | The assertion output prints the full JSON document. Update the two asserts in pin 1 to the observed values, and use those observed values everywhere this plan writes `"STREAMING_TABLE"` / `"delta"` (Task 8 gate + fixtures, Task 9 fixture). Commit as `test: pin observed describe values for streaming tables`, re-dispatch, confirm green. |
| Pin 2 fails (a tag statement is rejected) or pin 3 fails (plain `ALTER TABLE` succeeds) | The feature's premise is wrong. **Stop and report to Tom** — the design needs revisiting, not this plan.                                                                                                                                                                                                                                         |
| The creation helper skips (workspace cannot create streaming tables)                    | The feature cannot be verified live in this workspace. **Stop and report to Tom.**                                                                                                                                                                                                                                                               |

**Adjustment (2026-07-16, after the first Live run):** the run surfaced a fourth outcome — the pins skipped on `QUOTA_EXCEEDED_EXCEPTION` because the workspace tier allows **one active DBSQL pipeline at a time** and the suite's parallel per-test provisioning raced it. The pinned values were still confirmed from the same run's log (the pre-existing streaming-table test created a real streaming table, and its `DESCRIBE … AS JSON` reported `type="STREAMING_TABLE"`, `provider="delta"`). Agreed resolution (Tom, this session): every live test that provisions a streaming table carries `@pytest.mark.xdist_group("streaming_table")`, the Live workflow adds `--dist loadgroup` so those tests serialize on one worker, `_create_streaming_table` retries bounded on the quota error, and the four fact pins are merged into two tests sharing a provisioned table each. Task 10's reader pin and round-trip merge into one test the same way, so a full Live run provisions three streaming tables, sequentially.

**Third adjustment (2026-07-16, post-review design revision):** Tasks 4, 6, and 7 were reworked after Tom's review of the merged shape. `TableDrift` carries the `observed` table (not a forwarded `kind` scalar); `ActionPlan` carries `kind`, copied by `plan_diff` from `drift.observed.kind`; `PlanExecutor.compile(qualified_name, plan)` keeps its original two-argument signature; and the compiler's `_SqlTarget` string pair became `_StatementTarget`, a value holding the qualified name and kind that renders `.name` and `.alter_table` itself. See the design doc's dated revision notes (decision 5, Diff, Planning and the execution port).

**Second adjustment (2026-07-16, after the second Live run):** the serialized run confirmed the describe values and all four `ALTER STREAMING TABLE` tag statements, but the premise pin **failed in the informative direction** — plain `ALTER TABLE ... SET TAGS` was _tolerated_ on a streaming table (table level). Agreed resolution (Tom, this session): keep emitting the documented `ALTER STREAMING TABLE` dialect (all four statements live-verified); the rejection pin is removed because the engine relies on nothing being rejected. The spec's Problem, live-pin list, and Risks sections carry the dated correction.

---

### Task 3: Domain — `TableKind` on `ObservedTable`

**Files:**

- Modify: `src/delta_engine/domain/model/table.py` (after `ALL_ASPECTS`, line ~101)
- Modify: `src/delta_engine/domain/model/__init__.py`
- Test: `tests/domain/model/test_table.py`

**Interfaces:**

- Produces: `TableKind` enum with members `TABLE`, `STREAMING_TABLE`, importable as `from delta_engine.domain.model import TableKind`; `ObservedTable.kind: TableKind` defaulting to `TableKind.TABLE`. `DesiredTable` deliberately gets no kind — kind is discovered, never declared.

- [ ] **Step 1: Write the failing tests**

Append to `tests/domain/model/test_table.py` (add `TableKind` to the file's existing `from delta_engine.domain.model import ...` block; reuse the file's existing imports for `ObservedTable`, `ObservedColumn`, `Integer`, `QualifiedName` — add any of those that are missing):

```python
def test_observed_table_kind_defaults_to_an_ordinary_table():
    # Kind is an observed fact with a safe default: construction sites that
    # predate relation kinds still describe an ordinary table.
    table = ObservedTable(
        qualified_name=QualifiedName("cat", "sch", "tbl"),
        columns=(ObservedColumn("id", Integer()),),
    )

    assert table.kind is TableKind.TABLE


def test_observed_table_carries_a_streaming_table_kind():
    table = ObservedTable(
        qualified_name=QualifiedName("cat", "sch", "tbl"),
        columns=(ObservedColumn("id", Integer()),),
        kind=TableKind.STREAMING_TABLE,
    )

    assert table.kind is TableKind.STREAMING_TABLE
```

- [ ] **Step 2: Run them to verify they fail**

Run: `uv run pytest tests/domain/model/test_table.py -q --no-cov -k "kind"`
Expected: FAIL — `ImportError: cannot import name 'TableKind'`.

- [ ] **Step 3: Implement**

In `src/delta_engine/domain/model/table.py`, insert after the `ALL_ASPECTS` line:

```python
class TableKind(Enum):
    """
    The catalog relation kind an observed table resolved to.

    Discovered at read time, never declared. ``TABLE`` is an ordinary managed
    or external Delta table; ``STREAMING_TABLE`` is a pipeline-owned streaming
    table, which takes a distinct ALTER dialect and admits tag changes only
    (enforced by validation's scope gate, not here).
    """

    TABLE = auto()
    STREAMING_TABLE = auto()
```

In the same file, add the field to `ObservedTable` as its last field (after `referencing_foreign_keys`):

```python
    referencing_foreign_keys: tuple[ForeignKeyReference, ...] = ()
    kind: TableKind = TableKind.TABLE
```

and add one line to the `ObservedTable` docstring's `Attributes:` list:

```text
        kind: The relation kind this table resolved to; ``TableKind.TABLE``
            unless the reader observed otherwise.
```

In `src/delta_engine/domain/model/__init__.py`, extend the table import and `__all__`:

```python
from delta_engine.domain.model.table import (
    ALL_ASPECTS,
    DesiredTable,
    ObservedTable,
    TableAspect,
    TableKind,
)
```

and add `"TableKind",` to `__all__` (alphabetical: after `"TableAspect"`).

- [ ] **Step 4: Run the tests**

Run: `uv run pytest tests/domain/model/test_table.py -q --no-cov`
Expected: PASS (all, including the two new tests).

- [ ] **Step 5: Commit**

```bash
git add src/delta_engine/domain/model/table.py src/delta_engine/domain/model/__init__.py tests/domain/model/test_table.py
git commit -m "feat: model the observed relation kind in the domain"
```

---

### Task 4: Diff — `TableDrift` carries the observed kind

**Files:**

- Modify: `src/delta_engine/domain/plan/diff.py` (`TableDrift` at line ~91, `diff_table` return at line ~153)
- Test: `tests/domain/plan/test_diff.py`

**Interfaces:**

- Consumes: `TableKind`, `ObservedTable.kind` (Task 3).
- Produces: `TableDrift.kind: TableKind = TableKind.TABLE`; `diff_table` copies `observed.kind` onto the drift. `TableMissing` is unchanged (an absent table has no observed kind).

- [ ] **Step 1: Write the failing tests**

Append to `tests/domain/plan/test_diff.py` (add `TableKind` to the file's `delta_engine.domain.model` import; the file already imports `diff_table`, `TableDrift`, `DesiredTable`, `ObservedTable`, `DesiredColumn`, `ObservedColumn`, `Integer`, `QualifiedName` — add any that are missing):

```python
def test_drift_carries_the_observed_relation_kind():
    # The diff states the fact; judging it is validation's scope gate.
    qualified_name = QualifiedName("cat", "sch", "clicks")
    desired = DesiredTable(
        qualified_name=qualified_name,
        columns=(DesiredColumn("id", Integer()),),
    )
    observed = ObservedTable(
        qualified_name=qualified_name,
        columns=(ObservedColumn("id", Integer()),),
        kind=TableKind.STREAMING_TABLE,
    )

    diff = diff_table(desired, observed)

    assert isinstance(diff, TableDrift)
    assert diff.kind is TableKind.STREAMING_TABLE


def test_drift_against_an_ordinary_table_carries_the_table_kind():
    qualified_name = QualifiedName("cat", "sch", "orders")
    desired = DesiredTable(
        qualified_name=qualified_name,
        columns=(DesiredColumn("id", Integer()),),
    )
    observed = ObservedTable(
        qualified_name=qualified_name,
        columns=(ObservedColumn("id", Integer()),),
    )

    diff = diff_table(desired, observed)

    assert isinstance(diff, TableDrift)
    assert diff.kind is TableKind.TABLE
```

- [ ] **Step 2: Run them to verify they fail**

Run: `uv run pytest tests/domain/plan/test_diff.py -q --no-cov -k "kind"`
Expected: FAIL — `AttributeError: 'TableDrift' object has no attribute 'kind'` (or ImportError if `TableKind` was not yet importable in the file).

- [ ] **Step 3: Implement**

In `src/delta_engine/domain/plan/diff.py`:

1. Add `TableKind` to the existing domain-model import:

```python
from delta_engine.domain.model import (
    DesiredColumn,
    DesiredTable,
    ObservedColumn,
    ObservedTable,
    TableAspect,
    TableKind,
)
```

2. Add the field to `TableDrift` and one docstring sentence:

```python
@dataclass(frozen=True, slots=True)
class TableDrift:
    """
    Differences separating observed state from its declaration.

    ``actions`` are remedied differences, each carrying the executable
    operation that closes its gap. ``unresolvable`` are differences no action
    can close; they exist to be judged by validation. Both state every
    difference regardless of scope; deciding which the declaration is
    allowed to make is validation's scope gate, not the diff's concern.
    ``kind`` is the observed table's relation kind, carried as a fact for the
    same gate to judge.
    """

    desired: DesiredTable
    actions: tuple[Action, ...] = ()
    unresolvable: tuple[Unresolvable, ...] = ()
    kind: TableKind = TableKind.TABLE
```

3. In `diff_table`, change the final return to copy the kind:

```python
    return TableDrift(
        desired=desired, actions=actions, unresolvable=unresolvable, kind=observed.kind
    )
```

- [ ] **Step 4: Run the tests**

Run: `uv run pytest tests/domain/plan/test_diff.py -q --no-cov`
Expected: PASS.

- [ ] **Step 5: Commit**

```bash
git add src/delta_engine/domain/plan/diff.py tests/domain/plan/test_diff.py
git commit -m "feat: carry the observed relation kind on table drift"
```

---

### Task 5: Validation — the `StreamingTableTagsOnly` scope gate

**Files:**

- Modify: `src/delta_engine/application/validation.py` (new gate class after `MissingTableUnmanaged`, line ~538; `_scope_failures` at line ~571; two docstring touch-ups)
- Test: `tests/application/test_validation.py`

**Interfaces:**

- Consumes: `TableDrift.kind` (Task 4), `TableKind` (Task 3).
- Produces: class `StreamingTableTagsOnly` with `name: ClassVar[str] = "StreamingTableTagsOnly"` and `evaluate(drift: TableDrift) -> tuple[ValidationFailure, ...]`; wired unconditionally into `_scope_failures`, ordered before `UnmanagedAspectDrift`. Task 9's integration test and the docs (Task 11) rely on the exact rule name `StreamingTableTagsOnly`.

- [ ] **Step 1: Extend the `_drift` test helper**

In `tests/application/test_validation.py`, add `TableKind` to the `delta_engine.domain.model` import block, then give `_drift` a `kind` parameter:

```python
def _drift(
    *differences: Action | Unresolvable,
    managed_aspects: frozenset[TableAspect] = ALL_ASPECTS,
    desired: DesiredTable | None = None,
    kind: TableKind = TableKind.TABLE,
) -> TableDrift:
    if desired is None:
        desired = _desired_table(managed_aspects=managed_aspects)
    actions = tuple(item for item in differences if isinstance(item, Action))
    unresolvable = tuple(item for item in differences if not isinstance(item, Action))
    return TableDrift(desired=desired, actions=actions, unresolvable=unresolvable, kind=kind)
```

- [ ] **Step 2: Write the failing tests**

Append to `tests/application/test_validation.py`, after the tag-only-scope tests (line ~763). `_TAG_ASPECTS_ONLY` is a local constant to keep the tests readable:

```python
# ---- streaming tables


_TAG_ASPECTS_ONLY = frozenset({TableAspect.TABLE_TAGS, TableAspect.COLUMN_TAGS})


def test_streaming_table_passes_when_the_declaration_manages_only_tags():
    # Given a tags-scope declaration over a streaming table with tag drift
    diff = _drift(
        SetColumnTag(column_name="id", name="pii", value="true"),
        managed_aspects=_TAG_ASPECTS_ONLY,
        kind=TableKind.STREAMING_TABLE,
    )

    # Then the tag work is allowed
    assert validate_diff(diff).failed is False


def test_streaming_table_fails_a_full_scope_declaration_even_with_zero_drift():
    # Given a fully-managed declaration over an in-sync streaming table
    diff = _drift(managed_aspects=ALL_ASPECTS, kind=TableKind.STREAMING_TABLE)

    # When validating
    result = validate_diff(diff)

    # Then the declaration is rejected on kind alone: it claims authority the
    # engine must never exercise on a pipeline-owned table
    assert result.failed is True
    assert [failure.rule_name for failure in result.failures] == ["StreamingTableTagsOnly"]
    assert 'scope="tags"' in result.failures[0].message


def test_streaming_table_fails_a_metadata_scope_declaration():
    # Given a declaration managing comments as well as tags
    diff = _drift(
        managed_aspects=frozenset(
            {TableAspect.TABLE_TAGS, TableAspect.COLUMN_TAGS, TableAspect.TABLE_COMMENT}
        ),
        kind=TableKind.STREAMING_TABLE,
    )

    # Then managing anything beyond tags is rejected
    result = validate_diff(diff)
    assert [failure.rule_name for failure in result.failures] == ["StreamingTableTagsOnly"]


def test_streaming_table_gate_cannot_be_suppressed_by_empty_rules():
    diff = _drift(managed_aspects=ALL_ASPECTS, kind=TableKind.STREAMING_TABLE)

    result = validate_diff(diff, rules=())

    assert result.failed is True
    assert result.failures[0].rule_name == "StreamingTableTagsOnly"


def test_streaming_table_gate_short_circuits_safety_rules():
    # Given a full-scope declaration over a streaming table with an unsafe
    # type change
    diff = _drift(_type_drift("id"), managed_aspects=ALL_ASPECTS, kind=TableKind.STREAMING_TABLE)

    # Then the kind violation is reported alone; no safety rule runs
    result = validate_diff(diff)
    assert [failure.rule_name for failure in result.failures] == ["StreamingTableTagsOnly"]


def test_streaming_table_gate_reports_before_unmanaged_aspect_drift():
    # Given a metadata-ish scope with structure drift on a streaming table —
    # both scope-gate arms fire, kind first
    diff = _drift(
        AddColumn(DesiredColumn("extra", Integer())),
        managed_aspects=frozenset(
            {TableAspect.TABLE_TAGS, TableAspect.COLUMN_TAGS, TableAspect.TABLE_COMMENT}
        ),
        kind=TableKind.STREAMING_TABLE,
    )

    result = validate_diff(diff)

    assert [failure.rule_name for failure in result.failures] == [
        "StreamingTableTagsOnly",
        "UnmanagedAspectDrift",
    ]


def test_streaming_table_under_tags_scope_still_fails_unmanaged_drift():
    # Given a tags-scope declaration over a streaming table whose comment drifted
    diff = _drift(
        SetTableComment(desired_comment="new", observed_comment="old"),
        managed_aspects=_TAG_ASPECTS_ONLY,
        kind=TableKind.STREAMING_TABLE,
    )

    # Then the kind gate stays silent (tags scope is allowed here) and the
    # unmanaged comment drift is the failure
    result = validate_diff(diff)
    assert [failure.rule_name for failure in result.failures] == ["UnmanagedAspectDrift"]


def test_an_absent_streaming_table_under_tags_scope_still_fails_missing_table():
    # Given a tags-scope declaration whose table does not exist — absence has
    # no observed kind, so the existing TableMissing arm judges it unchanged
    desired = _desired_table(managed_aspects=_TAG_ASPECTS_ONLY)

    # When validating the diff of a missing table
    result = _validate(desired, None)

    # Then tags scope does not manage existence; nothing streaming-specific fires
    assert [failure.rule_name for failure in result.failures] == ["MissingTableUnmanaged"]
```

- [ ] **Step 3: Run them to verify they fail**

Run: `uv run pytest tests/application/test_validation.py -q --no-cov -k "streaming"`
Expected: FAIL — the zero-drift and short-circuit tests report `failed is False` / missing failures (no gate exists yet).

- [ ] **Step 4: Implement the gate**

In `src/delta_engine/application/validation.py`:

1. Add `TableKind` to the `delta_engine.domain.model` import block.

2. Insert after the `MissingTableUnmanaged` class:

```python
_STREAMING_TABLE_MANAGEABLE_ASPECTS: Final[frozenset[TableAspect]] = frozenset(
    {TableAspect.TABLE_TAGS, TableAspect.COLUMN_TAGS}
)


class StreamingTableTagsOnly:
    """
    Fail any declaration that manages more than tags on a streaming table.

    The relation-kind arm of the scope gate, peer to ``UnmanagedAspectDrift``
    and ``MissingTableUnmanaged``: it runs unconditionally and cannot be
    suppressed via ``rules``. A streaming table's definition is owned by its
    pipeline; Unity Catalog tags are the one aspect durably manageable from
    outside it. The gate judges the declaration's claimed aspects against the
    observed kind — not the drift — so it fires even when the table is
    currently in sync, and a dry run surfaces the misdeclaration immediately.
    """

    name: ClassVar[str] = "StreamingTableTagsOnly"

    def evaluate(self, drift: TableDrift) -> tuple[ValidationFailure, ...]:
        """Flag a streaming-table declaration whose managed aspects exceed the tag aspects."""
        if drift.kind is not TableKind.STREAMING_TABLE:
            return ()
        if drift.desired.managed_aspects <= _STREAMING_TABLE_MANAGEABLE_ASPECTS:
            return ()
        return (
            ValidationFailure(
                rule_name=self.name,
                message=(
                    "Operation not allowed: this relation is a streaming table,"
                    " whose definition is owned by its pipeline. Only Unity"
                    " Catalog tags can be managed on it: declare the table with"
                    ' scope="tags", or change its definition in the owning'
                    " pipeline."
                ),
            ),
        )
```

3. Update `_scope_failures` so the drift arm evaluates both invariants, kind first:

```python
def _scope_failures(diff: TableDiff) -> tuple[ValidationFailure, ...]:
    """Return the scope-gate failures for either diff arm; empty when in scope."""
    # TODO: these are stateless single-method classes, constructed per call and
    # invoked directly here (not pluggable rules in DEFAULT_RULES). Reconsider
    # whether they should be plain module-level functions rather than classes.
    match diff:
        case TableMissing() as missing:
            return MissingTableUnmanaged().evaluate(missing)
        case TableDrift() as drift:
            return (
                *StreamingTableTagsOnly().evaluate(drift),
                *UnmanagedAspectDrift().evaluate(drift),
            )
        case _ as unreachable:
            assert_never(unreachable)
```

4. In the `validate_diff` docstring, extend the second sentence:

Replace:

```text
    Scope is a gate, checked before any safety rule. An out-of-scope
    difference — a drifted aspect the declaration does not manage, or a
    missing table it may not create — fails here and short-circuits, so the
```

with:

```text
    Scope is a gate, checked before any safety rule. An out-of-scope
    difference — a drifted aspect the declaration does not manage, a missing
    table it may not create, or a streaming table it claims more than tags
    on — fails here and short-circuits, so the
```

- [ ] **Step 5: Run the tests**

Run: `uv run pytest tests/application/test_validation.py -q --no-cov`
Expected: PASS (all — the pre-existing scope-gate tests construct `TableDrift` with the default kind and must be untouched).

- [ ] **Step 6: Commit**

```bash
git add src/delta_engine/application/validation.py tests/application/test_validation.py
git commit -m "feat: gate streaming tables to tag-only declarations"
```

---

### Task 6: Compiler — kind-selected ALTER clause

Every ALTER-family statement adopts the clause mechanically via one `_SqlTarget` value; the compiler stays policy-free (validation keeps non-tag actions away from streaming tables; handed one anyway, the compiler emits it and the backend rejects it).

**Files:**

- Modify: `src/delta_engine/adapters/databricks/sql/compile.py`
- Test: `tests/adapters/databricks/sql/test_compile.py`

**Interfaces:**

- Consumes: `TableKind` (Task 3).
- Produces: `compile_plan(qualified_name: QualifiedName, plan: ActionPlan, kind: TableKind = TableKind.TABLE) -> tuple[str, ...]`. The default is a **temporary bridge** so the executors keep compiling until Task 7 threads `kind` through them; Task 7 removes it.

- [ ] **Step 1: Write the failing tests**

In `tests/adapters/databricks/sql/test_compile.py`, add `TableKind` to the `delta_engine.domain.model` import block, change `_compile_single` to:

```python
def _compile_single(action: Action, kind: TableKind = TableKind.TABLE) -> str:
    (statement,) = compile_plan(_TARGET, ActionPlan(actions=(action,)), kind)
    return statement
```

and append:

```python
def test_tag_statements_compile_with_the_streaming_table_dialect():
    cases = {
        SetTableTag(name="owner", value="gov"): (
            "ALTER STREAMING TABLE `cat`.`sch`.`tbl` SET TAGS ('owner'='gov')"
        ),
        UnsetTableTag(name="owner"): (
            "ALTER STREAMING TABLE `cat`.`sch`.`tbl` UNSET TAGS ('owner')"
        ),
        SetColumnTag(column_name="id", name="pii", value="low"): (
            "ALTER STREAMING TABLE `cat`.`sch`.`tbl` ALTER COLUMN `id` SET TAGS ('pii'='low')"
        ),
        UnsetColumnTag(column_name="id", name="pii"): (
            "ALTER STREAMING TABLE `cat`.`sch`.`tbl` ALTER COLUMN `id` UNSET TAGS ('pii')"
        ),
    }
    for action, expected in cases.items():
        assert _compile_single(action, kind=TableKind.STREAMING_TABLE) == expected


def test_ordinary_tables_keep_the_alter_table_dialect():
    statement = _compile_single(SetTableTag(name="owner", value="gov"))

    assert statement == "ALTER TABLE `cat`.`sch`.`tbl` SET TAGS ('owner'='gov')"


def test_every_alter_statement_adopts_the_dialect_mechanically():
    # The compiler is policy-free: validation keeps non-tag actions away from
    # streaming tables, but a statement compiled for one still targets it.
    statement = _compile_single(
        AddColumn(DesiredColumn("extra", Integer())), kind=TableKind.STREAMING_TABLE
    )

    assert statement.startswith("ALTER STREAMING TABLE `cat`.`sch`.`tbl` ADD COLUMN")


def test_non_alter_statements_ignore_the_dialect():
    create = _compile_single(
        _create_table(DesiredColumn("id", Integer())), kind=TableKind.STREAMING_TABLE
    )
    comment = _compile_single(
        SetTableComment(desired_comment="c", observed_comment=""),
        kind=TableKind.STREAMING_TABLE,
    )

    assert create.startswith("CREATE TABLE `cat`.`sch`.`tbl`")
    assert comment == "COMMENT ON TABLE `cat`.`sch`.`tbl` IS 'c'"
```

- [ ] **Step 2: Run them to verify they fail**

Run: `uv run pytest tests/adapters/databricks/sql/test_compile.py -q --no-cov -k "dialect"`
Expected: FAIL — `TypeError: compile_plan() takes 2 positional arguments but 3 were given`.

- [ ] **Step 3: Implement**

In `src/delta_engine/adapters/databricks/sql/compile.py`:

1. Replace the header (imports, `compile_plan`, and the singledispatch fallback) with:

```python
from collections.abc import Mapping
from dataclasses import dataclass
from functools import singledispatch
from types import MappingProxyType
from typing import Final

from delta_engine.adapters.databricks.sql.dialect import (
    backtick,
    backtick_qualified_name,
    quote_literal,
)
from delta_engine.adapters.databricks.sql.types import render_data_type
from delta_engine.domain.model import DesiredColumn, QualifiedName, TableKind
from delta_engine.domain.plan import (
    Action,
    ActionPlan,
    AddColumn,
    AlterClustering,
    AlterColumnType,
    CreateTable,
    DropColumn,
    DropForeignKey,
    DropPrimaryKey,
    RenameColumn,
    SetColumnComment,
    SetColumnNullability,
    SetColumnTag,
    SetForeignKey,
    SetPrimaryKey,
    SetProperty,
    SetTableComment,
    SetTableTag,
    UnsetColumnTag,
    UnsetProperty,
    UnsetTableTag,
)

# Streaming tables reject plain ALTER TABLE and take their own dialect
# (pinned live: tests/live/test_sql_warehouse_live_streaming_tables.py).
_ALTER_CLAUSES: Final[Mapping[TableKind, str]] = MappingProxyType(
    {
        TableKind.TABLE: "ALTER TABLE",
        TableKind.STREAMING_TABLE: "ALTER STREAMING TABLE",
    }
)


@dataclass(frozen=True, slots=True)
class _SqlTarget:
    """The statement target: its backticked name and its kind-correct ALTER clause."""

    name: str
    alter_table: str


def compile_plan(
    qualified_name: QualifiedName,
    plan: ActionPlan,
    kind: TableKind = TableKind.TABLE,
) -> tuple[str, ...]:
    """
    Compile an :class:`ActionPlan` for ``qualified_name`` into SQL statements, in plan order.

    ``kind`` selects the ALTER dialect every ALTER-family statement targets
    ``qualified_name`` with; non-ALTER statements (CREATE TABLE, COMMENT ON)
    are unaffected by it.
    """
    name = backtick_qualified_name(qualified_name)
    target = _SqlTarget(name=name, alter_table=f"{_ALTER_CLAUSES[kind]} {name}")
    return tuple(_compile_action(action, target) for action in plan)


@singledispatch
def _compile_action(action: Action, target: _SqlTarget) -> str:
    """Dispatch to action-specific SQL compiler."""
    raise NotImplementedError(f"No SQL compiler for action {type(action).__name__}")
```

(The `kind` default is temporary; Task 7 removes it once the executors pass it explicitly.)

2. Update every `@_compile_action.register` handler with this exact mechanical transformation, keeping each handler's docstring and comments as they are:

- parameter `backticked_table_name: str` → `target: _SqlTarget`
- `ALTER TABLE {backticked_table_name}` → `{target.alter_table}`
- `CREATE TABLE {backticked_table_name}` → `CREATE TABLE {target.name}` (CreateTable handler only)
- `COMMENT ON TABLE {backticked_table_name}` → `COMMENT ON TABLE {target.name}` (SetTableComment handler only)

The four tag handlers end up exactly:

```python
@_compile_action.register
def _(action: SetTableTag, target: _SqlTarget) -> str:
    pair = f"{quote_literal(action.name)}={quote_literal(action.value)}"
    return f"{target.alter_table} SET TAGS ({pair})"


@_compile_action.register
def _(action: UnsetTableTag, target: _SqlTarget) -> str:
    return f"{target.alter_table} UNSET TAGS ({quote_literal(action.name)})"


@_compile_action.register
def _(action: SetColumnTag, target: _SqlTarget) -> str:
    column = backtick(action.column_name)
    pair = f"{quote_literal(action.name)}={quote_literal(action.value)}"
    return f"{target.alter_table} ALTER COLUMN {column} SET TAGS ({pair})"


@_compile_action.register
def _(action: UnsetColumnTag, target: _SqlTarget) -> str:
    column = backtick(action.column_name)
    return f"{target.alter_table} ALTER COLUMN {column} UNSET TAGS ({quote_literal(action.name)})"
```

Two worked examples of the same transformation on ALTER handlers (apply identically to the rest — `AddColumn`, `DropColumn`, `RenameColumn`, `SetProperty`, `UnsetProperty`, `SetColumnComment`, `SetColumnNullability`, `AlterClustering`, `AlterColumnType`, `SetPrimaryKey`, `DropForeignKey`, `SetForeignKey`):

```python
@_compile_action.register
def _(action: DropColumn, target: _SqlTarget) -> str:
    """Compile an ALTER TABLE ... DROP COLUMN statement for a column name."""
    column_name = backtick(action.column.name)
    return f"{target.alter_table} DROP COLUMN {column_name}"


@_compile_action.register
def _(action: DropPrimaryKey, target: _SqlTarget) -> str:
    """Compile an ALTER TABLE ... DROP PRIMARY KEY IF EXISTS statement."""
    # IF EXISTS is the deliberate mirror of CreateTable's plain CREATE: a
    # constraint already gone is the end state this action wants, so an
    # out-of-band drop in the read-execute window converges instead of failing
    # the sync — whereas a table that appeared in that window has contents the
    # plan knows nothing about, so the create must surface it as a failure.
    return f"{target.alter_table} DROP PRIMARY KEY IF EXISTS"
```

In the `CreateTable` handler the `parts` list starts `f"CREATE TABLE {target.name}"`; the `SetTableComment` handler returns `f"COMMENT ON TABLE {target.name} IS {comment}"`. No handler keeps a reference to `backticked_table_name` when done: verify with `rg backticked_table_name src/delta_engine/adapters/databricks/sql/compile.py` → no matches.

- [ ] **Step 4: Run the tests**

Run: `uv run pytest tests/adapters/databricks/sql/test_compile.py -q --no-cov`
Expected: PASS — every pre-existing exact-SQL assertion is unchanged (`kind` defaults to `TABLE`).

- [ ] **Step 5: Broaden to the adapter and application suites**

Run: `uv run pytest tests/adapters tests/application -q --no-cov`
Expected: PASS (executors still call `compile_plan(qualified_name, plan)` through the temporary default).

- [ ] **Step 6: Commit**

```bash
git add src/delta_engine/adapters/databricks/sql/compile.py tests/adapters/databricks/sql/test_compile.py
git commit -m "feat: compile the ALTER dialect per relation kind"
```

---

### Task 7: Widen the `PlanExecutor` port and thread the observed kind

**Files:**

- Modify: `src/delta_engine/application/ports.py` (line ~166)
- Modify: `src/delta_engine/application/engine.py` (`_TableRun` line ~92, compile call line ~277)
- Modify: `src/delta_engine/adapters/databricks/spark/executor.py`, `src/delta_engine/adapters/databricks/warehouse/executor.py`
- Modify: `src/delta_engine/adapters/databricks/sql/compile.py` (remove the temporary default)
- Test: `tests/application/test_engine.py` (two fakes, one helper, two new tests), `tests/cli/conftest.py` (`FakeExecutor`), `tests/adapters/databricks/spark/test_executor.py`, `tests/adapters/databricks/warehouse/test_executor.py`

**Interfaces:**

- Consumes: `compile_plan(..., kind)` (Task 6), `ObservedTable.kind` (Task 3).
- Produces: `PlanExecutor.compile(self, qualified_name: QualifiedName, plan: ActionPlan, kind: TableKind) -> tuple[str, ...]` — `kind` required, no default; `_TableRun.observed_kind: TableKind` property (observed table's kind when present, `TableKind.TABLE` otherwise, so creates compile as ordinary tables).

- [ ] **Step 1: Write the failing engine tests**

In `tests/application/test_engine.py`:

1. Add `TableKind` to the `from delta_engine.domain.model import ...` line.
2. Give `_existing_tag_drifted_table` a kind parameter:

```python
def _existing_tag_drifted_table(fqn: str, kind: TableKind = TableKind.TABLE) -> TablePresent:
    """Build an observed table with tag drift against _tag_scoped_spec."""
    catalog, schema, table_name = _split_fqn(fqn)

    return TablePresent(
        table=ObservedTable(
            qualified_name=QualifiedName(catalog, schema, table_name),
            columns=(ObservedColumn("id", String(), tags={"stale": "true"}),),
            tags={"legacy": "yes"},
            kind=kind,
        )
    )
```

3. Append two tests (near `test_tag_scoped_dry_run_plans_only_tag_actions`, line ~493):

```python
def test_planned_sql_targets_the_observed_relation_kind():
    # Given a tags-scope declaration over a live streaming table with tag drift
    fqn = "c.s.streaming_events"
    reader = _RecordingReader(
        {fqn: _existing_tag_drifted_table(fqn, kind=TableKind.STREAMING_TABLE)}
    )
    executor = _RecordingExecutor(per_call_results=[])
    engine = Engine(reader=reader, executor=executor)

    # When syncing as a dry run
    report = engine.sync(_tag_scoped_spec(fqn), dry_run=True)

    # Then the compiled statements carry the observed kind through the port
    [table_report] = list(report)
    assert table_report.status is TableRunStatus.SUCCESS
    assert table_report.planned_sql_statements != ()
    assert all(
        f"AS STREAMING_TABLE FOR {fqn}" in statement
        for statement in table_report.planned_sql_statements
    )


def test_created_tables_compile_as_ordinary_tables():
    # Given an absent table — an absent table has no observed kind, and the
    # engine only creates ordinary tables
    fqn = "c.s.new_table"
    reader = _RecordingReader({fqn: TableAbsent()})
    executor = _RecordingExecutor(per_call_results=[])
    engine = Engine(reader=reader, executor=executor)

    # When syncing as a dry run
    report = engine.sync(_spec(fqn), dry_run=True)

    # Then the create path compiles with the ordinary kind
    [table_report] = list(report)
    assert table_report.planned_sql_statements != ()
    assert all(
        f"AS TABLE FOR {fqn}" in statement
        for statement in table_report.planned_sql_statements
    )
```

4. Update the two engine fakes so the fake statements embed the kind **before** `" FOR "` (the `executed_names` parser splits on `" FOR "` and must keep returning the bare name):

`_RecordingExecutor` (line ~282):

```python
    def compile(
        self, qualified_name: QualifiedName, plan: ActionPlan, kind: TableKind
    ) -> tuple[str, ...]:
        return tuple(
            f"STATEMENT {index} AS {kind.name} FOR {qualified_name}" for index in range(len(plan))
        )
```

`_EventRecordingExecutor` (line ~541) — same signature; its body keeps the existing comment and returns:

```python
        def compile(
            self, qualified_name: QualifiedName, plan: ActionPlan, kind: TableKind
        ) -> tuple[str, ...]:
            # Silent by design: the plan phase compiles every table, but this
            # test asserts read/execute event ordering, not compilation. The
            # name is embedded so execute can name the table in its event.
            return tuple(f"STATEMENT {index} FOR {qualified_name}" for index in range(len(plan)))
```

- [ ] **Step 2: Run them to verify they fail**

Run: `uv run pytest tests/application/test_engine.py -q --no-cov -k "relation_kind or ordinary_tables"`
Expected: FAIL — `TypeError` (the engine still calls `compile` with two arguments while the fake now requires three).

- [ ] **Step 3: Implement**

1. `src/delta_engine/application/ports.py` — add `TableKind` to the domain-model import, then widen the protocol method (replacing the current signature at line ~166; the docstring's last paragraph is unchanged):

```python
    def compile(
        self, qualified_name: QualifiedName, plan: ActionPlan, kind: TableKind
    ) -> tuple[str, ...]:
        """
        Return the statements that apply ``plan``, in execution order.

        ``kind`` is the observed relation kind of the target table — the
        statements a plan lowers to can differ by kind. The engine passes the
        kind it read for a present table and ``TableKind.TABLE`` for a create.

        The ordering is the plan's own deterministic order, which is the order
        ``execute`` runs the statements. An empty plan compiles to no statements.

        Pure and side-effect free: the engine calls this on every run -- dry or
        real -- to record the SQL on the table's report. Unlike ``execute``,
        this is not a total boundary: compiling a validated plan cannot fail
        against a backend, so an exception here is a programming error and
        propagates.
        """
        ...
```

2. `src/delta_engine/application/engine.py` — add `TableKind` to the domain-model import; add a property to `_TableRun` (after `has_failures`):

```python
    @property
    def observed_kind(self) -> TableKind:
        """The relation kind compiled DDL targets: as observed when present, TABLE when creating."""
        match self.read:
            case TablePresent(table=table):
                return table.kind
            case _:
                return TableKind.TABLE
```

and change the compile call in `_plan`:

```python
                case PlanningSucceeded(plan=plan):
                    run.plan = plan
                    run.planned_sql_statements = self.executor.compile(
                        run.qualified_name, run.plan, run.observed_kind
                    )
```

3. Both executors — add `TableKind` to their `delta_engine.domain.model` import and forward the kind.

`src/delta_engine/adapters/databricks/spark/executor.py`:

```python
    def compile(
        self, qualified_name: QualifiedName, plan: ActionPlan, kind: TableKind
    ) -> tuple[str, ...]:
        """Compile ``plan`` to its SQL statements in execution order, without touching Spark."""
        return compile_plan(qualified_name, plan, kind)
```

`src/delta_engine/adapters/databricks/warehouse/executor.py`:

```python
    def compile(
        self, qualified_name: QualifiedName, plan: ActionPlan, kind: TableKind
    ) -> tuple[str, ...]:
        """
        Compile ``plan`` to its SQL statements in execution order.

        Does not touch the warehouse.
        """
        return compile_plan(qualified_name, plan, kind)
```

4. `src/delta_engine/adapters/databricks/sql/compile.py` — remove the temporary default: the signature becomes

```python
def compile_plan(
    qualified_name: QualifiedName,
    plan: ActionPlan,
    kind: TableKind,
) -> tuple[str, ...]:
```

5. `tests/cli/conftest.py` — add `TableKind` to the file's domain-model imports and update `FakeExecutor`:

```python
    def compile(
        self, qualified_name: QualifiedName, plan: ActionPlan, kind: TableKind
    ) -> tuple[str, ...]:
        return tuple(f"-- {qualified_name}: {type(action).__name__}" for action in plan)
```

6. Executor tests — pass the kind explicitly at every `executor.compile(...)` call site, importing `TableKind` from `delta_engine.domain.model` in each file:

- `tests/adapters/databricks/spark/test_executor.py` lines ~30, ~275, ~285: `executor.compile(qualified_name, plan, TableKind.TABLE)` (line 285 becomes `assert executor.compile(QualifiedName("cat", "schema", "tbl"), ActionPlan(), TableKind.TABLE) == ()`).
- `tests/adapters/databricks/warehouse/test_executor.py` lines ~57, ~115: `executor.compile(QN, plan, TableKind.TABLE)`.
- `tests/adapters/databricks/sql/test_compile.py`: the `_compile_single` helper already passes `kind` positionally (Task 6); nothing further.

- [ ] **Step 4: Run the tests**

Run: `uv run pytest tests/application tests/adapters tests/cli -q --no-cov`
Expected: PASS.

- [ ] **Step 5: Type-check and import-lint the widened boundary**

Run: `uv run mypy src && uv run lint-imports`
Expected: clean — `TableKind` is domain vocabulary, legal in application and adapters.

- [ ] **Step 6: Commit**

```bash
git add src/delta_engine/application/ports.py src/delta_engine/application/engine.py \
  src/delta_engine/adapters/databricks/spark/executor.py \
  src/delta_engine/adapters/databricks/warehouse/executor.py \
  src/delta_engine/adapters/databricks/sql/compile.py \
  tests/application/test_engine.py tests/cli/conftest.py \
  tests/adapters/databricks/spark/test_executor.py tests/adapters/databricks/warehouse/test_executor.py
git commit -m "feat: thread the observed relation kind to the compiler"
```

---

### Task 8: Reader — admit streaming tables, keep everything else fail-closed

Written against the values Task 2 pinned. The code below assumes the confirmed values are `type="STREAMING_TABLE"`, `provider="delta"`; the alternative branch at the end of Step 3 covers a different pinned provider.

**Files:**

- Modify: `src/delta_engine/adapters/databricks/read.py` (constants at lines ~51-59, `read_catalog_state` body, `_require_supported_relation`, `_observed_table`)
- Test: `tests/adapters/databricks/test_read.py`

**Interfaces:**

- Consumes: `TableKind`, `ObservedTable.kind` (Task 3), Task 2's pinned describe values.
- Produces: reads where `type` is `MANAGED`/`EXTERNAL`/`STREAMING_TABLE` (provider `delta`) return `TablePresent` with the mapped kind; every other relation still raises `UnsupportedRelationError` inside the total boundary (surfacing as `ReadFailed`), materialized views included.

- [ ] **Step 1: Update and write the tests**

In `tests/adapters/databricks/test_read.py` (add `TableKind` and `TablePresent`-adjacent imports as needed — the file already imports `TablePresent`):

1. Replace `test_relation_kinds_the_engine_does_not_manage_read_as_failed` with (streaming tables leave the rejection list; materialized views staying listed is the deliberate regression pin):

```python
def test_relation_kinds_the_engine_does_not_manage_read_as_failed():
    # The engine reads ordinary tables and streaming tables. Every other
    # relation kind fails the read — materialized views deliberately included,
    # and kinds Databricks adds in the future — rather than being diffed and
    # planned against as though it were a table.
    for kind in ("VIEW", "MATERIALIZED_VIEW", "FOREIGN", "FUTURE_KIND"):
        doc = _describe_doc(type=kind)
        responses = _describe_responses(**{describe_json_query(QN): [(doc,)]})

        state = read_catalog_state(_router(responses), QN)

        assert isinstance(state, ReadFailed)
        assert state.failure.exception_type == "UnsupportedRelationError"
```

2. Replace `test_rejection_names_the_found_relation_and_the_supported_kinds` with:

```python
def test_rejection_names_the_found_relation_and_the_supported_kinds():
    doc = _describe_doc(type="MATERIALIZED_VIEW")
    responses = _describe_responses(**{describe_json_query(QN): [(doc,)]})

    state = read_catalog_state(_router(responses), QN)

    assert isinstance(state, ReadFailed)
    assert "MATERIALIZED_VIEW" in state.failure.message
    assert "MANAGED or EXTERNAL" in state.failure.message
    assert "streaming tables" in state.failure.message
```

3. Append three new tests:

```python
def test_a_streaming_table_reads_as_present_with_its_kind():
    # DESCRIBE AS JSON reports type=STREAMING_TABLE, provider=delta for a
    # streaming table (pinned live in
    # tests/live/test_sql_warehouse_live_streaming_tables.py).
    doc = _describe_doc(type="STREAMING_TABLE")
    responses = _describe_responses(**{describe_json_query(QN): [(doc,)]})

    state = read_catalog_state(_router(responses), QN)

    assert isinstance(state, TablePresent)
    assert state.table.kind is TableKind.STREAMING_TABLE


def test_an_ordinary_table_reads_with_the_table_kind():
    state = read_catalog_state(_router(_describe_responses()), QN)

    assert isinstance(state, TablePresent)
    assert state.table.kind is TableKind.TABLE


def test_a_non_delta_streaming_table_reads_as_failed():
    doc = _describe_doc(type="STREAMING_TABLE", provider="iceberg")
    responses = _describe_responses(**{describe_json_query(QN): [(doc,)]})

    state = read_catalog_state(_router(responses), QN)

    assert isinstance(state, ReadFailed)
    assert state.failure.exception_type == "UnsupportedRelationError"
```

- [ ] **Step 2: Run them to verify they fail**

Run: `uv run pytest tests/adapters/databricks/test_read.py -q --no-cov`
Expected: FAIL — the streaming-table document still reads as `ReadFailed`, and the rejection-message test finds the old wording.

- [ ] **Step 3: Implement**

In `src/delta_engine/adapters/databricks/read.py`:

1. Add `TableKind` to the `delta_engine.domain.model` import (`ObservedTable, QualifiedName, TableKind`).

2. Replace the comment block and the two constants at lines ~51-59 with:

```python
# The engine reads and reconciles the Delta relations it can ALTER: ordinary
# tables, managed or external (existing external tables can be altered;
# creating one is not yet supported — CREATE TABLE emits no LOCATION), and
# streaming tables, which take the ALTER STREAMING TABLE dialect and admit
# tag changes only — the reader states the kind; validation's scope gate
# enforces the restriction. Anything else a catalog name can resolve to — a
# view, a materialized view, a foreign table, a non-Delta format — cannot be
# represented as engine state, so the read admits exactly these kinds and
# fails closed on everything else, including kinds Databricks adds in the
# future.
_TABLE_KINDS_BY_RELATION_TYPE: Final[Mapping[str, TableKind]] = MappingProxyType(
    {
        "MANAGED": TableKind.TABLE,
        "EXTERNAL": TableKind.TABLE,
        "STREAMING_TABLE": TableKind.STREAMING_TABLE,
    }
)
_SUPPORTED_PROVIDERS: Final = {"delta"}
```

3. In `read_catalog_state`, replace the two lines after the absence check:

```python
        description = _describe_table(run_query, qualified_name)
        if description is None:
            return TableAbsent()
        kind = _supported_relation_kind(description)
        return TablePresent(table=_observed_table(run_query, description, kind))
```

4. Replace `_require_supported_relation` with:

```python
def _supported_relation_kind(description: TableDescription) -> TableKind:
    """
    Map the described relation onto the kind the engine manages it as.

    Managed and external Delta tables are ordinary tables; Delta streaming
    tables carry their own kind. Any other relation — a view, a materialized
    view, a foreign table, a non-Delta format, or an unknown future kind —
    raises rather than being modelled as a table.
    """
    kind = _TABLE_KINDS_BY_RELATION_TYPE.get(description.relation_type or "")
    if kind is not None and description.provider in _SUPPORTED_PROVIDERS:
        return kind
    raise UnsupportedRelationError(
        f"{description.qualified_name}: the engine manages MANAGED or EXTERNAL Delta"
        f" tables, and Delta streaming tables for tag governance; this relation has"
        f" type={description.relation_type!r}, provider={description.provider!r}"
    )
```

**If Task 2 pinned a different provider value for streaming tables** (absent, or not `"delta"`), gate the provider per kind instead — replace the two condition lines above with:

```python
    kind = _TABLE_KINDS_BY_RELATION_TYPE.get(description.relation_type or "")
    if kind is TableKind.STREAMING_TABLE and description.provider == <pinned value>:
        return kind
    if kind is TableKind.TABLE and description.provider in _SUPPORTED_PROVIDERS:
        return kind
```

and set the same pinned value in the unit fixtures (`_describe_doc(type="STREAMING_TABLE", provider=<pinned value>)` in the streaming-table tests, and drop or adapt `test_a_non_delta_streaming_table_reads_as_failed` to the pinned semantics).

5. Give `_observed_table` the kind (signature and construction; docstring unchanged):

```python
def _observed_table(
    run_query: RunQuery, description: TableDescription, kind: TableKind
) -> ObservedTable:
```

and add `kind=kind,` to the `ObservedTable(...)` construction (after `referencing_foreign_keys=...`).

- [ ] **Step 4: Run the tests**

Run: `uv run pytest tests/adapters/databricks/test_read.py tests/adapters/databricks/warehouse/test_reader.py -q --no-cov`
Expected: PASS.

- [ ] **Step 5: Commit**

```bash
git add src/delta_engine/adapters/databricks/read.py tests/adapters/databricks/test_read.py
git commit -m "feat: read streaming tables as observed state"
```

---

### Task 9: Integration test — a tags-scope dry run emits `ALTER STREAMING TABLE`

Pure verification, no `src` changes: wire the real warehouse reader, engine, validation, and compiler over a canned describe document. If a step fails here, a previous task has a bug — fix it there.

**Files:**

- Create: `tests/adapters/databricks/warehouse/test_streaming_table_dry_run.py`

**Interfaces:**

- Consumes: everything from Tasks 3–8 plus the public `DeltaTable` API; `WarehouseReader`/`WarehouseExecutor` (the executor's `compile` never touches the connection, and a dry run never calls `execute`, so the routed fake connection is enough).

- [ ] **Step 1: Write the test file**

```python
"""
End-to-end dry run against an observed streaming table.

Wires the real warehouse reader, engine, validation, and SQL compiler over a
canned DESCRIBE AS JSON document: a tags-scope declaration against a streaming
table plans ALTER STREAMING TABLE statements, and any wider scope fails
validation before SQL is planned.
"""

import json
from types import SimpleNamespace

from delta_engine.adapters.databricks.sql import (
    column_tags_query,
    describe_json_query,
    foreign_keys_query,
    primary_key_query,
    referencing_foreign_keys_query,
    table_tags_query,
)
from delta_engine.adapters.databricks.warehouse.executor import WarehouseExecutor
from delta_engine.adapters.databricks.warehouse.reader import WarehouseReader
from delta_engine.application.engine import Engine
from delta_engine.application.failures import ValidationFailure
from delta_engine.application.report import TableRunStatus
from delta_engine.domain.model import QualifiedName
from delta_engine.schema import Column, DeltaTable, Integer

QN = QualifiedName("cat", "sch", "clicks")

_STREAMING_DOC = json.dumps(
    {
        "table_name": "clicks",
        "catalog_name": "cat",
        "schema_name": "sch",
        "type": "STREAMING_TABLE",
        "provider": "delta",
        "columns": [{"name": "id", "type": {"name": "int"}, "nullable": True}],
        "comment": "",
        "table_properties": {},
    }
)


class RoutedCursor:
    def __init__(self, responses):
        self._responses = responses

    def execute(self, query):
        self._current = self._responses.get(query, [])

    def fetchall(self):
        return list(self._current)

    def close(self):
        pass


class RoutedConnection:
    def __init__(self, responses):
        self._responses = responses

    def cursor(self):
        return RoutedCursor(self._responses)


def _streaming_table_connection() -> RoutedConnection:
    return RoutedConnection(
        {
            describe_json_query(QN): [(_STREAMING_DOC,)],
            table_tags_query(QN): [SimpleNamespace(tag_name="stale", tag_value="remove-me")],
            column_tags_query(QN): [],
            primary_key_query(QN): [],
            foreign_keys_query(QN): [],
            referencing_foreign_keys_query(QN): [],
        }
    )


def _engine() -> Engine:
    connection = _streaming_table_connection()
    return Engine(
        reader=WarehouseReader(connection),
        executor=WarehouseExecutor(connection),
    )


def test_tags_scope_dry_run_plans_streaming_table_ddl():
    # Given a tags-scope declaration over a described streaming table with
    # one tag to set, one to unset, and one column tag to set
    declaration = DeltaTable(
        "cat",
        "sch",
        "clicks",
        columns=(Column("id", Integer(), tags={"pii": "low"}),),
        tags={"owner": "governance"},
        scope="tags",
    )

    # When dry-running a sync
    report = _engine().sync(declaration, dry_run=True)

    # Then the planned SQL carries the streaming-table dialect end to end
    [table_report] = list(report)
    assert table_report.status is TableRunStatus.SUCCESS
    assert set(table_report.planned_sql_statements) == {
        "ALTER STREAMING TABLE `cat`.`sch`.`clicks` SET TAGS ('owner'='governance')",
        "ALTER STREAMING TABLE `cat`.`sch`.`clicks` UNSET TAGS ('stale')",
        "ALTER STREAMING TABLE `cat`.`sch`.`clicks` ALTER COLUMN `id` SET TAGS ('pii'='low')",
    }


def test_full_scope_dry_run_fails_validation_against_a_streaming_table():
    # Given a full-scope declaration whose shape exactly matches the streaming
    # table — zero drift, but the declaration claims the whole table
    declaration = DeltaTable(
        "cat",
        "sch",
        "clicks",
        columns=(Column("id", Integer()),),
        tags={"stale": "remove-me"},
    )

    # When dry-running a sync
    report = _engine().sync(declaration, dry_run=True)

    # Then validation rejects the declaration on kind alone; no SQL is planned
    [table_report] = list(report)
    assert table_report.status is TableRunStatus.PLANNING_FAILED
    assert table_report.planned_sql_statements == ()
    rule_names = {
        failure.rule_name
        for failure in table_report.failures
        if isinstance(failure, ValidationFailure)
    }
    assert "StreamingTableTagsOnly" in rule_names
```

- [ ] **Step 2: Run it**

Run: `uv run pytest tests/adapters/databricks/warehouse/test_streaming_table_dry_run.py -q --no-cov`
Expected: PASS. (If the statement set differs only in ordering semantics, the set comparison already absorbs plan order; any other mismatch is a real defect in Tasks 6–8.)

- [ ] **Step 3: Run the full local suite**

Run: `uv run pytest`
Expected: PASS with coverage above the threshold.

- [ ] **Step 4: Commit**

```bash
git add tests/adapters/databricks/warehouse/test_streaming_table_dry_run.py
git commit -m "test: prove a tags-scope dry run emits streaming-table DDL"
```

---

### Task 10: Live round-trip and the flipped reader pin

The old live pin asserted a streaming table fails its read; the reader now admits them, so that pin is superseded by a positive one, and the round-trip (spec live pin 5) proves the whole feature against a real warehouse.

**Files:**

- Modify: `tests/live/test_sql_warehouse_live_streaming_tables.py` (two new tests)
- Modify: `tests/live/test_sql_warehouse_live_supported_relations.py` (delete the streaming-table test; one docstring sentence)

**Interfaces:**

- Consumes: `_create_streaming_table` (Task 1), `WarehouseReader`, `TableKind`, `build_sql_engine`, the full feature (Tasks 3–8).

- [ ] **Step 1: Add the two live tests**

Append to `tests/live/test_sql_warehouse_live_streaming_tables.py`, extending the import block with:

```python
from delta_engine.adapters.databricks.warehouse.reader import WarehouseReader
from delta_engine.application.ports import TablePresent
from delta_engine.databricks import build_sql_engine
from delta_engine.domain.model import QualifiedName, TableKind
from delta_engine.schema import Column, DeltaTable, Integer
```

(keep these below the `pytest.importorskip("databricks.sql")` line, alongside the existing imports) and adding:

```python
def test_the_engine_reads_a_streaming_table_with_its_kind(live_connection, live_tables):
    """WarehouseReader observes a streaming table as present, carrying its kind."""
    # Supersedes the old supported-relations pin that read streaming tables
    # as failed: they are now engine state, discovered — never declared.
    table_name = _create_streaming_table(live_connection, live_tables)

    reader = WarehouseReader(live_connection)
    state = reader.fetch_state(QualifiedName(live_catalog(), live_schema(), table_name))

    assert isinstance(state, TablePresent), state
    assert state.table.kind is TableKind.STREAMING_TABLE


def test_tags_scope_sync_reconciles_streaming_table_tags(live_connection, live_tables):
    """A tags-scope sync sets declared tags and unsets observed-only tags on a streaming table."""
    table_name = _create_streaming_table(live_connection, live_tables)
    target = qualified_table(table_name)
    execute_sql(live_connection, f"ALTER STREAMING TABLE {target} SET TAGS ('old'='remove-me')")

    build_sql_engine(live_connection).sync(
        DeltaTable(
            live_catalog(),
            live_schema(),
            table_name,
            columns=(Column("id", Integer(), tags={"pii": "low"}),),
            tags={"owner": "governance"},
            scope="tags",
        )
    )

    assert _table_tags(live_connection, table_name) == {"owner": "governance"}
    assert _column_tags(live_connection, table_name) == {("id", "pii"): "low"}
```

Note for the executor: the round-trip declaration must mirror the streaming table's shape exactly — one nullable `id INT` column, no comment, no keys — because non-tag drift fails a tags-scope sync by design. If the final Live run (Task 12) reveals the platform stamps something the declaration cannot mirror (for example an automatic table comment), adjust the declaration in this test to the observed value and note it in a comment; that is a discovered platform fact, not a test bug.

- [ ] **Step 2: Remove the superseded fail-closed pin**

In `tests/live/test_sql_warehouse_live_supported_relations.py`:

1. Delete the whole `test_a_streaming_table_is_not_read_as_a_table` function.
2. Update the module docstring's first paragraph to:

```python
"""
Live pins for the read boundary's relation acceptance.

The engine reads managed and external Delta tables, and streaming tables, as
observed state. Each test creates a catalog relation outside that set and
asserts the engine's own ``WarehouseReader`` fails the read rather than
admitting it as a table to diff and plan against.
"""
```

- [ ] **Step 3: Verify collection and lint**

Run: `uv run pytest tests/live -m databricks_e2e --collect-only --no-cov -q && uv run ruff check tests/live && uv run ruff format tests/live`
Expected: the streaming file collects 6 tests; supported-relations collects 2; no lint findings.

- [ ] **Step 4: Commit**

```bash
git add tests/live/test_sql_warehouse_live_streaming_tables.py tests/live/test_sql_warehouse_live_supported_relations.py
git commit -m "test: pin the live streaming-table tag round-trip"
```

---

### Task 11: Documentation

**Files:**

- Modify: `docs/how-to-deploy-metadata-only.md`, `docs/how-to-configure-table.md`, `docs/reference-safe-change-rules.md`, `docs/reference-limitations.md`, `docs/explanation-safety-model.md`, `docs/how-to-implement-adapter.md`, `docs/how-to-add-action-type.md`, `docs/explanation-architecture.md`, `src/delta_engine/api/delta_table.py`

- [ ] **Step 1: `docs/how-to-deploy-metadata-only.md`** — append a section before "Mixing scopes in one sync":

````markdown
## Tag a streaming table

`scope="tags"` extends to streaming tables. A streaming table's definition —
schema, comments, properties — is owned by its pipeline, and out-of-band
changes to those can be reverted on refresh; Unity Catalog tags persist, so
tags are the one aspect the engine can durably manage there. The engine
discovers the relation kind when it reads the table (nothing is declared),
compiles tag changes as `ALTER STREAMING TABLE`, and rejects any wider scope:
a `"full"` or `"metadata"` declaration against a streaming table fails
validation (`StreamingTableTagsOnly`) even when nothing has drifted.

```python
from delta_engine.schema import Column, DeltaTable, Integer

clicks = DeltaTable(
    catalog="dev",
    schema="silver",
    name="clicks",
    columns=[Column("id", Integer(), tags={"pii": "low"})],
    tags={"owner": "governance"},
    scope="tags",
)
```
````

Materialized views remain unsupported: a name that resolves to one still
fails its read. See [limitations](reference-limitations.md).

````

(Nest the inner code fence correctly when editing — the block above shows the final page content.)

- [ ] **Step 2: `docs/how-to-configure-table.md`** — in the "Manage tags only" section (line ~346), replace the closing paragraph:

Replace:

```markdown
The live table must already exist. If a non-tag aspect drifts from the
declaration, validation fails before any tag SQL runs; update the declaration
to match the live table or use the full scope. Properties are the exception:
a restricted scope never compares them, so live table properties cannot fail
the sync.
````

with:

```markdown
The live table must already exist. If a non-tag aspect drifts from the
declaration, validation fails before any tag SQL runs; update the declaration
to match the live table or use the full scope. Properties are the exception:
a restricted scope never compares them, so live table properties cannot fail
the sync.

Streaming tables are supported here and only here: the engine discovers the
relation kind at read time and compiles tag changes with the
`ALTER STREAMING TABLE` dialect, while any wider scope against one fails
validation. See
[tag a streaming table](how-to-deploy-metadata-only.md#tag-a-streaming-table).
```

- [ ] **Step 3: `docs/reference-safe-change-rules.md`** — the scope-invariants paragraph and table (lines 68-74) become:

```markdown
Three further checks are scope invariants rather than rules — they define what
a declaration is allowed to govern and always run, regardless of the rule set:

| Invariant                | What it blocks                                                                                          | How to resolve                                                                        |
| ------------------------ | ------------------------------------------------------------------------------------------------------- | ------------------------------------------------------------------------------------- |
| `UnmanagedAspectDrift`   | An unmanaged aspect (e.g. column structure) has drifted from the declaration in a restricted-scope sync | Sync the table fully, or update the declaration to match the live schema              |
| `MissingTableUnmanaged`  | The table does not exist but this definition does not manage table existence                            | Create the table out-of-band first, or manage it fully                                |
| `StreamingTableTagsOnly` | The observed table is a streaming table and the declaration manages more than tags                      | Declare it with `scope="tags"`; the table's definition belongs to its owning pipeline |

`StreamingTableTagsOnly` judges the declaration against the observed relation
kind, not against drift, so it fires even when the streaming table is
currently in sync. Comments and properties stay unmanageable on streaming
tables deliberately: the pipeline definition owns them and a refresh can
revert out-of-band changes, whereas Unity Catalog tags persist.
```

- [ ] **Step 4: `docs/reference-limitations.md`** — three edits:

1. Platform table, `Backend` row (line 15) — replace the row's text with:

```text
Delta Lake tables on Databricks with Unity Catalog — the supported target today; the reader reads managed and external Delta tables, plus Delta streaming tables for tag-only management, and any other relation a registered name resolves to (view, materialized view, foreign table, non-Delta format) fails its read ([architecture](explanation-architecture.md))
```

2. "What a sync manages" table — add a row after `Tag-only scope`:

```text
| Streaming tables          |   Tags only   | Discovered at read time; only `scope="tags"` declarations may target one — schema, comments, and properties belong to the owning pipeline ([guide](how-to-deploy-metadata-only.md#tag-a-streaming-table))                  |
```

3. "Outside the model" table — the `Views and materialized views` row's Meaning becomes:

```text
Unsupported; a registered name that resolves to a view or materialized view fails its read rather than being planned against (streaming tables, by contrast, are read for tag-only management)
```

- [ ] **Step 5: `docs/explanation-safety-model.md`** — two edits:

1. In the scopes table (line ~66), the `"tags"` row's "Use for" becomes:

```text
Tag governance for tables owned elsewhere — including streaming tables, where tags are the only manageable aspect
```

2. After the paragraph that follows the table ("See [how to deploy metadata only]..."), add:

```markdown
Streaming tables make the scope boundary literal. Their definition is owned by
a pipeline, so the engine reads one for tag governance only: the relation kind
is discovered at read time, and a declaration that manages anything beyond
tags fails validation (`StreamingTableTagsOnly`) before any SQL runs — even
with zero drift. See
[safe-change rules](reference-safe-change-rules.md) for the invariant and
[tag a streaming table](how-to-deploy-metadata-only.md#tag-a-streaming-table)
for the workflow.
```

- [ ] **Step 6: contributor docs** — three signature updates:

1. `docs/how-to-implement-adapter.md` (line ~52): the example becomes

```python
class MyExecutor:
    def compile(
        self, qualified_name: QualifiedName, plan: ActionPlan, kind: TableKind
    ) -> tuple[str, ...]:
        return tuple(self._render(action) for action in plan.actions)
```

with `TableKind` added to the example's `from delta_engine.domain.model import QualifiedName` import line, and this sentence appended to the paragraph below the example: "`kind` is the observed relation kind of the target table — backends whose DDL dialect differs by kind (Databricks streaming tables take `ALTER STREAMING TABLE`) dispatch on it; a backend with one dialect may ignore it."

2. `docs/how-to-add-action-type.md` (lines ~107-115): the register example becomes

```python
@_compile_action.register
def _(action: UpdateComment, target: _SqlTarget) -> str:
    col = backtick(action.column_name)
    comment = quote_literal(action.desired_comment)
    return f"{target.alter_table} ALTER COLUMN {col} COMMENT {comment}"
```

and the prose line below it becomes: "Each handler receives a `_SqlTarget` carrying the backticked table name (`target.name`) and the kind-correct ALTER clause (`target.alter_table` — `ALTER TABLE ...` or `ALTER STREAMING TABLE ...`); ALTER-family statements start from `target.alter_table` so every action follows the observed relation kind. A constraint action carries its complete constraint (named when the `DesiredTable` was built, or read from the catalog for an observed one), so the handler renders `action.constraint.constraint_name` directly rather than computing it."

3. `docs/explanation-architecture.md`: line ~115 `compile(qualified_name, plan)` → `compile(qualified_name, plan, kind)`, appending to that sentence: "— `kind` being the observed relation kind, so the SQL dialect follows what the reader saw"; and in the sequence diagram (line ~265) `Engine->>Executor: compile(qualified_name, plan)` → `Engine->>Executor: compile(qualified_name, plan, kind)`.

- [ ] **Step 7: `src/delta_engine/api/delta_table.py`** — in the `__init__` docstring (line ~447), replace the `scope:` entry's tags sentence:

Replace:

```text
            ``"tags"`` restricts it to table and column tags
                — for tables owned elsewhere (e.g. by a streaming pipeline)
                whose Unity Catalog tags this engine should still govern. A
```

with:

```text
            ``"tags"`` restricts it to table and column tags
                — for tables owned elsewhere whose Unity Catalog tags this
                engine should still govern. Streaming tables are supported
                under this scope and only this scope: their definition belongs
                to the owning pipeline, so any wider scope against one fails
                validation. A
```

(Adjust the surrounding line wrapping to keep lines under 100 characters.)

- [ ] **Step 8: Build the docs and run the API tests**

Run: `uv run --group docs sphinx-build -b html docs docs/_build/html -W && uv run pytest tests/api -q --no-cov`
Expected: docs build clean with `-W`; API tests pass (docstring-only change).

- [ ] **Step 9: Commit**

```bash
git add docs src/delta_engine/api/delta_table.py
git commit -m "docs: document tags-scope support on streaming tables"
```

---

### Task 12: Full validation and the final Live run

**Files:** none (fixes only, if a check fails).

- [ ] **Step 1: Run the full local gate**

```bash
uv run pytest
uv run ruff check .
uv run ruff format --check .
uv run mypy .
uv run lint-imports
uv run --group docs sphinx-build -b html docs docs/_build/html -W
```

Expected: all clean (CI lints the whole repo, hence `.` rather than `src tests`). Fix and commit anything that fails.

- [ ] **Step 2: Push and dispatch the final Live run**

```bash
git push origin claude/streaming-table-alter-scope-354849
gh workflow run live.yaml --ref claude/streaming-table-alter-scope-354849
sleep 20
gh run list --workflow live.yaml --limit 1 --json databaseId,status --jq '.[0].databaseId'
gh run watch <run-id> --exit-status
```

Expected: green, including the six streaming-table tests (four fact pins, the reader pin, the round-trip). If the round-trip fails on unmirrorable platform-stamped state, apply the adjustment note in Task 10 Step 1 and re-dispatch.

- [ ] **Step 3: Report and hand off**

Summarise: what changed (domain kind, gate, port widening, dialect dispatch, reader admit-gate, live pins, docs), which checks ran (full local gate + two Live runs), and remaining risks (none expected beyond the spec's accepted ones). Then open the PR against `main`:

```bash
gh pr create --title "feat: tags scope on streaming tables" --body "..."
```

PR body: link `docs/todo/2026-07-16-streaming-table-tags-design.md`, state the pinned platform facts (Task 2 values), name the two Live run URLs, and note that materialized views remain deliberately fail-closed with a regression pin.
