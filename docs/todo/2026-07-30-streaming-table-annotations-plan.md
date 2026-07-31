# Streaming Table Annotations Scope — Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** A new public scope `"annotations"` manages the table comment, column comments, table tags, and column tags; streaming tables become annotations territory rather than tags-only, enforced by a renamed `StreamingTableAnnotationsOnly` eligibility check.

**Architecture:** Vocabulary plus one eligibility check. `application/scopes.py` gains `COMMENT_ASPECTS`, `ANNOTATION_ASPECTS`, and `KEY_ASPECTS`, and redefines `METADATA_ASPECTS` as `ANNOTATION_ASPECTS | KEY_ASPECTS` so the containment `tags ⊂ annotations ⊂ metadata ⊂ full` is structural rather than four parallel literals that can drift apart. `application/validation.py` renames `StreamingTableTagsOnly` to `StreamingTableAnnotationsOnly` and widens its permitted set from `TAG_ASPECTS` to `ANNOTATION_ASPECTS`. No adapter, domain, or reader change: the compiler already emits both comment statements correctly for either relation kind, and the reader already admits streaming tables and reads both comment levels.

**Tech Stack:** Python 3.12, `uv`, pytest, ruff, mypy, import-linter. Databricks SQL warehouse for the opt-in `tests/live` suite.

**Source design:** `docs/todo/2026-07-30-streaming-table-annotations-design.md`. Read it before starting — this plan implements it and does not restate its reasoning.

## Global Constraints

- **Python 3.12**; ruff line-length 100, double quotes; `disallow_untyped_defs = true` in mypy.
- **Never commit to `main`.** Work on `feat/streaming-table-annotations` (already checked out in this worktree).
- **Conventional commits.** The rename is breaking and rides a `BREAKING CHANGE:` footer — see Task 8.
- **`schema_version` stays at `2`** (confirmed 2026-07-31). `report.py:312` is not touched by this plan. The key and its type do not change; only one value `rule_name` can take.
- **`TAG_ASPECTS` keeps its exact current membership** and still backs `scope="tags"`, which stays — it is shipped public API.
- **`METADATA_ASPECTS` keeps its exact current membership.** Only its spelling changes, from a six-member literal to a union.
- **Aspect constants are shared, never duplicated.** `StreamingTableAnnotationsOnly` imports `ANNOTATION_ASPECTS` from `application.scopes` so the gate and the public scope cannot diverge. This is the property `docs/todo/policy-visibility-review.md` records; Task 7 updates that record.
- **The exact string `StreamingTableAnnotationsOnly`** is asserted in unit tests, the dry-run test, the live suite, and three docs. Spell it identically everywhere.
- Verification commands, run from the worktree root:
  - `uv run pytest` — default suite (live tests are excluded by `-m "not databricks_e2e"`)
  - `uv run ruff format . && uv run ruff check .`
  - `uv run mypy src`
  - `uv run lint-imports`
  - `uv run pytest tests/live -m databricks_e2e --no-cov` — **live suite, opt-in, needs workspace credentials**

---

## File Structure

| File | Responsibility after this change |
| --- | --- |
| `src/delta_engine/application/scopes.py` | Owns the four public scope names and their aspect sets, composed as a lattice |
| `src/delta_engine/application/validation.py` | `StreamingTableAnnotationsOnly` judges claimed aspects against the observed relation kind |
| `src/delta_engine/api/delta_table.py` | `scope` docstring documents `"annotations"` and the key-mirroring requirement |
| `tests/application/test_scopes.py` | Scope membership **and** the lattice containments |
| `tests/application/test_validation.py` | Scope × kind matrix against a streaming table |
| `tests/application/test_planning.py` | Existing streaming-table planning coverage; import site only |
| `tests/api/test_delta_table.py` | `scope="annotations"` lowers to the four aspects |
| `tests/adapters/databricks/warehouse/test_streaming_table_dry_run.py` | End-to-end: an annotations sync plans streaming-table comment SQL |
| `tests/live/test_sql_warehouse_live_streaming_tables.py` | Backend facts (Task 1) and engine round-trips (Task 6) |
| `docs/how-to-deploy-metadata-only.md` | "Annotate a streaming table" — the first place a reader meets key mirroring |
| `docs/how-to-configure-table.md` | The new scope alongside the tags-only section |
| `docs/reference-safe-change-rules.md` | The law's row and the paragraph beneath it |
| `docs/reference-limitations.md` | Streaming-tables row, backend row, comment-revert caveat |
| `docs/explanation-safety-model.md` | Four-scope table; streaming tables as annotation territory |
| `docs/todo/policy-visibility-review.md` | Lines 78–81 record which constant the gate reuses |

**Deliberately untouched:** `domain/`, `adapters/`, `application/report.py`, `CHANGELOG.md` history, the archived `2026-07-16-streaming-table-tags-*` documents, and `docs/todo/todo.md`'s own entries.

---

## Task 1: Live backend-fact pins — **STOP for a live run after this task**

The design's risk section requires the documented comment facts pinned before the gate is written. This task pins the facts that need **no engine code** — raw SQL and information_schema only. The engine-level round-trip needs `scope="annotations"` to exist, so it lands in Task 6.

**A live pin states a platform fact the engine assumes, and earns its place only if it can fail for a reason no unit test would catch.** Everything about what the engine *emits*, *diffs*, or *refuses* is unit-testable and belongs in Tasks 3 and 5. What is left for the platform to answer is short:

1. `ALTER STREAMING TABLE … ALTER COLUMN c COMMENT '…'` is accepted.
2. `COMMENT ON TABLE <streaming table> IS '…'` is accepted.
3. `… COMMENT ''` is accepted under the streaming-table prefix — the design calls this "a live question, not an inherited guarantee".
4. Comments written that way are observable by the engine's reader, so a resync converges instead of re-emitting forever.

Facts 1–3 are this task. Fact 4 is Task 6, where convergence proves it better than any reader assertion could.

**No new live tests, and no new provisions.** The module's two existing tests already split exactly the right way — one owns the raw statement surface, one owns the engine end to end — so both widen rather than gaining neighbours. Against a one-pipeline quota that matters more than tidiness.

**Files:**
- Modify: `tests/live/test_sql_warehouse_live_streaming_tables.py` (module docstring lines 1–8; two helpers; widen `test_alter_streaming_table_tags_round_through_information_schema`)

**Interfaces:**
- Consumes: existing module helpers `_create_streaming_table`, `_table_tags`, `_column_tags`, and the `live_connection` / `live_tables` fixtures.
- Produces: `_table_comment(live_connection, table_name) -> str` and `_column_comments(live_connection, table_name) -> dict[str, str]`. Task 6 reuses both.

- [ ] **Step 1: Update the module docstring**

Replace lines 1–8 (the first paragraph) of `tests/live/test_sql_warehouse_live_streaming_tables.py`:

```python
"""
Live pins for the streaming-table facts the annotations scope is built on.

A streaming table's definition — schema, properties, and keys — is owned by
its pipeline. The line the platform draws is the defining SQL: comments and
Unity Catalog tags are alterable from outside the pipeline via the documented
ALTER STREAMING TABLE dialect and COMMENT ON, while schema, properties, and
constraints belong to CREATE OR REFRESH. Each test states platform facts the
engine's reader gate, validation gate, or SQL dialect dispatch assumes.
```

Leave the two "deliberately absent" paragraphs and the quota paragraph (lines 10–23) exactly as they are.

- [ ] **Step 2: Add the two read helpers**

Append after `_column_tags` (currently ending line 114):

```python
def _table_comment(live_connection, table_name: str) -> str:
    rows = fetch_rows(
        live_connection,
        f"SELECT comment "
        f"FROM {backtick(live_catalog())}.information_schema.tables "
        f"WHERE table_schema = {quote_literal(live_schema())} "
        f"AND table_name = {quote_literal(table_name)}",
    )
    [row] = rows
    return row["comment"] or ""


def _column_comments(live_connection, table_name: str) -> dict[str, str]:
    rows = fetch_rows(
        live_connection,
        f"SELECT column_name, comment "
        f"FROM {backtick(live_catalog())}.information_schema.columns "
        f"WHERE table_schema = {quote_literal(live_schema())} "
        f"AND table_name = {quote_literal(table_name)}",
    )
    return {row["column_name"]: row["comment"] or "" for row in rows}
```

Both mirror the shape of `_table_tags` / `_column_tags`: a live pin reads the platform through information_schema directly, independently of the engine's own queries, so a pin and the code it guards cannot be wrong together.

- [ ] **Step 3: Give the provisioned streaming table a primary key**

In `_create_streaming_table` (line ~68), give both the source and the streaming table a key. Replace the two statements:

```python
    execute_sql(
        live_connection,
        f"CREATE TABLE {qualified_table(source_name)} (id INT NOT NULL) USING DELTA",
    )
    create_statement = (
        f"CREATE STREAMING TABLE {qualified_table(table_name)} "
        f"(id INT NOT NULL, CONSTRAINT {table_name}_pk PRIMARY KEY (id)) "
        f"AS SELECT id FROM STREAM({qualified_table(source_name)})"
    )
```

and widen the capability label, since provisioning now depends on key-constraint support as well:

```python
        capability="streaming tables with key constraints",
```

**Why the key lives in the shared fixture.** The engine does not depend on whether a streaming table reports a primary key — `_diff_primary_key` is correct either way — so on its own the fact does not earn a pin. What it does gate is the **guide's advice**: if the platform did not report the key, "mirror the pipeline's key" would be wrong advice, because mirroring would emit `SetPrimaryKey` and fail `UnmanagedAspectDrift`. Making the shared fixture keyed verifies that advice through Task 6's convergence assertion at zero extra provisioning cost, instead of buying a dedicated test and a second pipeline provision for it.

`NOT NULL` is required twice over: Unity Catalog will not accept a primary key on a nullable column, and the engine refuses a declaration whose key column is nullable — so a nullable observed column would surface as column-structure drift under `"annotations"` rather than the clean sync Task 6 asserts. Key constraints are unsupported in `hive_metastore`; the live suite targets Unity Catalog, so this is a note rather than a guard.

**Trade-off, accepted deliberately:** both live tests now depend on key-constraint support on streaming tables. If the workspace cannot do it, both fail at provisioning rather than one failing for the reason it names. That is why the key goes in at Task 1 rather than Task 6 — this task ends in a live run, so a workspace that cannot support it is discovered before any code is written. If it does fail, split `_create_streaming_table` into keyed and keyless variants and give Task 6 the keyed one.

- [ ] **Step 4: Widen the raw-statement-surface test to comments**

`test_alter_streaming_table_tags_round_through_information_schema` (line 117) already owns "the statements the engine compiles against a streaming table, asserted through information_schema". Comments join tags there rather than starting a neighbouring test. Replace it entirely:

```python
def test_alter_streaming_table_manages_tags_and_comments(live_connection, live_tables):
    """ALTER STREAMING TABLE and COMMENT ON manage tags and comments from outside the pipeline."""
    # The statements are the entire surface the engine compiles against a
    # streaming table, and information_schema is where its reader observes
    # their effect — every statement's effect asserted, on one provisioned
    # table (see the module docstring on the pipeline quota).
    table_name = _create_streaming_table(live_connection, live_tables)
    target = qualified_table(table_name)

    execute_sql(live_connection, f"ALTER STREAMING TABLE {target} SET TAGS ('owner'='governance')")
    execute_sql(
        live_connection,
        f"ALTER STREAMING TABLE {target} ALTER COLUMN id SET TAGS ('pii'='low')",
    )
    assert _table_tags(live_connection, table_name) == {"owner": "governance"}
    assert _column_tags(live_connection, table_name) == {("id", "pii"): "low"}

    # Comments are the capability the annotations scope adds: the column
    # clause of ALTER STREAMING TABLE, and kind-independent COMMENT ON.
    execute_sql(live_connection, f"ALTER STREAMING TABLE {target} ALTER COLUMN id COMMENT 'the id'")
    execute_sql(live_connection, f"COMMENT ON TABLE {target} IS 'click events'")
    assert _column_comments(live_connection, table_name) == {"id": "the id"}
    assert _table_comment(live_connection, table_name) == "click events"

    execute_sql(live_connection, f"ALTER STREAMING TABLE {target} UNSET TAGS ('owner')")
    execute_sql(
        live_connection,
        f"ALTER STREAMING TABLE {target} ALTER COLUMN id UNSET TAGS ('pii')",
    )
    assert _table_tags(live_connection, table_name) == {}
    assert _column_tags(live_connection, table_name) == {}

    # An empty desired comment compiles to COMMENT '' rather than UNSET
    # COMMENT: SQL warehouses reject the latter. This pins that the ALTER
    # STREAMING TABLE prefix accepts it and that '' is what comes back.
    execute_sql(live_connection, f"ALTER STREAMING TABLE {target} ALTER COLUMN id COMMENT ''")
    assert _column_comments(live_connection, table_name) == {"id": ""}
```

- [ ] **Step 5: Lint and type-check (the default suite cannot run these tests)**

```bash
uv run ruff format . && uv run ruff check . && uv run pytest --collect-only tests/live -q
```

Expected: clean format/lint, and the widened test still collected (it is deselected from the default run by the `databricks_e2e` marker filter but must still import and collect).

- [ ] **Step 6: Commit**

```bash
git add tests/live/test_sql_warehouse_live_streaming_tables.py
git commit -m "test: pin the streaming-table comment statements live"
```

- [ ] **Step 7: STOP. Hand back for a live run.**

Report to the user that Task 1 is committed and the live suite must be run before Task 2:

```bash
uv run pytest tests/live -m databricks_e2e --no-cov -k "streaming_table"
```

Do **not** start Task 2 until those pins are reported green. If a pin fails, the design's backend facts are wrong and the design needs revisiting before any gate is written — that is exactly what this checkpoint exists to catch.

---

## Task 2: The `annotations` scope

**Files:**
- Modify: `src/delta_engine/application/scopes.py` (whole file)
- Test: `tests/application/test_scopes.py`

**Interfaces:**
- Produces: `ScopeName` gains `"annotations"`. New module constants `COMMENT_ASPECTS`, `ANNOTATION_ASPECTS`, `KEY_ASPECTS`, all `Final[frozenset[TableAspect]]`. `TAG_ASPECTS` and `METADATA_ASPECTS` keep their names, types, and exact membership. Task 3 imports `ANNOTATION_ASPECTS`; Task 4 and Task 7 name the scope `"annotations"`.

- [ ] **Step 1: Write the failing tests**

Replace the body of `tests/application/test_scopes.py` below the imports, keeping the existing four tests and adding three. The import line becomes:

```python
import pytest

from delta_engine.application.scopes import (
    ANNOTATION_ASPECTS,
    METADATA_ASPECTS,
    TAG_ASPECTS,
    managed_aspects_for,
)
from delta_engine.domain.model import ALL_ASPECTS, TableAspect
```

Append these three tests:

```python
def test_annotations_scope_manages_comments_and_tags():
    assert managed_aspects_for("annotations") == frozenset(
        {
            TableAspect.TABLE_COMMENT,
            TableAspect.COLUMN_COMMENTS,
            TableAspect.TABLE_TAGS,
            TableAspect.COLUMN_TAGS,
        }
    )


def test_the_scopes_form_a_containment_lattice():
    # Given the four public scopes, ordered by how much they claim
    # Then each is strictly contained by the next, so a caller who narrows a
    # scope can only ever lose authority, never trade it sideways
    assert TAG_ASPECTS < ANNOTATION_ASPECTS
    assert ANNOTATION_ASPECTS < METADATA_ASPECTS
    assert METADATA_ASPECTS < ALL_ASPECTS


def test_annotations_scope_does_not_manage_keys():
    # The distinction that earns "annotations" its own name rather than being
    # folded into "metadata": keys are what a streaming table cannot delegate
    assert TableAspect.PRIMARY_KEY not in ANNOTATION_ASPECTS
    assert TableAspect.FOREIGN_KEYS not in ANNOTATION_ASPECTS
```

- [ ] **Step 2: Run the tests to verify they fail**

```bash
uv run pytest tests/application/test_scopes.py -v --no-cov
```

Expected: FAIL — `ImportError: cannot import name 'ANNOTATION_ASPECTS'`.

- [ ] **Step 3: Rewrite `scopes.py`**

Replace lines 1–36 of `src/delta_engine/application/scopes.py` (everything above `def managed_aspects_for`):

```python
"""
Named ownership scopes for Delta table declarations.

The domain defines the complete ``TableAspect`` vocabulary. This module owns
the supported public combinations and resolves a scope name at the API boundary.

The four scopes form a total order — ``tags ⊂ annotations ⊂ metadata ⊂ full``
— so the sets are composed from each other rather than written as parallel
literals that can drift apart. Narrowing a scope can only ever drop authority.
"""

from typing import Final, Literal

from delta_engine.domain.model import ALL_ASPECTS, TableAspect

type ScopeName = Literal["full", "metadata", "annotations", "tags"]

TAG_ASPECTS: Final[frozenset[TableAspect]] = frozenset(
    {
        TableAspect.TABLE_TAGS,
        TableAspect.COLUMN_TAGS,
    }
)

COMMENT_ASPECTS: Final[frozenset[TableAspect]] = frozenset(
    {
        TableAspect.TABLE_COMMENT,
        TableAspect.COLUMN_COMMENTS,
    }
)

# The aspects manageable on a relation whose definition belongs to something
# else — a streaming table's pipeline, or simply another team. Shared with
# StreamingTableAnnotationsOnly so the public scope and the gate cannot diverge.
ANNOTATION_ASPECTS: Final[frozenset[TableAspect]] = TAG_ASPECTS | COMMENT_ASPECTS

KEY_ASPECTS: Final[frozenset[TableAspect]] = frozenset(
    {
        TableAspect.PRIMARY_KEY,
        TableAspect.FOREIGN_KEYS,
    }
)

METADATA_ASPECTS: Final[frozenset[TableAspect]] = ANNOTATION_ASPECTS | KEY_ASPECTS

_ASPECTS_BY_SCOPE: Final[dict[ScopeName, frozenset[TableAspect]]] = {
    "full": ALL_ASPECTS,
    "metadata": METADATA_ASPECTS,
    "annotations": ANNOTATION_ASPECTS,
    "tags": TAG_ASPECTS,
}
```

`managed_aspects_for` below is unchanged — its error message enumerates `_ASPECTS_BY_SCOPE`, so it picks up the new name for free.

- [ ] **Step 4: Run the tests to verify they pass**

```bash
uv run pytest tests/application/test_scopes.py -v --no-cov
```

Expected: PASS, 7 tests. `test_metadata_scope_manages_only_catalog_metadata` must still pass unchanged — that is the proof `METADATA_ASPECTS` kept its exact membership through the respelling.

- [ ] **Step 5: Check nothing else moved**

```bash
uv run pytest --no-cov -q && uv run mypy src && uv run lint-imports
```

Expected: PASS. The whole suite is expected green here — Task 2 adds a scope without changing any existing one.

- [ ] **Step 6: Commit**

```bash
git add src/delta_engine/application/scopes.py tests/application/test_scopes.py
git commit -m "feat: add the annotations scope over comments and tags"
```

---

## Task 3: `StreamingTableAnnotationsOnly`

**Files:**
- Modify: `src/delta_engine/application/validation.py:14` (import), `:604-642` (the class), `:645-654` (`ELIGIBILITY_CHECKS` and its comment), `:670-692` (`validate_diff` docstring)
- Test: `tests/application/test_validation.py` (the `# ---- streaming tables` section, currently lines 1106–1213, plus `test_column_spelling_check_reports_before_the_streaming_table_check` at line 1082)

**Interfaces:**
- Consumes: `ANNOTATION_ASPECTS` from Task 2.
- Produces: class `StreamingTableAnnotationsOnly` with `name: ClassVar[str] = "StreamingTableAnnotationsOnly"`, in `ELIGIBILITY_CHECKS` at its current position (third, before `UnmanagedAspectDrift`). Tasks 5, 6, and 7 assert that exact string.

- [ ] **Step 1: Update the existing streaming-table tests to the new boundary**

Three existing tests use `frozenset({TABLE_TAGS, COLUMN_TAGS, TABLE_COMMENT})` as their "wider than tags" scope. That set is now a **subset** of `ANNOTATION_ASPECTS`, so it must be permitted — each of those tests needs a genuinely wider scope to keep testing what it names. In `tests/application/test_validation.py`:

At line ~1088, in `test_column_spelling_check_reports_before_the_streaming_table_check`, replace the `managed_aspects=` argument:

```python
        managed_aspects=METADATA_ASPECTS,
```

and update the expected rule name at the end of that test:

```python
    assert [failure.rule_name for failure in failures] == [
        "ColumnSpellingMustMatchCatalog",
        "StreamingTableAnnotationsOnly",
    ]
```

At line ~1106, replace the section's local constant with both boundaries:

```python
# ---- streaming tables


_TAG_ASPECTS_ONLY = frozenset({TableAspect.TABLE_TAGS, TableAspect.COLUMN_TAGS})
_ANNOTATION_ASPECTS_ONLY = frozenset(
    {
        TableAspect.TABLE_TAGS,
        TableAspect.COLUMN_TAGS,
        TableAspect.TABLE_COMMENT,
        TableAspect.COLUMN_COMMENTS,
    }
)
```

Spelling these out as literals rather than importing `ANNOTATION_ASPECTS` is deliberate: the test states the aspect set independently, so a mistaken edit to the production constant fails a test instead of silently redefining what both sides mean.

Then, in `test_streaming_table_fails_a_metadata_scope_declaration` (line ~1137), replace the `managed_aspects` argument with `METADATA_ASPECTS` and rename the expectation:

```python
def test_streaming_table_fails_a_metadata_scope_declaration():
    # Given a declaration managing keys as well as annotations — the aspect
    # the pipeline owns and the engine can never reconcile
    diff = _drift(
        managed_aspects=METADATA_ASPECTS,
        kind=TableKind.STREAMING_TABLE,
    )

    # Then claiming key authority is rejected
    failures = validate_diff(diff)
    assert [failure.rule_name for failure in failures] == ["StreamingTableAnnotationsOnly"]
```

And in `test_streaming_table_check_reports_before_unmanaged_aspect_drift` (line ~1169), likewise:

```python
    diff = _drift(
        AddColumn(DesiredColumn("extra", Integer())),
        managed_aspects=METADATA_ASPECTS,
        kind=TableKind.STREAMING_TABLE,
    )

    failures = validate_diff(diff)

    assert [failure.rule_name for failure in failures] == [
        "StreamingTableAnnotationsOnly",
        "UnmanagedAspectDrift",
    ]
```

Rename `StreamingTableTagsOnly` to `StreamingTableAnnotationsOnly` in the three remaining assertions in this section — `test_streaming_table_fails_a_full_scope_declaration_even_with_zero_drift` (line ~1132), `test_streaming_table_gate_cannot_be_suppressed_by_empty_rules` (line ~1156), and `test_streaming_table_gate_short_circuits_safety_rules` (line ~1167). In the first of those, the message assertion also changes:

```python
    assert 'scope="annotations"' in failures[0].message
```

`test_streaming_table_passes_when_the_declaration_manages_only_tags`, `test_streaming_table_under_tags_scope_still_fails_unmanaged_drift`, and `test_an_absent_streaming_table_under_tags_scope_still_fails_missing_table` are unchanged and must stay passing: `"tags"` is still permitted, comment drift is still unmanaged **under a tags scope**, and absence still has no observed kind.

- [ ] **Step 2: Add the new coverage the widened gate needs**

Append to the same section, after `test_streaming_table_under_tags_scope_still_fails_unmanaged_drift`:

```python
def test_streaming_table_passes_when_the_declaration_manages_annotations():
    # Given an annotations-scope declaration over a streaming table with
    # comment drift — the capability this scope exists to grant
    diff = _drift(
        SetTableComment(desired_comment="new", observed_comment="old"),
        SetColumnComment(column_name="id", desired_comment="the id", observed_comment=""),
        managed_aspects=_ANNOTATION_ASPECTS_ONLY,
        kind=TableKind.STREAMING_TABLE,
    )

    # Then the comment work is allowed: comments are alterable from outside
    # the owning pipeline, unlike schema, properties, and keys
    assert not validate_diff(diff)


def test_streaming_table_passes_an_annotations_declaration_at_zero_drift():
    # The gate judges claimed aspects against the observed kind, so a
    # permitted scope must pass with nothing to do, not merely with drift
    diff = _drift(managed_aspects=_ANNOTATION_ASPECTS_ONLY, kind=TableKind.STREAMING_TABLE)

    assert not validate_diff(diff)


def test_streaming_table_fails_a_declaration_claiming_key_authority():
    # Given a declaration managing annotations plus the primary key
    diff = _drift(
        managed_aspects=_ANNOTATION_ASPECTS_ONLY | frozenset({TableAspect.PRIMARY_KEY}),
        kind=TableKind.STREAMING_TABLE,
    )

    # Then it is rejected: the key belongs to the defining SQL, and the engine
    # can never reconcile it here — one aspect past the line is enough
    failures = validate_diff(diff)
    assert [failure.rule_name for failure in failures] == ["StreamingTableAnnotationsOnly"]


def test_annotations_scope_passes_when_the_declaration_mirrors_the_pipelines_key():
    # Given a streaming table whose pipeline declared a primary key, and an
    # annotations-scope declaration that mirrors it
    key = PrimaryKeyConstraint(("id",), "test_pk")
    desired = _desired_table(managed_aspects=_ANNOTATION_ASPECTS_ONLY, primary_key=key)
    observed = _observed_table(kind=TableKind.STREAMING_TABLE, primary_key=key)

    # Then the key signatures match, so the differ emits no key action at all
    # and there is nothing out of scope to reject. This is the mirroring
    # contract: the declaration states the key, the engine never applies it.
    assert not _validate(desired, observed)


def test_annotations_scope_fails_when_the_declaration_omits_the_pipelines_key():
    # Given the same streaming table and a declaration that omits the key —
    # primary_key=None is a positive assertion of absence everywhere else in
    # the engine, so it is one here too
    desired = _desired_table(managed_aspects=_ANNOTATION_ASPECTS_ONLY, primary_key=None)
    observed = _observed_table(
        kind=TableKind.STREAMING_TABLE,
        primary_key=PrimaryKeyConstraint(("id",), "test_pk"),
    )

    # Then the differ emits DropPrimaryKey against the observed key, PRIMARY_KEY
    # is outside the annotations scope, and the drift is reported. Late, but
    # loud — and the only honest answer for an aspect the engine cannot
    # reconcile on a pipeline-owned table. This is the trap the design closes.
    failures = _validate(desired, observed)
    assert [failure.rule_name for failure in failures] == ["UnmanagedAspectDrift"]
    assert "primary key" in failures[0].message.lower()


def test_an_ordinary_table_may_manage_anything_under_any_scope():
    # The scope is relation-kind-independent: nothing streaming-specific fires
    # against an ordinary table, at any of the four scopes
    for managed_aspects in (ALL_ASPECTS, METADATA_ASPECTS, _ANNOTATION_ASPECTS_ONLY, _TAG_ASPECTS_ONLY):
        diff = _drift(managed_aspects=managed_aspects, kind=TableKind.TABLE)
        assert not validate_diff(diff)
```

The two mirroring tests go through `_validate(desired, observed)`, which runs the **real differ** — not `_drift(...)` with a hand-fed `DropPrimaryKey`. That distinction is the whole point: the contract rests on `_diff_primary_key` treating absence as its own identity, so a test that constructs the action itself asserts nothing about the logic it claims to cover.

This needs a `primary_key` parameter on both local builders, which they do not have today. Add it to `_desired_table` (line ~56) and `_observed_table` (line ~73), defaulting to `None` and passed straight through to the model's own `primary_key` field:

```python
    primary_key: PrimaryKeyConstraint | None = None,
```

`PrimaryKeyConstraint` is already imported at the top of the file (line ~25) and is constructed positionally as `PrimaryKeyConstraint(("id",), "test_pk")` — see the existing usages at lines ~908 and ~943. Its column accessor is `columns`, not `column_names`.

The final assertion checks the failure message names the aspect, using `TableAspect.PRIMARY_KEY.label` as `UnmanagedAspectDrift` renders it. If the label is not the string `"primary key"`, match the label rather than changing the check.

- [ ] **Step 3: Run the tests to verify they fail**

```bash
uv run pytest tests/application/test_validation.py -k streaming -v --no-cov
```

Expected: FAIL — the renamed assertions report `StreamingTableTagsOnly`, and the annotations-scope tests fail because the gate still permits only `TAG_ASPECTS`.

- [ ] **Step 4: Rename and widen the check**

In `src/delta_engine/application/validation.py`, change the import at line 14:

```python
from delta_engine.application.scopes import ANNOTATION_ASPECTS
```

Replace the class at lines 604–642:

```python
class StreamingTableAnnotationsOnly:
    """
    Fail any declaration that manages more than annotations on a streaming table.

    One of the ``ELIGIBILITY_CHECKS``: it runs unconditionally and cannot be
    suppressed via ``rules``. A streaming table's definition — schema,
    properties, and keys — is owned by its pipeline and belongs to
    ``CREATE OR REFRESH``; comments and Unity Catalog tags are alterable from
    outside it, and are exactly what ``ALTER STREAMING TABLE`` and
    ``COMMENT ON`` reach. It judges the declaration's claimed aspects against
    the observed kind — not the drift — so it fires even when the table is
    currently in sync, and a dry run surfaces the misdeclaration immediately.
    ``ANNOTATION_ASPECTS`` is shared with the public ``"annotations"`` scope so
    the two policies cannot diverge.

    Keys are excluded rather than silently ignored: a declaration mirrors the
    pipeline's key to keep the aspect quiet, exactly as it already mirrors the
    pipeline's columns. If the pipeline later changes the key, the mirror stops
    matching and ``UnmanagedAspectDrift`` reports it — late, but loud, and the
    only honest answer available for an aspect the engine cannot reconcile.
    """

    name: ClassVar[str] = "StreamingTableAnnotationsOnly"

    def evaluate(self, diff: TableDiff) -> tuple[ValidationFailure, ...]:
        """Flag a streaming-table declaration whose managed aspects exceed the annotations."""
        match diff:
            case TableMissing():
                return ()

            case TableDrift() as drift:
                if drift.observed.kind is not TableKind.STREAMING_TABLE:
                    return ()
                if drift.desired.managed_aspects <= ANNOTATION_ASPECTS:
                    return ()
                return (
                    ValidationFailure(
                        rule_name=self.name,
                        message=(
                            "Operation not allowed: this relation is a streaming table,"
                            " whose definition — schema, properties, and keys — is owned"
                            " by its pipeline. Only comments and Unity Catalog tags can"
                            " be managed on it: declare the table with"
                            ' scope="annotations" (or scope="tags"), or change its'
                            " definition in the owning pipeline."
                        ),
                    ),
                )
```

Update the comment and tuple at lines 645–654:

```python
# Position is report order, so a root defect leads what it causes: spelling
# before the two it can co-fire with, then StreamingTableAnnotationsOnly before
# UnmanagedAspectDrift. MissingTableUnmanaged sits anywhere — it alone judges
# TableMissing, so it never co-fires.
ELIGIBILITY_CHECKS: Final[tuple[EligibilityCheck, ...]] = (
    ColumnSpellingMustMatchCatalog(),
    MissingTableUnmanaged(),
    StreamingTableAnnotationsOnly(),
    UnmanagedAspectDrift(),
)
```

And in `validate_diff`'s docstring (line ~680), replace the one clause that names the old boundary:

```
    missing table it may not create, or a streaming table it claims more than
    annotations on all fail here and short-circuit, so the safety rules never
    run on a diff the engine has already rejected.
```

- [ ] **Step 5: Run the tests to verify they pass**

```bash
uv run pytest tests/application/test_validation.py -v --no-cov
```

Expected: PASS, whole file.

- [ ] **Step 6: Confirm no stale references remain in `src`**

```bash
rg -n 'StreamingTableTagsOnly' src/ && echo "STALE REFERENCE — fix before committing" || echo "clean"
```

Expected: `clean`.

- [ ] **Step 7: Full suite, types, imports**

```bash
uv run pytest --no-cov -q && uv run mypy src && uv run lint-imports
```

Expected: two known failures remain — `tests/adapters/databricks/warehouse/test_streaming_table_dry_run.py` (Task 5) and nothing else. If `tests/application/test_planning.py` fails, its streaming-table case at line ~377 uses a tags scope and should be unaffected; investigate rather than editing past it.

Note for anyone running the **live** suite between here and Task 6: its second test still asserts the old rule name and will fail on it. The default suite excludes live tests, so this is invisible to `uv run pytest`. Task 6 fixes it.

- [ ] **Step 8: Commit**

```bash
git add src/delta_engine/application/validation.py tests/application/test_validation.py
git commit -m "feat: streaming tables admit comments as well as tags"
```

---

## Task 4: The public `scope` docstring

**Files:**
- Modify: `src/delta_engine/api/delta_table.py:596-597` (class docstring), `:639-651` (the `scope` arg)
- Test: `tests/api/test_delta_table.py` (the `# ---- scope` section, from line 788)

**Interfaces:**
- Consumes: `ScopeName` from Task 2 — already imported at line 20, no import change needed.
- Produces: no new symbols. `DeltaTable(scope="annotations")` lowers to `ANNOTATION_ASPECTS`.

- [ ] **Step 1: Write the failing tests**

In `tests/api/test_delta_table.py`, change the import at line 5:

```python
from delta_engine.application.scopes import ANNOTATION_ASPECTS, METADATA_ASPECTS, TAG_ASPECTS
```

Append to the `# ---- scope` section:

```python
def test_annotations_scope_manages_comments_and_tags():
    # Given an annotations-scoped declaration of a full table shape
    table = DeltaTable(
        "dev",
        "silver",
        "clicks",
        columns=[Column("id", Integer(), comment="the id", tags={"pii": "low"})],
        comment="Click events, owned by the ingest pipeline.",
        tags={"owner": "governance"},
        scope="annotations",
    )

    # Then the lowered scope is exactly the four annotation aspects
    assert table._desired_table.managed_aspects == ANNOTATION_ASPECTS


def test_annotations_scope_carries_a_mirrored_primary_key_without_managing_it():
    # Given an annotations-scoped declaration mirroring the pipeline's key —
    # the contract that keeps the key aspect quiet on a streaming table
    table = DeltaTable(
        "dev",
        "silver",
        "clicks",
        columns=[Column("id", Integer(), nullable=False)],
        primary_key=["id"],
        scope="annotations",
    )

    # Then the key is declared and lowered, but is not a managed aspect: it is
    # mirrored so no difference is emitted, never applied
    assert table._desired_table.primary_key is not None
    assert TableAspect.PRIMARY_KEY not in table._desired_table.managed_aspects
```

Match the file's own accessor style — if the existing scope tests reach the lowered table by another route than `table._desired_table`, use theirs. Add `TableAspect` to the domain import if it is not already there.

- [ ] **Step 2: Run the tests to verify they fail**

```bash
uv run pytest tests/api/test_delta_table.py -k scope -v --no-cov
```

Expected: FAIL — `ImportError: cannot import name 'ANNOTATION_ASPECTS'` is already fixed by Task 2, so these fail only if the lowering is wrong. If they pass immediately, that is correct and expected: Task 2 wired the scope through `managed_aspects_for`, and this task documents it. Proceed to Step 3 either way.

- [ ] **Step 3: Update the class docstring**

At line 596–597:

```python
    ``scope`` selects how much of the table the declaration manages: the whole
    table (default), catalog metadata only, annotations only, or tags only.
```

- [ ] **Step 4: Update the `scope` argument documentation**

Replace lines 639–651:

```python
            scope: What this declaration manages. ``"full"`` (the default)
                manages the whole table. ``"metadata"`` restricts the sync to
                catalog metadata: comments, tags, and primary/foreign key
                constraints. ``"annotations"`` restricts it further to the
                table comment, column comments, table tags, and column tags —
                for a table whose structure and keys belong to someone else.
                ``"tags"`` restricts it to table and column tags alone. The
                scopes nest: tags ⊂ annotations ⊂ metadata ⊂ full.
                Streaming tables are supported under ``"annotations"`` and
                ``"tags"``: their definition — schema, properties, and keys —
                belongs to the owning pipeline, so any wider scope against one
                fails validation. A restricted scope still declares the full
                table shape; aspects outside the scope are never changed, and
                drift on them fails validation. A key the pipeline declared
                must therefore be mirrored in ``primary_key`` — it is never
                applied, and mirroring it is what keeps it from reading as
                drift. Properties are the exception: a declaration that does
                not manage properties never compares them at all.
```

- [ ] **Step 5: Run the tests to verify they pass**

```bash
uv run pytest tests/api/test_delta_table.py -v --no-cov && uv run ruff check src/delta_engine/api/delta_table.py
```

Expected: PASS.

- [ ] **Step 6: Commit**

```bash
git add src/delta_engine/api/delta_table.py tests/api/test_delta_table.py
git commit -m "docs: document the annotations scope on DeltaTable"
```

---

## Task 5: End-to-end dry run over an observed streaming table

**Files:**
- Modify: `tests/adapters/databricks/warehouse/test_streaming_table_dry_run.py` (docstring, the canned document, line 138's rule name; append one test)

**Interfaces:**
- Consumes: `scope="annotations"` from Task 2, `StreamingTableAnnotationsOnly` from Task 3.
- Produces: nothing importable. This is the file where "planned SQL carries the streaming-table dialect" is asserted end to end, so the two comment statements are pinned here rather than in a new file.

- [ ] **Step 1: Write the failing test**

Update the module docstring:

```python
"""
End-to-end dry run against an observed streaming table.

Wires the real warehouse reader, engine, validation, and SQL compiler over a
canned DESCRIBE AS JSON document: an annotations-scope declaration against a
streaming table plans ALTER STREAMING TABLE and COMMENT ON statements, and any
wider scope fails validation before SQL is planned.
"""
```

Give the canned document a comment to drift from, so the planned statement is a change rather than a no-op. Replace the `"comment"` line and the column entry in `_STREAMING_DOC`:

```python
        "columns": [
            {"name": "id", "type": {"name": "int"}, "nullable": True, "comment": "stale id"}
        ],
        "comment": "stale table comment",
```

Rename the assertion at line 138:

```python
    assert "StreamingTableAnnotationsOnly" in rule_names
```

The existing `test_tags_scope_dry_run_plans_streaming_table_ddl` now sees comment drift under a tags scope, which is unmanaged — so it would start failing validation. Keep it testing what it names by giving it a declaration that mirrors the observed comments:

```python
def test_tags_scope_dry_run_plans_streaming_table_ddl():
    # Given a tags-scope declaration over a described streaming table with
    # one tag to set, one to unset, and one column tag to set. The comments
    # mirror the live table: a restricted scope still declares the full shape,
    # and an unmirrored comment would read as unmanaged drift.
    declaration = DeltaTable(
        "cat",
        "sch",
        "clicks",
        columns=(Column("id", Integer(), comment="stale id", tags={"pii": "low"}),),
        comment="stale table comment",
        tags={"owner": "governance"},
        scope="tags",
    )
```

The rest of that test is unchanged. Likewise `test_full_scope_dry_run_fails_validation_against_a_streaming_table` gains the mirrored comments so it stays a pure kind refusal at zero drift:

```python
    declaration = DeltaTable(
        "cat",
        "sch",
        "clicks",
        columns=(Column("id", Integer(), comment="stale id"),),
        comment="stale table comment",
        tags={"stale": "remove-me"},
    )
```

Now append the new test:

```python
def test_annotations_scope_dry_run_plans_streaming_table_comment_ddl():
    # Given an annotations-scope declaration whose comments differ from the
    # live streaming table's, plus the tag work the tags scope already covered
    declaration = DeltaTable(
        "cat",
        "sch",
        "clicks",
        columns=(Column("id", Integer(), comment="the id", tags={"pii": "low"}),),
        comment="Click events, owned by the ingest pipeline.",
        tags={"owner": "governance"},
        scope="annotations",
    )

    # When dry-running a sync
    report = _engine().sync(declaration, dry_run=True)

    # Then both comment statements compile to the documented forms: the column
    # comment through the ALTER STREAMING TABLE prefix, the table comment
    # through kind-independent COMMENT ON
    [table_report] = list(report)
    assert table_report.status is TableRunStatus.SUCCESS
    assert set(table_report.planned_sql_statements) == {
        "ALTER STREAMING TABLE `cat`.`sch`.`clicks` SET TAGS ('owner'='governance')",
        "ALTER STREAMING TABLE `cat`.`sch`.`clicks` UNSET TAGS ('stale')",
        "ALTER STREAMING TABLE `cat`.`sch`.`clicks` ALTER COLUMN `id` SET TAGS ('pii'='low')",
        "ALTER STREAMING TABLE `cat`.`sch`.`clicks` ALTER COLUMN `id` COMMENT 'the id'",
        "COMMENT ON TABLE `cat`.`sch`.`clicks` IS "
        "'Click events, owned by the ingest pipeline.'",
    }


def test_annotations_scope_dry_run_clears_a_column_comment_with_an_empty_literal():
    # Given an annotations-scope declaration that clears the column comment
    declaration = DeltaTable(
        "cat",
        "sch",
        "clicks",
        columns=(Column("id", Integer()),),
        comment="stale table comment",
        tags={"stale": "remove-me"},
        scope="annotations",
    )

    # When dry-running a sync
    report = _engine().sync(declaration, dry_run=True)

    # Then the clear compiles to COMMENT '' rather than UNSET COMMENT: SQL
    # warehouses reject the latter, and '' round-trips as the empty comment
    # the reader observes, so the resync converges
    [table_report] = list(report)
    assert table_report.status is TableRunStatus.SUCCESS
    assert (
        "ALTER STREAMING TABLE `cat`.`sch`.`clicks` ALTER COLUMN `id` COMMENT ''"
        in table_report.planned_sql_statements
    )
```

- [ ] **Step 2: Run the tests to verify they fail**

```bash
uv run pytest tests/adapters/databricks/warehouse/test_streaming_table_dry_run.py -v --no-cov
```

Expected: the two new tests FAIL before Tasks 2–3 land; after them they should pass. If the canned `DESCRIBE AS JSON` document does not carry column comments under the key `"comment"`, check `adapters/databricks/warehouse/reader.py` for the key the parser reads and use that — the fixture must describe a real document shape, not an invented one.

- [ ] **Step 3: No implementation needed — verify**

This task is pure test. If either new test fails, the cause is in Task 2 or Task 3, not here. The design's decision 6 is exactly the claim under test: the compiler already emits both statements correctly for either relation kind.

```bash
uv run pytest tests/adapters/databricks/warehouse/test_streaming_table_dry_run.py -v --no-cov
```

Expected: PASS, 4 tests.

- [ ] **Step 4: Full suite**

```bash
uv run pytest -q && uv run ruff format . && uv run ruff check . && uv run mypy src && uv run lint-imports
```

Expected: PASS, coverage at or above the 70% floor.

- [ ] **Step 5: Commit**

```bash
git add tests/adapters/databricks/warehouse/test_streaming_table_dry_run.py
git commit -m "test: pin streaming-table comment SQL end to end"
```

---

## Task 6: The engine round-trip, live

The module's second existing test already owns "the engine end to end against a real streaming table" — the read, the sync, the convergence resync, and the wider-scope refusal, on one provisioned table. It widens to annotations rather than gaining a neighbour. **No new test, no new provision.**

What it pins that no unit test can: **convergence**. If the platform accepted the comment statements but the reader could not observe their effect on a streaming table, the resync would re-emit them and `has_changes` would be `True`. That single assertion covers reader observability more honestly than reaching into reader internals would.

**Files:**
- Modify: `tests/live/test_sql_warehouse_live_streaming_tables.py` (replace `test_tags_are_the_only_aspect_the_engine_manages_on_a_streaming_table`, lines 143–200)

**Interfaces:**
- Consumes: Task 1's `_table_comment` and `_column_comments` and its now-keyed `_create_streaming_table`; `scope="annotations"` from Task 2; `StreamingTableAnnotationsOnly` from Task 3.

- [ ] **Step 1: Widen the end-to-end test to annotations**

Replace the whole test at lines 143–200:

```python
def test_a_streaming_table_syncs_annotations_and_refuses_a_wider_scope(
    live_connection, live_tables
):
    """A streaming table syncs comments and tags to convergence; a wider scope is refused."""
    # Supersedes the old supported-relations pin that read streaming tables
    # as failed: they are now engine state, discovered — never declared. The
    # read, the round-trip, the convergence resync, and the wider-scope
    # refusal share one provisioned table (see the module docstring on the
    # pipeline quota).
    table_name = _create_streaming_table(live_connection, live_tables)
    target = qualified_table(table_name)
    execute_sql(live_connection, f"ALTER STREAMING TABLE {target} SET TAGS ('old'='remove-me')")

    reader = WarehouseReader(WarehouseSqlRunner(live_connection))
    state = reader.fetch_state(QualifiedName(live_catalog(), live_schema(), table_name))
    assert isinstance(state, TablePresent), state
    assert state.table.kind is TableKind.STREAMING_TABLE

    engine = build_sql_engine(live_connection)
    declaration = DeltaTable(
        live_catalog(),
        live_schema(),
        table_name,
        columns=(
            Column("id", Integer(), nullable=False, comment="the id", tags={"pii": "low"}),
        ),
        primary_key=["id"],  # mirrors the pipeline's key; never applied
        comment="Click events, owned by the ingest pipeline.",
        tags={"owner": "governance"},
        scope="annotations",
    )
    engine.sync(declaration)

    assert _table_comment(live_connection, table_name) == (
        "Click events, owned by the ingest pipeline."
    )
    assert _column_comments(live_connection, table_name) == {"id": "the id"}
    assert _table_tags(live_connection, table_name) == {"owner": "governance"}
    assert _column_tags(live_connection, table_name) == {("id", "pii"): "low"}

    # The reader must round-trip everything the executor just wrote through the
    # ALTER STREAMING TABLE and COMMENT ON statements: a resync finds nothing
    # left to do. This is also what verifies the mirroring contract — had the
    # platform not reported the pipeline's key, mirroring it would have emitted
    # SetPrimaryKey and failed UnmanagedAspectDrift instead of converging.
    assert engine.sync(declaration).has_changes is False

    # Anything wider than annotations is refused before planning — even when
    # the declaration mirrors the observed state exactly, so the refusal is
    # about the table's kind, not about drift.
    full_scope = DeltaTable(
        live_catalog(),
        live_schema(),
        table_name,
        columns=(
            Column("id", Integer(), nullable=False, comment="the id", tags={"pii": "low"}),
        ),
        primary_key=["id"],
        comment="Click events, owned by the ingest pipeline.",
        tags={"owner": "governance"},
    )
    with pytest.raises(SyncFailedError) as error:
        engine.sync(full_scope)

    [table_report] = error.value.report.table_reports
    assert table_report.status is TableRunStatus.PLANNING_FAILED
    assert "StreamingTableAnnotationsOnly" in {
        failure.rule_name
        for failure in table_report.failures
        if isinstance(failure, ValidationFailure)
    }
    assert table_report.planned_sql_statements == ()
    assert _table_tags(live_connection, table_name) == {"owner": "governance"}
    assert _column_tags(live_connection, table_name) == {("id", "pii"): "low"}
```

Two details that will bite if missed, both consequences of Task 1 making the fixture keyed:

- **`nullable=False` on `id` in both declarations.** The provisioned column is `INT NOT NULL`. A declaration defaulting to nullable would drift on column structure — an aspect `"annotations"` does not manage — and fail `UnmanagedAspectDrift` instead of converging.
- **`primary_key=["id"]` in both declarations.** The `full_scope` one needs it too, or its "mirrors the observed state exactly" comment becomes false and the refusal would be indistinguishable from ordinary key drift.

- [ ] **Step 2: Collection check**

```bash
uv run ruff format . && uv run ruff check . && uv run pytest --collect-only tests/live -q
```

Expected: clean, all tests collected.

- [ ] **Step 3: Commit**

```bash
git add tests/live/test_sql_warehouse_live_streaming_tables.py
git commit -m "test: pin the annotations scope against a live streaming table"
```

- [ ] **Step 4: Hand back for the second live run**

```bash
uv run pytest tests/live -m databricks_e2e --no-cov -k "streaming_table"
```

Report the result before starting Task 7. Unlike Task 1 this is not a hard gate — the docs can be written while the run happens — but the plan is not complete until it is green.

---

## Task 7: Documentation

**Files:**
- Modify: `docs/how-to-deploy-metadata-only.md:69-93`, `docs/how-to-configure-table.md:346-390`, `docs/reference-safe-change-rules.md:78` and `:87-91`, `docs/reference-limitations.md:15`, `:83-84`, `:98`, `docs/explanation-safety-model.md:60-79`, `docs/todo/policy-visibility-review.md:77-81`

**Interfaces:**
- Consumes: the exact rule name `StreamingTableAnnotationsOnly` and the scope name `"annotations"`.

- [ ] **Step 1: `how-to-deploy-metadata-only.md` — retitle and correct**

Replace the whole "Tag a streaming table" section (lines 69–93):

````markdown
## Annotate a streaming table

`scope="annotations"` extends to streaming tables. A streaming table's
definition — schema, properties, and keys — is owned by its pipeline and
belongs to `CREATE OR REFRESH`; comments and Unity Catalog tags sit outside
it, and are exactly what `ALTER STREAMING TABLE` and `COMMENT ON` reach. The
engine discovers the relation kind when it reads the table (nothing is
declared), compiles column comments and tag changes as `ALTER STREAMING
TABLE`, the table comment as `COMMENT ON TABLE`, and rejects any wider scope:
a `"full"` or `"metadata"` declaration against a streaming table fails
validation (`StreamingTableAnnotationsOnly`) even when nothing has drifted.

```python
from delta_engine.schema import Column, DeltaTable, Integer

clicks = DeltaTable(
    catalog="dev",
    schema="silver",
    name="clicks",
    columns=[Column("id", Integer(), nullable=False, tags={"pii": "low"})],
    primary_key=["id"],        # mirrors the pipeline's key; never applied
    tags={"owner": "governance"},
    comment="Click events, owned by the ingest pipeline.",
    scope="annotations",
)
```

### Mirror the pipeline's keys

A restricted scope still declares the full table shape, and keys are no
exception. If the pipeline's `CREATE STREAMING TABLE` declares a primary key,
the declaration must mirror it — as `primary_key=["id"]` above. The engine
never applies it: the declared and observed keys match, so no difference is
emitted at all. Omitting it is not neutral, because `primary_key=None` is a
positive assertion of absence everywhere else in the engine; the sync would
fail `UnmanagedAspectDrift` on a key this scope cannot manage.

If the pipeline later changes the key, the mirror stops matching and the next
sync fails the same way. That is late but loud, and it is the only honest
answer available: the engine cannot reconcile a key the pipeline owns, and
must not pretend otherwise. Update the declaration to the new reality.

A comment declared in the pipeline's own defining SQL is a different matter:
`CREATE OR REFRESH` is fully declarative, so a refresh can revert a comment
this engine set, and the next sync will set it again. The engine cannot read
the pipeline's SQL and so cannot warn about it. Do not manage a comment from
both places.

Materialized views remain unsupported: a name that resolves to one still
fails its read. See [limitations](reference-limitations.md).
````

- [ ] **Step 2: `how-to-configure-table.md` — the new scope alongside tags-only**

At line 346, retitle `### Manage tags only` and replace the closing paragraph about streaming tables (lines 382–386) — the tags-only section keeps its example and its own semantics, and gains a sibling. Replace the paragraph beginning "Streaming tables are supported here and only here" with:

````markdown
### Manage comments and tags only

Use `scope="annotations"` when the table's structure belongs to someone else
but its catalog documentation should still be governed here. It manages the
table comment, column comments, table tags, and column tags — a superset of
`"tags"` and a subset of `"metadata"`, which adds key constraints on top.

```python
from delta_engine.schema import Column, DeltaTable, String

events = DeltaTable(
    catalog="dev",
    schema="silver",
    name="streaming_events",
    columns=[
        Column("id", String(), comment="Event identifier"),
        Column("email", String(), comment="Contact address", tags={"pii": "true"}),
    ],
    comment="Raw events, owned by the ingest pipeline.",
    tags={"domain": "events"},
    scope="annotations",
)
```

Streaming tables are supported under `"annotations"` and `"tags"`, and no
wider scope: the engine discovers the relation kind at read time, compiles
column comments and tags with the `ALTER STREAMING TABLE` dialect and the
table comment with `COMMENT ON TABLE`, and rejects a scope claiming schema,
properties, or keys. A key the owning pipeline declared must be mirrored in
the declaration. See
[annotate a streaming table](how-to-deploy-metadata-only.md#annotate-a-streaming-table).
````

- [ ] **Step 3: `reference-safe-change-rules.md` — the law's row and the paragraph**

Replace the table row at line 78:

```markdown
| `StreamingTableAnnotationsOnly`  | The observed table is a streaming table and the declaration manages more than comments and tags | Declare it with `scope="annotations"` or `scope="tags"`; the table's schema, properties, and keys belong to its owning pipeline |
```

Replace the paragraph at lines 87–91:

```markdown
`StreamingTableAnnotationsOnly` judges the declaration against the observed
relation kind, not against drift, so it fires even when the streaming table is
currently in sync. The line it draws is the defining SQL: schema, properties,
and keys belong to `CREATE OR REFRESH` and stay unmanageable, while comments
and Unity Catalog tags are alterable from outside the pipeline and are
manageable. A key the pipeline declared must be mirrored in the declaration to
keep it from reading as unmanaged drift.
```

- [ ] **Step 4: `reference-limitations.md` — three edits**

Line 15, the backend row — replace `plus Delta streaming tables for tag-only management` with:

```
plus Delta streaming tables for comment and tag management
```

Lines 83–84, the two scope rows:

```markdown
| Tag-only scope            |       ✓       | `scope="tags"` restricts a sync to table and column tags ([tags](how-to-configure-table.md#manage-tags-only))                                                                                                             |
| Annotations scope         |       ✓       | `scope="annotations"` restricts a sync to comments and tags ([annotations](how-to-configure-table.md#manage-comments-and-tags-only))                                                                                      |
| Streaming tables          | Comments and tags | Discovered at read time; only `scope="annotations"` and `scope="tags"` declarations may target one — schema, properties, and keys belong to the owning pipeline and must be mirrored, not managed ([guide](how-to-deploy-metadata-only.md#annotate-a-streaming-table)) |
```

Line 98, the views row — replace `(streaming tables, by contrast, are read for tag-only management)` with:

```
(streaming tables, by contrast, are read for comment and tag management)
```

Add the comment-revert caveat to the "Outside the model" section (after line 90):

```markdown
A comment this engine sets on a streaming table can be reverted by the owning
pipeline. `CREATE OR REFRESH` is fully declarative, so a refresh re-applies
the pipeline's own comment and deletes metadata the refresh does not specify.
The engine cannot read the pipeline's defining SQL, so it cannot warn: a
contested comment re-drifts on every pipeline update and each sync sets it
again. Manage a given comment from one place only.
```

- [ ] **Step 5: `explanation-safety-model.md` — four scopes**

Replace line 60 and the table at lines 61–66:

```markdown
There are four public scopes, selected by `DeltaTable`'s `scope` parameter.
They nest — `tags ⊂ annotations ⊂ metadata ⊂ full` — so narrowing a scope only
ever drops authority:

| Scope                  | Manages                                                                                 | Use for                                                                                                           |
| ---------------------- | --------------------------------------------------------------------------------------- | ----------------------------------------------------------------------------------------------------------------- |
| `"full"` (the default) | Everything: columns, comments, properties, tags, partitioning, primary and foreign keys | Tables this declaration owns end to end                                                                           |
| `"metadata"`           | Comments, tags, and key constraints only                                                | Rolling out governance metadata with a hard guarantee that no schema change can slip in                           |
| `"annotations"`        | Table and column comments, table and column tags                                        | Documenting and tagging a table whose structure and keys belong elsewhere — including streaming tables            |
| `"tags"`               | Table and column tags only                                                              | Tag governance alone, where even a comment would be too much authority to claim                                   |
```

Replace the streaming-tables paragraph (lines 72–79):

```markdown
Streaming tables make the scope boundary literal. Their definition is owned by
a pipeline, and the line the platform draws is the defining SQL: schema,
properties, and keys belong to `CREATE OR REFRESH`, while comments and tags are
alterable from outside it. So the engine reads one for annotation governance:
the relation kind is discovered at read time, and a declaration that manages
anything beyond comments and tags fails validation
(`StreamingTableAnnotationsOnly`) before any SQL runs — even with zero drift.
A declaration claiming authority the engine must never exercise is wrong now,
not when drift eventually materialises, which is why the capability has its own
scope name rather than a kind-dependent reading of `"metadata"`. See
[safe-change rules](reference-safe-change-rules.md) for the invariant and
[annotate a streaming table](how-to-deploy-metadata-only.md#annotate-a-streaming-table)
for the workflow.
```

Also update the "See ... for tag-only declarations" sentence beneath the table to point at both restricted scopes.

- [ ] **Step 6: `docs/todo/policy-visibility-review.md` — the stale claim**

Replace lines 77–81:

```markdown
`application/scopes.py` owns the public scope names, their aspect sets, and
the name-to-aspects translation. The four sets are composed as a lattice
(`tags ⊂ annotations ⊂ metadata ⊂ full`) rather than written as parallel
literals. `DeltaTable` resolves its `scope` at the API boundary, while
`StreamingTableAnnotationsOnly` reuses `ANNOTATION_ASPECTS`; the public
`"annotations"` definition and the streaming-table allowance cannot diverge.
```

- [ ] **Step 7: Verify no stale references survive anywhere**

```bash
rg -n 'StreamingTableTagsOnly' src/ tests/ docs/ \
  -g '!docs/CHANGELOG.md' \
  -g '!docs/todo/2026-07-16-streaming-table-tags-*' \
  -g '!docs/todo/todo.md' \
  -g '!docs/todo/2026-07-30-streaming-table-annotations-*'
```

Expected: no matches. The excluded paths are the archived design and plan, `todo.md`'s dated entries, and this plan — all deliberately left alone as records of what was decided when.

Then check the anchors the docs now cross-reference actually exist:

```bash
rg -n '^#+ ' docs/how-to-deploy-metadata-only.md docs/how-to-configure-table.md
```

Expected: `## Annotate a streaming table`, `### Mirror the pipeline's keys`, and `### Manage comments and tags only` are present, matching every `#annotate-a-streaming-table` and `#manage-comments-and-tags-only` link written above.

- [ ] **Step 8: Build the docs**

```bash
uv sync --locked --group docs && uv run sphinx-build -W -b html docs docs/_build/html
```

Expected: clean build, no warnings. `-W` makes warnings fatal and the `docs` dependency group is not in the default sync — both taken from `.github/workflows/docs.yaml:33-36`, so this is exactly what CI runs.

- [ ] **Step 9: Commit**

```bash
git add docs/
git commit -m "docs: streaming tables are annotation territory"
```

---

## Task 8: Release footer and final verification

**Files:**
- No source changes. This task produces the breaking-change commit footer and runs the full gate.

- [ ] **Step 1: Run everything**

```bash
uv run ruff format --check . && uv run ruff check . && uv run mypy src && uv run lint-imports && uv run pytest
```

Expected: all green, coverage at or above 70%.

- [ ] **Step 2: Confirm the report schema is untouched**

```bash
git diff main...HEAD -- src/delta_engine/application/report.py
```

Expected: empty. `schema_version` stays at 2 (confirmed 2026-07-31): the key and its type do not change, only one of the values `rule_name` can take, following the `DiffOperation` precedent.

- [ ] **Step 3: Record the breaking change**

`CHANGELOG.md` is generated from commit footers by commitizen and edited by nobody. The rename must ride a `BREAKING CHANGE:` footer so it reaches the v0.7.0 notes. Amend the Task 3 commit, or add an empty commit if it has already been pushed:

```bash
git commit --allow-empty -m "refactor!: rename StreamingTableTagsOnly to StreamingTableAnnotationsOnly" -m "BREAKING CHANGE: ValidationFailure.rule_name now reports 'StreamingTableAnnotationsOnly' where it previously reported 'StreamingTableTagsOnly'. Consumers matching on that value in a run report's to_dict() projection must update. schema_version stays at 2: the key and its type are unchanged." 
```

v0.7.0 is unreleased and already carries `BREAKING CHANGE:` footers, so the window is open.

- [ ] **Step 4: Confirm the live suite is green**

Both live runs (Task 1, Task 6) must have been reported green. If Task 6's run has not happened yet, it happens now:

```bash
uv run pytest tests/live -m databricks_e2e --no-cov -k "streaming_table"
```

- [ ] **Step 5: Mark the design as implemented**

In `docs/todo/2026-07-30-streaming-table-annotations-design.md`, change line 4:

```markdown
Status: implemented (2026-07-31)
```

and resolve the open decision in the Blast radius section by replacing the final paragraph's "a decision to confirm rather than assumed" sentence with the settled answer:

```markdown
`schema_version` stays at 2 (confirmed 2026-07-31). The key and its type do not
change, only one of the values it can take, following the `DiffOperation`
precedent. The rename rides a `BREAKING CHANGE:` footer instead, which is where
a consumer matching on a rule name will see it.
```

```bash
git add docs/todo/2026-07-30-streaming-table-annotations-design.md
git commit -m "docs: mark the annotations design implemented"
```

- [ ] **Step 6: Open the PR**

```bash
git push -u origin feat/streaming-table-annotations
gh pr create --title "feat: annotations scope on streaming tables" --body "Implements docs/todo/2026-07-30-streaming-table-annotations-design.md."
```

---

## Self-Review

**Spec coverage** — every section of the design maps to a task:

| Design section | Task |
| --- | --- |
| Decision 1–2 (goal, `"annotations"` vocabulary) | 2 |
| Decision 3 (authority stays honest; metadata/full still refused) | 3 (unit: both scopes), 6 (live: full scope) |
| Decision 4 (keys excluded, mirrored not ignored) | 3 (unit, through the differ), 4, 6 (live, via convergence), 7 |
| Decision 5 (check stays streaming-table-specific, renamed) | 3 |
| Decision 6 (no adapter or domain changes) | 5 — asserted, not assumed |
| Decision 7 (live verification) | 1, 6 |
| Design § Scopes | 2 |
| Design § Validation | 3 |
| Design § Adapters and domain — unchanged | 5 |
| Design § Keys on a streaming table | 3 (contract), 4 (docstring), 6 (live), 7 (guide) |
| Design § Error handling (no new channels) | no task — nothing to build |
| Design § Testing → Unit | 2, 3, 4 |
| Design § Testing → Compiler | 5 |
| Design § Live pins 1–3 (comment statements) | 1 |
| Design § Live pin 4 (annotations round-trip) | 6 |
| Design § Live pin 5 (metadata refused) | **3, as a unit test.** The refusal is `managed_aspects <= ANNOTATION_ASPECTS` — nothing platform-specific, and the live suite already pins a wider-scope refusal. |
| Design § Live pin 6 (key reported; mirroring) | **3, as unit tests, plus 6 implicitly.** The mirroring contract is `_diff_primary_key` + `UnmanagedAspectDrift`. The platform half is folded into Task 1's fixture and verified by Task 6's convergence, rather than buying its own provision. |
| Design § Documentation (7 items) | 7, and Task 8 for the changelog footer |
| Design § Blast radius — `schema_version` decision | settled 2026-07-31; Task 8 records it |
| Design § Risks — comment-revert limitation | 7 (`reference-limitations.md`) |

Blast radius reconciliation: the design counts 15 files, 9 code. This plan touches those 15 plus two the design did not count — `docs/todo/2026-07-30-streaming-table-annotations-design.md` (its own status line, Task 8) and this plan file. `docs/explanation-architecture.md:517-520` is checked and needs nothing, as the design records.

**Ordering note:** the design's risk section requires the gate written after the facts are pinned. Task 1 pins the facts that need no engine code and stops for a live run; the round-trip necessarily follows the code, in Task 6. That split is what makes "pins before gate" achievable rather than circular.

**On the live/unit split.** The design lists six live pins. Three of them — the metadata refusal, the mirroring contract, and the emitted SQL — assert engine logic, not platform behaviour: they would pass or fail identically against a canned document, so paying a pipeline provision for them buys nothing but runtime. They are unit tests here, and the design's Testing § Unit section already asks for most of them. What survives as live is the short list of things only Databricks can answer: three statement forms are accepted, and their effect is observable enough that a resync converges.

The result is **two live tests over two provisions — unchanged from today** — where a literal reading of the design would have grown it to six tests over six provisions, against a workspace quota of one active pipeline. The design's own risk register lists that growth as a concern and names sharing a provisioned table as the mitigation; this is that mitigation, taken further than the design took it.

**Known-fragile points, called out rather than hidden:**
- Task 5 Step 2 depends on the `DESCRIBE AS JSON` column-comment key, which this plan inferred rather than read from a real document. The step names `adapters/databricks/warehouse/reader.py` as the place to confirm it.
- Task 1 makes the shared streaming-table fixture keyed, so **both** live tests now depend on key-constraint support. That is deliberate — it verifies the guide's mirroring advice for free — but it is a coupling. Task 1 Step 3 states the fallback (split the helper into keyed and keyless variants) and the reason the key goes in at Task 1: its live run discovers an unsupporting workspace before any code is written.
- Task 6's declarations must carry `nullable=False` and `primary_key=["id"]` for the same reason; both are called out inline, because either omission fails the test for a reason unrelated to what it names.
- The `CREATE STREAMING TABLE ... CONSTRAINT ... PRIMARY KEY` syntax was verified against the Databricks reference on 2026-07-31; `NOT NULL` on the key column is required by Unity Catalog and by the engine's own declaration validation.
