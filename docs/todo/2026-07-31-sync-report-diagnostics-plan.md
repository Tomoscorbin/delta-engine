# Sync Report Diagnostics Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Make a sync report say *what* drifted, *what* was rejected, and *what* actually happened to the catalog, instead of naming a rule and discarding the evidence.

**Architecture:** Three independent pull requests over the application layer. PR1 fixes message text — no public type changes. PR2 carries the computed `TableDiff` onto `TableRunReport` so a rejected table can show its drift, and teaches `UnmanagedAspectDrift` to name the differences it collapsed into a bare aspect label. PR3 makes real-run output honest about what executed. Each PR is independently mergeable and revertable.

They were written as a stack. They are not one any more: PR2 landed off `main` before PR1 existed as a branch, and everything remaining branches off `main` too.

**Tech Stack:** Python 3.12+, `uv`, pytest, mypy, ruff, import-linter. No new runtime dependencies.

---

## Status — 2026-07-31

**PR2 is done and merged. PR1 is half done. PR3 has not started.**

| | Task | State |
| --- | --- | --- |
| **PR1** | 1. One function that names a data type | ✅ landed in #313, **differently** — see below |
| | 2. The CREATE diff keeps its type detail | ✅ satisfied by #313 |
| | 3. Validation messages stop leaking dataclass reprs | ✅ satisfied by #313 |
| | 4. Statement numbers read from 1 | ❌ **outstanding** |
| | 5. `NonNullableColumnAdd` says what to do about it | ❌ **outstanding** |
| | 6. The grid names what a validation failure is about | ❌ **outstanding** |
| | 7. The run summary counts in English | ❌ **outstanding** |
| **PR2** | 8–12 | ✅ all merged in #316 |
| **PR3** | 13–16 | ❌ not started |

### What PR1 actually did, and what it changed underneath this plan

#313 (`feat: improve rendering`) was written independently of this plan. It covered task 1's territory but chose a different design, and reshaped `diff_entries.py` while it was there. **Every later task in this document was written against the pre-#313 code.** The substitutions:

- **No `application/type_display.py`, and no `describe_type`.** A data type names itself: `DataType.__str__` returns `type(self).__name__`, with `Decimal`, `Struct`, `Array` and `Map` overriding it. Read `str(data_type)` wherever this plan says `describe_type(data_type)`. The SQL spelling stays in the adapter (`adapters/databricks/sql/types.py::render_data_type`) — two audiences, two functions.
- **`DiffEntry` gained a `subject`.** It is now `DiffEntry(category, operation, subject, detail)` where `detail` is a `tuple[str, ...]` of phrases, not the old flat `cells` tuple. `entry.cells` survives as `(subject, *detail)` for renderers. Any `DiffEntry(...)` literal in this document is written in the old three-argument form and needs the subject split out.
- **`CATEGORY_NOUN` is gone**; `DiffCategory` carries `.plural` and `.counted(n)` itself.
- **Layout moved into the renderer.** `_render_entry_groups` owns grouping and column alignment; interpretation returns unaligned phrases. This is the organising principle to preserve: *facts on the object that owns the data, phrasing on the object being described, layout in the renderer.*
- **`report.py` gained derived facts** — `StatementProgress`, `RunCounts`, `SyncReport.counts`, `SyncReport.duration_seconds`, `TableRunReport.statement_progress`, `TableRunReport.creates_table`. PR3's tasks 14–16 hand-roll several of these; use the properties instead.

### What PR2 added, and its two deviations

Merged in #316, off `main` (not stacked — PR1 never existed as a branch).

- `TableRunReport.diff: TableDiff | None = None`, with the invariant that a failed read produces no diff. The engine's `to_report()` passes it through.
- `unresolvable_entries(unresolvable) -> tuple[DiffEntry, ...]` in `diff_entries.py`, singledispatch over all four `Unresolvable` variants, with `test_every_unresolvable_type_has_registered_diff_entries` guarding a fifth.
- **`drift_entries(drift) -> tuple[DiffEntry, ...]`**, beside `plan_entries`. This replaced the plan's `_rejected_entries` in `rendering.py` *and* the duplicated comprehension in `_rejected_change_records` — "every entry this diff lowers to" is a question about meaning, so it lives in the meaning layer and both consumers call it.
- `render_diff_block` distinguishes `plan is None` (rejected — show the drift under a `(REJECTED — no SQL planned)` header) from an empty plan (a validated no-op).
- `to_dict()` gained `rejected_changes`; `schema_version` stays `2`.
- `UnmanagedAspectDrift` names each difference and its remedy no longer says "sync the table fully".

**Deviation 1 — the drift lines are `ValidationFailure.details`, not newlines inside `message`.** The plan had the rule build its own indented block. That composes wrongly: `render_failures_section` indents the first element of `format_lines()` by four spaces and nests later elements by eight, so an embedded remedy line rendered at *less* indentation than the failure owning it, and the JSON `message` gained raw `\n    ` runs no other failure has. `ValidationFailure` now carries `details: tuple[str, ...] = ()` and `format_lines()` returns `(headline, *details)`. Layout stays in the renderer.

**Deviation 2 — no `ValidationFailure.subject`.** Task 12's tests used it, but it comes from task 6, which has not landed. Adding the field and populating it for one rule out of fourteen would be a half-done version of task 6, so those tests assert on `message` and `details` instead. **When task 6 is done, `subject` slots in beside `details` and task 12's tests can be tightened to use it.**

### Picking this up tomorrow

Do **PR1's remaining tasks (4, 5, 6, 7) first** — PR3's task 16 depends on task 7's `_count_phrase`, and task 6's `subject` is the thing that makes a forty-table grid readable. Branch each off `main`; nothing is stacked any more.

Before starting PR3, read "PR3 — what changed underneath it" at the head of that section.

---

## Global Constraints

- Work in the existing worktree at `.claude/worktrees/feat-sync-report-diagnostics`. Never commit to `main`.
- Conventional commit messages. No `Co-authored-by` trailers.
- Run `uv run pytest -q` after every task; coverage gate is 70% and the suite sits at 96.52% over 1149 tests as of #316.
- `sphinx-build` needs its dependency group: `uv run --group docs sphinx-build -W -b html docs docs/_build/html`. The bare `uv run sphinx-build` written throughout this document will not resolve.
- Layer order is enforced by import-linter: `adapters|api → application → domain`. Nothing in `application/` may import from `adapters/`, `api/`, or `cli/`. Intra-layer imports inside `application/` are legal.
- `SyncReport.to_dict()` / `TableRunReport.to_dict()` are a versioned public contract (`schema_version: 2`, `docs/reference-run-report.md`). **Adding** a field is backwards-compatible; renaming or removing one is breaking and bumps the version. This plan adds fields only — `schema_version` stays at `2`.
- `delta_engine.__all__` exports `Engine`, `SyncReport`, `TableRunReport`, `TableRunStatus`, `Failure`, `FailurePhase`, `ReadFailure`, `ValidationFailure`, `ExecutionFailure`, `ForeignKeyFailure`, `SyncFailedError`, `DuplicateTableDefinitionError`, `render_diff`, `render_report`. Anything else is internal and free to change.
- Docs live in `docs/` and are built with `sphinx-build -W` (warnings are errors), which renders hand-written Markdown only — a green docs build never verifies a Python docstring.
- The CLI is deliberately out of scope for this plan (see "Deliberately not doing" at the end).

---

## Background: what is wrong today

Verified by running the real engine against fake adapters on `main` (v0.7.0).

A metadata-scoped declaration against a catalog with one type change and two extra columns produces exactly this and nothing else:

```
DIFF
====

main.sales.orders
  (no changes — see failures)

Failures
--------
  main.sales.orders
    Validation failed: UnmanagedAspectDrift - Operation not allowed: column structure
    has drifted but is not managed by this definition. Sync the table fully or update
    the declaration to match the live schema.
```

The engine computed a complete `TableDiff` naming `customer_id`, `legacy_region` and `shipped_at`, then threw it away. Three separate defects combine to produce that output, and this plan fixes all three plus eight smaller ones.

**As of #316 that same scenario now reads:**

```
main.sales.orders  (REJECTED — no SQL planned)
  columns
    - legacy_region
    ~ customer_id    Integer → Long

Failures
--------
  main.sales.orders
    Validation failed: UnmanagedAspectDrift - Operation not allowed: column structure
    has drifted but is not managed by this definition. Update the declaration to match
    the live table, or widen its scope to manage this aspect.
        - legacy_region
        ~ customer_id Integer → Long
```

The headline defect is fixed. What remains is PR1's text quality (tasks 4–7) and PR3's real-run honesty (tasks 13–16) — this preview is a dry run, and on a real run the same block would still describe intent as though it were outcome.

## File Structure

**New files**

~~`src/delta_engine/application/type_display.py` / `tests/application/test_type_display.py`~~ — **superseded.** #313 put the answer on the type itself (`DataType.__str__`) rather than in a third module. The reasoning below is kept because it still explains *why* one spelling must be shared; it just landed on the object instead of beside it. No new files remain in this plan.

**Modified files**

| File | Change |
| --- | --- |
| `src/delta_engine/application/diff_entries.py` | ~~Delete the two private type-name helpers; consume `describe_type`.~~ Done in #313. ~~Add `unresolvable_entries` as a sibling of `action_entries`.~~ Done in #316, along with `drift_entries` beside `plan_entries`. |
| `src/delta_engine/application/validation.py` | ~~Consume `describe_type` in type-change messages~~ (#313); **give `NonNullableColumnAdd` a remedy (task 5)**; **pass `subject=` from rules that name one (task 6)**; ~~make `UnmanagedAspectDrift` enumerate the differences~~ (#316). |
| `src/delta_engine/application/failures.py` | **1-based statement display (task 4)**; **`ValidationFailure.subject` (task 6)**. `ValidationFailure.details` landed in #316. |
| `src/delta_engine/application/report.py` | ~~`TableRunReport.diff` field~~ (#316), **`unapplied_statements` property (task 13)**, ~~`rejected_changes` in `to_dict()`~~ (#316). |
| `src/delta_engine/application/engine.py` | ~~Pass the run's diff into `to_report()`.~~ Done in #316 — nothing further. |
| `src/delta_engine/application/rendering.py` | ~~Rejected-change blocks~~ (#316); **real-run outcome markers, honest STATEMENTS cell, honest footer, pluralisation (tasks 7, 13–16)**. |
| `docs/reference-safe-change-rules.md` | ~~Updated `UnmanagedAspectDrift` remedy wording.~~ Done in #316. |
| `docs/reference-run-report.md` | ~~Document the new `rejected_changes` field.~~ Done in #316. |
| `docs/how-to-handle-sync-failures.md` | **Point at `unapplied_statements` (task 13)**. |

**Why `type_display.py` was going to be its own module** — superseded by `DataType.__str__`, kept because the reasoning about sharing one spelling still applies. Three modules need a human-readable type name and none of them owns the concept: `diff_entries.py` shows types in a diff line, `validation.py` shows them in a rejection message, and a future `generate` command will need the same spelling again. Putting it in `diff_entries.py` would make "how do I name a type" a sub-fact of "how do I describe a diff", which it is not — the two happen to agree today only because there is one right answer. A third module both can depend on keeps that answer in one place, and matches the existing precedent of small single-purpose application modules (`scopes.py` is 62 lines).

This is **not** an argument that `validation.py` must avoid importing from `diff_entries.py` — Task 12 deliberately creates exactly that edge, because building a failure message that enumerates drift needs the same interpretation the diff view uses. See "On package structure" below.

**On package structure.** `application/` is flat (12 modules) and stays that way in this plan. The presentation-shaped code is really two layers with different dependency shapes, and only one of them is a leaf:

- `rendering.py` — text output. Nothing inside `application/` imports it; only `application/__init__.py` re-exports it and the CLI consumes it. A genuine leaf, ~280 lines after PR3.
- `diff_entries.py` — the shared *meaning* layer, on its own now that `type_display.py` is superseded. Imported by `rendering.py`, `report.py`, and since Task 12 by `validation.py` as well.

A folder named `rendering/` would therefore either hold one module, or hold a module that `validation.py` imports from — which would misdescribe the arrows. If a package is wanted later, the honest one groups the meaning layer, not the text layer, and it should be a separate pure-move commit **after PR3**, when the final module sizes are known. The trigger worth watching: `diff_entries.py` was 363 lines when this was written and is **531 after #316** — past the ~500 threshold named here, though it now holds two sources of entries rather than the third that was meant to trigger a move. Revisit after PR3 rather than mid-flight; if it moves, it goes to `application/interpretation/` and `rendering.py` stays put.

---

# PR 1 — `fix/sync-failure-message-quality` — ⚠️ HALF DONE

Six user-visible text defects. No public type changes, no report-contract changes. Ships on its own.

**Tasks 1, 2 and 3 are done** (#313, task 1 by a different design — see Status). **Tasks 4, 5, 6 and 7 are outstanding and are the next work to pick up.** They are independent of each other; each could ship alone, though one branch for the four is simpler.

Branch from `main`:

```bash
git checkout main && git pull
git checkout -b fix/sync-failure-message-quality
```

Every line number in the tasks below predates #313 and #316 — locate by symbol name, not by line.

---

### Task 1: One function that names a data type — ✅ DONE (#313, differently)

> **Superseded.** No `type_display.py` and no `describe_type`: a data type names itself via `DataType.__str__`. `Decimal`, `Struct`, `Array` and `Map` override it; everything else returns its class name. Read `str(data_type)` wherever the tasks below say `describe_type(data_type)`.
>
> #316 fixed two delimiter bugs in that work (`Struct` dropped its closing `>`, `Map` opened with `>`), which #315 landed separately. The lesson is recorded there: a *scalar* spelling is cosmetic and not worth a test, but a *composite* one carries a structural invariant — its delimiters must balance and nest — and that is worth exactly one test, which is why `test_action_entries_render_expected` has a row nesting all three composites.
>
> The original task is kept below for its reasoning about why one spelling must be shared.

Today there are two private helpers in `diff_entries.py` (`_type_name` at line 43, `_type_display` at line 48) and `validation.py` uses neither — it interpolates the dataclass directly, so a user sees `Decimal(precision=12, scale=4)` and `Array(element=String())` in failure messages.

**Files:**
- Create: `src/delta_engine/application/type_display.py`
- Test: `tests/application/test_type_display.py`

**Interfaces:**
- Consumes: `delta_engine.domain.model` data types (`DataType`, `Decimal`, `Array`, `Map`, `Struct`).
- Produces: `describe_type(data_type: DataType) -> str`. Tasks 2, 3 and 12 import it.

- [ ] **Step 1: Write the failing test**

Create `tests/application/test_type_display.py`:

```python
import pytest

from delta_engine.application.type_display import describe_type
from delta_engine.domain.model import (
    Array,
    Decimal,
    Long,
    Map,
    String,
    Struct,
    StructField,
)


@pytest.mark.parametrize(
    ("data_type", "expected"),
    [
        (String(), "String"),
        (Long(), "Long"),
        # Precision and scale are the whole point of a decimal: a widen from
        # Decimal(10,2) to Decimal(12,2) is invisible without them.
        (Decimal(12, 4), "Decimal(12,4)"),
        (Decimal(10), "Decimal(10,0)"),
        (Array(String()), "Array<String>"),
        (Map(String(), Long()), "Map<String, Long>"),
        (
            Struct((StructField("city", String()), StructField("zip", String()))),
            "Struct<city: String, zip: String>",
        ),
        # Nesting is where the bare class name loses the most.
        (
            Array(Struct((StructField("id", Long()),))),
            "Array<Struct<id: Long>>",
        ),
        (
            Map(String(), Array(Decimal(9, 3))),
            "Map<String, Array<Decimal(9,3)>>",
        ),
    ],
)
def test_describe_type_names_a_type_the_way_a_reader_needs_it(data_type, expected):
    assert describe_type(data_type) == expected
```

- [ ] **Step 2: Run the test to verify it fails**

```bash
uv run pytest tests/application/test_type_display.py -v --no-cov
```

Expected: collection error — `ModuleNotFoundError: No module named 'delta_engine.application.type_display'`.

- [ ] **Step 3: Write the implementation**

Create `src/delta_engine/application/type_display.py`:

```python
"""
How a domain data type is named for a human reader.

One spelling, shared by every view that shows a type to a user: the diff
entries, the grid, and the validation messages that reject a type change. It
is deliberately not the backend's DDL spelling — that belongs to the SQL
compiler in the adapters — and deliberately not the dataclass repr, which
leaks field names (``Decimal(precision=12, scale=4)``) into user-facing text.
"""

from delta_engine.domain.model import Array, DataType, Decimal, Map, Struct


def describe_type(data_type: DataType) -> str:
    """
    Return a compact, backend-neutral display name for ``data_type``.

    Parameterised and composite types carry their parameters, because that is
    exactly what distinguishes one from another: a reader comparing
    ``Decimal(10,2)`` with ``Decimal(12,2)`` is looking at the only two
    numbers that differ.
    """
    match data_type:
        case Decimal(precision=precision, scale=scale):
            return f"Decimal({precision},{scale})"
        case Array(element=element):
            return f"Array<{describe_type(element)}>"
        case Map(key=key, value=value):
            return f"Map<{describe_type(key)}, {describe_type(value)}>"
        case Struct(fields=fields):
            described = ", ".join(
                f"{field.name}: {describe_type(field.data_type)}" for field in fields
            )
            return f"Struct<{described}>"
        case _:
            return type(data_type).__name__
```

- [ ] **Step 4: Run the test to verify it passes**

```bash
uv run pytest tests/application/test_type_display.py -v --no-cov
```

Expected: 9 passed.

- [ ] **Step 5: Run the type checker and linter**

```bash
uv run mypy src/delta_engine/application/type_display.py
uv run ruff check src/delta_engine/application/type_display.py tests/application/test_type_display.py
uv run ruff format --check src/delta_engine/application/type_display.py tests/application/test_type_display.py
```

Expected: all clean.

- [ ] **Step 6: Commit**

```bash
git add src/delta_engine/application/type_display.py tests/application/test_type_display.py
git commit -m "feat: add one display name for domain data types"
```

---

### Task 2: The CREATE diff keeps its type detail — ✅ DONE (#313)

> Satisfied by `_column_add_entry` in `diff_entries.py`, which every `CreateTable` column goes through. Verify before assuming: `rg -n '_column_add_entry' src/`.

`_column_add_entry` (`diff_entries.py:138-143`) uses `_type_name`, which is the bare class name. So a newly created table renders `+ price Decimal`, `+ tags Array`, `+ address Struct` — losing exactly the information a reviewer needs before the type is baked in permanently. Meanwhile `AlterColumnType` in the same view uses `_type_display` and renders `Decimal(10,2) → Decimal(12,2)`. Two vocabularies inside one diff.

**Files:**
- Modify: `src/delta_engine/application/diff_entries.py:43-52` (delete both helpers), `:138-143` (`_column_add_entry`), `:232-234` (`AlterColumnType` registration)
- Test: `tests/application/test_rendering.py`

**Interfaces:**
- Consumes: `describe_type` from Task 1.
- Produces: no signature changes. `action_entries` output for `AddColumn` and `CreateTable` gains type parameters.

- [ ] **Step 1: Write the failing test**

Append to `tests/application/test_rendering.py`, after the existing `action_entries` parametrize block (which ends around line 390):

```python
def test_added_columns_keep_their_type_parameters():
    # Given columns whose types are only distinguishable by their parameters
    entries = action_entries(
        AddColumn(DesiredColumn("price", Decimal(12, 4))),
    )

    # Then the diff names the parameters, not the bare class
    assert entries == (
        DiffEntry(DiffCategory.COLUMNS, DiffOperation.ADD, ("price", "Decimal(12,4)")),
    )


def test_a_created_table_names_composite_column_types_in_full():
    # Given a create whose columns are composite
    table = DesiredTable(
        qualified_name=QualifiedName("cat", "sch", "products"),
        columns=(
            DesiredColumn("tags", Array(String())),
            DesiredColumn("address", Struct((StructField("city", String()),))),
        ),
    )

    # When the create is interpreted as diff entries
    entries = action_entries(CreateTable(table))

    # Then each column states what it actually is
    assert entries == (
        DiffEntry(DiffCategory.COLUMNS, DiffOperation.ADD, ("tags", "Array<String>")),
        DiffEntry(
            DiffCategory.COLUMNS,
            DiffOperation.ADD,
            ("address", "Struct<city: String>"),
        ),
    )
```

Add `Array`, `String`, `Struct`, `StructField` to the existing `from delta_engine.domain.model import (...)` block at the top of the file if not already present (`Decimal`, `String` and `DesiredTable` already are).

- [ ] **Step 2: Run the test to verify it fails**

```bash
uv run pytest tests/application/test_rendering.py -k "type_parameters or composite_column_types" -v --no-cov
```

Expected: 2 failed — `('price', 'Decimal')` != `('price', 'Decimal(12,4)')`.

- [ ] **Step 3: Write the implementation**

In `src/delta_engine/application/diff_entries.py`, delete lines 43-52 entirely:

```python
def _type_name(data_type: DataType) -> str:
    """Backend-agnostic display name for a domain data type (e.g. 'String')."""
    return type(data_type).__name__


def _type_display(data_type: DataType) -> str:
    """Display name including decimal parameters, so a precision widen is visible."""
    if isinstance(data_type, Decimal):
        return f"Decimal({data_type.precision},{data_type.scale})"
    return _type_name(data_type)
```

Replace the import block near the top so it reads:

```python
from delta_engine.application.type_display import describe_type
from delta_engine.domain.model import DesiredColumn
```

(`DataType` and `Decimal` are no longer referenced by this module — remove them from the `delta_engine.domain.model` import.)

In `_column_add_entry`, change line 140 from `_type_name(column.data_type)` to:

```python
    cells = [column.name, describe_type(column.data_type)]
```

In the `AlterColumnType` registration (line 233), change:

```python
    change = f"{describe_type(action.observed_type)} → {describe_type(action.desired_type)}"
```

- [ ] **Step 4: Run the full suite**

```bash
uv run pytest -q
```

Expected: all pass. The existing `AlterColumnType` decimal test at `tests/application/test_rendering.py:131-146` still asserts `"Decimal(10,2) → Decimal(12,2)"`, which `describe_type` produces identically.

- [ ] **Step 5: Type-check and lint**

```bash
uv run mypy src/delta_engine/application/
uv run ruff check src/ tests/
```

Expected: clean. If mypy reports an unused import in `diff_entries.py`, remove it.

- [ ] **Step 6: Commit**

```bash
git add src/delta_engine/application/diff_entries.py tests/application/test_rendering.py
git commit -m "fix: keep type parameters in the diff for added columns"
```

---

### Task 3: Validation messages stop leaking dataclass reprs — ✅ DONE (#313)

> `DataType.__str__` did this for free — the rule bodies interpolate the type and get a clean spelling. Verify before assuming: `rg -n '!r\}|\{action\}' src/delta_engine/application/validation.py` should find nothing.

`validation.py` interpolates `DataType` instances directly, so a rejected decimal narrowing reads:

```
cannot change the type of existing column 'amount' from Decimal(precision=12, scale=4)
to Decimal(precision=8, scale=2)
```

**Files:**
- Modify: `src/delta_engine/application/validation.py:194` (`NonWideningColumnTypeChange`), `:228` (`TypeWideningRequiredForTypeChange`)
- Test: `tests/application/test_validation.py`

**Interfaces:**
- Consumes: `describe_type` from Task 1.
- Produces: no signature changes.

- [ ] **Step 1: Write the failing test**

Append to `tests/application/test_validation.py`:

```python
def test_a_rejected_type_change_names_types_the_way_the_diff_does():
    # Given an in-place decimal narrowing, which no widening can apply
    drift = _drift_with_actions(
        AlterColumnType(
            column_name="amount",
            desired_type=Decimal(8, 2),
            observed_type=Decimal(12, 4),
        )
    )

    # When the rule judges it
    (failure,) = NonWideningColumnTypeChange().evaluate(drift)

    # Then the message spells the types as a reader sees them elsewhere,
    # not as a dataclass repr
    assert "from Decimal(12,4) to Decimal(8,2)" in failure.message
    assert "precision=" not in failure.message
```

Use whatever drift-building helper `test_validation.py` already provides; if the local helper has a different name, match it. Find it with:

```bash
grep -n "def _drift" tests/application/test_validation.py
```

- [ ] **Step 2: Run the test to verify it fails**

```bash
uv run pytest tests/application/test_validation.py -k "names_types_the_way" -v --no-cov
```

Expected: FAIL — the message contains `Decimal(precision=12, scale=4)`.

- [ ] **Step 3: Write the implementation**

In `src/delta_engine/application/validation.py`, add to the imports:

```python
from delta_engine.application.type_display import describe_type
```

In `NonWideningColumnTypeChange.evaluate` (line 194), change:

```python
                    f" from {describe_type(change.observed_type)}"
                    f" to {describe_type(change.desired_type)}."
```

In `TypeWideningRequiredForTypeChange.evaluate` (line 228), change:

```python
                    f" from {describe_type(change.observed_type)}"
                    f" to {describe_type(change.desired_type)} requires"
```

- [ ] **Step 4: Run the full suite**

```bash
uv run pytest -q
```

Expected: all pass. If an existing test pins the old repr text, update it to the new spelling — that is the point of this task.

- [ ] **Step 5: Commit**

```bash
git add src/delta_engine/application/validation.py tests/application/test_validation.py
git commit -m "fix: name data types consistently in validation messages"
```

---

### Task 4: Statement numbers read from 1 — ❌ OUTSTANDING

> Still 0-based: `ExecutionFailure.format_lines` reads `f"Execution failed at statement {self.statement_index}"`, so the first statement is statement 0.

`ExecutionFailure.statement_index` is 0-based (`engine.py:428` uses `enumerate(statements)`), and `format_lines` prints it raw. A run that fails on the third of three statements shows `Execution failed at statement 2` in the failures section beside `2/3` in the STATEMENTS column. Same number, two meanings, on one screen.

The field stays 0-based; only the display shifts.

**Files:**
- Modify: `src/delta_engine/application/failures.py:131-139`
- Test: `tests/application/test_failures.py:31-44`, `:116-122`; `tests/application/test_errors.py:184`

**Interfaces:**
- Consumes: nothing new.
- Produces: `ExecutionFailure.format_lines()[0]` and `.headline()` now say `statement {index + 1}`. `statement_index` itself is unchanged.

- [ ] **Step 1: Update the existing tests to the intended behaviour**

In `tests/application/test_failures.py`, change line 43 from:

```python
    assert lines[0] == "Execution failed at statement 2: SparkException - boom"
```

to:

```python
    # statement_index is 0-based; the display is 1-based so it reads the same
    # way as the "applied/total" count in the report grid.
    assert lines[0] == "Execution failed at statement 3: SparkException - boom"
```

Change line 121 from `"Execution failed at statement 2: SparkException"` to `"Execution failed at statement 3: SparkException"`.

In `tests/application/test_errors.py`, change line 184 from `"Execution failed at statement 2: SparkException - boom"` to `"Execution failed at statement 3: SparkException - boom"`.

- [ ] **Step 2: Run the tests to verify they fail**

```bash
uv run pytest tests/application/test_failures.py tests/application/test_errors.py -v --no-cov
```

Expected: 3 failed, each showing `statement 2` where `statement 3` was expected.

- [ ] **Step 3: Write the implementation**

In `src/delta_engine/application/failures.py`, replace `ExecutionFailure.format_lines` and `.headline` (lines 131-139):

```python
    def format_lines(self) -> tuple[str, ...]:
        return (
            f"Execution failed at statement {self.statement_number}: "
            f"{self.exception_type} - {_message_head(self.message)}",
            f"    SQL: {self.statement}",
        )

    def headline(self) -> str:
        return f"Execution failed at statement {self.statement_number}: {self.exception_type}"

    @property
    def statement_number(self) -> int:
        """
        The failing statement's 1-based position, as displayed.

        ``statement_index`` is the 0-based position in the compiled tuple and
        stays that way — it indexes ``planned_sql_statements``. Only what a
        reader sees is 1-based, so that "statement 3" and the grid's "2/3"
        describe the same run of three statements.
        """
        return self.statement_index + 1
```

- [ ] **Step 4: Run the full suite**

```bash
uv run pytest -q
```

Expected: all pass.

- [ ] **Step 5: Commit**

```bash
git add src/delta_engine/application/failures.py tests/application/test_failures.py tests/application/test_errors.py
git commit -m "fix: number statements from 1 in execution failure messages"
```

---

### Task 5: `NonNullableColumnAdd` says what to do about it — ❌ OUTSTANDING

> Still bare: `Operation not allowed: cannot add non-nullable column 'email'` with no remedy, where every sibling rule has one.

Every other safety rule ends with a remedy. This one is bare: `Operation not allowed: cannot add non-nullable column 'email'`. Its sibling `NullabilityTighteningOnExistingColumn` (`validation.py:137-157`) already carries the right words, and `docs/reference-safe-change-rules.md:12` already documents the same remedy — it just never reached the message.

**Files:**
- Modify: `src/delta_engine/application/validation.py:118-134`
- Test: `tests/application/test_validation.py`

**Interfaces:** no signature changes.

- [ ] **Step 1: Write the failing test**

Append to `tests/application/test_validation.py`:

```python
def test_rejecting_a_not_null_column_add_tells_the_author_how_to_proceed():
    # Given a NOT NULL column added to an existing table
    drift = _drift_with_actions(
        AddColumn(column=DesiredColumn("email", String(), nullable=False))
    )

    # When the rule judges it
    (failure,) = NonNullableColumnAdd().evaluate(drift)

    # Then the message names the column and the way forward, as its sibling does
    assert "'email'" in failure.message
    assert "backfill" in failure.message
    assert "nullable=False" in failure.message
```

- [ ] **Step 2: Run the test to verify it fails**

```bash
uv run pytest tests/application/test_validation.py -k "how_to_proceed" -v --no-cov
```

Expected: FAIL — `assert "backfill" in failure.message`.

- [ ] **Step 3: Write the implementation**

In `src/delta_engine/application/validation.py`, replace the message in `NonNullableColumnAdd.evaluate` (lines 126-133):

```python
            ValidationFailure(
                rule_name=self.name,
                message=(
                    f"Operation not allowed: cannot add non-nullable column"
                    f" '{change.column.name}'. Delta cannot add a NOT NULL column to a"
                    " table that already has rows. Add it nullable, backfill any NULLs,"
                    " set NOT NULL outside the engine"
                    " (ALTER TABLE ... ALTER COLUMN ... SET NOT NULL), then declare"
                    " nullable=False — the next sync sees no drift."
                ),
            )
```

- [ ] **Step 4: Run the full suite**

```bash
uv run pytest -q
```

Expected: all pass.

- [ ] **Step 5: Commit**

```bash
git add src/delta_engine/application/validation.py tests/application/test_validation.py
git commit -m "fix: tell the author how to add a non-nullable column"
```

---

### Task 6: The grid names what a validation failure is about — ❌ OUTSTANDING

> **`ValidationFailure` has changed since this was written.** #316 added `details: tuple[str, ...] = ()` for the drift lines. `subject` goes *before* it (`rule_name`, `message`, `subject=""`, `details=()`), and `format_lines` already returns `(headline, *details)` — only `headline()` needs the subject. Once this lands, tighten task 12's `test_unmanaged_drift_reports_one_failure_per_aspect` to assert on `subject` as originally written.
>
> The nine-rule table in step 5 still holds; line numbers do not.

`ValidationFailure.headline()` returns only the rule name, so the DETAIL column of a forty-table grid reads `Validation failed: NonNullableColumnAdd (+2 more)` — you learn the rule but not the column. The full message in the failures section already names the subject; the grid just cannot reach it.

`subject` is a display field on `ValidationFailure` only. It is deliberately **not** added to `to_dict()`: the JSON failure record already carries the full `message`, which names the subject, and extending the versioned contract for a redundant field is not worth it.

**Files:**
- Modify: `src/delta_engine/application/failures.py:106-118`; `src/delta_engine/application/validation.py` (nine rule bodies)
- Test: `tests/application/test_failures.py`, `tests/application/test_validation.py`

**Interfaces:**
- Produces: `ValidationFailure(rule_name, message, subject="")`. `subject` is a keyword field with a default, so every existing construction site keeps working.

- [ ] **Step 1: Write the failing test**

Append to `tests/application/test_failures.py`:

```python
def test_a_validation_headline_names_its_subject_when_it_has_one():
    # Given a failure that is about one named column
    failure = ValidationFailure(
        rule_name="NonNullableColumnAdd",
        message="Operation not allowed: cannot add non-nullable column 'email'.",
        subject="email",
    )

    # Then the compact grid headline says which column, not just which rule
    assert failure.headline() == "Validation failed: NonNullableColumnAdd (email)"


def test_a_validation_headline_omits_an_absent_subject():
    # Given a table-level failure with no single subject
    failure = ValidationFailure(rule_name="ColumnMappingRequiredForDrop", message="nope")

    # Then the headline is unchanged — no empty parentheses
    assert failure.headline() == "Validation failed: ColumnMappingRequiredForDrop"
```

- [ ] **Step 2: Run the test to verify it fails**

```bash
uv run pytest tests/application/test_failures.py -k "names_its_subject or omits_an_absent" -v --no-cov
```

Expected: 1 failed (`TypeError: unexpected keyword argument 'subject'`), 1 passed.

- [ ] **Step 3: Write the implementation**

In `src/delta_engine/application/failures.py`, replace `ValidationFailure` (lines 106-118):

```python
@dataclass(frozen=True, slots=True)
class ValidationFailure(Failure):
    """
    Description of a validation rule failure.

    ``subject`` is what the failure is about — a column, a property key, an
    aspect — used only by the compact ``headline`` the report grid shows. The
    full ``message`` already names it; the headline cannot afford the whole
    sentence. Rules that judge the table as a whole leave it empty.
    """

    phase: ClassVar[FailurePhase] = FailurePhase.PLANNING
    rule_name: str
    message: str
    subject: str = ""

    def format_lines(self) -> tuple[str, ...]:
        return (f"Validation failed: {self.rule_name} - {self.message}",)

    def headline(self) -> str:
        if not self.subject:
            return f"Validation failed: {self.rule_name}"
        return f"Validation failed: {self.rule_name} ({self.subject})"
```

- [ ] **Step 4: Run the test to verify it passes**

```bash
uv run pytest tests/application/test_failures.py -v --no-cov
```

Expected: all pass.

- [ ] **Step 5: Populate `subject` from every rule that has one**

In `src/delta_engine/application/validation.py`, add `subject=` to these nine `ValidationFailure(...)` constructions. Leave the other five rules alone — they judge the table as a whole and have no single subject.

| Rule | Line (approx.) | Add |
| --- | --- | --- |
| `NonNullableColumnAdd` | 126 | `subject=str(change.column.name),` |
| `NullabilityTighteningOnExistingColumn` | 145 | `subject=str(change.column_name),` |
| `NonWideningColumnTypeChange` | 189 | `subject=str(change.column_name),` |
| `TypeWideningRequiredForTypeChange` | 224 | `subject=str(change.column_name),` |
| `PropertyTransitionNotSupported` (both arms) | 284, 297 | `subject=name,` |
| `PropertyMustBeDeclared` | 330 | `subject=unresolvable.name,` |
| `AmbiguousColumnRename` | 392 | `subject=str(unresolvable.old_name),` |
| `ColumnSpellingMustMatchCatalog` | 497 | `subject=str(unresolvable.declared_name),` |
| `UnmanagedAspectDrift` | 554 | `subject=aspect.label,` |

`str(...)` is deliberate on the column ones: those values may be `Identifier` instances, whose equality is case-insensitive, and `subject` is display text that must compare and render as exact spelling.

Example, for `PropertyMustBeDeclared` at line 329-333:

```python
        return tuple(
            ValidationFailure(
                rule_name=self.name,
                message=self._message(unresolvable),
                subject=unresolvable.name,
            )
            for unresolvable in drift.unresolvable
            if isinstance(unresolvable, PropertyUndeclared)
        )
```

- [ ] **Step 6: Write the test proving the grid benefits**

Append to `tests/application/test_rendering.py`:

```python
def test_the_grid_detail_names_the_column_a_rule_rejected():
    # Given a table rejected by a rule that names a column
    report = _grid_report(
        "orders",
        failures=(
            ValidationFailure(
                rule_name="NonNullableColumnAdd",
                message="Operation not allowed: cannot add non-nullable column 'email'.",
                subject="email",
            ),
        ),
    )
    sync = SyncReport(
        started_at=datetime(2025, 1, 1, 0, 0, 0),
        ended_at=datetime(2025, 1, 1, 0, 0, 3),
        table_reports=(report,),
    )

    # Then the DETAIL cell says which column, not only which rule
    assert "NonNullableColumnAdd (email)" in render_grid(sync.table_reports)
```

- [ ] **Step 7: Run the full suite**

```bash
uv run pytest -q
uv run mypy src/delta_engine/
```

Expected: all pass, mypy clean.

- [ ] **Step 8: Commit**

```bash
git add src/delta_engine/application/failures.py src/delta_engine/application/validation.py tests/
git commit -m "feat: name the subject of a validation failure in the report grid"
```

---

### Task 7: The run summary counts in English — ❌ OUTSTANDING

> Still `1 tables: 0 changed, 0 unchanged, 1 failed`. **PR3's task 16 depends on the `_count_phrase` helper this task introduces — do this one first.** Note that #313 moved the counting itself onto `SyncReport.counts` (a `RunCounts` named tuple) and the duration onto `SyncReport.duration_seconds`; this task now only changes how the footer *words* those numbers.

`run_summary_footer` (`rendering.py:133-148`) prints `1 tables: ...` on every single-table run.

**Files:**
- Modify: `src/delta_engine/application/rendering.py:133-148`
- Test: `tests/application/test_rendering.py:655`, `:681`, `:700`

**Interfaces:**
- Produces: `_count_phrase(count: int, singular: str, plural: str) -> str`, module-private. PR3 Task 16 reuses it.

- [ ] **Step 1: Write the failing test**

Append to `tests/application/test_rendering.py`:

```python
def test_a_single_table_run_says_table_not_tables():
    # Given a run over exactly one table
    sync = SyncReport(
        started_at=datetime(2025, 1, 1, 0, 0, 0),
        ended_at=datetime(2025, 1, 1, 0, 0, 3),
        table_reports=(_grid_report("orders"),),
    )

    # Then the footer reads as English
    assert run_summary_footer(sync).startswith("1 table:")
```

- [ ] **Step 2: Run the test to verify it fails**

```bash
uv run pytest tests/application/test_rendering.py -k "table_not_tables" -v --no-cov
```

Expected: FAIL — the footer starts `1 tables:`.

- [ ] **Step 3: Write the implementation**

In `src/delta_engine/application/rendering.py`, add above `run_summary_footer`:

```python
def _count_phrase(count: int, singular: str, plural: str) -> str:
    """Render a count with the noun that agrees with it."""
    return f"{count} {singular if count == 1 else plural}"
```

and change the return of `run_summary_footer` (lines 145-148) to:

```python
    return (
        f"{_count_phrase(total, 'table', 'tables')}: {changed} changed,"
        f" {unchanged} unchanged, {failed} failed ({seconds:.1f}s)"
    )
```

- [ ] **Step 4: Run the full suite**

```bash
uv run pytest -q
```

Expected: all pass — the three existing footer assertions all use multi-table or zero-table runs, so `tables` is still correct for them.

- [ ] **Step 5: Commit**

```bash
git add src/delta_engine/application/rendering.py tests/application/test_rendering.py
git commit -m "fix: agree the run summary noun with its count"
```

---

### PR1 close-out

- [ ] **Run every gate**

```bash
uv run pytest -q
uv run mypy src/delta_engine/
uv run ruff check src/ tests/
uv run ruff format --check src/ tests/
uv run lint-imports
uv run sphinx-build -W -b html docs docs/_build/html
```

- [ ] **Open the PR**

```bash
git push -u origin fix/sync-failure-message-quality
gh pr create --title "fix: make sync failure messages say what and how" --body "$(cat <<'EOF'
Six user-visible defects in what a sync tells you, none of which change a public type.

- Data types render as `Decimal(12,4)` / `Array<String>` everywhere, instead of `Decimal(precision=12, scale=4)` in failure messages and a bare `Decimal` in a CREATE diff. `DataType.__str__` is the single home for that spelling (this bullet already landed via #313 — drop it from the PR text and keep only tasks 4–7).
- Execution failures number statements from 1, so "statement 3" and the grid's "2/3" describe the same three statements.
- `NonNullableColumnAdd` carries the remedy that `reference-safe-change-rules.md` already documents.
- The grid's DETAIL column names the column a rule rejected, not just the rule.
- A one-table run says "1 table".

Verified: full suite, mypy, ruff, import-linter, `sphinx-build -W`.
EOF
)"
```

---

# PR 2 — `feat/report-carries-the-diff` — ✅ MERGED (#316)

The structural change. **All of tasks 8–12 are done and on `main`.** Nothing below needs doing; it is kept as the record of what was built and why, and because PR3 reads several of the interfaces it produced. Two deviations from what is written here are described in Status at the top: the drift lines are `ValidationFailure.details` rather than newlines in `message`, and `ValidationFailure.subject` was left to task 6.

The one interface that appears nowhere below, because it was introduced during the work: **`drift_entries(drift) -> tuple[DiffEntry, ...]`** in `diff_entries.py`, beside `plan_entries`. It replaced both the plan's `_rejected_entries` helper in `rendering.py` and the duplicated comprehension in `_rejected_change_records`.

The original branching instruction is stale — this shipped off `main`, since PR1 never existed as a branch:

```bash
git checkout -b feat/report-carries-the-diff
```

---

### Task 8: The report keeps the diff it was built from

`_TableRun.diff` (`engine.py:136`) holds a complete `TableDiff` — every action and every unresolvable difference the engine found. `to_report()` (`engine.py:168-178`) never passes it on, so `TableRunReport` cannot answer "what drifted?" for any table whose plan was rejected. That is precisely the table whose user most needs the answer.

`diff` is optional with a `None` default rather than required. `TableRunReport` is public and hand-constructible, and requiring a domain `TableDiff` for every hand-built report would be hostile; the engine, its only production constructor, always supplies one. The one direction that *is* impossible gets an invariant: a failed read produces no diff.

**Files:**
- Modify: `src/delta_engine/application/report.py:94-146`, `src/delta_engine/application/engine.py:168-178`
- Test: `tests/application/test_report.py`, `tests/application/test_engine.py`

**Interfaces:**
- Produces: `TableRunReport.diff: TableDiff | None = None`. Tasks 10 and 11 read it.

- [ ] **Step 1: Write the failing test**

Append to `tests/application/test_engine.py`:

```python
def test_a_rejected_table_still_reports_the_drift_that_was_found():
    # Given a declaration whose only change is one the safety rules refuse
    reader = _RecordingReader({"cat.sch.orders": _existing_id_table("cat.sch.orders")})
    engine = Engine(reader=reader, executor=_RecordingExecutor())

    # When the sync runs and planning rejects it
    report = engine.sync(_spec_adding_not_null("cat.sch.orders"), dry_run=True)

    # Then the table has no plan, but the diff that caused the rejection survives
    (table_report,) = report.table_reports
    assert table_report.plan is None
    assert table_report.diff is not None
    assert any(
        isinstance(action, AddColumn) and action.column.name == "order_id"
        for action in table_report.diff.actions
    )


def test_a_failed_read_leaves_no_diff_on_the_report():
    # Given a table whose catalog read fails
    reader = _FailingReader()
    engine = Engine(reader=reader, executor=_RecordingExecutor())

    # When the sync runs
    report = engine.sync(_spec("cat.sch.orders"), dry_run=True)

    # Then there is nothing to have diffed
    (table_report,) = report.table_reports
    assert table_report.diff is None
```

Add `AddColumn` to the `delta_engine.domain.plan.actions` import block at the top of the file. For `_FailingReader`, reuse whatever the file already has — find it with:

```bash
grep -n "class _.*Reader" tests/application/test_engine.py
```

If no failing reader exists, add one next to `_RecordingReader`:

```python
class _FailingReader:
    def fetch_state(self, qualified_name: QualifiedName) -> CatalogState:
        raise ReadError(exception_type="IOError", message="boom")
```

- [ ] **Step 2: Run the tests to verify they fail**

```bash
uv run pytest tests/application/test_engine.py -k "still_reports_the_drift or leaves_no_diff" -v --no-cov
```

Expected: first FAILS with `AttributeError: 'TableRunReport' object has no attribute 'diff'`; second FAILS the same way.

- [ ] **Step 3: Write the implementation**

In `src/delta_engine/application/report.py`, add to the imports:

```python
from delta_engine.domain.plan import ActionPlan, TableDiff
```

(the module already imports `ActionPlan` from `delta_engine.domain.plan`; extend that line.)

Add the field after `blocked_failures` (line 115):

```python
    blocked_failures: tuple[ForeignKeyFailure, ...] = ()
    diff: TableDiff | None = None
```

Extend the class docstring (after the `blocked_failures` sentence at line 107):

```
    ``diff`` is the complete set of differences the engine found — actions and
    unresolvable differences alike — retained so a table whose plan was
    rejected can still show what drifted. It is ``None`` when the read failed,
    and may be ``None`` on a hand-constructed report.
```

Add the invariant to `__post_init__`, immediately after the existing read/planning checks (after line 125):

```python
        if read_failed and self.diff is not None:
            raise ValueError("A failed read produces no diff")
```

In `src/delta_engine/application/engine.py`, change `to_report()` (lines 172-178) to pass it through:

```python
        return TableRunReport(
            read=self.read,
            planning=self.planning,
            planned_sql_statements=self.planned_sql_statements,
            resolution=self.resolution,
            execution=self.execution,
            diff=self.diff,
        )
```

- [ ] **Step 4: Run the tests to verify they pass**

```bash
uv run pytest tests/application/test_engine.py -k "still_reports_the_drift or leaves_no_diff" -v --no-cov
```

Expected: 2 passed.

- [ ] **Step 5: Write the invariant test**

Append to `tests/application/test_report.py`:

```python
def test_a_report_cannot_claim_a_diff_after_a_failed_read():
    # Given a read that failed, there is nothing to have compared
    qualified_name = QualifiedName("cat", "sch", "orders")
    desired = DesiredTable(qualified_name=qualified_name, columns=(DesiredColumn("id", Integer()),))
    observed = ObservedTable(
        qualified_name=qualified_name, columns=(ObservedColumn("id", Integer()),)
    )

    # Then constructing a report that claims both is rejected
    with pytest.raises(ValueError, match="failed read produces no diff"):
        TableRunReport(
            read=ReadFailure(exception_type="IOError", message="boom"),
            planning=None,
            planned_sql_statements=(),
            resolution=TableResolution(desired, (), ()),
            execution=None,
            diff=diff_table(desired, observed),
        )
```

Add `diff_table` to the imports from `delta_engine.domain.plan` in that file, plus any of `DesiredColumn`, `ObservedColumn`, `ObservedTable`, `ReadFailure` that are not already imported.

- [ ] **Step 6: Run the full suite**

```bash
uv run pytest -q
uv run mypy src/delta_engine/
```

Expected: all pass.

- [ ] **Step 7: Commit**

```bash
git add src/delta_engine/application/report.py src/delta_engine/application/engine.py tests/
git commit -m "feat: carry the computed diff onto the run report"
```

---

### Task 9: Interpret unresolvable differences as diff entries

`action_entries` turns an `Action` into display cells. There is no equivalent for the four `Unresolvable` types (`ColumnCaseDrift`, `ColumnRenameConflict`, `PropertyUndeclared`, `PartitioningChanged`), so a rejected table's non-action differences have no way to be shown. Tasks 10 and 12 both need one.

**Files:**
- Modify: `src/delta_engine/application/diff_entries.py` (append)
- Test: `tests/application/test_rendering.py`

**Interfaces:**
- Produces: `unresolvable_entries(unresolvable: Unresolvable) -> tuple[DiffEntry, ...]`. Tasks 10 and 12 import it.

- [ ] **Step 1: Write the failing test**

Append to `tests/application/test_rendering.py`:

```python
@pytest.mark.parametrize(
    ("unresolvable", "expected"),
    [
        (
            ColumnCaseDrift(declared_name="SKU", observed_name="sku"),
            (
                DiffEntry(
                    DiffCategory.COLUMNS,
                    DiffOperation.CHANGE,
                    ("SKU", "spelled 'sku' in the catalog"),
                ),
            ),
        ),
        (
            ColumnRenameConflict(old_name="old_id", new_name="id"),
            (
                DiffEntry(
                    DiffCategory.COLUMNS,
                    DiffOperation.CHANGE,
                    ("old_id", "renamed → id, but both columns exist"),
                ),
            ),
        ),
        (
            PropertyUndeclared(name="delta.enableChangeDataFeed", observed_value="true"),
            (
                DiffEntry(
                    DiffCategory.PROPERTIES,
                    DiffOperation.CHANGE,
                    ("delta.enableChangeDataFeed = 'true' (set on the table, undeclared)",),
                ),
            ),
        ),
        (
            PartitioningChanged(
                desired_partitioning=("region",), observed_partitioning=("country",)
            ),
            (
                DiffEntry(
                    DiffCategory.PARTITIONING,
                    DiffOperation.CHANGE,
                    ("partitioning (country) → (region)",),
                ),
            ),
        ),
    ],
)
def test_unresolvable_differences_describe_themselves(unresolvable, expected):
    assert unresolvable_entries(unresolvable) == expected
```

Add to the imports at the top of the file:

```python
from delta_engine.application.diff_entries import unresolvable_entries
from delta_engine.domain.plan import (
    ColumnCaseDrift,
    ColumnRenameConflict,
    PartitioningChanged,
    PropertyUndeclared,
)
```

(merge into the existing `diff_entries` import block rather than adding a second one).

- [ ] **Step 2: Run the test to verify it fails**

```bash
uv run pytest tests/application/test_rendering.py -k "unresolvable_differences_describe" -v --no-cov
```

Expected: collection error — `ImportError: cannot import name 'unresolvable_entries'`.

- [ ] **Step 3: Write the implementation**

Append to `src/delta_engine/application/diff_entries.py`:

```python
@functools.singledispatch
def unresolvable_entries(unresolvable: Unresolvable) -> tuple[DiffEntry, ...]:
    """
    Describe one unresolvable difference as category-tagged diff entries.

    The sibling of :func:`action_entries` for differences no action can close.
    Every entry is a ``CHANGE``: an unresolvable difference is neither an
    addition nor a removal the engine could make, it is a disagreement between
    the declaration and the catalog that something outside this sync must
    settle.
    """
    raise NotImplementedError(
        f"No diff entries for unresolvable {type(unresolvable).__name__}"
    )


@unresolvable_entries.register
def _(unresolvable: ColumnCaseDrift) -> tuple[DiffEntry, ...]:
    return (
        DiffEntry(
            DiffCategory.COLUMNS,
            DiffOperation.CHANGE,
            (unresolvable.declared_name, f"spelled '{unresolvable.observed_name}' in the catalog"),
        ),
    )


@unresolvable_entries.register
def _(unresolvable: ColumnRenameConflict) -> tuple[DiffEntry, ...]:
    return (
        DiffEntry(
            DiffCategory.COLUMNS,
            DiffOperation.CHANGE,
            (unresolvable.old_name, f"renamed → {unresolvable.new_name}, but both columns exist"),
        ),
    )


@unresolvable_entries.register
def _(unresolvable: PropertyUndeclared) -> tuple[DiffEntry, ...]:
    text = (
        f"{unresolvable.name} = '{unresolvable.observed_value}'"
        " (set on the table, undeclared)"
    )
    return (DiffEntry(DiffCategory.PROPERTIES, DiffOperation.CHANGE, (text,)),)


@unresolvable_entries.register
def _(unresolvable: PartitioningChanged) -> tuple[DiffEntry, ...]:
    observed = ", ".join(unresolvable.observed_partitioning)
    desired = ", ".join(unresolvable.desired_partitioning)
    return (
        DiffEntry(
            DiffCategory.PARTITIONING,
            DiffOperation.CHANGE,
            (f"partitioning ({observed}) → ({desired})",),
        ),
    )
```

Extend the existing `from delta_engine.domain.plan import (...)` block with:

```python
    ColumnCaseDrift,
    ColumnRenameConflict,
    PartitioningChanged,
    PropertyUndeclared,
    Unresolvable,
)
```

- [ ] **Step 4: Run the test to verify it passes**

```bash
uv run pytest tests/application/test_rendering.py -k "unresolvable_differences_describe" -v --no-cov
```

Expected: 4 passed.

- [ ] **Step 5: Verify the dispatch is exhaustive**

```bash
uv run python -c "
from delta_engine.application.diff_entries import unresolvable_entries
import delta_engine.domain.plan.unresolvable as u
import typing
members = typing.get_args(u.Unresolvable.__value__)
registered = set(unresolvable_entries.registry) - {object}
missing = [m.__name__ for m in members if m not in registered]
print('unregistered:', missing or 'none')
assert not missing, missing
"
```

Expected: `unregistered: none`. If a fifth `Unresolvable` variant is ever added, this catches it.

- [ ] **Step 6: Run the full suite and commit**

```bash
uv run pytest -q
uv run mypy src/delta_engine/
git add src/delta_engine/application/diff_entries.py tests/application/test_rendering.py
git commit -m "feat: interpret unresolvable differences as diff entries"
```

---

### Task 10: A rejected table shows what drifted

`render_diff_block` (`rendering.py:57-70`) prints `(no changes — see failures)` whenever there is no plan. For a rejected table that is false: changes were found and refused.

**Files:**
- Modify: `src/delta_engine/application/rendering.py:57-70`
- Test: `tests/application/test_rendering.py:410-415`

**Interfaces:**
- Consumes: `TableRunReport.diff` (Task 8), `unresolvable_entries` (Task 9).
- Produces: no signature change to `render_diff_block`.

- [ ] **Step 1: Write the failing test**

Append to `tests/application/test_rendering.py`:

```python
def test_a_rejected_table_shows_the_drift_that_was_refused():
    # Given a table whose declaration adds a NOT NULL column and drops another,
    # both refused by validation
    qualified_name = QualifiedName("cat", "sch", "orders")
    desired = DesiredTable(
        qualified_name=qualified_name,
        columns=(
            DesiredColumn("id", Integer(), nullable=False),
            DesiredColumn("email", String(), nullable=False),
        ),
    )
    observed = ObservedTable(
        qualified_name=qualified_name,
        columns=(
            ObservedColumn("id", Integer(), nullable=False),
            ObservedColumn("obsolete", String()),
        ),
    )
    report = TableRunReport(
        read=TablePresent(table=observed),
        planning=PlanningFailed(
            (ValidationFailure(rule_name="NonNullableColumnAdd", message="nope"),)
        ),
        planned_sql_statements=(),
        resolution=TableResolution(desired, (), ()),
        execution=None,
        diff=diff_table(desired, observed),
    )

    # When the block is rendered
    block = render_diff_block(report)

    # Then it names the refused changes rather than claiming there were none
    assert "(no changes" not in block
    assert "REJECTED" in block.splitlines()[0]
    assert "+ email" in block
    assert "- obsolete" in block


def test_a_table_with_no_diff_at_all_still_points_at_its_failures():
    # Given a report that carries failures but no diff (a hand-built report,
    # or a table that failed before the diff phase)
    block = render_diff_block(_report_with_empty_plan_and_failure())

    # Then the old wording stands — there is genuinely nothing to show
    assert "(no changes — see failures)" in block
```

Add `diff_table` to the `delta_engine.domain.plan` imports in the file.

- [ ] **Step 2: Run the tests to verify the first fails**

```bash
uv run pytest tests/application/test_rendering.py -k "drift_that_was_refused or no_diff_at_all" -v --no-cov
```

Expected: 1 failed (`assert "(no changes" not in block`), 1 passed.

- [ ] **Step 3: Write the implementation**

In `src/delta_engine/application/rendering.py`, add to the imports:

```python
from delta_engine.application.diff_entries import (
    CATEGORY_NOUN,
    DiffCategory,
    DiffEntry,
    action_entries,
    unresolvable_entries,
)
from delta_engine.domain.plan import ActionPlan, CreateTable, TableDrift
```

Add above `render_diff_block`:

```python
def _rejected_entries(report: TableRunReport) -> list[DiffEntry]:
    """
    Interpret the differences a rejected table's diff found, actions and all.

    A rejected diff has no plan — that is what rejection means — so its
    differences are read straight off the diff rather than off an action plan.
    Both streams are included: an unresolvable difference is often the very
    thing that caused the rejection.
    """
    if not isinstance(report.diff, TableDrift):
        return []
    return [
        *(entry for action in report.diff.actions for entry in action_entries(action)),
        *(
            entry
            for unresolvable in report.diff.unresolvable
            for entry in unresolvable_entries(unresolvable)
        ),
    ]
```

Replace `render_diff_block` (lines 57-70):

```python
def render_diff_block(report: TableRunReport) -> str:
    """Render one table's change block: its name then its changes, grouped."""
    header = str(report.qualified_name)
    if isinstance(report.read, ReadFailure):
        return f"{header}\n  (could not read — no diff)"

    plan = report.plan
    if plan is None:
        # No plan means the diff was rejected. Show what it found: the failure
        # list says which rule refused, this says what it refused.
        rejected = _rejected_entries(report)
        if rejected:
            return "\n".join(
                [f"{header}  (REJECTED — no SQL planned)", *_render_entry_groups(rejected)]
            )
        return f"{header}\n  ({_NO_CHANGES} — see failures)"

    if not plan:
        if report.has_failures:
            return f"{header}\n  ({_NO_CHANGES} — see failures)"
        return f"{header}\n  ({_NO_CHANGES})"

    if _plan_creates_table(plan):
        header = f"{header}  (CREATE)"
    entries = [entry for action in plan for entry in action_entries(action)]
    return "\n".join([header, *_render_entry_groups(entries)])
```

Note the restructure: the old code checked `if not plan:` which is true for both `None` and an empty plan. Those are different states — `None` means rejected, empty means a validated no-op — and only the first has a diff worth showing.

- [ ] **Step 4: Run the full suite**

```bash
uv run pytest -q
```

Expected: all pass. The existing test at line 410 (`test_diff_block_points_to_failures_when_no_plan_exists_and_failures_exist`) still passes because its fixture carries no `diff`.

- [ ] **Step 5: See it for real**

```bash
uv run python - <<'PY'
from delta_engine.adapters.databricks.sql.compile import compile_plan
from delta_engine.application.engine import Engine
from delta_engine.application.ports import TableAbsent, TablePresent
from delta_engine.application.rendering import render_diff, render_report
from delta_engine.domain.model import ObservedColumn, ObservedTable, QualifiedName
from delta_engine.schema import Column, DeltaTable, Integer, Long, String

class R:
    def __init__(self, s): self.s = s
    def fetch_state(self, q): return self.s.get(str(q), TableAbsent())

class E:
    def compile(self, plan): return compile_plan(plan)
    def execute(self, s): raise AssertionError

orders = DeltaTable("main", "sales", "orders", columns=(
    Column("id", Long(), nullable=False, comment="order id"),
    Column("customer_id", Long()),
), comment="orders", scope="metadata")

observed = TablePresent(table=ObservedTable(
    qualified_name=QualifiedName("main", "sales", "orders"),
    columns=(
        ObservedColumn("id", Long(), nullable=False, comment="order id"),
        ObservedColumn("customer_id", Integer()),
        ObservedColumn("legacy_region", String()),
    ),
    comment="orders",
))

report = Engine(reader=R({"main.sales.orders": observed}), executor=E()).sync(orders, dry_run=True)
print(render_diff(report)); print(); print(render_report(report))
PY
```

Expected: the DIFF section now names `customer_id`, `legacy_region` and `shipped_at` under a `(REJECTED — no SQL planned)` header instead of `(no changes — see failures)`.

- [ ] **Step 6: Commit**

```bash
git add src/delta_engine/application/rendering.py tests/application/test_rendering.py
git commit -m "feat: show the drift a rejected table refused"
```

---

### Task 11: The machine projection carries the rejected changes too

`to_dict()` gives a rejected table `"changes": []`, which reads as "nothing drifted". Add a sibling field so a CI job or run-history store sees the same thing the text view now shows.

This is an **additive** field; `schema_version` stays `2`.

**Files:**
- Modify: `src/delta_engine/application/report.py:59-79`, `:203-226`
- Modify: `docs/reference-run-report.md`
- Test: `tests/application/test_report.py`

**Interfaces:**
- Produces: `TableRunReport.to_dict()["rejected_changes"]`, same record shape as `changes`.

- [ ] **Step 1: Write the failing test**

Append to `tests/application/test_report.py`:

```python
def test_a_rejected_table_projects_the_changes_it_refused():
    # Given a table whose diff was rejected
    qualified_name = QualifiedName("cat", "sch", "orders")
    desired = DesiredTable(
        qualified_name=qualified_name,
        columns=(
            DesiredColumn("id", Integer(), nullable=False),
            DesiredColumn("email", String(), nullable=False),
        ),
    )
    observed = ObservedTable(
        qualified_name=qualified_name,
        columns=(ObservedColumn("id", Integer(), nullable=False),),
    )
    report = TableRunReport(
        read=TablePresent(table=observed),
        planning=PlanningFailed(
            (ValidationFailure(rule_name="NonNullableColumnAdd", message="nope"),)
        ),
        planned_sql_statements=(),
        resolution=TableResolution(desired, (), ()),
        execution=None,
        diff=diff_table(desired, observed),
    )

    # When it is projected
    record = report.to_dict()

    # Then `changes` stays empty — nothing was planned — and the refused
    # differences are stated separately
    assert record["changes"] == []
    assert record["rejected_changes"] == [
        {
            "kind": "columns",
            "operation": "add",
            "subject": "email",
            "detail": "String NOT NULL",
        }
    ]


def test_a_successful_table_projects_no_rejected_changes():
    # Given a table that planned cleanly
    report = _successful_report()

    # Then the rejected list is empty rather than absent
    assert report.to_dict()["rejected_changes"] == []
```

Use whatever helper `test_report.py` already has for a clean report; find it with:

```bash
grep -n "^def _" tests/application/test_report.py
```

- [ ] **Step 2: Run the tests to verify they fail**

```bash
uv run pytest tests/application/test_report.py -k "rejected_changes or refused" -v --no-cov
```

Expected: 2 failed with `KeyError: 'rejected_changes'`.

- [ ] **Step 3: Write the implementation**

In `src/delta_engine/application/report.py`, add to the `diff_entries` import:

```python
from delta_engine.application.diff_entries import DiffEntry, action_entries, unresolvable_entries
```

Refactor `_change_records` (lines 59-79) so both projections share one shape:

```python
def _entry_records(entries: Iterable[DiffEntry]) -> list[dict[str, str]]:
    """Project interpreted diff entries as flat records, in the order given."""
    return [
        {
            "kind": entry.category.name.lower(),
            "operation": entry.operation.value,
            "subject": entry.subject,
            "detail": entry.detail,
        }
        for entry in entries
    ]


def _change_records(plan: ActionPlan | None) -> list[dict[str, str]]:
    """
    Summarise the plan as flat change records, in plan order.

    These share the interpretation vocabulary of the text renderers: they are
    human-oriented summaries, not one record per action (a CreateTable expands
    into several), and not a complete description of the change — the
    authoritative description is the planned SQL.
    """
    if plan is None:
        return []
    return _entry_records(entry for action in plan for entry in action_entries(action))


def _rejected_change_records(diff: TableDiff | None, plan: ActionPlan | None) -> list[dict[str, str]]:
    """
    Summarise the differences a rejected diff found, in diff order.

    Empty whenever a plan exists: an accepted diff's differences are its
    changes, already projected by ``_change_records``. Both of the diff's
    streams are included, because an unresolvable difference is frequently the
    reason the diff was rejected at all.
    """
    if plan is not None or not isinstance(diff, TableDrift):
        return []
    return _entry_records(
        [
            *(entry for action in diff.actions for entry in action_entries(action)),
            *(
                entry
                for unresolvable in diff.unresolvable
                for entry in unresolvable_entries(unresolvable)
            ),
        ]
    )
```

Add `Iterable` to the `collections.abc` import at line 8, and `TableDrift` to the `delta_engine.domain.plan` import.

In `TableRunReport.to_dict()` (line 217-226), add the field after `changes`:

```python
            "changes": _change_records(self.plan),
            "rejected_changes": _rejected_change_records(self.diff, self.plan),
```

- [ ] **Step 4: Run the tests to verify they pass**

```bash
uv run pytest tests/application/test_report.py -v --no-cov
```

Expected: all pass.

- [ ] **Step 5: Document the field**

In `docs/reference-run-report.md`, add a row to the table-level fields table, immediately after the `changes` row:

```markdown
| `rejected_changes`       | `list[dict]`     | Differences found but refused, when the plan was rejected; empty otherwise |
```

And add a paragraph immediately after the "Change records" section:

```markdown
### Rejected change records

When a table's diff is rejected, no plan exists, so `changes` is empty. The
differences the engine did find are projected into `rejected_changes` in the
same record shape, so a reader can see *what* was refused alongside the
`failures` list that says *why*. It includes both the actions the engine would
have taken and the differences no action can close (a column spelled
differently from the catalog, a property set but undeclared, a partitioning
change). It is always empty for a table that planned successfully.
```

Add to the "Stability" section, after the version 2 paragraph:

```markdown
`rejected_changes` was added without a version bump: adding a field is
backwards-compatible, and a reader that does not know the key sees exactly the
payload it saw before.
```

- [ ] **Step 6: Run every gate and commit**

```bash
uv run pytest -q
uv run mypy src/delta_engine/
uv run sphinx-build -W -b html docs docs/_build/html
git add src/delta_engine/application/report.py docs/reference-run-report.md tests/application/test_report.py
git commit -m "feat: project the changes a rejected table refused"
```

---

### Task 12: `UnmanagedAspectDrift` names the differences

The rule holds `drift.actions` and `drift.unresolvable` and deliberately collapses them to bare aspect labels with `dict.fromkeys` (`validation.py:546-551`), so the message is `column structure has drifted` and nothing more. Group the differences by aspect instead, and name them.

The remedy also changes. "Sync the table fully **or** update the declaration" tells a metadata-scoped author to do something their scope exists to prevent; the actionable half is the second.

**Files:**
- Modify: `src/delta_engine/application/validation.py:513-563`
- Modify: `docs/reference-safe-change-rules.md:76`
- Test: `tests/application/test_validation.py`

**Interfaces:**
- Consumes: `action_entries` and `unresolvable_entries` from `diff_entries`.

- [ ] **Step 1: Write the failing test**

Append to `tests/application/test_validation.py`:

```python
def test_unmanaged_drift_names_the_columns_that_drifted():
    # Given a metadata-scoped declaration against a table with column drift
    qualified_name = QualifiedName("cat", "sch", "orders")
    desired = DesiredTable(
        qualified_name=qualified_name,
        columns=(DesiredColumn("id", Long(), nullable=False),),
        managed_aspects=METADATA_ASPECTS,
    )
    observed = ObservedTable(
        qualified_name=qualified_name,
        columns=(
            ObservedColumn("id", Long(), nullable=False),
            ObservedColumn("legacy_region", String()),
        ),
    )

    # When the eligibility check judges it
    (failure,) = UnmanagedAspectDrift().evaluate(diff_table(desired, observed))

    # Then the message names the drifted column, not only the aspect
    assert "column structure" in failure.message
    assert "legacy_region" in failure.message


def test_unmanaged_drift_reports_one_failure_per_aspect():
    # Given drift in two unmanaged aspects at once
    qualified_name = QualifiedName("cat", "sch", "orders")
    desired = DesiredTable(
        qualified_name=qualified_name,
        columns=(DesiredColumn("id", Long(), nullable=False),),
        partitioned_by=("region",),
        managed_aspects=METADATA_ASPECTS,
    )
    observed = ObservedTable(
        qualified_name=qualified_name,
        columns=(
            ObservedColumn("id", Long(), nullable=False),
            ObservedColumn("legacy_region", String()),
        ),
        partitioned_by=("country",),
    )

    # When the check judges it
    failures = UnmanagedAspectDrift().evaluate(diff_table(desired, observed))

    # Then each aspect gets its own failure, each naming its own differences
    assert len(failures) == 2
    subjects = {failure.subject for failure in failures}
    assert subjects == {"column structure", "partitioning"}
```

Add whatever imports are missing: `METADATA_ASPECTS` from `delta_engine.application.scopes`, `diff_table` from `delta_engine.domain.plan`, and the domain model names.

- [ ] **Step 2: Run the tests to verify the first fails**

```bash
uv run pytest tests/application/test_validation.py -k "names_the_columns_that_drifted or one_failure_per_aspect" -v --no-cov
```

Expected: first FAILS (`assert "legacy_region" in failure.message`); second passes (the aspect split already works, only the wording changes).

- [ ] **Step 3: Write the implementation**

In `src/delta_engine/application/validation.py`, add to the imports:

```python
from delta_engine.application.diff_entries import DiffEntry, action_entries, unresolvable_entries
```

Add above the `UnmanagedAspectDrift` class:

```python
def _describe_entry(entry: DiffEntry) -> str:
    """Render one interpreted difference as a single indented display line."""
    return f"    {entry.symbol} {' '.join(entry.cells)}".rstrip()
```

Replace the body of `UnmanagedAspectDrift.evaluate` (lines 539-563):

```python
    def evaluate(self, diff: TableDiff) -> tuple[ValidationFailure, ...]:
        """Flag every drifted aspect the declaration does not manage."""
        match diff:
            case TableMissing():
                return ()

            case TableDrift() as drift:
                lines_by_aspect: dict[TableAspect, list[str]] = {}
                for difference, entries in self._interpreted(drift):
                    if difference.aspect in drift.desired.managed_aspects:
                        continue
                    lines_by_aspect.setdefault(difference.aspect, []).extend(
                        _describe_entry(entry) for entry in entries
                    )

                return tuple(
                    ValidationFailure(
                        rule_name=self.name,
                        message=self._message(aspect, lines),
                        subject=aspect.label,
                    )
                    for aspect, lines in lines_by_aspect.items()
                )

            case _ as unreachable:
                assert_never(unreachable)

    @staticmethod
    def _interpreted(
        drift: TableDrift,
    ) -> Iterator[tuple[Action | Unresolvable, tuple[DiffEntry, ...]]]:
        """
        Pair every difference with its display entries, in diff order.

        ``ColumnCaseDrift`` is skipped: a column spelled differently from the
        catalog is a defect in the declaration rather than drift in the column
        structure, and ``ColumnSpellingMustMatchCatalog`` names it at every
        scope.
        """
        for action in drift.actions:
            yield action, action_entries(action)
        for unresolvable in drift.unresolvable:
            if isinstance(unresolvable, ColumnCaseDrift):
                continue
            yield unresolvable, unresolvable_entries(unresolvable)

    @staticmethod
    def _message(aspect: TableAspect, lines: list[str]) -> str:
        """State which aspect drifted, exactly how, and what to do about it."""
        return "\n".join(
            [
                f"Operation not allowed: {aspect.label} has drifted but is not managed"
                " by this definition:",
                *lines,
                "  Update the declaration to match the live table, or widen its scope"
                " to manage this aspect.",
            ]
        )
```

Add `Iterator` to the `collections.abc` import, `TableAspect` to the domain-model import (it is already imported), and `Action` / `Unresolvable` to the `delta_engine.domain.plan` import.

Note `dict.setdefault` replaces `dict.fromkeys`: it preserves first-seen aspect order exactly as before, while accumulating the lines the old version discarded.

- [ ] **Step 4: Run the tests to verify they pass**

```bash
uv run pytest tests/application/test_validation.py -v --no-cov
```

Expected: all pass. Existing tests that assert `"has drifted"` or `"is not managed by this definition"` still match — those substrings survive verbatim. A test asserting the old trailing sentence (`"Sync the table fully"`) must be updated to the new wording; that is the intended change.

- [ ] **Step 5: Update the reference doc**

In `docs/reference-safe-change-rules.md`, change the `UnmanagedAspectDrift` row (line 76) remedy cell from:

```
Sync the table fully, or update the declaration to match the live schema
```

to:

```
Update the declaration to match the live table, or widen its scope to manage this aspect
```

- [ ] **Step 6: See it for real**

Re-run the script from Task 10 Step 5. Expected output now names the differences twice over — once in the DIFF block, once in the failure message:

```
Failures
--------
  main.sales.orders
    Validation failed: UnmanagedAspectDrift - Operation not allowed: column structure
    has drifted but is not managed by this definition:
        ~ customer_id Integer → Long
        + legacy_region String
      Update the declaration to match the live table, or widen its scope to manage
      this aspect.
```

- [ ] **Step 7: Run every gate and commit**

```bash
uv run pytest -q
uv run mypy src/delta_engine/
uv run ruff check src/ tests/
uv run lint-imports
uv run sphinx-build -W -b html docs docs/_build/html
git add src/delta_engine/application/validation.py docs/reference-safe-change-rules.md tests/application/test_validation.py
git commit -m "feat: name the differences that unmanaged aspect drift found"
```

---

### PR2 close-out — ✅ DONE

Merged as **#316**, based on `main` rather than PR1. Gates at merge: 1149 passed, coverage 96.52%, mypy, ruff check + format, `lint-imports` (7 kept), `sphinx-build -W`, plus a manual run of the metadata-scope scenario through the real engine.

<details>
<summary>The PR text as planned</summary>

```bash
git push -u origin feat/report-carries-the-diff
gh pr create --base fix/sync-failure-message-quality --title "feat: let a rejected table say what drifted" --body "$(cat <<'EOF'
Stacked on #<PR1>. The engine computed a complete `TableDiff` for every readable table and then discarded it, so a table whose plan was rejected could only ever say which rule refused it — never what it refused.

- `TableRunReport` carries the diff it was built from. A failed read still produces no diff, and that is now an invariant.
- `render_diff` shows a rejected table's differences under a `(REJECTED — no SQL planned)` header instead of `(no changes — see failures)`.
- `to_dict()` gains `rejected_changes` (additive; `schema_version` stays 2).
- `UnmanagedAspectDrift` enumerates the differences per aspect instead of naming the aspect alone, and its remedy no longer tells a metadata-scoped author to sync the table fully.

Verified: full suite, mypy, ruff, import-linter, `sphinx-build -W`, plus a manual run of the metadata-scope scenario from the issue.
EOF
)"
```

</details>

---

# PR 3 — `feat/real-run-report-honesty` — ❌ NOT STARTED

On a real run the renderers describe what was *planned* as though it happened. A table blocked by a failed dependency renders an identical `+` block to one that committed; the footer counts a partially-applied table as neither changed nor unchanged; and nothing says which statements were never attempted.

This matters for library callers — `render_diff` and `render_report` are both in `delta_engine.__all__`. The CLI is dry-run only and is unaffected.

## PR3 — what changed underneath it

**Read this before writing any of tasks 13–16.** They were written against `rendering.py` and `report.py` as they stood before #313 and #316, and several code blocks below would now undo work that has landed. Line numbers are all stale; locate by symbol.

1. **Task 13 step 7 would revert #313 and break #316.** Its replacement `render_failures_section` collapses every failure line to one indent level:

   ```python
   lines.extend(f"    {line}" for line in failure.format_lines())   # ← do NOT apply
   ```

   The current body deliberately splits head from supporting detail and nests the rest by eight spaces, and `ValidationFailure.details` (#316) depends on that nesting to render the drift lines under their failure. **Keep the existing loop; only append `_not_attempted_lines(report)` after it.**

2. **`report.statement_progress` already exists** (a `StatementProgress` named tuple of `applied`/`planned`). Task 14's `_outcome_marker` and task 15's `_grid_statements_cell` both hand-roll it from `execution.applied_count` and `len(planned_sql_statements)`. Use the property; it is the one place that pairing is derived.

3. **`_plan_creates_table(plan)` is gone** — task 14's `_diff_header` calls it. It is now `report.creates_table`, a property, which is why `_diff_header` can take the report alone.

4. **`render_diff_block` has a third branch now.** #316 split `plan is None` (rejected — renders the drift under a `(REJECTED — no SQL planned)` header) from an empty plan (a validated no-op). Task 14 rewrites "the last two lines of the function body" and adds a header composer; make sure the rejected branch keeps its own header and is not silently folded into `_diff_header`. **Decide deliberately whether a rejected table on a real run should read `(REJECTED — no SQL planned)`, `(REJECTED — not applied)`, or stay as it is** — the plan predates the branch existing, so it has no answer. The honest default is to leave it: nothing was planned, so there is no outcome to report.

5. **Task 16 depends on PR1 task 7.** It calls `_count_phrase`, which task 7 introduces. Do task 7 first. Note too that `SyncReport.counts` and `SyncReport.duration_seconds` now exist (#313) — `_planned_counts` should read `report.counts` rather than re-walking the tables, and the `seconds = (ended_at - started_at).total_seconds()` line is already a property.

6. **`entries = [entry for action in plan for entry in action_entries(action)]`** appears in task 14. That is now `plan_entries(plan)`.

```bash
git checkout main && git pull
git checkout -b feat/real-run-report-honesty
```

---

### Task 13: The report knows which statements never ran

After a mid-plan failure the table is partially migrated. The data to say so is present (`planned_sql_statements` and `execution.results`) but nothing derives it.

**Files:**
- Modify: `src/delta_engine/application/report.py` (add property), `src/delta_engine/application/rendering.py:161-172`
- Modify: `docs/how-to-handle-sync-failures.md`
- Test: `tests/application/test_report.py`, `tests/application/test_rendering.py`

**Interfaces:**
- Produces: `TableRunReport.unapplied_statements -> tuple[str, ...]`. Task 14 reads it.

- [ ] **Step 1: Write the failing test**

Append to `tests/application/test_report.py`:

```python
def test_a_partially_applied_table_names_the_statements_that_never_ran():
    # Given three planned statements where the second failed
    report = _report_with_statements(
        planned=("SQL 0", "SQL 1", "SQL 2"),
        execution=ExecutionSummary(
            results=(
                ExecutionSucceeded(statement_index=0, statement="SQL 0"),
                ExecutionFailure(
                    statement_index=1,
                    exception_type="AnalysisException",
                    message="boom",
                    statement="SQL 1",
                ),
            )
        ),
    )

    # Then the statements after the failure are named as never attempted
    assert report.unapplied_statements == ("SQL 2",)


def test_a_table_that_never_executed_has_every_statement_unapplied():
    # Given a table that was blocked before execution
    report = _report_with_statements(planned=("SQL 0", "SQL 1"), execution=None)

    # Then nothing ran, so nothing was applied
    assert report.unapplied_statements == ("SQL 0", "SQL 1")
```

Add a helper next to the file's existing builders:

```python
def _report_with_statements(*, planned, execution):
    qualified_name = QualifiedName("cat", "sch", "orders")
    desired = DesiredTable(qualified_name=qualified_name, columns=(DesiredColumn("id", Integer()),))
    observed = ObservedTable(
        qualified_name=qualified_name, columns=(ObservedColumn("id", Integer()),)
    )
    return TableRunReport(
        read=TablePresent(table=observed),
        planning=PlanningSucceeded(
            ActionPlan(
                target=qualified_name,
                actions=(SetTableComment(desired_comment="c", observed_comment=""),),
            )
        ),
        planned_sql_statements=planned,
        resolution=TableResolution(desired, (), ()),
        execution=execution,
    )
```

Note: `TableRunReport.__post_init__` requires executed statements to be a prefix of the planned tuple, which is why the fake statement text matches positionally.

- [ ] **Step 2: Run the tests to verify they fail**

```bash
uv run pytest tests/application/test_report.py -k "never_ran or never_executed" -v --no-cov
```

Expected: 2 failed with `AttributeError: 'TableRunReport' object has no attribute 'unapplied_statements'`.

- [ ] **Step 3: Write the implementation**

In `src/delta_engine/application/report.py`, add to `TableRunReport` after the `plan` property (line 151):

```python
    @property
    def unapplied_statements(self) -> tuple[str, ...]:
        """
        Planned statements this run did not execute, in plan order.

        The executor stops at the first failure, so these are the statements
        after it — the work a retry still has to do, and the measure of how far
        a partially-migrated table is from its declaration. Every planned
        statement is unapplied when execution never ran at all: a dry run, a
        rejected plan, or a table blocked by a failed dependency.
        """
        if self.execution is None:
            return self.planned_sql_statements
        return self.planned_sql_statements[len(self.execution.results) :]
```

- [ ] **Step 4: Run the tests to verify they pass**

```bash
uv run pytest tests/application/test_report.py -k "never_ran or never_executed" -v --no-cov
```

Expected: 2 passed.

- [ ] **Step 5: Write the renderer test**

Append to `tests/application/test_rendering.py`:

```python
def test_the_failures_section_names_the_statements_that_were_not_attempted():
    # Given a table where statement 2 of 3 failed
    report = _grid_report(
        "orders",
        plan=_plan(
            "orders",
            SetTableComment(desired_comment="c", observed_comment=""),
            AddColumn(DesiredColumn("age", Integer())),
            AddColumn(DesiredColumn("city", String())),
        ),
        execution=_execution(applied=1, failed=1),
    )

    # When the failures section is rendered
    section = render_failures_section((report,))

    # Then the statement that never ran is named, so a reader knows what a
    # retry still has to do
    assert "Not attempted:" in section
    assert "SQL 2" in section


def test_the_failures_section_omits_the_not_attempted_block_when_nothing_remains():
    # Given a table whose last statement was the one that failed
    report = _grid_report(
        "orders",
        plan=_plan("orders", SetTableComment(desired_comment="c", observed_comment="")),
        execution=_execution(applied=0, failed=1),
    )

    # Then there is no empty block to read past
    assert "Not attempted:" not in render_failures_section((report,))
```

Add `render_failures_section` to the `delta_engine.application.rendering` import block.

- [ ] **Step 6: Run the tests to verify the first fails**

```bash
uv run pytest tests/application/test_rendering.py -k "not_attempted" -v --no-cov
```

Expected: 1 failed, 1 passed.

- [ ] **Step 7: Extend the failures section**

In `src/delta_engine/application/rendering.py`, replace `render_failures_section` (lines 161-172):

```python
def render_failures_section(reports: tuple[TableRunReport, ...]) -> str:
    """Render full per-table failure detail for every failed table; empty when none failed."""
    failed = [report for report in reports if report.has_failures]
    if not failed:
        return ""
    blocks: list[str] = []
    for report in failed:
        lines = [f"  {report.qualified_name}"]
        for failure in report.failures:
            lines.extend(f"    {line}" for line in failure.format_lines())
        lines.extend(_not_attempted_lines(report))
        blocks.append("\n".join(lines))
    return "\n".join([_heading("Failures", rule="-"), *blocks])


def _not_attempted_lines(report: TableRunReport) -> list[str]:
    """
    Name the statements a part-way failure left un-run.

    Only for a table that actually started executing: a table that ran nothing
    is fully described by its failure, while a table stopped mid-plan leaves
    the catalog between two states and the remaining statements are what a
    retry still owes.
    """
    if report.execution is None or not report.unapplied_statements:
        return []
    return [
        "    Not attempted:",
        *(f"        {statement}" for statement in report.unapplied_statements),
    ]
```

- [ ] **Step 8: Document it**

In `docs/how-to-handle-sync-failures.md`, replace the "Act on execution failures" paragraph (line 106) with:

```markdown
Execution failures are partial: statements before the failure ran and
committed; statements after were not attempted.
`table_report.unapplied_statements` names exactly those, in plan order, so a
caller can log or diff the work a retry still owes. Tables whose foreign keys
depend on the failed table are blocked in the same run and report
`FOREIGN_KEY_FAILED`. Fix the root cause and re-run — the engine re-reads live
state and plans only the remaining drift.
```

- [ ] **Step 9: Run every gate and commit**

```bash
uv run pytest -q
uv run mypy src/delta_engine/
uv run sphinx-build -W -b html docs docs/_build/html
git add src/delta_engine/application/report.py src/delta_engine/application/rendering.py docs/how-to-handle-sync-failures.md tests/
git commit -m "feat: name the statements a partial failure never attempted"
```

---

### Task 14: The diff says what happened to each table on a real run

`render_diff_block` cannot distinguish a dry run from a real one — `execution is None` means "not run yet" on one and "never attempted" on the other — because `dry_run` lives on `SyncReport`. Pass it down.

**Files:**
- Modify: `src/delta_engine/application/rendering.py:57-70`, `:187-190`
- Test: `tests/application/test_rendering.py`

**Interfaces:**
- Produces: `render_diff_block(report: TableRunReport, *, dry_run: bool = True) -> str`. Keyword-only with a default, so existing call sites and tests keep working and keep their current output.

- [ ] **Step 1: Write the failing test**

Append to `tests/application/test_rendering.py`:

```python
def test_a_real_run_marks_a_block_that_was_only_partly_applied():
    # Given a table where one of three statements committed before a failure
    report = _grid_report(
        "orders",
        plan=_plan(
            "orders",
            SetTableComment(desired_comment="c", observed_comment=""),
            AddColumn(DesiredColumn("age", Integer())),
            AddColumn(DesiredColumn("city", String())),
        ),
        execution=_execution(applied=1, failed=1),
    )

    # Then the header states how far the run actually got
    header = render_diff_block(report, dry_run=False).splitlines()[0]
    assert "partially applied, 1/3" in header


def test_a_real_run_marks_a_block_that_never_executed():
    # Given a table blocked by a failed dependency: a valid plan, no execution
    report = _grid_report(
        "orders", plan=_plan("orders", AddColumn(DesiredColumn("age", Integer())))
    )
    blocked = replace(
        report,
        blocked_failures=(
            ForeignKeyFailure(
                table=QualifiedName("cat", "sch", "orders"),
                local_columns=("customer_id",),
                references=QualifiedName("cat", "sch", "customers"),
                reason=ForeignKeyFailureReason.BLOCKED_BY_FAILED_DEPENDENCY,
            ),
        ),
    )

    # Then the block does not read as though the change happened
    header = render_diff_block(blocked, dry_run=False).splitlines()[0]
    assert "not applied" in header


def test_a_dry_run_block_carries_no_outcome_marker():
    # Given the same plan previewed rather than executed
    report = _grid_report(
        "orders", plan=_plan("orders", AddColumn(DesiredColumn("age", Integer())))
    )

    # Then nothing claims an outcome — the run banner already says it is a plan
    assert render_diff_block(report, dry_run=True).splitlines()[0] == "cat.sch.orders"
```

Add `from dataclasses import replace` and `ForeignKeyFailure`, `ForeignKeyFailureReason` to the `delta_engine.application.failures` import block.

- [ ] **Step 2: Run the tests to verify they fail**

```bash
uv run pytest tests/application/test_rendering.py -k "real_run_marks or dry_run_block_carries" -v --no-cov
```

Expected: 2 failed (`TypeError: unexpected keyword argument 'dry_run'`), 1 failed the same way.

- [ ] **Step 3: Write the implementation**

In `src/delta_engine/application/rendering.py`, add above `render_diff_block`:

```python
def _outcome_marker(report: TableRunReport) -> str:
    """
    State what a real run actually did to this table, or nothing when it did all of it.

    A diff block describes intent. On a real run, intent and outcome can differ
    — a blocked table runs nothing, a failed one stops mid-plan — and a block
    rendered without saying so reads as a change that happened.
    """
    if report.execution is None:
        return "not applied" if report.has_failures else ""
    if not report.execution.failures:
        return ""
    applied = report.execution.applied_count
    return f"partially applied, {applied}/{len(report.planned_sql_statements)}"


def _diff_header(name: str, plan: ActionPlan, *, dry_run: bool, report: TableRunReport) -> str:
    """Compose the block header from the plan's nature and the run's outcome."""
    notes = []
    if _plan_creates_table(plan):
        notes.append("CREATE")
    if not dry_run and (marker := _outcome_marker(report)):
        notes.append(marker)
    return f"{name}  ({' — '.join(notes)})" if notes else name
```

Change `render_diff_block`'s signature and its final branch:

```python
def render_diff_block(report: TableRunReport, *, dry_run: bool = True) -> str:
    """
    Render one table's change block: its name then its changes, grouped.

    ``dry_run`` decides whether the header may claim an outcome. A previewed
    plan has none to claim; an executed one does, and a block that stays silent
    about a blocked or half-applied table reads as a change that happened.
    """
```

and replace the last two lines of the function body:

```python
    entries = [entry for action in plan for entry in action_entries(action)]
    return "\n".join(
        [_diff_header(header, plan, dry_run=dry_run, report=report), *_render_entry_groups(entries)]
    )
```

(delete the old `if _plan_creates_table(plan): header = f"{header}  (CREATE)"` lines — `_diff_header` now owns that.)

Change `render_diff` (line 187-190) to pass the flag down:

```python
def render_diff(report: SyncReport) -> str:
    """Render every table's planned changes as +/-/~ blocks, under a DIFF title."""
    blocks = [
        render_diff_block(table_report, dry_run=report.dry_run)
        for table_report in report.table_reports
    ]
    return "\n\n".join([_heading("DIFF"), *blocks])
```

- [ ] **Step 4: Run the full suite**

```bash
uv run pytest -q
```

Expected: all pass. The existing `test_diff_block_marks_a_create_in_the_header` still asserts `"cat.sch.orders  (CREATE)"`, which `_diff_header` produces unchanged for the default `dry_run=True`.

- [ ] **Step 5: Commit**

```bash
git add src/delta_engine/application/rendering.py tests/application/test_rendering.py
git commit -m "feat: mark what a real run actually applied to each table"
```

---

### Task 15: A blocked table's statement count is a number, not a dash

`_grid_statements_cell` (`rendering.py:73-79`) returns `—` for any failed table with no execution. For a table blocked by a dependency that is misleading: it has a valid compiled plan that was deliberately skipped, and `—` reads as "nothing to do" rather than "two statements skipped".

**Files:**
- Modify: `src/delta_engine/application/rendering.py:73-79`
- Test: `tests/application/test_rendering.py`

- [ ] **Step 1: Write the failing test**

Append to `tests/application/test_rendering.py`:

```python
def test_a_blocked_table_shows_how_much_work_was_skipped():
    # Given a table with a valid two-statement plan, blocked by a dependency
    report = _grid_report(
        "orders",
        plan=_plan(
            "orders",
            AddColumn(DesiredColumn("age", Integer())),
            AddColumn(DesiredColumn("city", String())),
        ),
    )
    blocked = replace(
        report,
        blocked_failures=(
            ForeignKeyFailure(
                table=QualifiedName("cat", "sch", "orders"),
                local_columns=("customer_id",),
                references=QualifiedName("cat", "sch", "customers"),
                reason=ForeignKeyFailureReason.BLOCKED_BY_FAILED_DEPENDENCY,
            ),
        ),
    )

    # Then the STATEMENTS cell counts the skipped work rather than erasing it
    row = next(
        line for line in render_grid((blocked,)).splitlines() if line.startswith("cat.sch.orders")
    )
    assert "0/2" in row


def test_a_table_that_failed_before_planning_still_shows_a_dash():
    # Given a table rejected at validation, which compiled nothing
    report = _grid_report("orders", failures=(ValidationFailure(rule_name="R", message="m"),))

    # Then there is no count to give
    row = next(
        line for line in render_grid((report,)).splitlines() if line.startswith("cat.sch.orders")
    )
    assert "—" in row
```

- [ ] **Step 2: Run the tests to verify the first fails**

```bash
uv run pytest tests/application/test_rendering.py -k "how_much_work_was_skipped or still_shows_a_dash" -v --no-cov
```

Expected: 1 failed (the row holds `—`), 1 passed.

- [ ] **Step 3: Write the implementation**

Replace `_grid_statements_cell` (lines 73-79):

```python
def _grid_statements_cell(report: TableRunReport) -> str:
    """
    STATEMENTS cell: applied over planned, or an em dash when nothing was compiled.

    A table that compiled statements has a denominator worth stating even when
    none of them ran — a blocked table's ``0/2`` is the size of the work its
    dependency cost it. Only a table that never got as far as compiling has
    nothing to count.
    """
    planned = len(report.planned_sql_statements)
    if report.execution is not None:
        return f"{report.execution.applied_count}/{planned}"
    if report.has_failures:
        return f"0/{planned}" if planned else "—"
    return str(planned)
```

- [ ] **Step 4: Run the full suite**

```bash
uv run pytest -q
```

Expected: all pass. Existing grid tests use tables that failed at validation (no compiled statements), so they still get `—`.

- [ ] **Step 5: Commit**

```bash
git add src/delta_engine/application/rendering.py tests/application/test_rendering.py
git commit -m "fix: count the statements a blocked table skipped"
```

---

### Task 16: The footer counts what happened, not what was planned

`run_summary_footer` classifies a table as `changed` when it holds a plan. On a dry run that is the right question. On a real run it is not: a table that committed two of three statements is counted under `failed` only, so nothing in the summary says the catalog was mutated at all.

**Files:**
- Modify: `src/delta_engine/application/rendering.py:133-148`
- Test: `tests/application/test_rendering.py:640-655`

- [ ] **Step 1: Write the failing test**

Append to `tests/application/test_rendering.py`:

```python
def test_a_real_run_footer_says_what_reached_the_catalog():
    # Given a real run: one table applied, one part-applied, one blocked
    applied = _grid_report(
        "a", plan=_plan("a", AddColumn(DesiredColumn("age", Integer())))
    )
    applied = replace(applied, execution=_execution(applied=1, failed=0))
    partial = _grid_report(
        "b",
        plan=_plan(
            "b",
            AddColumn(DesiredColumn("age", Integer())),
            AddColumn(DesiredColumn("city", String())),
        ),
        execution=_execution(applied=1, failed=1),
    )
    blocked = replace(
        _grid_report("c", plan=_plan("c", AddColumn(DesiredColumn("age", Integer())))),
        blocked_failures=(
            ForeignKeyFailure(
                table=QualifiedName("cat", "sch", "c"),
                local_columns=("x",),
                references=QualifiedName("cat", "sch", "a"),
                reason=ForeignKeyFailureReason.BLOCKED_BY_FAILED_DEPENDENCY,
            ),
        ),
    )
    sync = SyncReport(
        started_at=datetime(2025, 1, 1, 0, 0, 0),
        ended_at=datetime(2025, 1, 1, 0, 0, 3),
        table_reports=(applied, partial, blocked),
        dry_run=False,
    )

    # Then the footer distinguishes the catalog that was touched from the one
    # that was not
    assert run_summary_footer(sync) == (
        "3 tables: 1 applied, 1 partially applied, 1 not applied (3.0s)"
    )


def test_a_dry_run_footer_still_counts_planned_work():
    # Given a preview over one changed, one unchanged and one failed table
    changed = _grid_report(
        "a", plan=_plan("a", SetTableComment(desired_comment="c", observed_comment=""))
    )
    unchanged = _grid_report("b")
    failed = _grid_report("c", failures=(ValidationFailure(rule_name="R", message="m"),))
    sync = SyncReport(
        started_at=datetime(2025, 1, 1, 0, 0, 0),
        ended_at=datetime(2025, 1, 1, 0, 0, 3),
        table_reports=(changed, unchanged, failed),
        dry_run=True,
    )

    # Then the wording is unchanged: a plan has no outcome to report
    assert run_summary_footer(sync) == "3 tables: 1 changed, 1 unchanged, 1 failed (3.0s)"
```

- [ ] **Step 2: Run the tests to verify the first fails**

```bash
uv run pytest tests/application/test_rendering.py -k "reached_the_catalog or still_counts_planned" -v --no-cov
```

Expected: 1 failed, 1 passed.

- [ ] **Step 3: Write the implementation**

Replace `run_summary_footer` (lines 133-148):

```python
def run_summary_footer(report: SyncReport) -> str:
    """
    One-line summary: table total, per-outcome counts, duration.

    A dry run and a real run are summarised by different questions. A preview
    can only report what it *would* do, so it counts plans: changed, unchanged,
    failed. A real run can report what it *did*, and must — a table that
    committed two of its three statements is neither simply "changed" nor
    simply "failed", and a summary that cannot say so hides a mutated catalog.
    """
    seconds = (report.ended_at - report.started_at).total_seconds()
    total = _count_phrase(len(report.table_reports), "table", "tables")
    counts = _planned_counts(report) if report.dry_run else _applied_counts(report)
    return f"{total}: {counts} ({seconds:.1f}s)"


def _planned_counts(report: SyncReport) -> str:
    """Classify each table by the plan it holds: the only question a preview can answer."""
    changed = unchanged = failed = 0
    for table_report in report.table_reports:
        if table_report.has_failures:
            failed += 1
        elif table_report.plan:
            changed += 1
        else:
            unchanged += 1
    return f"{changed} changed, {unchanged} unchanged, {failed} failed"


def _applied_counts(report: SyncReport) -> str:
    """Classify each table by how much of its plan reached the catalog."""
    applied = partial = not_applied = unchanged = 0
    for table_report in report.table_reports:
        execution = table_report.execution
        if execution is not None and execution.failures:
            partial += 1
        elif table_report.has_failures:
            not_applied += 1
        elif execution is not None:
            applied += 1
        else:
            unchanged += 1
    parts = [
        (applied, "applied"),
        (partial, "partially applied"),
        (not_applied, "not applied"),
        (unchanged, "unchanged"),
    ]
    return ", ".join(f"{count} {label}" for count, label in parts if count)
```

Note: `_applied_counts` omits zero categories so a clean run reads `3 tables: 3 applied (1.2s)` rather than trailing three zeroes. `_planned_counts` keeps all three unconditionally, preserving the existing dry-run wording exactly.

- [ ] **Step 4: Run the full suite**

```bash
uv run pytest -q
```

Expected: all pass. The three pre-existing footer assertions construct `SyncReport` without `dry_run`, which defaults to `False`… **check this**: `SyncReport.dry_run` defaults to `False`, so those tests take the real-run branch and will now fail. Update each to pass `dry_run=True`, since every one of them describes a preview (they assert `changed`/`unchanged`/`failed` wording):

- `tests/application/test_rendering.py:646` — add `dry_run=True,`
- `tests/application/test_rendering.py:670` — add `dry_run=True,`
- `tests/application/test_rendering.py:686` — add `dry_run=True,`

- [ ] **Step 5: Run the full suite again**

```bash
uv run pytest -q
uv run mypy src/delta_engine/
```

Expected: all pass.

- [ ] **Step 6: See the whole thing**

```bash
uv run python - <<'PY'
from delta_engine.adapters.databricks.sql.compile import compile_plan
from delta_engine.application.engine import Engine
from delta_engine.application.errors import ExecutionError, SyncFailedError
from delta_engine.application.ports import TableAbsent
from delta_engine.application.rendering import render_diff, render_report
from delta_engine.domain.model import QualifiedName
from delta_engine.schema import Column, DeltaTable, ForeignKey, Long, String

class R:
    def fetch_state(self, q): return TableAbsent()

class E:
    def __init__(self): self.n = 0
    def compile(self, plan): return compile_plan(plan)
    def execute(self, s):
        self.n += 1
        if self.n == 3:
            raise ExecutionError(exception_type="ServerOperationError", message="boom")

customers = DeltaTable("main", "sales", "customers",
    columns=(Column("id", Long(), nullable=False), Column("name", String())),
    primary_key=["id"], comment="customers", tags={"domain": "sales", "tier": "gold"})
orders = DeltaTable("main", "sales", "orders",
    columns=(Column("id", Long(), nullable=False), Column("customer_id", Long())),
    primary_key=["id"],
    foreign_keys=[ForeignKey(columns={"customer_id": "id"}, references=customers)])

try:
    Engine(reader=R(), executor=E()).sync(customers, orders)
except SyncFailedError as error:
    print(render_diff(error.report)); print(); print(render_report(error.report))
PY
```

Expected: `customers` header marked `(CREATE — partially applied, 2/3)`, `orders` marked `(CREATE — not applied)`, the failures section naming the un-run statements, `orders` showing `0/2`, and the footer reading `2 tables: 1 partially applied, 1 not applied`.

- [ ] **Step 7: Commit**

```bash
git add src/delta_engine/application/rendering.py tests/application/test_rendering.py
git commit -m "feat: summarise a real run by what reached the catalog"
```

---

### PR3 close-out

- [ ] **Run every gate**

```bash
uv run pytest -q
uv run mypy src/delta_engine/
uv run ruff check src/ tests/
uv run ruff format --check src/ tests/
uv run lint-imports
uv run sphinx-build -W -b html docs docs/_build/html
```

- [ ] **Open the PR**

```bash
git push -u origin feat/real-run-report-honesty
gh pr create --base main --title "feat: report a real run by what happened, not what was planned" --body "$(cat <<'EOF'
Not stacked — #316 landed on `main`, so this branches from `main` too. `render_diff` and `render_report` are public API, so library callers see them after real runs — where they described intent as though it were outcome.

- `TableRunReport.unapplied_statements` names the work a partial failure left un-run; the failures section lists it.
- Diff block headers state a real run's outcome per table (`(CREATE — partially applied, 2/3)`, `(CREATE — not applied)`). Dry runs are unchanged.
- A blocked table's STATEMENTS cell reads `0/2` instead of an em dash.
- The run summary counts applied / partially applied / not applied on a real run, and keeps changed / unchanged / failed on a dry run.

The CLI is dry-run only and its output is byte-for-byte unchanged.

Verified: full suite, mypy, ruff, import-linter, `sphinx-build -W`, plus a manual mid-plan failure run.
EOF
)"
```

---

## Deliberately not doing

Recorded so a future reader knows these were considered, not missed.

- **`delta-engine plan --json`.** Raised and dropped by explicit decision. `to_dict()` stays reachable from Python only; the CLI's text output and exit-code contract are untouched.
- **`--fail-on-changes` on the CLI.** Would contradict `how-to-gate-changes-in-ci.md`, which states that pending valid changes exit successfully. Out of scope with the rest of the CLI work.
- **"statement 3 of 7" in execution failure messages.** `ExecutionFailure` does not know the plan's length, and adding a required field to a public frozen dataclass is a breaking change for a marginal gain. Task 4 makes the number 1-based, and Task 13's "Not attempted" block supplies the total context instead.
- **`subject` in the `to_dict()` failure record.** The JSON already carries the full `message`, which names the subject. Extending a versioned contract for a redundant field is not worth it. Task 6 keeps `subject` a display concern.
- **Line-level applied/skipped annotation in the diff.** One `CreateTable` action expands into many diff entries but compiles to a single statement, so there is no entry-to-statement mapping to render from. Building one is a much larger change than the honesty problem justifies; Task 14's per-table markers use the attribution that does exist.
- **The metadata-scope property asymmetry.** A `metadata`-scoped declaration silently ignores property drift while hard-failing on column drift. Verified as deliberate and documented in `how-to-deploy-metadata-only.md`.

## Risks

- **Test churn is concentrated in `tests/application/test_rendering.py`.** Tasks 14, 15 and 16 all change what the renderers emit; Task 16 in particular requires adding `dry_run=True` to three existing footer tests. Run the full suite after every task rather than at the end of a PR.
- ~~**`UnmanagedAspectDrift` messages become multi-line.**~~ **Resolved in #316.** The drift lines are `ValidationFailure.details`, carrying no indentation of their own, so the JSON projection flattens to a clean single line: `Validation failed: UnmanagedAspectDrift - Operation not allowed: column structure has drifted … - legacy_region ~ customer_id Integer → Long`. Note `_failure_records` no longer calls `.strip()` — #313 removed it once nothing emitted pre-indented lines, which is the reason the `details` design had to keep indentation out.
- **`_grid_detail` truncates at 60 characters.** Task 6's longer headlines (`Validation failed: NonNullableColumnAdd (email)` is 46) fit, but a long property key as a subject could push a headline past the limit and get an ellipsis. That is the existing, intended behaviour of the grid; the failures section carries the full text.
