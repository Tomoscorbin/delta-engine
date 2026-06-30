# Task 2 Report: Replace `validation`/`foreign_key_failures` with `pre_execution_failures` on `TableRunReport`

## Files Changed

- `src/delta_engine/application/results.py` — Replaced `TableRunReport` dataclass fields `validation: ValidationResult` and `foreign_key_failures: tuple[ForeignKeyFailure, ...]` with `pre_execution_failures: tuple[Failure, ...] = ()`. Updated `status` property to inspect `pre_execution_failures` with `isinstance` (FK failures take priority over validation failures). Updated `all_failures` to extend from `pre_execution_failures` directly.

- `src/delta_engine/application/engine.py` — Updated `TableRunReport` construction in `sync()` to merge `candidate.failures` (FK failures from resolution) with `validations[...].failures` (validation failures) into `pre_execution_failures`. Removed `ForeignKeyFailure` from imports (no longer needed after removing the `isinstance` filter). `ValidationResult` import retained — still used as return type annotation on `_validate`.

- `tests/application/test_results.py` — Replaced the three `TableRunReport` construction tests (lines 191–348) with seven new tests covering the unified field. Added tests for: FK-only failure, validation-only failure, mixed FK+validation failure with priority checking, and no-pre-execution-failures success. Removed `validation=` and `foreign_key_failures=` from all `TableRunReport` constructions. `ValidationResult` import retained (still used by `test_validation_result_failed_property_reflects_presence_of_failures`).

- `tests/application/test_errors.py` — Replaced entirely. Switched helper `_table_report()` from `validation: ValidationResult` to `pre_execution_failures: tuple`. Added `test_message_renders_fk_failure_detail` covering the `FOREIGN_KEY_FAILED` status path. Removed `ValidationResult` import.

- `tests/application/test_engine.py` — Updated four tests that accessed `tr.foreign_key_failures` directly to use `tr.pre_execution_failures` filtered by `isinstance(f, ForeignKeyFailure)`. Added `ForeignKeyFailure` to imports.

## Test Commands and Output

### Target tests

```
$ uv run pytest tests/application/test_results.py tests/application/test_errors.py --no-cov -v
26 passed in 0.30s
```

### Full suite

```
$ uv run pytest --no-cov -q
305 passed, 16 errors in 4.92s
```

The 16 errors are all `PySparkRuntimeError: [JAVA_GATEWAY_EXITED]` — PySpark integration tests that require a running JVM. Pre-existing and unrelated to this change.

### Ruff

```
$ uv run ruff check src/ tests/
All checks passed!
```

## Deviations from Brief

- `test_table_run_report_status_is_foreign_key_failed_when_both_fk_and_validation_failures_present` was shortened to `test_table_run_report_status_is_fk_failed_when_both_fk_and_validation_failures_present` to satisfy the project's 100-character line-length limit (ruff E501).
- `test_engine.py` was not listed in the brief's file list but required updates to four FK-related assertions. All changes are mechanical: `tr.foreign_key_failures` → filtered comprehension over `tr.pre_execution_failures`.

## Concerns

None. All application tests pass. Linting is clean.

---

## Post-review Fix: Specific tuple type hint in `_table_report` helper

**Commit:** `6a77597`

**What changed:** In `tests/application/test_errors.py`, the `_table_report` helper had `pre_execution_failures: tuple = ()` (bare `tuple`). Changed to `pre_execution_failures: tuple[Failure, ...] = ()` and added `Failure` to the imports from `delta_engine.application.results`.

**Test output:** `6 passed in 0.15s` — all tests in `tests/application/test_errors.py` pass. `ruff check src/ tests/` passes clean.

---

# Task 2 (Phase 2): Reorder engine phases and rename `_apply_validation` → `_validate_plans`

**Commit:** `d9d69f3`

## Files Changed

- `src/delta_engine/application/engine.py` — Replaced the four-phase design (resolve → read → plan → validate+mutate → execute) with a five-phase design (read → plan → validate_plans → resolve → execute). Key changes:
  - `_read` and `_plan` now take `tuple[DesiredTable, ...]` instead of `tuple[SyncCandidate, ...]`; they no longer depend on FK resolution having run first.
  - `_apply_validation` (mutated candidates in place) replaced by `_validate_plans` (returns `dict[QualifiedName, tuple[Failure, ...]]`).
  - `sync()` calls `resolve(tables, external_failures=validation_failures)` so validation failures propagate to FK dependents as `BLOCKED_BY_FAILED_DEPENDENCY`.
  - `SyncCandidate` import removed (no longer needed in engine.py).
  - `Failure` added to imports (needed as return type for `_validate_plans`).
  - Module docstring updated to describe the five-phase flow and the rationale for the new ordering.

- `tests/application/test_engine.py` — Added one new test: `test_validation_failure_in_upstream_blocks_fk_dependent`. It verifies that when `customers` fails validation (adds NOT NULL to existing table) and `orders` has a FK on `customers`, the engine correctly reports `customers` as `VALIDATION_FAILED` and `orders` as `FOREIGN_KEY_FAILED` with `BLOCKED_BY_FAILED_DEPENDENCY`, with neither table executing.

## Test Output

### New test (pre-implementation — confirms it failed)

```
FAILED tests/application/test_engine.py::test_validation_failure_in_upstream_blocks_fk_dependent
orders.status was SUCCESS instead of FOREIGN_KEY_FAILED
```

### New test (post-implementation)

```
PASSED tests/application/test_engine.py::test_validation_failure_in_upstream_blocks_fk_dependent
```

### Full engine suite

```
20 passed in 0.13s
```

### Full suite

```
309 passed, 16 errors in 4.48s
```

The 16 errors are pre-existing PySpark JVM errors unrelated to this change.

### Ruff

```
All checks passed!
```

## Deviations from Brief

None. The brief specified the exact replacement content for `engine.py` and the exact test to add. Both were applied verbatim.

## Concerns

None. All tests pass. Linting is clean. The existing `test_sync_surfaces_both_fk_and_validation_failures_for_a_blocked_table` passes unchanged — with the new design, `_validate_plans` runs on all tables and passes results into `resolve()` as `external_failures`, which prepends them to `candidate.failures`, so both FK and validation failures still surface together.

---

# Task 2 (Final Review Fix Pass): Read failures propagate to FK dependents; FakeSpark simplified; stale docstring corrected

## Issues Fixed

### Issue 1 (Important): Read-failed tables now block FK dependents

**File:** `src/delta_engine/application/engine.py`

In `sync()`, after computing `validation_failures`, a merged `external_failures` dict is built. For any table whose `catalog_states` entry is `ReadFailed` and that has no existing external failure entry, the `ReadFailed.failure` (`ReadFailure`, a `Failure` subclass) is inserted as a single-element tuple. This dict is then passed to `resolve()`. The FK propagation pass in `_classify_failures` seeds `already_failed` from non-empty entries, so read-failed upstreams now correctly block their dependents with `BLOCKED_BY_FAILED_DEPENDENCY`.

No new imports were needed: `ReadFailure` is already a `Failure` subclass, and `ReadFailed` was already imported.

**Regression test added:** `test_read_failure_in_upstream_blocks_fk_dependent` in `tests/application/test_engine.py`. Table A is `ReadFailed`; table B has a FK on A. Asserts: A is `READ_FAILED`, B is `FOREIGN_KEY_FAILED` with `BLOCKED_BY_FAILED_DEPENDENCY`, executor is never called.

### Issue 2 (Important): Stale `SyncCandidate.failures` docstring

**File:** `src/delta_engine/application/foreign_key_planning.py`

Updated the `failures` attribute docstring from the old intermediate-design wording ("the engine may append validation failures before gating execution") to the accurate final-design wording ("Assembled by `resolve()`; the engine does not modify this after construction").

### Issue 3 (Important): `FakeSpark.sql()` table-identity bug in FK reader tests

**File:** `tests/adapters/databricks/test_reader.py`

`FakeSpark` now accepts `fk_rows: list | None = None` (flat list of rows) instead of a dict. The `sql()` handler returns `FakeDataFrame(self._fk_rows or [])` directly. The two existing test usages of `FakeSpark(fk_rows={"cat.sch.orders": rows})` were updated to `FakeSpark(fk_rows=rows)`.

## Test Output

### Targeted tests (47 collected)

```
47 passed in 0.24s
```

### Full suite

```
310 passed, 16 errors in 4.65s
```

(16 pre-existing JVM/PySpark errors, unrelated to these changes.)

### Ruff

```
All checks passed!
```

## Deviations from Brief

None.

## Concerns

None. All tests pass. Linting is clean.
