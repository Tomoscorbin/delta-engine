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
