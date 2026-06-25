# Design Review

A deep review of delta-engine against John Ousterhout's *A Philosophy of Software Design* (APoSD), conducted 2026-06-25 against commit `25bacd0`.

## Verdict

delta-engine is a strong example of the philosophy it set out to follow. The domain layer is pure and Spark-free. Ports are narrow. `compute_plan` and `validate_plan` are deep functions: a simple signature over real logic. The `ExecutionSucceeded | ExecutionFailed` union makes "succeeded but carries a failure" unrepresentable — defining errors out of existence, done right.

The findings below are refinements, not rework. Two are correctness hazards worth acting on. The rest are mechanical cleanups and documentation balance. The architecture should stay as it is.

Method: nine reviewers each applied a distinct APoSD lens. Every finding was then re-checked against the source by a separate pass. Of roughly 78 raised findings, 49 survived; 29 were rejected as taste, documented intent, or worse-if-changed.

## Priority 1 — Correctness hazards

### One unmappable column type makes the whole table unmanageable

`DatabricksReader._read` wraps the entire read in a single `try/except` ([reader.py:83](../src/delta_engine/adapters/databricks/reader.py#L83)). `domain_type_from_spark` raises `TypeError` for any type outside its match arms ([types.py:109](../src/delta_engine/adapters/databricks/sql/types.py#L109)). A single `STRUCT`, `TimestampNTZType`, `BinaryType`, or `VariantType` column therefore turns the table into `ReadFailed`. The engine then cannot manage any other column or any property on that table.

These types are common in PySpark 4.0 production tables. A table with one nested column and ten simple ones the user wants to manage becomes wholly unmanageable.

**Fix:** Catch `TypeError` per column in `_to_column_mapping` and skip the unmappable column instead of failing the table. Log a warning naming the column and its Spark type. This matches the existing "properties are a declared subset" design: manage what you understand, ignore the rest. Document the supported type set in the README. Do not add an `Unsupported` sentinel to the domain — that would pollute the pure layer.

### Properties may never reach a no-op on adopted tables

`DeltaTable.default_properties` injects `delta.enableDeletionVectors=true` and `delta.columnMapping.mode=name` into every desired state ([table.py:22](../src/delta_engine/schema/table.py#L22)). `_fetch_properties` reads the `properties` column of `DESCRIBE DETAIL` ([reader.py:126](../src/delta_engine/adapters/databricks/reader.py#L126)). If a `delta.*` key is enabled by a workspace or catalog default but absent from the table's own `TBLPROPERTIES`, `_diff_properties` sees a mismatch and emits `SetProperty` on every sync. The table never reaches a true no-op.

Tables created by the engine are safe — it always sets these properties at creation. The risk applies to pre-existing tables adopted into management.

**Status:** Unconfirmed, and **deferred** — not pursued now. Local verification was blocked by an environment issue: the dev machine cannot resolve Delta jars from Maven (SSL trust), and the installed pyspark (4.1.1) mismatches the cached Delta build (4.2.0 for Spark 4.0). The Delta docs describe the field only as "all the properties set for this table." This finding only bites *adopted* tables (pre-existing tables brought under management), which the engine does not yet support as a first-class flow — it is folded into the [Future work: adopting existing tables](#future-work-adopting-existing-tables) section below rather than chased in isolation.

**Fix, regardless of the runtime answer:** Strengthen the idempotency test (see Priority 3). The current test cannot catch this because `SET TBLPROPERTIES` is idempotent at the DDL level, so the schema stays equal even when properties are re-set every run.

## Priority 2 — Quick wins

Low-risk, behaviour-preserving, high-clarity.

### `assert` is disabled under `python -O`

The `AddColumn` compiler guards a contract with `assert action.column.nullable` ([compile.py:79](../src/delta_engine/adapters/databricks/sql/compile.py#L79)). Python strips `assert` under the `-O` flag. If validation is bypassed and the flag is set, a `NOT NULL` column compiles to a plain `ADD COLUMN` — the constraint is dropped, the column is created nullable, and execution reports success. The same file uses `raise AssertionError(...)` for the same purpose 45 lines later ([compile.py:126](../src/delta_engine/adapters/databricks/sql/compile.py#L126)), so the guard is also inconsistent.

**Fix:** Replace the `assert` with `if not action.column.nullable: raise AssertionError(...)` to match lines 126 and 136.

### Clearing a comment writes an empty string, not NULL

The `SetColumnComment` compiler always emits `COMMENT '...'`, even when the comment is empty ([compile.py:103](../src/delta_engine/adapters/databricks/sql/compile.py#L103)). `_column_definition` guards the same fragment with `if column.comment` ([compile.py:151](../src/delta_engine/adapters/databricks/sql/compile.py#L151)). The two paths handle the empty-string sentinel differently. Clearing a comment stores a literal `''` in the catalog rather than removing it.

**Fix:** When the comment is empty, emit `ALTER COLUMN ... UNSET COMMENT` instead of `COMMENT ''`. This makes all compiler paths consistent.

### `statement_preview` is stored twice

`ExecutionFailed` carries `statement_preview`, and its nested `ExecutionFailure` carries the same value ([results.py:131-147](../src/delta_engine/application/results.py#L131)). The executor passes the same string to both constructors ([executor.py:88](../src/delta_engine/adapters/databricks/executor.py#L88)). Production code never reads `ExecutionFailed.statement_preview` independently — every consumer reaches it through `result.failure`.

**Fix:** Remove `statement_preview` from `ExecutionFailed`. Read it via `result.failure.statement_preview`. This drops a duplicated field that two call sites must keep in sync.

### Two test-only seams sit in production signatures

`DatabricksExecutor.__init__` accepts a `compiler` parameter ([executor.py:37](../src/delta_engine/adapters/databricks/executor.py#L37)). `_to_column_mapping` accepts a `type_mapper` parameter ([reader.py:33](../src/delta_engine/adapters/databricks/reader.py#L33)). Both exist for test injection. No production code overrides either default. No test even injects `type_mapper`. APoSD's guidance: make the real code testable rather than widen the boundary for tests.

**Fix:** Remove `type_mapper` outright; the unsupported-type path is already covered with `BinaryType` against the real mapper. Extract the executor's stop-on-first-failure loop into a module-private function the tests call directly, then drop the `compiler` parameter.

### Type-hint gaps

Type hints on all signatures are a project standard. These are missing them:

| Location | Gap |
| --- | --- |
| [executor.py:69](../src/delta_engine/adapters/databricks/executor.py#L69) | `action` parameter on `_run_statement` |
| [compile.py:146](../src/delta_engine/adapters/databricks/sql/compile.py#L146) | `column` parameter on `_column_definition` |
| [engine.py:35](../src/delta_engine/application/engine.py#L35) | return type on `_utc_now` |
| [actions.py:238](../src/delta_engine/domain/plan/actions.py#L238) | `ActionPlan.__iter__` / `__getitem__` |
| [registry.py:61](../src/delta_engine/application/registry.py#L61) | `Registry.__iter__` / `__len__` |
| [reader.py:33](../src/delta_engine/adapters/databricks/reader.py#L33) | `type_mapper` parameter (removed by the fix above) |
| [\_\_init\_\_.py:74](../src/delta_engine/__init__.py#L74) | return type on `__getattr__` |

`mypy` passes today but does not flag these.

### `ActionPlan.__getitem__` is dead interface

`ActionPlan.__getitem__` supports indexing and slicing ([actions.py:242](../src/delta_engine/domain/plan/actions.py#L242)). No caller in `src/` or `tests/` uses it. Tests inspect `plan.actions` directly. A narrow interface is a virtue; every unused member is complexity to carry.

**Fix:** Remove it. Re-add with proper type hints if indexed access is ever needed.

## Priority 3 — Tests

### Idempotency test cannot prove a no-op

`test_engine_idempotent_when_already_in_desired_state` syncs twice and asserts the schemas match ([test_engine_e2e.py:168](../tests/e2e/test_engine_e2e.py#L168)). It does not assert that the second sync planned zero actions. `SET TBLPROPERTIES` is idempotent at the DDL level, so the schema stays equal even if the differ re-emits property actions every run. The test cannot catch the Priority 1 property hazard.

**Fix:** Capture the second `SyncReport` and assert a true no-op:

```python
second_report = engine.sync(reg)
assert all(len(t.execution.results) == 0 for t in second_report)
```

### E2E tests patch a private method of the class under test

Every e2e test calls `_patch_table_exists_for_local`, which monkey-patches `DatabricksReader._table_exists` ([test_engine_e2e.py:15](../tests/e2e/test_engine_e2e.py#L15)). This couples the suite to a private implementation detail and changes the method's semantics (three-part name versus two-part). The e2e tests no longer exercise the real existence-check path. It also breaks the project's own rule against mocking internal collaborators.

**Fix:** Check whether `spark.catalog.tableExists("spark_catalog.schema.table")` resolves under the local DeltaCatalog. If it does, delete the patch. If three-part resolution genuinely fails locally, fix it at the data level in the fixture rather than patching the adapter.

**Status: Deferred.** Settling this needs a session that can run the e2e suite, which the dev machine cannot (the same Delta-jar/version blocker above). The patch stays in place for now. It is most naturally resolved alongside the [Future work: adopting existing tables](#future-work-adopting-existing-tables) effort, when real-catalog behaviour gets exercised properly.

### Read-failure isolation is under-tested

`test_engine_reads_all_tables_then_raises_on_any_read_failure` checks that both tables appear in the report when one read fails ([test_engine.py:177](../tests/application/test_engine.py#L177)). It does not assert that the surviving table was executed. `TableRunStatus.SUCCESS` is consistent with an empty, never-run `ExecutionSummary`, so the status check alone does not prove the engine honoured "failures in individual tables do not halt the sync of others."

**Fix:** Assert `tr_b.execution.results != ()` for the surviving table.

## Decisions to weigh, not fix

These are judgement calls. The current design is defensible.

### Unsupported-change actions flow through the executable pipeline

`ColumnTypeChange` and `PartitioningChange` are `Action` subclasses that can never execute ([actions.py:163](../src/delta_engine/domain/plan/actions.py#L163), [actions.py:186](../src/delta_engine/domain/plan/actions.py#L186)). They occupy `ActionPhase` slots, need compiler handlers that only raise, and need validation rules to intercept them. This overloads "action" to also mean "detected drift."

The docstrings document the intent clearly, which is why the finding survived only as a decision rather than a fix. The APoSD-purist alternative: have `compute_plan` return `(ActionPlan, tuple[DriftConflict, ...])`, where `DriftConflict` is not an `Action`. That removes two enum members, two raising compiler handlers, and simplifies the mental model. It is also a real change.

**Recommendation:** Leave it. Revisit if a third unsupported-change category appears.

### `CREATE TABLE IF NOT EXISTS` after an absence check

The reader reports `TableAbsent`, the differ emits `CreateTable`, and the compiler renders `CREATE TABLE IF NOT EXISTS` ([compile.py:57](../src/delta_engine/adapters/databricks/sql/compile.py#L57)). If another process creates the table between read and execution, the clause silently no-ops and the run reports success despite a possibly divergent schema. Either choice — keep `IF NOT EXISTS` for resilience, or use plain `CREATE TABLE` to fail loud — is reasonable. The gap is that the behaviour is undocumented.

**Recommendation:** Add a comment in the compiler stating the intent, and a note in the README's non-goals section.

**Status: ✅ Done (`docs/aposd-comments-and-docs`).** The compiler now carries a comment on the `CREATE TABLE IF NOT EXISTS` clause explaining the read-then-create race and the resilience trade-off, and the README non-goals section documents that creation is not race-safe against concurrent writers. The `ColumnTypeChange`/`PartitioningChange`-as-`Action` decision above is left as recommended.

## Comments and documentation

The codebase has excellent comments where they matter. The `zip(strict=True)` comment captures the cross-module one-statement-per-action contract ([executor.py:57](../src/delta_engine/adapters/databricks/executor.py#L57)). The `_fetch_properties` comment explains why two quoting paths must stay separate ([reader.py:122](../src/delta_engine/adapters/databricks/reader.py#L122)). The `ActionPhase` docstring explains the ordering invariant, not just the enum.

Two adjustments:

- **Trim restate-the-code docstrings.** The `D` (pydocstyle) ruleset forces low-value docstrings: nine near-identical `subject()` docstrings, trivial `__len__`/`__bool__`/`__iter__` docstrings, and `format_lines` docstrings that name the method. APoSD treats these as noise.
- **Thicken the port docstrings.** `CatalogStateReader` and `PlanExecutor` are the extension points every future adapter implements ([ports.py:13](../src/delta_engine/application/ports.py#L13)). The totality contract — "always returns a `CatalogState`, never raises" — currently lives on the concrete `DatabricksReader`. Lift it to the Protocol.

**Status: ✅ Done (`docs/aposd-comments-and-docs`).** The restate-the-code docstrings were removed (the nine `subject()` overrides, the trivial `__len__`/`__bool__`/`__iter__` dunders, and the three concrete `format_lines` overrides), keeping the contract-bearing docstrings on the abstract `Action.subject` and `Failure.format_lines`. Because the `D` ruleset forced these, `D105` is now ignored project-wide (magic methods are documented by the protocol they implement) and `D102` is ignored for the two value-object modules (`actions.py`, `results.py`); `D102` stays enforced everywhere else. The totality contract is lifted onto both `CatalogStateReader` and `PlanExecutor` Protocol docstrings.

## Future work: adopting existing tables

Today the engine assumes it owns the tables it manages — the well-trodden path is "engine creates the table, engine keeps it in sync." It has no deliberate flow for **adopting** a pre-existing table: one created outside the engine (by another team, an ETL job, or a migration) and then brought under management. Two of this review's findings are really symptoms of that gap rather than independent bugs, which is why both are deferred here rather than chased in isolation:

- **Property idempotency on adopted tables** (Priority 1). The engine injects `delta.*` defaults into every desired state. For an engine-created table those are set at creation, so the table reaches a no-op. For an adopted table they may be enabled by a workspace/catalog default but absent from the table's own `TBLPROPERTIES` — and if `DESCRIBE DETAIL` does not report them back, the differ re-emits `SetProperty` forever. The table never settles. This is squarely an adoption problem: it cannot happen to a table the engine created.

- **E2E existence-check patch** (Priority 3). The e2e suite patches `DatabricksReader._table_exists` because three-part-name resolution against a real catalog is unverified locally. Adoption is exactly the scenario that exercises "does this table already exist, under its real catalog name?" — so confirming and removing the patch belongs with this work.

Questions to settle when this is picked up (on a real Unity Catalog session):

1. **Should adoption be an explicit, opt-in step**, distinct from "create and own"? An explicit `adopt` path could read the live table, reconcile only what the user declares, and avoid touching catalog-default properties at all.
2. **Should property reconciliation compare against *effective* (`DESCRIBE`-reported) properties rather than *declared* keys**, so a default-enabled key is treated as already-satisfied? This likely resolves the idempotency finding regardless of the runtime answer.
3. **What is the desired stance on properties the engine does not manage** but that exist on an adopted table — leave untouched (current "declared subset" behaviour), or surface them in the plan for visibility?
4. **How should an adopted table with unmappable column types behave** — the unmappable-column skip (Priority 1, done) already partly covers this, but adoption may want to *report* what it is ignoring rather than silently skip.

Until there is a concrete need to manage pre-existing tables, this stays as a documented gap, not a half-built feature.

## Summary

| Priority | Item | Action |
| --- | --- | --- |
| 1 | Unmappable column fails whole table | Skip column, log, document |
| 1 | Property idempotency on adopted tables | Deferred → [Future work: adopting existing tables](#future-work-adopting-existing-tables) |
| 2 | `assert` disabled under `-O` | Use `raise AssertionError` |
| 2 | Comment cleared as `''` not NULL | Emit `UNSET COMMENT` |
| 2 | `statement_preview` duplicated | Drop from `ExecutionFailed` |
| 2 | Test-only seams in production | Remove `type_mapper`; extract loop |
| 2 | Missing type hints | Add them |
| 2 | Dead `__getitem__` | Remove |
| 3 | Idempotency test | Assert empty second plan |
| 3 | E2E patches private method | Deferred → [Future work: adopting existing tables](#future-work-adopting-existing-tables) |
| 3 | Read-failure isolation | Assert survivor executed |
