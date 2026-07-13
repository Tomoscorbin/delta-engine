# delta-engine

Declarative schema management for Delta Lake tables on Databricks.

Users declare desired table state with `DeltaTable`. The engine reads the current catalog state, computes typed drift, validates whether that drift is safe, plans deterministic DDL actions, resolves foreign-key dependencies, and executes through a backend adapter.

This file contains project-level instructions that should be available in every Claude Code session. File-specific rules live in `.claude/rules/`. Multi-step workflows belong in `.claude/skills/`.

## Project shape

Main source layout:

- `src/delta_engine/domain`: backend-free domain model, table snapshots, diffs, actions, and deterministic plans.
- `src/delta_engine/application`: orchestration, ports, validation, dependency resolution, reporting, and errors.
- `src/delta_engine/adapters`: backend integration. The Databricks adapter has two backends: Spark and SQL warehouse.
- `src/delta_engine/api`: implementation of public declaration objects.
- `src/delta_engine/schema.py`: public schema declaration import surface.
- `src/delta_engine/databricks.py`: public Databricks helper import surface with lazy adapter imports.
- `src/delta_engine/cli`: read-only `plan` command over the warehouse backend; requires the `cli` extra.
- `tests`: unit, integration, adapter, and end-to-end tests.
- `docs`: Sphinx/MyST documentation.

The library should remain importable for schema declaration and planning without requiring PySpark unless the Spark backend is used; the SQL warehouse backend runs without PySpark entirely.

## Commands

Set up development dependencies:

```bash
uv sync --group dev
```

Run tests:

```bash
uv run pytest
uv run pytest tests/domain/plan/test_diff.py
uv run pytest tests/domain/plan/test_diff.py::test_name
uv run pytest -m "not local_e2e and not databricks_e2e"
```

The first test run starts a local Spark session through the `spark` fixture in `tests/conftest.py` and can take 30-60 seconds.

Coverage is enabled by default and must stay above the configured threshold.

Lint, format, and type-check:

```bash
uv run ruff check src tests
uv run ruff format src tests
uv run mypy src
```

Check import architecture:

```bash
uv run lint-imports
```

Build docs:

```bash
uv sync --group docs
uv run --group docs sphinx-build -b html docs docs/_build/html -W
```

## Validation workflow

Prefer the narrowest useful check while iterating, then broaden before finishing.

For ordinary code changes, start with the relevant focused test:

```bash
uv run pytest tests/path/to/test_file.py::test_name
```

Then broaden as needed:

```bash
uv run pytest tests/path/to/test_file.py
uv run pytest
```

Before opening a PR, run:

```bash
uv run pytest
uv run ruff check src tests
uv run mypy src
uv run lint-imports
uv run --group docs sphinx-build -b html docs docs/_build/html -W
```

This repo carries no tests that need Databricks credentials; the full suite runs locally. Live verification against a real workspace happens in a separate project outside this repo.

## Architecture boundaries

This project uses hexagonal architecture: domain core, application use cases, backend adapters, and public API declarations.

The import architecture is enforced by `import-linter` in `pyproject.toml`. Do not rely on convention alone.

Rules:

- `domain` must stay backend-free, immutable, and deterministic.
- `application` owns orchestration, ports, safety policy, dependency resolution, reports, and failure propagation.
- `adapters` own backend integration, SQL compilation, Spark/Databricks parsing, identifier quoting, and backend exception translation.
- `api` owns public declaration implementation and lowers declarations into domain snapshots.
- `schema.py` and `databricks.py` are user-facing facades; keep them thin.
- `cli.connection` may import the Databricks SDK and SQL connector only to
  resolve unified authentication and own a connection; backend reads and
  execution stay in adapters.
- PySpark, Delta, Py4J, Spark SQL details, and Databricks-specific assumptions must not leak into `domain` or `application`.

Expected dependency direction:

```text
cli -> databricks | schema | adapters | api -> application -> domain
```

Two more import-linter contracts forbid `delta` and `pyspark` imports: one covering `schema`, `api`, `application`, and `domain`; another covering `cli`, with a carve-out for the one legitimate edge (the lazy Spark facade import). A further contract confines `typer`/`click`/`rich` imports to `cli`.

## Sync lifecycle

`Engine.sync(...)` prepares desired tables, then runs a phase chain over per-table run state.

Core phases:

1. Prepare desired declarations.
2. Read current catalog state through `CatalogStateReader`.
3. Diff desired vs observed state with `diff_table`.
4. Validate drift with `validate_diff`.
5. Plan actions from validated diff.
6. Resolve foreign-key dependency order.
7. Execute through `PlanExecutor`, unless this is a dry run.
8. Return `SyncReport`, or raise `SyncFailedError` with the report on a real failed run.

A table that fails an early phase keeps that failure in its report and is skipped by later mutating phases. The engine should still process other tables and return a complete run report.

Both application ports are total from the engine's perspective:

- `CatalogStateReader.fetch_state(...)` returns `TablePresent`, `TableAbsent`, or `ReadFailed`.
- `PlanExecutor.execute(...)` returns `ExecutionSummary`.

Adapters should catch backend exceptions and convert them into typed failures rather than raising backend-specific exceptions through the port.

## Planning and validation invariants

Diffs state facts. Validation decides safety.

Do not make diff code decide whether a change is safe.

Do not make adapter code decide whether a domain change is safe.

`ActionPlan` owns action ordering. Do not sort actions manually elsewhere.

Action ordering is defined by `ActionPhase` and then by action `subject`.

Properties and tags intentionally use different semantics:

- Properties use exact-declaration semantics. A declared `None` asserts absence and plans an unset when present.
- Tags use full-state semantics. An observed-only tag is drift and should be unset when tags are managed.

`managed_aspects` is part of the safety model. Drift outside the aspects managed by a declaration should fail validation rather than be silently reconciled.

Constraint names are generated at the API-to-domain lowering boundary and then carried as data. The differ and SQL compiler should read constraint names, not re-derive them.

Primary-key and foreign-key identity is structural, not name-based.

Foreign-key declarations reference the target `DeltaTable` object, or `Self`, rather than a dotted table name. This lets the API infer referenced columns from the target table's primary key.

## Where to make common changes

| Change                               | Main location                                                                            |
| ------------------------------------ | ---------------------------------------------------------------------------------------- |
| New backend                          | `src/delta_engine/adapters`: implement `CatalogStateReader` and `PlanExecutor`           |
| New change/diff type                 | `src/delta_engine/domain/plan/diff.py`                                                   |
| New action type                      | `src/delta_engine/domain/plan/actions.py` and `src/delta_engine/adapters/databricks/sql` |
| New safety rule                      | `src/delta_engine/application/validation.py`                                             |
| New data type                        | `src/delta_engine/domain/model/data_type.py` and Databricks type mapping                 |
| Public declaration change            | `src/delta_engine/api`, surfaced through `src/delta_engine/schema.py`                    |
| Foreign-key ordering/blocking policy | `src/delta_engine/application/dependency_resolution.py`                                  |
| Report/output formatting             | `src/delta_engine/application/report.py` and `src/delta_engine/application/rendering.py` |
| Databricks SQL generation            | `src/delta_engine/adapters/databricks/sql`                                               |
| Public Databricks helper             | `src/delta_engine/databricks.py`                                                         |
| CLI commands, output, exit codes     | `src/delta_engine/cli`                                                                   |
| Documentation                        | `docs`                                                                                   |

Before changing architecture, action planning, validation, adapters, or public behaviour, read the relevant docs instead of guessing:

- `docs/explanation-architecture.md`
- `docs/how-to-implement-adapter.md`
- `docs/how-to-add-action-type.md`
- `docs/reference-safe-change-rules.md`
- `docs/how-to-handle-sync-failures.md`
- `docs/how-to-deploy-metadata-only.md`

## Documentation

When changing public behaviour, architecture, validation policy, action types, adapter behaviour, or failure semantics, consider whether docs need updating.

Do not duplicate detailed docs in this file. Link to the relevant doc and keep this file as the high-signal project map.

## Working rules

Start by reading nearby code and tests.

Preserve architecture boundaries. Do not solve adapter problems by leaking backend concepts inward.

Prefer focused changes. Avoid opportunistic refactors unless they directly support the task.

When changing behaviour, add or update tests at the right level.

When a change touches domain, application, adapter, and docs, keep the conceptual flow consistent across all of them.

Before finishing, state:

- what changed
- what checks were run
- what risks or follow-ups remain
