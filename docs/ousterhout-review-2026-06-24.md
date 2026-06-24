# Delta-Engine: Ousterhout Review — 2026-06-24

Delta-engine applies *A Philosophy of Software Design* (Ousterhout) seriously, and it shows. Deep modules hide real complexity behind narrow interfaces, information is hidden at clean boundaries, and comments explain *why* rather than *what*. **No finding here warrants a redesign.** Every item is low- or medium-severity polish.

This document records the review so the findings aren't lost. It supersedes [ousterhout-review.md](ousterhout-review.md), whose top findings describe pre-refactor code (pass-through methods in `Engine`, `ReadResult` as a two-`Optional` tri-state) that no longer exists. Archive that file when convenient.

**Method.** All 21 source modules were read. Findings were generated under six lenses — module depth, information hiding, interface/cognitive load, error handling, comments/naming, consistency/generality — plus correctness and test quality. Each finding was adversarially verified against the actual code before inclusion.

---

## Verdict

This is well-designed code. Keep the spine as-is. The recommendations below refine it.

---

## Strengths (preserve deliberately)

- **Deep modules where it counts.** [`Engine.sync`](../src/delta_engine/application/engine.py), [`compute_plan`](../src/delta_engine/domain/plan/differ.py), and [`compile_plan`](../src/delta_engine/adapters/databricks/sql/compile.py) each present a narrow interface over substantial hidden implementation. The `CatalogStateReader` / `PlanExecutor` [ports](../src/delta_engine/application/ports.py) decouple the application layer from Spark with no leakage in either direction.
- **Complexity pulled downward.** [`ActionPlan`](../src/delta_engine/domain/plan/actions.py) sorts itself into execution order as a construction invariant, not a caller's responsibility. `QualifiedName`, `Column`, and `TableSnapshot` validate eagerly in `__post_init__`. The `ExecutionResult` union makes "succeeded but carries a failure" unrepresentable ([results.py:149](../src/delta_engine/application/results.py)).
- **Errors defined out of existence.** The marker actions `ColumnTypeChange` and `PartitioningChange` make unsupported drift *visible* in the plan and produce a clear validation failure, rather than silently dropping the diff. The broad `except` in [`fetch_state`](../src/delta_engine/adapters/databricks/reader.py) is intentional and documented — boundary totality, not masking.
- **Comments earn their place.** The [`_to_column_mapping` casefold rationale](../src/delta_engine/adapters/databricks/reader.py) and the `tableExists` temp-view caveat capture knowledge the code cannot.

---

## Recommendations, by value

### R1 — Export `Property` from the public surface — *medium* — ✅ done (branch `refactor/r-section-public-api-polish`)

**Files:** [schema/__init__.py](../src/delta_engine/schema/__init__.py), [delta_engine/__init__.py](../src/delta_engine/__init__.py)

`DeltaTable` accepts `dict[str, str]` for properties but silently enforces a closed vocabulary defined by [`Property`](../src/delta_engine/schema/properties.py) (`MANAGED_PROPERTY_KEYS`). It rejects unknown keys at construction. Yet `Property` — the enum that defines the valid keys — is not importable from any public surface. A caller must guess `"delta.enableDeletionVectors"`, read internal source, or hit the runtime error. This is an unknown unknown.

`Property` is a `StrEnum`, so it *is* a `str`. Exporting it costs nothing and changes no logic.

**Change:** Add `from delta_engine.schema.properties import Property` and `"Property"` to `__all__` in both files.

This is the only finding with behavioural or API impact.

**Done.** `Property` is exported from both [schema/__init__.py](../src/delta_engine/schema/__init__.py) and the [top-level package](../src/delta_engine/__init__.py) (eager — it is pyspark-free). The original note suggested also annotating the constructor as `dict[Property | str, str]`; this was **rejected on inspection**. `Property` subclasses `str` (`StrEnum`), so `Property | str` collapses to `str` for the type checker — the annotation conveys nothing to mypy and adds visual noise. Enum members already work as dict keys at runtime and type-check fine under `dict[str, str]`; this is locked in by `test_accepts_property_enum_members_as_keys`.

### R2 — Make the `AddColumn` compiler's hidden contract loud — *low* — ✅ done

**File:** [compile.py:68-79](../src/delta_engine/adapters/databricks/sql/compile.py)

The `AddColumn` compiler omits `NOT NULL`, while [`_column_definition`](../src/delta_engine/adapters/databricks/sql/compile.py) (used by `CreateTable`) renders it. The same column produces different SQL on the two paths, guarded only by a docstring. Its siblings `ColumnTypeChange` and `PartitioningChange` *raise* when reached.

**Change:** Add as the first line of the `AddColumn` dispatcher:

```python
assert action.column.nullable, (
    f"AddColumn reached compiler with non-nullable column {action.column.name!r}; "
    "validation should have blocked this"
)
```

One line. A bypassed or customised rule set then fails loudly instead of emitting wrong DDL.

### R3 — Pair statements to actions explicitly in the executor — *low* — ✅ done

**File:** [executor.py:55-61](../src/delta_engine/adapters/databricks/executor.py)

`execute()` relies on an undocumented 1:1 correspondence between compiled statements and `plan[action_index]`. `zip(plan, statements)` makes the pairing explicit and removes the integer index — an obscurity removal.

**Change:** Replaced `enumerate(statements)` + `plan[action_index]` with `enumerate(zip(plan, statements, strict=True))`. `strict=True` (added to satisfy ruff B905, and a genuine improvement) turns a compiler/plan length mismatch into a loud error instead of a silent truncation.

### R4 — Use `AssertionError`, not `NotImplementedError`, for invariant guards — *low* — ✅ done

**File:** [compile.py:115-131](../src/delta_engine/adapters/databricks/sql/compile.py)

The `ColumnTypeChange` and `PartitioningChange` dispatchers raise `NotImplementedError`, which signals "abstract stub not yet written." These are "validation should have caught this" invariant violations. `AssertionError` reads correctly. Keep the user-facing hint in the message.

### R5 — Cheap consistency wins — *low* — ✅ mostly done (one item deferred)

- ✅ **Iterate the plan, not its field.** All four [validation rules](../src/delta_engine/application/validation.py) now use `for action in plan`.
- ✅ **Drop the redundant class-var alias.** `_managed_property_keys` removed from [schema/table.py](../src/delta_engine/schema/table.py); the call site uses `MANAGED_PROPERTY_KEYS` directly.
- ✅ **Naming** (per coding standard: avoid abbreviations):
  - `lg` → `py4j_logger` in [log_config.py](../src/delta_engine/adapters/databricks/log_config.py) (named for what it is, not the generic `logger`).
  - Local `type` → `sql_type` and `cols`/`c` → `quoted_columns`/`column` in [compile.py](../src/delta_engine/adapters/databricks/sql/compile.py).
  - `exc_type_name` → `exception_type_name` across [preview.py](../src/delta_engine/adapters/databricks/sql/preview.py), `sql/__init__.py`, `reader.py`, `executor.py`, and tests. Also `exc` → `exception` in `reader.py`.
  - Inlined `df` at [reader.py](../src/delta_engine/adapters/databricks/reader.py): `row = self.spark.sql(query).first()`.
- ⚠️ **DEFERRED — Remove restating docstrings.** The proposal was to delete the docstrings on `ActionPlan`'s dunders and the nine `subject` overrides in [actions.py](../src/delta_engine/domain/plan/actions.py). **This conflicts with the project's own lint gate:** `pyproject.toml` enables ruff's `D` (pydocstyle) ruleset for `src/`, and `D105` (magic-method docstrings) / `D102` (public-method docstrings) are *not* in the ignore list — removing them fails `ruff check`. Resolving this means either (a) adding `D105`/`D102` to the per-path ignores for `actions.py`, or (b) accepting the docstrings as the project standard. Left as-is pending that call; reverted cleanly.

### R6 — Test hygiene (Detroit-school) — *low* — ✅ done

- ✅ [test_results.py](../tests/application/test_results.py): `_FakeObservedTable` replaced with a real `ObservedTable` via an `_an_observed_table()` builder.
- ✅ [test_reader.py](../tests/adapters/databricks/test_reader.py): the two white-box `_table_exists` tests deleted (the present/absent paths are covered at `fetch_state` level, and the e2e suite monkeypatches `_table_exists` with `raising=True`, keeping the name honest). The two `exc_type_name` tests here were redundant with `test_preview.py` and were removed. The `_fetch_properties`/`_fetch_table_comment` private tests were **kept** — they assert the read-only/`MappingProxy` immutability guard and the None-comment edge case, which aren't redundantly covered elsewhere; deleting them would lose real coverage.
- ✅ [test_validation.py](../tests/application/test_validation.py): the brittle `message.split("'")[1]` asserts replaced with substring-membership checks.

---

## Documentation gaps worth closing — *low* — ✅ done

- ✅ [table.py](../src/delta_engine/domain/model/table.py): the `TableSnapshot.__post_init__` docstring now states the lowercase-partition invariant (and the existence/uniqueness ones), as a multi-line docstring to stay within the line limit.
- ✅ [reader.py](../src/delta_engine/adapters/databricks/reader.py): `_fetch_properties` now carries a comment explaining why SQL DDL needs `backtick_qualified_name()` while the `catalog.*` calls take the plain `str()` form — with a "don't unify the two" warning.
- ✅ [executor.py](../src/delta_engine/adapters/databricks/executor.py): `_run_statement`'s broad `except` now documents the same total-function rationale as `fetch_state`. The inner reflection catch in `exception_type_name` was also narrowed from `except Exception` to `except (AttributeError, TypeError)` (the review's "narrow the bare except" nit).
- ✅ [engine.py](../src/delta_engine/application/engine.py) and [build_engine.py](../src/delta_engine/adapters/databricks/build_engine.py): the mechanical `Args:` blocks were removed; the non-obvious "no logging side effect" prose in `build_databricks_engine` was kept.

---

## Cross-cutting note — the "unsupported action" contract

The unsupported-action rule spans three modules: the [differ](../src/delta_engine/domain/plan/differ.py) emits marker actions, [validation](../src/delta_engine/application/validation.py) blocks them, and the [compiler](../src/delta_engine/adapters/databricks/sql/compile.py) raises if one reaches it. This layering is deliberate and correct, but the convention is implicit.

A single test would formalise it without adding structural complexity: assert that every action type whose compiler raises has a corresponding blocking validation rule. This guards against a future action type that the differ emits and the compiler rejects, but validation forgets to block — which would surface as a hard crash mid-migration.

---

## Explicitly not worth changing

Ousterhout warns against over-engineering as much as against shallow modules. These earn their keep:

- **The `Rule` protocol + four rule classes.** Structurally repetitive, but not over-engineered at this scale. A `(predicate, message-template)` refactor would lose `rule_name` identity and per-rule type safety.
- **The `DeltaTable` / `DesiredTable` split.** `DeltaTable` adds default-property merging and managed-key validation that the domain `DesiredTable` deliberately does not know about. The class is not too shallow to exist. (`effective_properties` is the one genuine pass-through; remove it only if no caller uses it.)
- **`DesiredTableSource` protocol** in the registry — a legitimate extension point, not gratuitous indirection.
- **`ExecutionSucceeded` / `ExecutionFailed` field symmetry.** The duplicated `action_index` and `statement_preview` are load-bearing: they let consumers iterate results uniformly without `isinstance` branching.
- **Lowercase normalisation across adapter and domain.** The adapter transforms, the domain validates. That is correct separation of concerns, documented with an exemplary comment — not duplication. Adding `Column.normalize_name()` would couple the layers in the wrong direction.
- **`build_databricks_engine` in its own file.** Mildly verbose, but the separate file makes its "no logging side effect" contract natural to test.

---

## Suggested order of attack

1. **R1** on its own branch — the only behavioural/API change.
2. **R2–R6** plus the documentation gaps on a second branch — pure polish, no behaviour change.

Tests should stay green throughout.
