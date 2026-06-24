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

### R2 — Make the `AddColumn` compiler's hidden contract loud — *low*

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

### R3 — Pair statements to actions explicitly in the executor — *low*

**File:** [executor.py:55-61](../src/delta_engine/adapters/databricks/executor.py)

`execute()` relies on an undocumented 1:1 correspondence between compiled statements and `plan[action_index]`. `zip(plan, statements)` makes the pairing explicit and removes the integer index — an obscurity removal.

**Change:** Replace `for action_index, statement in enumerate(statements)` with `zip(plan, statements)`, deriving the index from `enumerate(zip(...))` only if a result still needs it.

### R4 — Use `AssertionError`, not `NotImplementedError`, for invariant guards — *low*

**File:** [compile.py:115-131](../src/delta_engine/adapters/databricks/sql/compile.py)

The `ColumnTypeChange` and `PartitioningChange` dispatchers raise `NotImplementedError`, which signals "abstract stub not yet written." These are "validation should have caught this" invariant violations. `AssertionError` reads correctly. Keep the user-facing hint in the message.

### R5 — Cheap consistency wins — *low*

- **Iterate the plan, not its field.** Replace `for action in plan.actions` with `for action in plan` in all four [validation rules](../src/delta_engine/application/validation.py) (lines 61, 92, 120, 148). `ActionPlan.__iter__` exists precisely to hide `.actions`; the rules are the only callers that reach past it.
- **Drop the redundant class-var alias.** [`_managed_property_keys`](../src/delta_engine/schema/table.py) (line 29) is a private `ClassVar` set to `MANAGED_PROPERTY_KEYS`, used once. Delete it and use `MANAGED_PROPERTY_KEYS` directly at line 45.
- **Naming** (per coding standard: avoid abbreviations):
  - `lg` → `logger` in [log_config.py:72](../src/delta_engine/adapters/databricks/log_config.py).
  - Local `type` shadows the builtin in [compile.py:140](../src/delta_engine/adapters/databricks/sql/compile.py) → `sql_type`.
  - `exc_type_name` → `exception_type_name` ([preview.py](../src/delta_engine/adapters/databricks/sql/preview.py), plus `sql/__init__.py`, `reader.py`, `executor.py`, and tests). It sits on the public adapter surface.
  - Inline `df` at [reader.py:123](../src/delta_engine/adapters/databricks/reader.py): `row = self.spark.sql(query).first()`.
- **Remove restating docstrings.** Delete the docstrings on `ActionPlan`'s dunders (`__len__`, `__bool__`, `__iter__`, `__getitem__`) and the nine concrete `subject` overrides in [actions.py](../src/delta_engine/domain/plan/actions.py). The abstract `Action.subject` already documents the contract.

### R6 — Test hygiene (Detroit-school) — *low*

- [test_results.py:26](../tests/application/test_results.py) uses a `_FakeObservedTable`. Replace it with a real `ObservedTable`; mocking internal collaborators violates the classical-testing rule.
- [test_reader.py](../tests/adapters/databricks/test_reader.py) tests private methods directly (`_table_exists`, `_fetch_properties`, `_fetch_table_comment`). Promote the meaningful cases to `fetch_state`-level scenarios so the tests survive refactors, then delete the private-method tests.
- [test_validation.py:53,97,135](../tests/application/test_validation.py) asserts via `message.split("'")[1]`. Replace with `any(name in f.message for f in failures)`, which survives message rephrasing without changing what behaviour is verified.

---

## Documentation gaps worth closing — *low*

- [table.py:32](../src/delta_engine/domain/model/table.py): the `TableSnapshot.__post_init__` docstring omits the lowercase-partition invariant. Add it to the existing sentence.
- [reader.py:118,122](../src/delta_engine/adapters/databricks/reader.py): the two `QualifiedName` rendering strategies (`str()` for the Catalog API, `backtick_qualified_name()` for SQL DDL) are correct but undocumented. A one-line comment explaining that SQL DDL requires identifier quoting while the Catalog API parses dot-separated parts internally closes the unknown unknown.
- [executor.py:64](../src/delta_engine/adapters/databricks/executor.py): `_run_statement`'s broad `except` has no rationale, unlike `fetch_state`'s. Note that Spark raises heterogeneous exceptions (`Py4JJavaError`, `AnalysisException`, Python-level errors) and the executor's contract is to wrap any failure in `ExecutionFailed`. The asymmetry is a cognitive-load tax: a future maintainer could narrow the catch and reintroduce silent propagation.
- [engine.py:53-62](../src/delta_engine/application/engine.py) and [build_engine.py](../src/delta_engine/adapters/databricks/build_engine.py): `Args:` blocks that restate typed parameter names add nothing. Keep only prose that documents non-obvious constraints, such as the "no logging side effect" note.

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
