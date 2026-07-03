# Special-case audit

**Date:** 2026-07-03
**Scope:** all of `src/delta_engine/` (~4k lines), on main at `fe6d8e5`.
**Method:** six pattern sweeps (None-guards, empty-collection branches, type dispatch, flags/modes, duplicated guards, structural shape), 30 raw findings deduplicated to 23 sites, each independently verified against the Ousterhout criteria: can the general case absorb the edge input, can the invalid state be made unconstructible, or is the branch a genuine domain boundary?

Every finding is grouped by root cause. Sections A–G are eliminable (or mostly so); section H needs a design decision; the final section lists sites that were challenged and survived as legitimate boundaries, so they are not re-litigated later.

---

## A. One type carrying two lifecycle states — `constraint_name: str | None`

**Root cause:** `ForeignKeyConstraint.constraint_name` and `PrimaryKeyConstraint.constraint_name` are typed `str | None`, but the `None` state exists only transiently before `DesiredTable.__post_init__` calls `with_generated_name`. Every `DesiredTable` that constructs successfully carries fully named constraints, and every observed FK from the reader carries its catalog name. The type is wider than the values that flow, so downstream code re-proves the invariant at each use site.

**Guards that exist only to compensate:**

| Site | Guard |
|---|---|
| `domain/plan/differ.py:282` | `assert desired_pk.constraint_name is not None` |
| `domain/plan/differ.py:318` | `assert foreign_key.constraint_name is not None` |
| `domain/plan/differ.py:332` | `if foreign_key.constraint_name is not None` filter on the drop path — its own comment says "never false in practice" |
| `adapters/databricks/sql/compile.py:64` | `assert constraint_name is not None` in the CREATE TABLE compiler |
| `domain/model/foreign_key.py:42` / `primary_key.py:48` | `with_generated_name` rejecting an already-named constraint |

**Elimination:** introduce named variants (e.g. `NamedForeignKeyConstraint` / `NamedPrimaryKeyConstraint` with `constraint_name: str`) returned by `with_generated_name`. Type `DesiredTable.primary_key` and `DesiredTable.foreign_keys` to the named variants. All four downstream guards become deletable and mypy proves the deletion safe.

**Wrinkle:** the observed PK built by `reader._fetch_primary_key` (reader.py:193) is constructed *without* a name — `None` has a second, permanent occupant on the observed side. The split must either keep the unnamed type for observed PKs or extend the reader query to fetch the constraint name. Observed FKs are already always named, so `ObservedTable.foreign_keys` can take the named variant directly.

**Effort:** medium — touches `foreign_key.py`, `primary_key.py`, `table.py`, `differ.py`, `compile.py`, `reader.py`. Supersedes and expands the existing todo item on tightening `ForeignKeyConstraint.constraint_name`.

**Implementation plan:** [2026-07-03-constraint-naming-plan.md](2026-07-03-constraint-naming-plan.md). It takes the always-named route (a required `constraint_name: str` with a `generate()` factory, the reader fetching the observed PK name) rather than the named-variant route sketched above — the same guards are eliminated, and there is no lingering unnamed type. Decision 4 below is resolved there as "fetch the observed PK name".

---

## B. Optional-collection widening on the public API

**`api/table.py:152`** — `partitioned_by: Iterable[str] | None = None` forces `tuple(partitioned_by) if partitioned_by is not None else ()`. `None` and `()` mean the same thing; no caller passes `None` explicitly. Default the parameter to `()` and the ternary collapses to `tuple(partitioned_by)`. The `foreign_keys` parameter is the same shape and already uses the tighter `(foreign_keys or ())` idiom.

**Caveat:** `properties` and `tags` look like the same pattern but are **not** safely fixable the same way — a `dict` default is the mutable-default trap (ruff B006). `| None = None` is the legitimate idiom for mapping parameters; only the iterable/tuple parameters should be tightened.

**Effort:** small, one file.

---

## C. Dead generality — parameters and unions no caller uses

**`adapters/databricks/sql/preview.py:6`** — `sql_preview(..., single_line: bool = True)`. The only production caller (`executor.py:94`) uses the default; the `False` arm is exercised only by a test written to cover it. Delete the flag and its test; inline the whitespace-normalising behaviour unconditionally. (`max_chars` is a different category — plausibly varied per call site — and stays.)

**`adapters/databricks/sql/types.py:89`** — `domain_type_from_spark(spark_type: str | SparkType)` opens with `if isinstance(spark_type, str): spark_type = SparkType.fromDDL(spark_type)`. The two shapes have a fixed relationship: the `str` only ever enters at the top (the reader passes the catalog column's DDL string), and the `Array`/`Map` recursion always passes `SparkType`. Split into a public `domain_type_from_ddl(ddl: str)` and a private `SparkType` matcher; the isinstance goes away and each function has one input shape.

**`adapters/databricks/sql/compile.py:255`** — `_set_properties(props: Mapping[str, str] | None)`. The `| None` is dead: the sole caller passes `table.properties`, which the domain model defaults to `{}` and never sets to `None`. Tighten to `Mapping[str, str]`. The `if not props: return ""` **stays** — an empty mapping legitimately produces no clause (`TBLPROPERTIES ()` is not valid SQL).

**Effort:** small each.

---

## D. Empty-collection branches the general path already handles

**`application/report.py:115`** — `SyncReport.__str__` special-cases zero tables with `return "Sync report: 0 tables"`. Verified: the general path already renders a header-only grid plus `"0 tables: 0 changed, 0 unchanged, 0 failed (0.0s)"` — well-formed and more informative. No test pins the sentinel string. Deleting the branch is a small user-visible output change (see Decisions).

**`domain/model/table.py:56` and `table.py:76`** — the `if self.partitioned_by:` and `if self.foreign_keys:` wrappers in `TableSnapshot.__post_init__` guard loops and comprehensions that all no-op naturally on empty collections. Deleting both changes nothing except removing a nesting level.

**`application/rendering.py:170` (and `:153`)** — the empty-plan substitution (`"no changes"` in the grid detail, `"(no changes)"` in the diff block) is deliberate display *policy*, not a crash guard — but the policy is written twice in the same module. Consolidate into one private rendering helper (`", ".join(...) or "no changes"`). Do **not** move it onto `ActionPlan`: that would leak display vocabulary into the domain layer.

**Effort:** small each.

---

## E. Wrong data structure encoding a special case

**`application/dependency_resolution.py:132`** — Tarjan's `on_stack: dict[QualifiedName, bool]` is a set wearing a dict costume: every write is `True` (push) or `False` (pop), the only read is a truthiness check via `.get()`, and the implicit-`None` fallback is unreachable (the `elif` branch is only taken for already-visited nodes, which always have an entry). Replace with `set[QualifiedName]`: `add`/`discard`/`in`. Purely local to `_strongly_connected_components`.

**Effort:** trivial.

---

## F. The same unwrap repeated across layers

Three sites hand-unwrap the optional primary key into a column tuple:

- `api/table.py:166` — `DeltaTable.primary_key` property
- `api/table.py:65` — `ForeignKey._to_constraint` (reaches into `desired.primary_key` on the *referenced* table)
- `application/dependency_resolution.py:29` — `_primary_key_columns` module helper

**Elimination:** add a `primary_key_columns: tuple[str, ...]` property to `TableSnapshot` (returns `()` when no PK) and delete all three unwraps. The `api/table.py:65` site can go further: it calls `target.to_desired_table()` and re-unwraps the domain optional when `DeltaTable.primary_key` (the public property) already returns the unwrapped tuple — the code reaches one layer deeper than it needs to. The `if not referenced_columns: raise ValueError` immediately below stays: a missing PK on an FK target is a genuine user error.

**Effort:** small, three files, no public API change.

---

## G. A guard that masks an invariant violation instead of failing loud

**`adapters/databricks/reader.py:158`** — `_fetch_properties` does `if not row: return MappingProxyType({})` after `DESCRIBE DETAIL`. For a table that `_table_exists` just confirmed, an empty result is not a normal state — it signals a race or a catalog inconsistency. The current fallback silently reports `TablePresent` with no properties, so the differ would re-apply every property. The Ousterhoutian fix here is the *opposite* of a fallback: let the unexpected state raise, and `fetch_state`'s total error boundary converts it to `ReadFailed` — the correct outcome for "could not determine state".

**Ripple:** two reader tests currently pin the silent behaviour (`describe_rows=[]` asserting `TablePresent` with empty properties); they should assert `ReadFailed` instead. Related to the existing todo items on reviewing the reader's `AnalysisException` handling.

**Effort:** small, but changes pinned behaviour (see Decisions).

---

## H. Defence-in-depth to decide on deliberately — compiler guards vs a `ValidatedActionPlan`

The compiler carries three loud guards that re-state what validation already enforces:

- `compile.py:104` — `AddColumn` with a non-nullable column raises (validation rule `NonNullableColumnAdd` blocks it)
- `compile.py:219` — `ColumnTypeChange` raises unconditionally (rule `UnsupportedColumnTypeChange` blocks it)
- `compile.py:229` — `PartitioningChange` raises unconditionally (rule `DisallowPartitioningChange` blocks it)

These are *not* dead in the strict sense: `validate_plan(rules=...)` is deliberately overridable, so the "validation blocked this" invariant is genuinely bypassable by design. True elimination requires making unvalidated plans unable to reach the compiler — e.g. a `ValidatedActionPlan` type that only `validate_plan` can produce and that `compile_plan`/`PlanExecutor.execute` require. That ripples through the engine, ports, and executor signatures.

**This is a design decision, not a cleanup:** either accept the guards as the documented cost of an open rule set, or invest in the validated-plan type. Do not simply delete the guards — that would trade a loud failure for silent emission of unsupported DDL.

---

## Decisions needed before implementing

1. **`SyncReport.__str__` zero-table sentinel (D):** deleting it changes visible output from `"Sync report: 0 tables"` to a header-only grid + zero-count footer. Better output, but it is a behaviour change.
2. **Reader empty-DESCRIBE fallback (G):** flips two pinned tests from `TablePresent`-with-empty-properties to `ReadFailed`. Arguably a bug fix; still a behaviour change.
3. **Compiler guards (H):** keep as defence-in-depth, or design the `ValidatedActionPlan` type.
4. **Observed PK naming (A wrinkle):** keep an unnamed constraint type for observed PKs, or extend the reader to fetch PK constraint names.

## Suggested implementation grouping

1. **Quick-wins PR** (no API ripple, no behaviour change): B (`partitioned_by`), C (all three), D (`table.py` wrappers, rendering consolidation), E, F.
2. **Named-constraint refactor PR** (cluster A) — supersedes the existing `constraint_name` todo item.
3. **Decision items** (1–4 above) — each handled separately once decided.

---

## Examined and kept — legitimate boundaries

These sites were challenged by the audit and survived; recorded here so they are not re-flagged later.

- **`domain/plan/differ.py:96`** — `observed is None` → `CreateTable`. A real behavioural fork: CREATE TABLE and per-column ALTERs are not equivalent DDL, and `ObservedTable(columns=())` is unconstructible by design (`TableSnapshot` requires at least one column). The empty-observed-collections trick works for tags/FKs (and is already used, per the comment at differ.py:113) but cannot subsume table creation itself.
- **`isinstance` dispatch on `CatalogState`** (`engine.py:169`, `engine.py:189`, `rendering.py:150`) — the three variants have structurally different fields and drive genuinely different behaviour (skip planning vs plan-create vs plan-diff; distinct log levels; distinct rendering). This is how Python sum types are consumed; each check appears exactly once per concern.
- **`isinstance(result, ExecutionFailed)`** (`executor.py:74`, `ports.py:115/121`) — dispatch on the `ExecutionResult` sum type. Adding a shared `failed: bool` would make `ExecutionSucceeded(failed=True)` representable — reintroducing exactly the invalid state the union was designed to exclude (ports.py:94 documents this). A possible internal refinement (partition results once in `ExecutionSummary.__post_init__`) exists but moves the isinstance rather than removing information-hiding cost; not worth it at current size.
- **`dry_run` on `Engine.sync`** — a genuine mode with three coordinated forks (skip execution, suppress `SyncFailedError`, log wording). A no-op executor could absorb the first fork but not the other two; moving the flag to construction time changes the public API without net simplification.
- **`_diff_primary_key`'s `set(...) if ... else set()`** (differ.py:272-273) — this is the general case done *right*: converting optionals to sets lets a single `==` handle all four present/absent combinations with no further branching.
- **`render_diff_block`'s `(no changes)` / `(could not read — no diff)` branches** (rendering.py:150-153) — deliberate display contract distinguishing semantically different outcomes, not collection-safety guards. (The duplication of the "no changes" *string* with `_grid_detail` is addressed in D.)
- **`domain_type_from_spark` returning `None` for unmapped types** (types.py:78) — a routine, expected condition (new Spark types appear over time); the reader's skip-and-warn is the designed handling, not a masked error.
