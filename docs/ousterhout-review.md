# Delta-Engine: An Ousterhout Review

A design and architecture review of `delta-engine` through the lens of John
Ousterhout's *A Philosophy of Software Design*: deep modules, information
hiding, semantic consolidation, defining errors out of existence, and avoiding
shallow / temporal-decomposition layers. The review also covers real-world
fitness — what breaks in production and what is hard to extend.

**Method.** Every module across all four layers (`domain`, `application`,
`ports`, `adapters`) was read. Findings were generated under five distinct
Ousterhout lenses and then **adversarially verified against the actual code** —
39 raised, 5 rejected as misreads, 34 survived. The verification pass matters:
Ousterhout reviews tend to over-merge, so several proposed "consolidations" were
killed because they would only relocate complexity or conflate distinct
semantics.

## Verdict

This is genuinely good code. The hexagonal layering is clean, the domain is
pure, the diff → plan → validate → execute flow is the right spine, and the
`ActionPhase` / `subject` ordering contract (each action orders itself) is
exactly the kind of deep design Ousterhout praises. The findings below are
refinements and — more importantly — real-world gaps that will bite in
production.

The review is split into **(A) Ousterhout / readability** and **(B) real-world
fitness**, which is where the highest-impact issues live.

---

## A. Ousterhout: depth, consolidation, readability

### A1 — Inline the four pass-through methods in `Engine` — *high*

`application/engine.py:162-176`

`_read`, `_plan`, `_validate`, `_execute` are each a single-line delegation:

```python
def _read(self, desired):     return self.reader.fetch_state(desired.qualified_name)
def _plan(self, desired, o):  return make_plan_context(desired, o)
def _validate(self, context): return ValidationResult(failures=self.validator.validate(context))
def _execute(self, context):  return self.executor.execute(context.plan)
```

This is **temporal decomposition dressed as abstraction** — the method names
encode "what phase runs next," but `_sync_table` already says that through its
variable names (`read_result`, `context`, `validation`, `executions`) and
logging. The wrappers hide nothing; they make you jump across five definitions
to follow one linear pipeline. None are overridden anywhere, and tests only
exercise the public `sync`. **Inline all four.**

This is the direct analogue of the insert/delete example — except the move here
is the reverse: don't add semantic-free names that fragment a single coherent
operation.

### A2 — `ReadResult` is a tri-state encoded in two Optionals; make it a sum type — *medium*

`application/results.py:72-97`

```python
class ReadResult:
    observed: ObservedTable | None = None
    failure: ReadFailure | None = None
```

Three legal states (present / absent / failed), but the fourth combination
`(observed=X, failure=X)` is structurally constructible and meaningless — which
is *why* the three `create_present/absent/failed` classmethods exist: they are a
convention guarding an over-permissive constructor. Every caller decodes the
encoding manually (`engine.py:118` tests `.failure`, then `:129` tests
`.observed is not None`).

This is Ousterhout's "define errors out of existence." Replace with a real sum
type and the illegal state, the classmethods, and the two-step decode all
vanish (the project targets 3.12, so `match` is idiomatic).

**Decision (deviation from the original three-variant sketch below):** the
implemented shape is **two variants** — `ReadSucceeded(observed: ObservedTable |
None) | ReadFailed(failure)`. Rationale: the engine treats *present* and
*absent* identically — both flow into `make_plan_context(desired, observed)`
(the differ already reads `observed is None` as "create the table"), and the
only place that distinguishes them is a log string. A three-way split would
force the engine to re-derive `observed | None` for the shared planning path,
re-introducing a decode step. Two variants eliminate the illegal
`(observed=X, failure=X)` state — the whole point of the finding — while
collapsing the engine to a clean 2-way `match`. Present/absent remains an
`observed is not None` check at the one log site that cares.

```python
@dataclass(frozen=True, slots=True)
class ReadSucceeded:
    observed: ObservedTable | None   # None = table absent

@dataclass(frozen=True, slots=True)
class ReadFailed:
    failure: ReadFailure

ReadResult = ReadSucceeded | ReadFailed
```

Original three-variant sketch (superseded by the decision above):

```python
@dataclass(frozen=True, slots=True)
class ReadPresent:  observed: ObservedTable
@dataclass(frozen=True, slots=True)
class ReadAbsent:   pass
@dataclass(frozen=True, slots=True)
class ReadFailed:   failure: ReadFailure

ReadResult = ReadPresent | ReadAbsent | ReadFailed
```

**`ExecutionResult.__post_init__`**
(`results.py:128-133`) is the same smell (`FAILED` requires a `failure`; the
others forbid it) and splits the same way into
`ExecutionOk | ExecutionNoop | ExecutionFailed` — lower priority since the
runtime guard at least prevents silent misuse.

### A3 — Push failure formatting onto the failure types — *medium*

`application/format_report.py:27-36`

The formatter `isinstance`-dispatches over
`ReadFailure | ValidationFailure | ExecutionFailure`. There is no `else`, so a
fourth failure type silently emits *nothing*. This is the "merge insert+delete"
lesson applied to a closed union: the shared concept is "a failure that can
render itself." Give each variant `format_line(self) -> str`, declare it on a
`Failure(Protocol)`, and the loop collapses to:

```python
for failure in table_report.all_failures:
    lines.append(f"    {failure.format_line()}")
```

The dispatch moves inside the types where the data already lives, and a new
variant *must* implement it.

### A4 — Collapse the three diff sub-modules into `differ.py` — *medium*

`domain/services/differ.py:31-33`, `domain/services/column_diff.py`,
`domain/services/table_diff.py`

`diff_columns`, `diff_properties`, `diff_table_comments` share one contract —
*(desired_X, observed_X) → tuple[Action, ...]* — and their **only** caller is
`diff_tables`, which concatenates them. They are publicly importable but
de-facto private; the separate test files couple to internal cut-points rather
than the `diff_tables` contract. Move them into `differ.py` as private helpers
(`_diff_columns` stays a named ~41-line function; the two trivial ones can
inline), shrinking the public surface to one symbol. Re-home tests onto
`test_differ.py` (which already has a cross-cutting integration test).

> The narrower "split `diff_table_comments` out of `table_diff.py`" finding was
> **rejected** — properties + comments are coherently "table-level scalar
> attributes." The only real defect there is the stale `table_diff.py` docstring
> ("table property mappings") which omits comments.

### A5 — Shallow-module cluster around planning — *low*

- `application/ordering.py` — a whole file + 7-line docstring for
  `return (action.phase, action.subject)`. Move it into `plan.py` as a named
  constant.
- The injectable `sort_key` param on `make_plan_context` / `_order_actions`
  (`plan.py:18,38-51`) is speculative generality — nothing injects a non-default.
  *Caveat:* it is the seam `test_plan.py` uses to test ordering mechanics with
  fake actions, so keep the param on `_order_actions` if you want that unit test;
  just drop the file and the cross-module import.
- `_AppliedStep` (`adapters/databricks/catalog/executor.py:29-36`) — an
  intermediate type between `_apply` and `_to_results` that could collapse into
  one method appending `ExecutionResult` directly.
- `errors.py` / `format_report.py` split — one concept (how a sync failure is
  communicated) across two files with a trivial boundary; fold the formatter
  into `errors.py`.

### A6 — Small correctness / consistency wins — *low, high-confidence*

- **`AddColumn` always emits `COMMENT ''`** (`adapters/databricks/sql/compile.py:67-76`)
  — spurious DDL for commentless columns; `_column_definition` already does this
  right (omits when empty). Align them.
- **Misleading comment** at `compile.py:71` — "added as nullable and then
  tightened later." There is *no* tighten-later path for added columns; NOT NULL
  adds are *rejected* at validation. (The larger "protocol split across 3
  modules" finding was rejected as a misread — but the comment is confirmed
  wrong.) Fix the comment.
- **`Action.subject`** raises `NotImplementedError`
  (`domain/plan/actions.py:47-49`) — make `Action(ABC)` and `@abstractmethod` so
  it fails at definition, not call.
- **`_fetch_table_comment(str)`** (`adapters/databricks/catalog/reader.py:115`)
  is the lone private method taking `str` not `QualifiedName`; the `str()` leaks
  to the call site. Make it consistent.
- **`Registry`** keeps a parallel `list` + `set` and re-sorts on every
  `__iter__` (`application/registry.py`) — a single `dict[str, DesiredTable]`
  does insertion, dedup, and ordering with no shadow index.

---

## B. Real-world fitness — what breaks in production

This is where to focus first. Three of these are silent-correctness or crash
issues that the test suite misses because it runs against a clean local /
Unity-Catalog Spark.

### B1 — Silent type-change drift — *high* 🔴

`domain/services/column_diff.py:20-29`

`diff_columns` matches columns by name and **never compares `data_type`**.
Change `Integer` → `Long` (or `String` → `Date`) on an existing column and the
diff produces **zero actions** — `sync()` returns `SUCCESS`, the schema silently
drifts from the declared spec. There is even a test asserting this
(`test_ignores_type_changes_until_type_migrations_supported`).

**Fix (verified as the right one):** a `TypeChangeDrift` validation rule — *not*
a sentinel action (which would force the compiler to special-case a
non-executable action and corrupt the "every Action is executable" invariant).
The rule iterates desired/observed common columns, emits a `ValidationFailure`
on type mismatch, flips status to `VALIDATION_FAILED`. Slots into the existing
`Rule` mechanism with zero domain / compiler changes. Later, when type
migrations are supported, lift it to an action.

### B2 — Reader crashes the whole run on common production schemas — *high* 🔴

`adapters/databricks/catalog/reader.py:77-98`, `adapters/databricks/sql/types.py:102`

Two paths escape the `except _SPARK_EXCEPTION` isolation and take down the
**entire sync** (not just one table) — defeating the per-table failure isolation
the README advertises:

1. **`_table_exists` is called *before* the `try` block opens.** A missing
   namespace → `AnalysisException` propagates uncaught out of `fetch_state`.
2. **`domain_type_from_spark` raises `TypeError`** for `StructType` /
   `BinaryType` / `ShortType` / `ByteType` — all ubiquitous in real Delta tables
   — and `TypeError` is not in `_SPARK_EXCEPTION`. One struct column kills the
   run.

**Fix:** move `_table_exists` inside the try; broaden to convert these into a
per-table `ReadFailure`. (The "SkippedType sentinel" alternative was flagged as
*worse* — it would silently drop columns and trigger spurious `AddColumn`s.) The
`CatalogStateReader` contract says it returns a `ReadResult`; right now it does
not honor that on two paths.

Related crash, same root: **mixed-case column names** (`reader.py:54`) —
`Column.__post_init__` rejects non-lowercase, but Hive Metastore preserves case
(`EventId`). Apply `.casefold()` at the adapter boundary (lines 54 *and* 84 for
partitions). The adapter should absorb the case mismatch — "pull complexity
downward."

### B3 — Nullability tightening passes validation, fails at runtime, leaves table half-migrated — *high* 🔴

`domain/services/column_diff.py:35-39` + `adapters/databricks/catalog/executor.py:57-93`

Setting an existing nullable column to `NOT NULL` emits
`SetColumnNullability(nullable=False)` with **no validation guard**. If the
column has NULLs, Spark fails the `ALTER` — but by then earlier actions in the
plan have already committed, **and the executor does not stop**: `_apply` loops
over all statements without `break`, so subsequent actions run against an
inconsistent table with no rollback. Half-migrated DDL is the worst failure this
engine can produce.

**Two fixes, both warranted:**

- Add a `NullabilityTighteningOnExistingColumn` rule (mirrors
  `NonNullableColumnAdd`; message should explain the safe path: stay nullable →
  backfill → tighten). Target only `nullable=False` actions on existing columns.
- **`break` on first execution failure** in `_apply`. The actions form a
  dependency chain (ADD < DROP < … < SET NULLABILITY) — continuing past a
  failure is never the safe default. Fail-stop makes the report honest: "stopped
  at action N; 0..N-1 committed."

### B4 — Column rename = silent drop+add = data loss — *medium*

`domain/services/column_diff.py:17-22`

Rename `customer_name` → `full_name` and you get `DropColumn` + `AddColumn` —
data gone. *However*, both the severity and the fix are tempered: the drop is
explicit in the user's declared state (visible in logs / plan counts), and a
"possible rename" heuristic on matching type would **false-positive constantly**
given the coarse type vocabulary, blocking legitimate drop+add. So: **do not**
add the heuristic. Either accept-and-document this as a known boundary, or
implement a real `RenameColumn` action (needs column-mapping mode + a new phase
slot between ADD and DROP). Recommendation: document it now, build it when a
user actually needs it.

### B5 — `columnMapping.mode` precondition for DROP is unchecked — *medium, narrow* — **resolved: documented, not validated**

`adapters/schema/delta/table.py`

The default `columnMapping.mode=name` protects the common case, but a user can
override it to `"none"` (it is a managed key) and a `DropColumn` then fails at
runtime.

**Decision:** documented rather than enforced with a rule. Two reasons:
1. **Layering.** A rule that hardcodes the `delta.columnMapping.mode` string
   would leak a Delta/Databricks-specific concept into the backend-agnostic
   `application/validation.py`, where every other rule is generic. The right home
   for such a check would be a Databricks-specific rule set composed in at engine
   build time — worth doing if more backend-specific preconditions accumulate,
   but not justified by this single narrow case today.
2. **Narrow residual risk.** The default protects every normal case, and B3's
   fail-stop means a failed DROP no longer cascades into later actions. Triggering
   it requires a deliberate `properties={"delta.columnMapping.mode": "none"}`
   override *and* a column drop in the same sync.

Documented on the `DeltaTable` class docstring so the precondition is visible at
the point a user would override the property. Revisit as a Databricks-scoped
rule (per layering option above) if backend-specific validation grows.

### B6 — `build_databricks_engine` clears root logging as a side effect — *medium*

`adapters/databricks/build_engine.py:13-30`

A factory returning a value object calls `configure_logging`, which does
`root.handlers.clear()` — silently destroying a caller's pre-configured handler
(e.g. a JSON formatter on a Databricks job), and again on every call. Make
logging opt-in; **pair that with exporting `configure_logging`** from the public
API (today it is buried in `log_config`, so the escape hatch the docstring
advertises is not reachable).

### B7 — Structural gaps the model forecloses — *context, not bugs*

No transactional boundary (B3 is the acute symptom). Properties are a declared
subset (never unset) — a deliberate, defensible choice, but not obvious to
users; document it. No support for views, constraints, generated / identity
columns, or liquid clustering — fine for a DDL-only v1, but each interacts with
DROP / type changes, so worth a "non-goals" note in the README.

> On extensibility to a second backend (Snowflake / BigQuery): the worry that
> the `singledispatch` compiler is a fragile seam was **rejected** — it is
> adapter-internal, fully covered, and the real port is `PlanExecutor`. The
> seams are right. One cheap guard: a unit test asserting every concrete
> `Action` subclass compiles without `NotImplementedError`, so a new domain
> action cannot silently lack a handler.

---

## Suggested order of attack

1. **B2, B3** (crash + half-migration) — reliability bugs, not polish. Highest
   blast radius.
2. **B1** (silent type drift) — a `sync` that reports SUCCESS while the schema is
   wrong undermines the whole declarative premise.
3. **A1, A2, A3** — the highest-leverage readability / depth wins, and exactly
   the semantic-consolidation spirit being sought.
4. **A4, B6**, then the low-severity cluster (A5, A6) opportunistically.

Suggested branching: the B2 / B3 reliability fixes as one focused, test-driven
branch (they are currently-passing-but-wrong behaviors that need failing tests
first); the A1–A3 refactor as a separate branch since it touches the result
types broadly.

---

## Appendix: findings rejected on verification

These were raised by a reviewer and then rejected against the actual code —
recorded so they are not re-litigated:

- **`DesiredTableSource` Protocol is needless ceremony.** Rejected: it is a
  deliberate hexagonal boundary keeping `application/registry.py` from importing
  the adapter-layer `DeltaTable`. Removing it would leak `DesiredTable` into
  user call sites — a worse public API.
- **Split `diff_table_comments` out of `table_diff.py`.** Rejected: properties
  and comments are coherently "table-level scalar attributes." Only the module
  docstring is stale.
- **`_order_actions` is a pass-through wrapper.** Rejected: it has dedicated
  unit tests using an injected fake `sort_key`; the param is a real testability
  seam, not boilerplate.
- **"Add nullable then tighten" protocol split across three modules.** Rejected:
  that two-step protocol does not exist for added columns. New tables render NOT
  NULL inline; NOT NULL adds to existing tables are *rejected* at validation.
  Only the misleading `compile.py:71` comment is real (see A6).
- **`singledispatch` compiler needs a compile-time-enforced port.** Rejected:
  the compiler is adapter-internal and fully covered; the real port is
  `PlanExecutor`. Promoting it to a public interface would add a shallow layer.

---

## C. Investigations — run later, separate from Part A/B

Stage C is a set of read-and-report passes, deliberately separated from the
concrete Part A refactors (now landed on `main`) and the Part B fixes. Each
produces an assessment and a recommendation rather than an immediate change;
findings worth acting on get folded back into a future A/B stage with concrete
locations and severities. They are grouped here because they are exploratory and
share that "investigate, argue, recommend — don't just change" character.

### C1 — Encapsulation sweep: misused privates and reach-through access — *complete*

Swept the whole codebase for two encapsulation smells: (1) `_private` members
used from outside their owning class/module, and (2) reach-through / Law of
Demeter chains. Four scanners (cross-object privates in `src/`, tests reaching
into production privates, reach-through into behavioural objects, and
destructure-and-rebuild), each finding adversarially verified against the code
with a default-to-reject skeptic. 13 candidates raised → **6 confirmed, 7
rejected**.

**Headline: production code (`src/`) has no encapsulation violations.** Every
`src/`-level candidate was rejected — they were all either same-object `self._x`
access (fine), reads of frozen data records that exist to be read field-by-field
(`Column`, `ReadFailure`, `ExecutionResult` — explicitly exempt), or traversals
of PySpark's external API (`spark.catalog.getTable(...).description`), which we
don't own and can't redesign. The hexagonal layering and value-object discipline
hold up.

**All 6 confirmed findings are tests reaching into production privates** — a
test-hygiene issue, not an architectural one. They reduce to two fixes:

1. **`DatabricksReader` private read-helpers are tested directly** — *low*.
   `tests/adapters/databricks/catalog/test_reader.py` calls `reader._table_exists`
   (lines 144, 155), `reader._fetch_properties` (245, 267) and
   `reader._fetch_table_comment` (292) from outside the class, and
   `tests/e2e/test_engine_e2e.py:20` monkeypatches `_table_exists` with
   `raising=False`. These are private helpers of `_read`, behind the public
   `fetch_state`. Two viable fixes, judged per case:
   - **Promote to public** (drop the underscores: `table_exists`,
     `fetch_properties`, `fetch_table_comment`) — `DatabricksReader` is a concrete
     adapter, not a domain type, so widening its surface costs nothing and turns
     the e2e monkeypatch into a legitimate seam (drop `raising=False`). Do **not**
     add them to the `CatalogStateReader` Protocol — they are adapter-internal,
     not part of the port.
   - **Or route through `fetch_state`** and assert on `result.observed.{properties,
     comment}` — most of this coverage already exists at the `fetch_state` level
     (e.g. the empty-properties path), so several of these tests are redundant and
     could simply be deleted. One gap: the `None`-description→`""` coercion is only
     tested via `_fetch_table_comment`, so it needs a replacement `fetch_state`
     test before that one is removed.

   The e2e monkeypatch (medium) is the strongest case for the *promote* option —
   the existence check is a genuine injection seam the local-Spark e2e suite
   depends on, so the private name sends the wrong signal.

2. **`test_compile.py` interrogates the private `_compile_action`** — *low*.
   The B7 completeness guard imports `_compile_action` and reads its
   `.dispatch(...)` registry (lines 3, 60, 67) to assert every `Action` has a
   compiler. The intent is sound (the invariant can't be checked through
   `compile_plan` without an `ActionPlan` per action type), but reaching into a
   private singledispatch function is fragile. Fix: rename `_compile_action` →
   `compile_action` in `compile.py` (update `compile_plan`, the seven
   `@register` decorators, and the test import). It is already indirectly public
   via `compile_plan`; the underscore is a false privacy signal.

**Recommendation:** all findings are low/medium test hygiene. The cleanest single
move is to **make the genuinely-needed seams public** — `DatabricksReader`'s
read-helpers and `compile_action` — and **delete the now-redundant private-poking
tests** in favour of the existing public-interface coverage, adding the one
missing `fetch_state`-level test for the null-comment case. No production
behaviour changes; this is a tests + naming pass. Worth doing opportunistically,
not urgent.

**Rejected (recorded so they're not re-litigated):** `TableRunReport._any_action_failed`
(same-object); `engine` reading `read_result.failure.{exception_type,message}` for
logging (frozen data record; and `format_line()` would double-prefix the log line);
`spark.catalog.getTable(...).description` (external API); `compile.py`
`action.column.name`/`data_type` (data records); `errors._format_failure_detail`
(uses public surface correctly); `errors` reading `execution_results` for SQL
previews (public field of a data record; a `failed_execution_previews` accessor
would be a shallow wrapper coupling a result value object to error-formatting).

### C2 — Deep-simplification pass: question the core abstractions — *complete*

Stepped back from the central abstractions and asked, for each, whether it is the
right shape in Ousterhout's sense or an accidental one. Method: for each question,
two opposing proposals (e.g. unify vs. keep-separate) were generated, each
adversarially critiqued against the code, then synthesised into one recommendation
— with "leave as is" treated as a first-class answer. The bar throughout: does a
change *hide more* complexity, or just move it?

**Bottom line: the three core abstractions are fundamentally sound.** No
restructuring is warranted. The pass surfaced one genuinely free win, two
structural clean-ups worth batching into a future tidy-up, and one real
prerequisite question (`NOOP`) that must be answered before a tempting change.

> **Status (implemented):** Q1a (declare public API) and Q1b (delete `NOOP`,
> split `ExecutionResult`) have since been actioned on `docs/ousterhout-stage-c`.
> The `NOOP` question below was resolved by **deleting** it (Option B): the
> diff-based design never produces an already-satisfied action, so `NOOP` had no
> production meaning. `ExecutionResult` is now `ExecutionSucceeded |
> ExecutionFailed`, `ActionStatus` and the `__post_init__` guard are gone. Q2 and
> Q3 below remain recommendations only.

#### Q1 — Result/outcome types: keep separate; declare the public surface; the `ExecutionResult` split is blocked on `NOOP`

- **Do not merge `ReadResult` / `ValidationResult` / `ExecutionResult`** into one
  generic outcome type. They are genuinely distinct concepts (the A2/A3 verifiers
  warned about this); a shared `Result[T]` would conflate them and the engine's
  per-phase handling does not simplify. *Leave as is.*
- **Free win — declare the public surface.** `application/__init__.py` is empty,
  so `SyncReport` / `SyncFailedError` / `Failure` are reachable-but-undeclared
  (callers get them via the engine return value / raised error). Export the
  intended public contract (`Engine`, `SyncFailedError`, `SyncReport`, `Failure`)
  from `application/__init__.py` — four import lines, zero structural change,
  formalises what the type annotations already say (`all_failures` /
  `failures_by_table` already return `tuple[Failure, ...]`). Keep
  `ExecutionResult`/`TableRunReport` internals out of the declared surface unless a
  user needs them. *Low severity; safe to do anytime.*
  - *Dissent:* the empty `__init__` may be deliberate; dual import paths for the
    same name are an occasional footgun. With no external users yet, "leave empty
    until asked" is defensible.
- **`ExecutionResult` OK/NOOP/FAILED split — deferred, blocked on a decision.**
  The `__post_init__` invariant (`FAILED ⇔ failure present`, `results.py:147-152`)
  is a real Ousterhout signal that a sum type would make structural. *But* the
  `NOOP` variant is a phantom: `DatabricksExecutor` only ever emits `OK`/`FAILED`
  (`executor.py:70-88`), yet the test suite constructs `NOOP` and relies on it for
  `SUCCESS` status. Splitting now would promote a never-emitted state into the type
  hierarchy — worse, not better. **Decide first:** either (A) give `NOOP` a real
  emitter (e.g. executor marks already-satisfied actions as no-ops), or (B) delete
  `NOOP`, collapse into `OK`, and split into `ExecutionSucceeded | ExecutionFailed`
  (then `ActionStatus` becomes dead code). Both lead to a cleaner union; the split
  is sound *after* the `NOOP` question is answered, not before.

#### Q2 — `PlanContext`: keep it; optionally name the `is_create` guard later

- **`PlanContext{desired, observed, plan}` is the right abstraction — leave the
  structure unchanged.** It is a deep module: `make_plan_context(desired, observed)`
  hides the diff-and-sort derivation and hands the validator one argument that
  carries the type-level guarantee that `plan` was derived from that exact
  desired/observed pair. Dissolving it into three positional args would widen every
  rule signature and move complexity outward.
- **Optional, deferred:** all four rules open with `if ctx.observed is None: return
  None`. An `is_create` property on `PlanContext` would name that as a domain
  concept ("creating, not altering"). Marginal — `observed is None` is already
  transparent — and it carries a test cost (`_FakeContext` in `test_validation.py`
  must gain the property too). Worth doing when a fifth rule is added (the naming
  benefit compounds) or in a deliberate naming pass; not on its own today.
- **Do not** promote `observed_columns_by_name` onto `PlanContext`: only
  `UnsupportedColumnTypeChange` builds that lookup; single-consumer derived state
  belongs in a local variable.

#### Q3 — Validation: the `Rule` ABC + `PlanValidator` are shallow; consider functions in a future clean-up

- **The `Rule(ABC)` + `PlanValidator` pairing is shallow ceremony.** No concrete
  rule holds state; `evaluate`'s `self` is never used; `PlanValidator.validate` is
  a four-line for-loop whose interface is as wide as its implementation. Tellingly,
  `_FakeValidator` in `test_engine.py` does **not** subclass `PlanValidator` — the
  nominal type enforces nothing today. Plain predicate functions
  (`Callable[[PlanContext], ValidationFailure | None]`) collected in a tuple, with
  `validate_plan` a module function and `DEFAULT_VALIDATOR` a `functools.partial`,
  would lose nothing and remove the instantiation/`self`-dispatch theatre. Keep
  `PlanValidator` as a `Callable` type alias so the engine signature is untouched;
  give `rule_name` explicit string literals (not `__class__.__name__`, which would
  flip PascalCase→snake_case and break any downstream parsing). *Low severity,
  purely structural, no behaviour change — batch into a future tightening pass.*
  - *Dissent:* the ABC is a visible, greppable extension point; a team with many
    contributors may prefer that explicitness over the (real) Ousterhout win. An
    ergonomic preference, not a correctness one.
- **Altitude — keep "diff proposes, validator disposes."** The rule mechanism as
  the home for "what is unsafe/unsupported" is the right seam (it absorbed B1/B3/B5
  cleanly). Folding safety checks into the differ, or scattering them as domain
  invariants, would couple concerns the current separation keeps clean. The rule
  bag is not yet a dumping ground; revisit only if it grows large or the
  unsupported-for-now vs. genuinely-dangerous distinction starts to matter.

**Net recommendation:** do the one free win (declare the public API) opportunistically;
hold the `ExecutionResult`/`NOOP` and `Rule`-as-functions changes for a future
structural pass, with the `NOOP` decision as an explicit gate on the former. None of
these are urgent and none change runtime behaviour.

### C3 — File/folder structure and test-suite review — *investigation, pending*

Step back from individual modules and assess two things: the package layout and
the test suite as wholes.

**Package structure** — the layout is `domain/` → `application/` → `adapters/`,
with `model/`, `plan/`, `services/` under domain and `databricks/`, `schema/`
under adapters. Questions:

- Does the directory tree still match the architecture now that A4/A5 removed
  several modules (`column_diff`, `table_diff`, `ordering`, `format_report`)? Are
  any folders now thin enough that they should collapse (e.g. is
  `domain/services/` justified holding only `differ.py`)?
- Is each module in the layer its dependencies imply? Anything in `domain/` that
  reaches toward `application/` or an adapter? Anything Databricks-specific
  sitting outside `adapters/databricks/`?
- Are the public entry points obvious from the structure? A new user should be
  able to find "define a table" (`adapters/schema`) and "run a sync"
  (`adapters/databricks/build_engine`) without spelunking. Do the package
  `__init__.py` exports tell that story, or are they empty/inconsistent?
- Naming and granularity: are any files too small to justify their own module,
  or too large and doing two jobs? Do file names describe their single
  responsibility?

**Test suite** — assess the tests as a designed artefact, not just coverage:

- Does the `tests/` tree mirror `src/` cleanly after the A4/A5/A6 re-homing, and
  are the unit vs. e2e boundaries clear and consistently applied?
- Are tests written against behaviour and public interfaces (per the project's
  Detroit-school stance), or are any still coupled to private helpers or
  implementation detail? Flag any remaining `._private` calls from tests.
- Fakes vs. mocks: are test doubles used only for genuine outgoing boundaries
  (Spark), with real domain objects everywhere else? Any over-mocking?
- Gaps: which behaviours are only covered transitively (e.g. the compiler before
  A6 had no direct tests)? Are the Part B hazards untested because the suite runs
  against clean local/Unity-Catalog Spark? What would a mixed-case-HMS or
  struct-column fixture catch that the current suite cannot?
- Redundancy and clarity: duplicated setup that wants a fixture/builder,
  unclear test names, or Given/When/Then comments that have drifted from what
  the test does.
- Is the 90% coverage gate measuring the right things, or does it create
  pressure to test trivia? Are there critical paths under-covered despite the
  high number?

Deliverable: a short assessment of what (if anything) to restructure in both the
package and the tests, weighed against churn cost — structure changes are cheap
to propose and disruptive to land, so the recommendation should be explicit about
whether it earns its keep.
