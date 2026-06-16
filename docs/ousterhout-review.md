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
vanish (the project targets 3.12, so `match` is idiomatic):

```python
@dataclass(frozen=True, slots=True)
class ReadPresent:  observed: ObservedTable
@dataclass(frozen=True, slots=True)
class ReadAbsent:   pass
@dataclass(frozen=True, slots=True)
class ReadFailed:   failure: ReadFailure

ReadResult = ReadPresent | ReadAbsent | ReadFailed
```

Inside a `ReadPresent` branch, `observed` is typed `ObservedTable`, not
`| None` — the guard disappears. **`ExecutionResult.__post_init__`**
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

### B5 — `columnMapping.mode` precondition for DROP is unchecked — *medium, narrow*

`adapters/schema/delta/table.py:14-18`

The default `columnMapping.mode=name` protects the common case, but a user can
override it to `"none"` (it is a managed key) and a `DropColumn` then fails at
runtime. Worth a validation rule — but the obvious implementation checks
`desired.properties`, where the key is *always* present via
`effective_properties`, so the `.get` default is dead code. A correct rule
checks `observed.properties` and/or that the plan contains a preceding
`SetProperty` for the mode. Lower priority than B1–B3.

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
