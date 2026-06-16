# Delta Engine — Ousterhoutian Design Review

A review of `delta-engine` through the lens of John Ousterhout's *A Philosophy
of Software Design*. The goal is **deeper modules, simpler interfaces, lower
cognitive load, and self-documenting code** — not cosmetic churn and not new
shallow abstractions. Every recommendation here either deepens a module, hides
a leaked decision, or removes a comment that misleads the reader.

Each finding was independently checked against the source before inclusion;
where a candidate turned out to be a style nit dressed up as a design issue, or
where the "fix" would have pushed complexity *upward*, it was discarded (see
[Deliberately not recommended](#deliberately-not-recommended)).

The review has two parts:

- **[Part 1 — Module & function level](#part-1--module--function-level)**
  (findings 1–11): deep vs. shallow modules, information leakage, misleading
  comments.
- **[Part 2 — Architecture & design](#part-2--architecture--design)**
  (findings 12–18): layering, lifecycle, the domain model, extensibility seams,
  and where the *documented* design diverges from the *built* one.

The [implementation plan](#implementation-plan) at the end is a single,
dependency-ordered checklist across both parts — the list we step through one
finding at a time.

---

## Part 1 — Module & function level

## What is already deep (keep doing this)

These are genuine Ousterhoutian strengths — worth naming so they survive future
refactors:

- **Self-ordering actions.** Each `Action` subclass declares its own `phase`
  and `subject` ([domain/plan/actions.py](../src/delta_engine/domain/plan/actions.py)),
  so [`action_sort_key`](../src/delta_engine/application/ordering.py) stays
  agnostic of concrete types. A new action orders itself — the ordering
  knowledge lives in one place and the dispatcher never grows.
- **`singledispatch` SQL compiler** ([sql/compile.py](../src/delta_engine/adapters/databricks/sql/compile.py)).
  Adding an action variant adds one registered function; no central `if/elif`
  ladder to edit. Deep behind a one-line `compile_plan` interface.
- **Tri-state `ReadResult`** ([application/results.py](../src/delta_engine/application/results.py))
  with `create_present` / `create_absent` / `create_failed`. "Missing" is a
  first-class outcome rather than an exception or a `None` the caller must
  interpret — a special case defined out of existence.
- **Protocol-based ports** ([application/ports.py](../src/delta_engine/application/ports.py)).
  The engine depends on narrow interfaces (`fetch_state`, `execute`), so the
  whole Databricks backend hides behind two methods.

The findings below are about tightening an already-clean design, not rescuing a
messy one.

---

## Findings by leverage

| # | Severity | Finding | Principle |
|---|----------|---------|-----------|
| 1 | **High** | Added columns emit redundant comment/nullability actions | Define special cases out of existence |
| 2 | **High** | Case-insensitivity is documented but never enforced | Information hiding; one decision in one place |
| 3 | **High** | `_apply` docstring describes the opposite of its behaviour | Comments must match code |
| 4 | Medium | Catalog read fires `listColumns` twice per table | Avoid temporal decomposition |
| 5 | Medium | Property allow-list is defined in two places | One decision in one place |
| 6 | Medium | Engine phase methods are logging wrappers; FQN threaded through | Temporal decomposition; pull complexity down |
| 7 | Medium | Failure rendering is split, and one copy is dead code | Better together; dead code |
| 8 | Low | `adapters/schema/Column` is a pass-through wrapper | Shallow module |
| 9 | Low | `backtick` docstrings contain a copy-pasted junk token | Comments must inform |
| 10 | Low | Seven committed TODO / design-question comments | Stale comments are defects |
| 11 | Low | `diff_partition_columns` docstring leaks validation policy | Information leakage |

---

### 1. Added columns redundantly emit `SetColumnComment` and `SetColumnNullability` — High

**Where:** [domain/services/column_diff.py:44-69](../src/delta_engine/domain/services/column_diff.py#L44-L69)

**Behaviour today.** `diff_columns` makes four independent passes over the
desired columns. Two of them — `_diff_column_comments` and
`_diff_column_nullability` — build a lookup of *observed* columns and call
`.get(column.name)`. For a column that was **just added**, there is no observed
entry, so `.get()` returns `None`. Because `None != ""` and `None != True`, a
newly-added nullable column produces **three** actions instead of one:

```text
AddColumn(new_col)                       # already renders  ... ADD COLUMN new_col INT COMMENT ''
SetColumnComment(new_col, '')            # redundant — AddColumn set the comment
SetColumnNullability(new_col, True)      # compiles to ALTER COLUMN ... DROP NOT NULL on a column
                                         #   that was just added as nullable
```

This is a latent bug, not just noise: the plan executes two extra SQL
statements per added column on every run. The existing test
`test_combines_add_drop_and_updates_without_duplicates`
([test_column_diff.py:92](../tests/domain/services/test_column_diff.py#L92))
does not catch it because it asserts with `in actions` rather than equality —
its name promises "without duplicates" but it never checks for their absence.

**Why it is the special case talking.** This is Ousterhout's backspace/delete
example. Three functions each re-derive "which columns are new vs. existing,"
and two of them get the edge case subtly wrong. The fix is to make the special
case *impossible to encounter*.

**Deeper design.** Partition the columns **once**, then feed each helper only
the inputs it can reason about:

```python
def diff_columns(desired, observed):
    observed_by_name = {c.name: c for c in observed}
    desired_names = {c.name for c in desired}

    added   = tuple(c for c in desired if c.name not in observed_by_name)
    dropped = tuple(c for c in observed if c.name not in desired_names)
    common  = tuple((d, observed_by_name[d.name]) for d in desired
                    if d.name in observed_by_name)

    return (
        tuple(AddColumn(c) for c in added)
        + tuple(DropColumn(c.name) for c in dropped)
        + tuple(SetColumnComment(d.name, d.comment) for d, o in common if d.comment != o.comment)
        + tuple(SetColumnNullability(d.name, d.nullable) for d, o in common if d.nullable != o.nullable)
    )
```

The comment/nullability helpers now only ever see columns that exist in **both**
schemas — no `.get()`, no `None`, no edge case. The "how do I categorise a
column" decision lives in exactly one place instead of being rebuilt (and
mis-built) four times.

**Impact:** Fixes a latent bug. Add an equality-based regression test for the
add-to-existing-table case.

---

### 2. Case-insensitivity is documented but never enforced — High

**Where:**
[domain/model/column.py:13-16](../src/delta_engine/domain/model/column.py#L13-L16),
[domain/model/qualified_name.py:11](../src/delta_engine/domain/model/qualified_name.py#L11),
[domain/model/table.py:44-49](../src/delta_engine/domain/model/table.py#L44-L49),
[domain/services/column_diff.py:28-68](../src/delta_engine/domain/services/column_diff.py#L28-L68)

**Behaviour today.** `Column`'s docstring says *"Immutable, case-insensitive…
name (normalized to lowercase)"* and `QualifiedName`'s says *"Case-insensitive,
fully qualified identifier."* Neither type has a `__post_init__`, a `casefold()`,
or a custom `__eq__`. They store whatever string they are handed and compare
with default (case-sensitive) dataclass equality. `Column("ID") != Column("id")`.

Meanwhile the *consumers* improvise their own case rules:

- `TableSnapshot.__post_init__` casefolds for duplicate detection — but writes
  the raw name back, so the normalisation is invisible downstream.
- `diff_columns` matches names with **raw** string membership.

Unity Catalog returns lowercase names. So a user who authors
`Column("UserId", …)` passes duplicate-detection (because `"userid"` is not yet
seen) and then, on diff, `"UserId" not in {"userid"}` is `True` — producing a
spurious `AddColumn("UserId") + DropColumn("userid")` **on every sync**.

**The leak.** "Names compare case-insensitively" is a single design decision
that is currently re-expressed (and contradicted) in four modules. The
docstrings promise an invariant that nothing implements.

**Deeper design.** Let the type own its own invariant:

```python
@dataclass(frozen=True, slots=True)
class Column:
    name: str
    ...
    def __post_init__(self) -> None:
        object.__setattr__(self, "name", self.name.casefold())
```

(and the same for `QualifiedName`'s three parts). Once the value object
normalises at construction, `TableSnapshot`'s casefold becomes a plain name
comparison, all four diff helpers become correct *by construction*, the
spurious add/drop disappears, and the docstrings become true. No consumer needs
to know anything about case rules — the complexity is pulled down into the one
place that should own it.

> If you'd rather *not* commit to case-insensitivity, the minimum fix is to
> delete the false claims from both docstrings. Do one or the other — the
> current state (claim it, don't do it) is the worst option.

**Impact:** Fixes a latent bug; aligns code with its own contract.

---

### 3. `DatabricksExecutor._apply` docstring is the opposite of the code — High

**Where:** [adapters/databricks/catalog/executor.py:57-65](../src/delta_engine/adapters/databricks/catalog/executor.py#L57-L65)

**Behaviour today.** The docstring declares:

```text
Raises:
    RuntimeError: if any statement fails.
    ValueError: if the compiler returns a mismatched count.
```

The method does neither. It wraps each `spark.sql(...)` in `except Exception`,
stores the error in an `_AppliedStep`, and continues — it is *deliberately*
fault-tolerant and there is no count check anywhere. A caller who trusts the
docstring writes `except RuntimeError` that never fires and misses the real
contract (inspect `ExecutionResult.status` / `.failure`).

A comment that states the inverse of the behaviour is worse than no comment.

**Fix.** Replace the fabricated `Raises:` block with the true contract:

```text
Runs every statement to completion, capturing any exception in the
corresponding _AppliedStep rather than re-raising.
```

**Impact:** Docstring only; no behaviour change.

---

### 4. Catalog read fires `listColumns` twice for the same table — Medium

**Where:** [adapters/databricks/catalog/reader.py:109-117](../src/delta_engine/adapters/databricks/catalog/reader.py#L109-L117)
(called at [reader.py:87](../src/delta_engine/adapters/databricks/catalog/reader.py#L87) and [:90](../src/delta_engine/adapters/databricks/catalog/reader.py#L90))

**Behaviour today.** `_fetch_columns` and `_fetch_partition_columns` each call
`self.spark.catalog.listColumns(fqn)` — the same RPC, with the same argument —
and `fetch_state` calls both. Partition membership is an *attribute* of the very
`SparkColumn` objects the first call already returned; splitting it into a
second read is temporal decomposition (structured by "first columns, then
partitions" rather than by the knowledge owned).

**Deeper design.** One call, one pass, two derived tuples:

```python
catalog_columns = self.spark.catalog.listColumns(fqn)
columns = tuple(_to_domain_column(c) for c in catalog_columns)
partition_columns = tuple(c.name for c in catalog_columns
                          if bool(getattr(c, "isPartition", False)))
```

Removes a method, removes a redundant network round-trip, and makes explicit
that `partitioned_by` and `columns` come from one source.

**Impact:** Behaviour-preserving; halves catalog round-trips in the read path.

---

### 5. Property handling: wrong diff semantics, patched by a read-time allow-list — High

**Where:**
[domain/services/table_diff.py:17-45](../src/delta_engine/domain/services/table_diff.py#L17-L45),
[adapters/databricks/policy.py](../src/delta_engine/adapters/databricks/policy.py),
[adapters/schema/delta/properties.py](../src/delta_engine/adapters/schema/delta/properties.py),
[adapters/schema/delta/table.py:24](../src/delta_engine/adapters/schema/delta/table.py#L24),
`UnsetProperty` ([domain/plan/actions.py:117-127](../src/delta_engine/domain/plan/actions.py#L117-L127))

**Behaviour today.** `diff_properties` reconciles properties *bidirectionally*:
it emits `SetProperty` for declared properties whose value differs **and**
`UnsetProperty` for any observed property not in desired (`set(observed) -
set(desired)`). But Databricks is a *second writer* to the property namespace —
it auto-sets properties on `CREATE` (and the set varies by runtime version:
`delta.minReaderVersion`, `delta.feature.*`, etc.). A pure "make observed equal
desired" diff would therefore emit a spurious `UnsetProperty` for every
Databricks-injected property on every sync.

The current code suppresses that by filtering observed properties through a
read-time allow-list (`PropertyPolicy.enforce`, driven by `SUPPORTED_PROPERTIES`
in `policy.py`). The allow-list is also duplicated against
`DeltaTable._allowed_property_keys` — two hand-maintained sets that must agree.

**Why this is the special case talking.** The allow-list is a *compensating
control for the wrong diff model*. Column diffing correctly uses "make observed
equal desired" because the engine fully owns columns. Properties are different:
the engine owns only what the user *declared*. Bidirectional reconciliation is
not merely awkward — it is **unsafe**. `UnsetProperty` against a property
Databricks also manages either oscillates across syncs (Databricks re-sets what
the engine unsets) or corrupts the table (e.g. unsetting
`delta.columnMapping.mode` on an active column-mapped table). The user has never
needed to unset a property in practice; it is a capability with zero
demonstrated value and real demonstrated risk.

**Deeper design (declared-subset diff).** Make properties a *declared subset*,
not a complete desired state. Removing `UnsetProperty` is a correctness fix, not
a simplification trade-off — it deletes a special case that cannot be
implemented safely against a second writer.

- `diff_properties(desired, observed)` iterates **only** over `desired.items()`,
  emitting `SetProperty` when `observed.get(key) != desired[key]`. It never
  inspects observed-only keys. Return type narrows to `tuple[SetProperty, ...]`.
- Delete `UnsetProperty` end-to-end (action, `ActionPhase.UNSET_PROPERTY`,
  compiler handler, `__all__`, tests).
- Delete `PropertyPolicy` / `policy.py` and the read-time filter entirely — once
  the diff ignores observed-only keys, the filter has no purpose. The reader
  returns the full, unfiltered catalog property map.
- **Keep** the `Property` enum and `DeltaTable` construction-time validation —
  but decouple them from the read path. Their *only* remaining job is a
  user-facing fast-fail on typo'd keys (a misspelled `delta.enableDeletionVectorz`
  must raise at construction, not become a silent perpetual `SetProperty`).
  Rename `SUPPORTED_PROPERTIES` → `MANAGED_PROPERTY_KEYS` to name the real
  semantics.

The ownership decision now lives in exactly one runtime place — `diff_properties`
("the engine acts only on keys you declared") — with a separate, independent
authoring-time validation contract at the `DeltaTable` boundary.

> **Residual trade-off (accepted):** remove a property from a `DeltaTable`
> declaration and its old value lingers in the catalog indefinitely — the engine
> emits nothing. If that ever bites, the right fix is an *explicit*
> `unset_properties=[...]` field, never implicit unset-everything.

**Impact:** Fixes a latent correctness bug (unsafe/oscillating unsets); deletes
an action, a policy module, an enum-derived allow-list, and the read-time filter.
The only observable behaviour change is that `sync` no longer unsets properties.
*(This finding subsumes the original "allow-list defined in two places" issue —
the allow-list is removed rather than deduplicated.)*

---

### 6. Engine phase methods are logging wrappers; the FQN is threaded only for logs — Medium

**Where:** [application/engine.py:133-188](../src/delta_engine/application/engine.py#L133-L188)

**Behaviour today.** `_sync_table` delegates to `_read`, `_plan`, `_validate`,
`_execute`. Each is essentially one call to a port/helper plus log lines, and
`fully_qualified_name` is passed into all four **solely** so it can appear in
those log messages — even though it is already bound in `_sync_table`
([engine.py:111](../src/delta_engine/application/engine.py#L111)). The methods
decompose the pipeline along the time axis (read → plan → validate → execute),
which is exactly the structure `_sync_table` already makes obvious.

The tell is the threaded parameter: it exists only because the logging concern
was scattered across the phases instead of kept where the data lives.

**Deeper design.** This one needs a lighter touch than "inline everything" —
`_read` has genuinely non-trivial branch formatting and `_execute` does a small
aggregation, so collapsing all four into one 60-line method is not obviously
better. The Ousterhoutian win is narrower and clear:

- Own the per-table logging in `_sync_table`, where `fully_qualified_name` is
  already in scope, and drop the parameter from the helper signatures (make them
  zero-/single-argument internal steps that just *do the work* and return).

That removes the information-leakage symptom (the threaded string) without
trading away the readability the decomposition currently buys. If logging later
grows structured/timed, extract a single `_log_phase(result)` helper that takes
a typed result — don't re-scatter it.

**Impact:** Behaviour-preserving; removes parameter threading and consolidates
the logging concern.

---

### 7. Failure rendering is duplicated — and one copy is dead code — Medium

**Where:** [application/errors.py:14-48](../src/delta_engine/application/errors.py#L14-L48)
and [application/format_report.py:21-74](../src/delta_engine/application/format_report.py#L21-L74)

**Behaviour today.** `SyncFailedError.__init__` contains its own failure
formatter: it filters failed tables, `isinstance`-dispatches over
`ReadFailure` / `ValidationFailure` / `ExecutionFailure`, and re-walks
`execution_results` for SQL previews. `format_report.py` separately knows how to
summarise a run. The `isinstance` ladder means `errors.py` has to understand the
failure-type hierarchy to render its own message — leaked knowledge.

And `format_sync_report` **is never called** — `rg format_sync_report` returns
only its definition. So today there is exactly one live renderer and one dead
one, both encoding overlapping knowledge of how a report turns into text.

**Two things to decide:**

1. **The dead code.** Either wire `format_sync_report` into the success path
   (e.g. have callers print it) or delete it. Leaving a defined-but-unused
   public formatter invites it to drift from the live one.
2. **The leak.** Extract the failure-detail rendering into one helper that owns
   the `isinstance` dispatch — e.g. `format_failure_detail(table_report)` in
   `format_report.py` — and have `SyncFailedError.__init__` call it.

> Do **not** simply call `format_sync_report` from `SyncFailedError`: the two
> outputs are intentionally different documents (a compact failure list vs. a
> full timestamped run summary). Merging them wholesale would change the
> exception text and conflate two concerns. Share the *failure-rendering*
> sub-routine, not the whole formatter.

**Impact:** Behaviour-preserving if you extract the shared sub-routine; removes
the type-dispatch leak and resolves the dead code.

---

### 8. `adapters/schema/Column` is a pass-through wrapper — Low

**Where:** [adapters/schema/column.py:1-24](../src/delta_engine/adapters/schema/column.py#L1-L24),
used at [delta/table.py:72](../src/delta_engine/adapters/schema/delta/table.py#L72)

**Behaviour today.** The user-facing `Column` has the same four fields, same
types, and same defaults as the domain `Column`, and its only method
(`to_domain_column`) copies them across unchanged — no validation, no coercion,
no friendly errors. The interface is exactly as complex as what it wraps: a
textbook shallow module. Every field added to the domain `Column` must be
mirrored here plus in the conversion line.

**Deeper design.** Re-export the domain type from the schema package and drop
the wrapper:

```python
# adapters/schema/__init__.py
from delta_engine.domain.model import Column
```

`DeltaTable` then stores domain `Column`s directly and `to_desired_table` passes
them through. Callers doing `from delta_engine.adapters.schema import Column`
are unaffected — they just get the real type. Reintroduce a wrapper only when
there is an actual user-facing concern to hide (e.g. accepting string type
names), with a concrete reason.

**Impact:** Removes a file, a class, a method, and a sync-point. No runtime
change.

---

### 9. `backtick` docstrings contain a copy-pasted junk token — Low

**Where:** [adapters/databricks/sql/dialect.py:9](../src/delta_engine/adapters/databricks/sql/dialect.py#L9)
and [:19](../src/delta_engine/adapters/databricks/sql/dialect.py#L19)

`backtick`'s docstring says it escapes *"backtick_qualified_names"* — the name
of the next function in the file, pasted where a description belongs.
`backtick_qualified_name`'s docstring repeats the same garbled token. A reader
learns nothing and may hunt for a non-existent concept. The one non-obvious fact
— that an embedded backtick is **doubled** — is never stated. Mirror the good
sibling `quote_literal`:

```python
def backtick(raw: str) -> str:
    """Backtick-quote an identifier part, doubling any embedded backtick."""

def backtick_qualified_name(qualified_name: QualifiedName) -> str:
    """Render a QualifiedName as backtick-quoted, dot-separated parts."""
```

---

### 10. Seven committed TODO / design-question comments — Low

**Where:** [compile.py:42](../src/delta_engine/adapters/databricks/sql/compile.py#L42)
& [:126](../src/delta_engine/adapters/databricks/sql/compile.py#L126),
[validation.py:11-12](../src/delta_engine/application/validation.py#L11-L12)
& [:33](../src/delta_engine/application/validation.py#L33),
[results.py:119](../src/delta_engine/application/results.py#L119),
[policy.py:9](../src/delta_engine/adapters/databricks/policy.py#L9),
[actions.py:175](../src/delta_engine/domain/plan/actions.py#L175)

Design questions left in the source (*"Are classes and ABCs the best approach?"*,
*"do we want an ActionResult…?"*) leave every reader unsure whether the current
design is final or provisional. The worst offender is **actively wrong**:
`# TODO: fix unaccessed backticked_table_name` at compile.py:42 — that parameter
*is* used by every registered dispatch handler, so the comment sends readers
hunting for a non-bug. Delete all seven; move any that represent real work to
the issue tracker.

---

### 11. `diff_partition_columns` docstring leaks validation policy — Low

**Where:** [domain/services/table_diff.py:60-71](../src/delta_engine/domain/services/table_diff.py#L60-L71)

The docstring says *"This is operation is currently disallowed. We surface this
action only to warn the user."* The diff layer's only job is to detect a
difference and emit an action; the decision that the action is *disallowed* lives
in `DisallowPartitioningChange`
([validation.py:57](../src/delta_engine/application/validation.py#L57)). Stating
the policy here couples two layers in a comment and the "currently" hedges at a
transience that doesn't exist. Remove the note (and fix the "parition" typo); the
function body is self-documenting and the validator already owns the rule.

> **Note:** if [finding 14](#14-partitionby-can-be-planned-but-cannot-be-compiled--medium)
> is implemented (delete `PartitionBy`), `diff_partition_columns` disappears
> entirely and this finding is subsumed. Do 14 first and 11 evaporates.

---

## Part 2 — Architecture & design

Part 1 looked at modules and functions. This part looks at the **architecture**:
layering, lifecycle, the domain model, extensibility seams, and whether the
documented design matches the built design. The bar is the same — deep modules,
Keep It Simple — plus one architectural rule: **no speculative generality.** A
seam that exists only because "we might need it for backend X someday" is a
cost, not an asset.

### The headline: the code is simpler and more honest than the docs claim

The clearest theme: the README and architecture diagram describe a grander
system than the one that exists — FK/PK guardrails, namespace policy, a
constraint-naming scheme, an injectable `Differ` port, a "preview" mode. The
code does none of these, and is *better* for it (a pure diff function needs no
port). The risk isn't in the code; it's that a new contributor reads the docs
first and forms a wrong mental model — looking for seams that aren't there, or
assuming safety checks that don't run. So most of Part 2 is **making the docs
tell the truth**, which is mostly deletion.

### What is well-architected (and why — so a refactor doesn't "fix" it)

- **Pure-domain diffing, hardwired on purpose.** `diff_tables` has no I/O, so it
  is *correct* that it is not a port. (The diagram wrongly implies otherwise —
  finding 13.)
- **Idempotency by re-reading, not replay.** `sync` calls `fetch_state` fresh
  every run and plans only the delta
  ([engine.py:117](../src/delta_engine/application/engine.py#L117)); re-running
  after a partial failure is already clean. *(A "partial failure isn't
  idempotent" finding was rejected on this basis.)*
- **Per-table independence is a feature.** One table's failure doesn't halt the
  others; all outcomes are collected and surfaced together. Documented intent,
  and the right model for independent DDL.
- **Continue-on-action-failure is defensible.** `ActionPhase` orders *successful*
  actions; it is not a fail-fast contract. Continuing yields better diagnostics.

---

### 12. README claims Registry guardrails that do not exist — High

**Where:** [README.md:31-35](../README.md), against
[registry.py](../src/delta_engine/application/registry.py)

The single-registry section makes three claims the code does not back:

- *"centralizes inter-table dependencies (PK/FK, uniqueness, references)"* —
  `Registry` has **no** concept of foreign/primary keys or cross-table
  references. It holds a `list[DesiredTable]` and a `set[str]` of names.
- *"Detects duplicate logical definitions pointing at the same physical table"*
  — listed as a *separate* guardrail, but there is only **one** check: the FQN
  string set ([registry.py:46](../src/delta_engine/application/registry.py#L46)).
- *"Optionally enforces a declared namespace/schema policy"* —
  `Registry.__init__` takes no arguments; no policy parameter, hook, or type
  exists anywhere.

A reader expecting FK or namespace enforcement gets none, and a contributor
can't tell which guardrails are real.

**Fix (docs only).** Delete the three unbuilt sub-claims; keep *"Enforces unique
fully-qualified names."* Track any genuinely-planned ones as issues, not
present-tense prose.

---

### 13. The architecture diagram documents a `Differ` port that isn't real — Medium

**Where:** [docs/architecture.md:17-19, 49, 54, 61-63, 85-91](architecture.md)

The class diagram shows `Differ` as a component with `diff(desired, observed)`
and draws `Engine ..> Differ : builds plan` — implying an **injectable port**,
co-equal with `CatalogStateReader` and `PlanExecutor`. No such class or protocol
exists: diffing is the pure function `diff_tables`, hardwired via
`make_plan_context` ([plan.py:16,44](../src/delta_engine/application/plan.py#L16)).
Two further drifts in the same diagram:

- The reader adapter is labelled `DatabricksStateReader`; the real class is
  [`DatabricksReader`](../src/delta_engine/adapters/databricks/catalog/reader.py#L62).
- The sequence diagram jumps `fetch_state` → `validate` with **no plan step**,
  hiding the central domain operation.

A phantom port is the strongest speculative-generality smell: someone wanting a
second backend hunts for a `Differ` interface and finds nothing, while the two
*real* ports get diluted by a fake third.

**Fix (docs only).** Remove `Differ` from the class diagram; show
`diff(desired, observed)` as an internal application step; rename to
`DatabricksReader`; add the plan step to the sequence diagram. **Do not** add a
real `Differ` port to match the diagram — fix the diagram to match the code.

---

### 14. `PartitionBy` can be planned but cannot be compiled — Medium

**Where:** [actions.py:174-189](../src/delta_engine/domain/plan/actions.py#L174-L189),
[table_diff.py:60-71](../src/delta_engine/domain/services/table_diff.py#L60-L71),
[compile.py:39-44](../src/delta_engine/adapters/databricks/sql/compile.py#L39-L44),
[validation.py:57-79](../src/delta_engine/application/validation.py#L57-L79)

There are nine `Action` subclasses; `compile_plan`'s `singledispatch` registers
**eight**. `PartitionBy` has no handler, so it hits the `NotImplementedError`
fallback. `diff_partition_columns` *does* emit it when partitioning differs on an
existing table. The only thing between that action and a runtime crash is the
`DisallowPartitioningChange` validation rule — and only when the default
validator is used (the `Engine` constructor lets a caller supply their own).

The `singledispatch` design's value is its closed contract: one action, one
handler. `PartitionBy` quietly breaks it — it has a phase, participates in
ordering tests, but is unexecutable. Its own comment
(`# consider replacing with a RequireTableRecreate action`) admits the design is
unsettled. **An adapter capability gap is being papered over by an
application-layer rule** — the dependency is inverted.

**Fix (net deletion).** Make the gap structurally impossible:

- Delete the `PartitionBy` action and remove it from `domain/plan/__init__.py`.
- Make `diff_partition_columns` return `()` (or remove it).
- Rewrite `DisallowPartitioningChange` to compare fields directly —
  `ctx.observed.partitioned_by != ctx.desired.partitioned_by` — which is
  *simpler* than the current isinstance-on-action proxy.

Behaviour is unchanged on the `sync` path; the latent crash on the
custom-validator path disappears. Update the partition ordering/validation tests.
(The alternative — register a handler that raises a nice error — only adds code
to document a limitation the validator already owns; rejected as bloat.)

---

### 15. `TableFormat` is a one-member enum that carries no weight — Medium

**Where:** [table.py:13-16, 60-68](../src/delta_engine/domain/model/table.py#L13-L16),
[adapters/schema/delta/table.py:15](../src/delta_engine/adapters/schema/delta/table.py#L15),
[compile.py:59](../src/delta_engine/adapters/databricks/sql/compile.py#L59)

`TableFormat` is a `StrEnum` with exactly one member, `DELTA`. It's a kw-only
field on `DesiredTable`, hardcoded by `DeltaTable`, and **never present on
`ObservedTable`** — so it can never be compared, never drift, never trigger an
action. Its only runtime effect is `f"USING {table.format}"`, always rendering
`USING delta`. It's exported in the domain `__all__`, advertising a
format-agnosticism the engine does not have — a "maybe Iceberg someday" hook as a
live domain concept.

**Fix (net deletion).** Remove the enum and the field; render the literal
`USING delta` in the `CreateTable` compiler (which already lives in the Databricks
adapter, so it already knows it targets Delta). Behaviour-preserving. Update the
test call sites that pass `format=TableFormat.DELTA`.

> Minimum move if full deletion feels premature: drop `TableFormat` from the
> domain `__all__` so it stops advertising a capability that isn't there.

---

### 16. README "Deterministic naming / constraint scheme" describes nothing — Medium

**Where:** [README.md:29](../README.md)

> *"Deterministic naming: Constraint names are derived from a stable scheme to
> avoid collisions and churn across runs."*

`rg constraint src/` returns nothing. The compiler emits `CREATE TABLE`,
`ALTER TABLE ADD/DROP COLUMN`, `TBLPROPERTIES`, and `COMMENT` — no named
constraints, and no action carries a constraint-name field. (Delta on Databricks
treats nullability as a column attribute, not a named constraint object, so the
claim is both unbuilt and largely inapplicable.)

**Fix (docs only).** Delete the bullet. The real guarantee — deterministic
*action ordering* — is already covered by the "Deterministic planning" bullet
directly above it.

---

### 17. README sells "previews changes" but there is no plan/preview path — Low

**Where:** [README.md:7](../README.md), [engine.py:70](../src/delta_engine/application/engine.py#L70)

`sync` is the only public entry point: it reads, plans, validates, **and
executes** in one call. `compile_plan` runs only inside the executor, and
`statement_preview` is populated *after* execution. There is no way to inspect
the planned DDL without mutating a live catalog — yet the README says the engine
"previews changes … and applies updates," implying a dry-run that doesn't exist.

**Two honest options:**

- **(a) Docs fix (recommended now):** reword to describe what's real — the
  post-execution report and its SQL excerpts. Zero code, closes the gap.
- **(b) Opt-in feature (only if you actually want it):** expose
  `Engine.plan(registry) -> reports + SQL` by lifting compilation to the
  application layer (a `Compiler` port, or split `PlanExecutor`). Genuinely
  useful — `compile_plan` is already nearly pure — but it adds a seam, so don't
  build it speculatively. Decide based on whether dry-run/CI is a real need.

Given the no-bloat bar, do (a) unless `plan()` is a deliberate roadmap item.

---

### 18. `DesiredTableSource` is in `ports.py` but is not an Engine port — Low

**Where:** [ports.py:37-42](../src/delta_engine/application/ports.py#L37-L42),
[registry.py:9,27](../src/delta_engine/application/registry.py#L27)

`ports.py` is documented as *"adapter interfaces the Engine talks to."*
`CatalogStateReader` and `PlanExecutor` are exactly that. `DesiredTableSource` is
different: it's the protocol for *"what may a user pass to `Registry.register()`"*,
and the `Engine` never touches it. Grouping it with the I/O ports blurs the two
distinct extension axes (new **backend** vs. new **authoring format**).

**Fix.** Move `DesiredTableSource` into `registry.py`, next to its only consumer.
`ports.py` then reads as exactly the Engine's external dependencies. One class
moved, two import lines; no behaviour change.

---

## Implementation plan

A single dependency-ordered checklist across both parts. We work through it one
finding at a time on a branch. Severity in brackets; **(bug)** marks the two that
deliberately change output to correct it — everything else is behaviour-preserving
or docs-only.

**Stage A — latent bugs (do first, each with a regression test):**

- [ ] **1** — Added columns emit redundant comment/nullability actions **(bug, High)**
- [ ] **2** — Case-insensitivity documented but never enforced **(bug, High)**

**Stage B — misleading docstrings/comments (fast, removes active misdirection):**

- [ ] **3** — `_apply` docstring describes the opposite of its behaviour (High)
- [ ] **9** — `backtick` docstrings contain a junk token (Low)
- [ ] **10** — Seven committed TODO / design-question comments (Low)
- [ ] **11** — `diff_partition_columns` docstring leaks validation policy (Low)
      — *subsumed by 14 if 14 is done first*

**Stage C — documentation honesty pass (one docs commit, zero code risk):**

- [ ] **12** — README Registry guardrails that don't exist (High)
- [ ] **13** — Architecture diagram `Differ` port / wrong name / missing step (Medium)
- [ ] **16** — README constraint-naming scheme describes nothing (Medium)
- [ ] **17a** — README "previews changes" reworded (Low)

**Stage D — structural tightening (behaviour-preserving code changes):**

- [ ] **14** — Delete `PartitionBy` (unexecutable action) (Medium) — *do before 11*
- [ ] **15** — Delete `TableFormat` one-member enum (Medium)
- [ ] **4** — Catalog read fires `listColumns` twice per table (Medium)
- [ ] **5** — Property handling: declared-subset diff; delete `UnsetProperty` + read-time allow-list **(bug, High)**
- [ ] **6** — Engine phase methods thread FQN only for logging (Medium)
- [ ] **7** — Failure rendering split + dead `format_sync_report` (Medium)
- [ ] **8** — `adapters/schema/Column` pass-through wrapper (Low)
- [ ] **18** — Move `DesiredTableSource` out of `ports.py` (Low)

**Stage E — opt-in, only if wanted:**

- [ ] **17b** — Add an `Engine.plan()` dry-run seam (adds a port; skip unless CI/dry-run is a real requirement)

Ordering notes: do **14 before 11** (deleting `PartitionBy` removes the function
finding 11 targets). Stage A changes test expectations, so land it before the
Stage D refactors that touch the same diff/plan code.

---

## Deliberately not recommended

Considered and rejected — true observations whose fix is style churn, or whose
"fix" adds more structure than it removes. Recorded so the reasoning is visible.

**From Part 1 (module level):**

- **Renaming local variables** (`type` → `sql_type`, `props` → `properties`,
  single-letter loop vars). Real against the naming standard, but they hide no
  complexity and simplify no interface. A linter's job, not a design review's.
  *(Worth a separate lint-only pass if desired.)*
- **Adding return hints to dunder methods / `_utc_now`.** A type-completeness
  concern, not a module-depth one.
- **Unifying the four `diff_*` function shapes.** The two single-attribute
  variants own the decision of *which* attribute to compare; "fixing" them pushes
  that decision **up** to the caller — shallower, not deeper.
- **Merging `_AppliedStep` / `_to_results` in the executor.** A clean split
  between an I/O concern and a mapping concern, with `_apply` tested
  independently. Merging entangles two concerns in one loop.

**From Part 2 (architecture):**

- **Move the result value-types out of `application` so adapters don't import
  "up".** The fix forces `results.py` to import back from `ports.py` (it embeds
  those types in `TableRunReport`), splitting one coherent module and inventing a
  layering rule. The types are the protocol's vocabulary. Net structure *added*.
- **Inline `application/ordering.py`** (an 18-line one-liner module). Shallow,
  but it's an independently-imported, separately-tested ordering *contract*.
  Inlining degrades the explicitness of a tested behaviour for no structural gain.
- **Make `sync` validate-all-then-apply-all** (global barrier). Per-table
  independence is documented intent; a global barrier holds every table hostage
  to the slowest validation — a regression for independent DDL.
- **Make the executor fail-fast on the first action error.** The motivating
  cascade isn't reachable (the planner emits `CreateTable` *or* alters, never
  both); continue-on-failure is tested and gives better diagnostics. (The real
  issue here — the false `Raises:` docstring — is finding 3.)
- **Add `IF NOT EXISTS` guards for idempotency.** Not needed: `sync` re-reads
  state each run and plans only the delta, so re-running is already clean.
- **Collapse `ObservedTable`/`TableSnapshot`** (zero-extension subclass). Rests
  on a false premise — `TableSnapshot` *is* instantiated directly (in tests) —
  and the `Desired`/`Observed` split is a useful zero-cost nominal type. The
  composition alternative adds delegation boilerplate to hide nothing.

The discipline behind both lists is the same as behind the findings: change the
design only when it removes complexity a reader would otherwise have to carry.
