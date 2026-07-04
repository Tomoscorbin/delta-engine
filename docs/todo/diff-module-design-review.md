# Diff module design review — open concerns and adjudications

Raised 2026-07-04 while reviewing the metadata-only deployment branch (PR #127).
Each concern was adversarially scrutinised against the code on
`feat/metadata-only-deployment` (head `09ad49d`). Line references are to that
revision. This is a working document: concerns are recorded with their verdicts
and candidate fixes so they can be picked up later without re-deriving the
analysis.

## Summary of verdicts

| # | Concern | Verdict |
|---|---------|---------|
| 1 | Module docstring says "facts only" but dimensions produce actions | Docstring is wrong, not the design |
| 2 | Naming gradient `Added[T]` / `ColumnAdded` / `ColumnStructureDimension` / `AddColumn` | Partly real; `Added[Column]` collision is a phantom |
| 3 | Column entries "satisfy the Dimension protocol" | False since `aspect` was added — stale docstring |
| 4 | Dimension protocol "isn't doing much work" | Overstated — it is load-bearing in exactly two places |
| 5 | `validate_diff(diff, desired)` should take one argument | Right instinct; `TableDrift` breaks the self-contained claim |
| 6 | `_diff_mapping` leaks planning policy into fact detection | Category error in `PropertiesDimension.diff`, not `_diff_mapping` |
| 7 | Drop `Changed[T]`, keep only `Added`/`Removed` | Harmful for scalar changes; defensible for mapping diffs only |
| 8 | **BUG: metadata-only + previously-synced table = permanent validation failure** | **Confirmed and reproduced — blocks PR #127** |
| 9 | "The whole module needs rethinking" | No — it needs 3–4 targeted fixes, not a rearchitecture |

---

## 1. Facts-vs-actions dual model

**Concern.** `diff.py`'s docstring claims the diff "states facts only" and
"carries no knowledge of how a difference is acted on", yet the `Dimension`
protocol's one method is `actions()` and the module imports 15 concrete action
types.

**Adjudication.** The docstring is internally incoherent — line 7 contradicts
line 12 of the same docstring. The *design* (co-locating drift detection and
action lowering in one typed object) is a defensible cohesion choice: each
dimension is the only object that knows both what its drift looks like and what
the minimal corrective action is. Splitting them would double the class count
without hiding more information. The pipeline boundary stays clean in practice:
`validate_diff` never calls `actions()`; `plan()` only runs after validation.

**Fix.** Rewrite the module docstring to describe what the module actually is:
each dimension records the facts for one aspect of drift *and* produces the
actions to reconcile it; permission policy lives in the validator. Stop
claiming "facts only".

## 2. Naming gradient

**Concern.** Four near-synonyms span fact/entry/dimension/action:
`Added[Column]`, `ColumnAdded`, `ColumnStructureDimension`, `AddColumn`.

**Adjudication.** Partly a phantom: `Added[T]` is never instantiated with
`Column` anywhere in `src/` — it is used only with `ForeignKeyConstraint`,
`PrimaryKeyConstraint`, and `KeyValue`. The real (bounded) friction is the
convention shift between past-participle facts (`ColumnAdded`,
`ColumnRemoved`) and imperative actions (`AddColumn`, `DropColumn`). That
convention is uniform across every pair and the two vocabularies live in
separate modules that no consumer imports together.

**Fix (optional).** State the convention once in the module docstring:
`Column*`-past-participle = drift fact, verb-noun = imperative action.
Renames are not warranted.

## 3. Column entries and the Dimension protocol — stale docstring

**Concern.** "ColumnAdded is not a Dimension in the English sense, but because
it has `actions()` it satisfies the same protocol."

**Adjudication.** The claim describes the code as it was *before* `aspect` was
added. Today the `Dimension` protocol ([diff.py:85-92](../../src/delta_engine/domain/plan/diff.py#L85-L92))
requires both `aspect: ClassVar[TableAspect]` and `actions()`. None of the six
column entry types has `aspect`, so none satisfies `Dimension` — verified by
runtime `__protocol_attrs__` inspection. The docstring at lines 16-18 and the
comment block at lines 104-108 are false. No runtime harm: entries never leave
their parent dimension, so nothing type-checks them against `Dimension`.

**Fix.** Correct the docstring/comment: entries implement a weaker internal
`actions()`-only contract and are not Dimensions. Optionally name that
contract (`ColumnEntry` protocol) so the `entries` field types can reference
it. Do **not** give entries an `aspect` — that would let them leak into places
`UnmanagedDimensionDrift` iterates.

## 4. Is the Dimension protocol earning its place?

**Concern.** "Dimension means 'has actions()' but validation still has to
isinstance-search for `ColumnStructureDimension`, so the abstraction isn't
doing much."

**Adjudication.** The protocol is load-bearing in exactly two consumers, and
both matter: `TableDrift.plan()` iterates dimensions uniformly
([diff.py:559-561](../../src/delta_engine/domain/plan/diff.py#L559-L561)), and
`UnmanagedDimensionDrift` reads `d.aspect` generically
([validation.py:170-181](../../src/delta_engine/application/validation.py#L170-L181))
— together they mean a new dimension type needs zero changes to planning or
metadata-only gating. The four specific rules isinstance-matching concrete
types are not protocol failures; they are sum-type match arms — the style this
codebase deliberately prefers. A rule about NOT NULL additions *should* name
`ColumnStructureDimension`.

**Residual smell.** Rules reach two levels deep: find the dimension, then
re-isinstance its `entries`. If rules multiply, consider typed accessors on
`ColumnStructureDimension` (e.g. `data_type_changes()`) so rules stop touching
raw entries. Not worth doing at the current rule count.

## 5. `validate_diff(diff, desired)` two-argument shape

**Concern.** "It should simply validate a diff."

**Adjudication.** The instinct is grounded: the module docstring claims "every
variant carries the data its consumers need, so the diff is self-contained",
and that is false for `TableDrift` — it lacks `managed_aspects`, which is why
`desired` must ride alongside. `TableMissing` already carries `desired`, so
the parameter is redundant for that arm. Of the five rules, only
`UnmanagedDimensionDrift` reads `desired` at all, and only for
`managed_aspects`.

**Preferred fix.** Add `managed_aspects: frozenset[TableAspect]` to
`TableDrift` (populated by `diff_table` from `desired`). Then:
`validate_diff(diff)` takes one argument; the `TableMissing` arm reads
`diff.desired.managed_aspects`; `Rule.evaluate` can drop its unused `desired`
parameter. Coupling is shallow — a frozenset of enum members, not the whole
`DesiredTable`. Rejected alternative: a `DriftContext` envelope object (a
pass-through layer that hides nothing).

## 6. `_diff_mapping` and Removed-entry policy

**Concern.** `_diff_mapping` always reports Removed entries and its docstring
punts the decision to "lowering policy"; `PropertiesDimension.actions()`
ignores Removed while `TableTagsDimension.actions()` acts on it.

**Adjudication.** The leak is real but the culprit is
`PropertiesDimension.diff`, not `_diff_mapping`. Tags are full-state, so
Removed is a genuine fact there. Properties are declared-subset — under those
semantics an observed-only property is *not a deviation at all*, so recording
`Removed` for it manufactures a fact that no consumer treats as one. The
consequence is not hypothetical: a `PropertiesDimension` whose only entries
are Removed still exists, still counts as "drift", and feeds concern #8.

**Preferred fix.** `PropertiesDimension.diff` should diff only the declared
keys (desired-projection): iterate `desired`, report Added/Changed, never
produce Removed. The `Removed` arm disappears from its `actions()`, the
"declared-subset semantics" caveats disappear from two docstrings, and a
properties dimension exists only when there is actionable drift.
`_diff_mapping` stays generic for the three full-state callers, or is inlined.

## 7. Dropping `Changed[T]`

**Concern.** Model a changed value as `Removed(old)` + `Added(new)` and delete
`Changed[T]`.

**Adjudication.** Harmful where `Changed` is load-bearing, defensible where it
is redundant — and it is load-bearing in six places:
`NullabilityTighteningOnExistingColumn` reads `change.desired is False`;
`ColumnDataTypeChangeNotSupported` prints from/to;
`PrimaryKeyDimension.actions()` emits Drop+Set for the Changed arm;
`TableCommentDimension` / `PartitioningDimension` carry a single atomic pair.
Splitting those into sibling entries forces consumers to re-correlate by key
and makes orphaned states representable (an `Added` with no `Removed` twin) —
the opposite of defining errors out of existence. In the mapping diffs,
however, every `Changed` arm already collapses to "use the desired value" and
discards `observed` — there it is a heavier `Added`.

**Verdict.** Keep `Changed[T]` for scalar/atomic changes. Optionally narrow
`Entry[KeyValue]` to `Added | Removed` (changed keys emit both) if #6's fix
doesn't already dissolve the question for properties. Full removal: no.

## 8. BUG — metadata-only mode dead-ends on previously-synced tables

**Status: confirmed by reproduction. Must be fixed before or with PR #127.**

**Mechanism.**
1. Every full sync writes `delta.columnMapping.mode=name`
   (`DeltaTable.default_properties`, [api/table.py:108-112](../../src/delta_engine/api/table.py#L108-L112)).
2. `metadata_only=True` forces `properties={}` and discards user properties
   ([api/table.py:156-158](../../src/delta_engine/api/table.py#L156-L158)); there is no way to declare a property on a
   metadata-only table.
3. The reader returns all catalog properties unfiltered
   ([adapters/databricks/reader.py:175-179](../../src/delta_engine/adapters/databricks/reader.py#L175-L179)).
4. `_diff_mapping` therefore yields `Removed(delta.columnMapping.mode)`, so a
   `PropertiesDimension` exists.
5. `PROPERTIES` is not in `METADATA_ASPECTS`, so `UnmanagedDimensionDrift`
   fails the sync — every run, forever, with no declaration-side workaround.

Reproduced output:
`PropertiesDimension(entries=(Removed(item=KeyValue(name='delta.columnMapping.mode', value='name')),))`
→ `validate_diff` failed with `[UnmanagedDimensionDrift] Operation not
allowed: properties has drifted…`.

Net effect: **any table that was ever fully synced by this engine cannot be
downgraded to metadata-only mode.** The e2e happy-path test passes only
because its fixture table is created out-of-band without engine properties.

**Candidate fixes** (first two compose with #6's fix, which also resolves this
on its own):
- Fix #6 (declared-projection diff for properties): empty desired properties →
  no `PropertiesDimension` ever → no false drift. Root-cause fix.
- Add `PROPERTIES` to `METADATA_ASPECTS`: one line; safe today because the
  Removed arm produces no actions — but it relies on that subset-semantics
  behaviour staying put, and blurs what "metadata" means.
- Filter reader output to `MANAGED_PROPERTY_KEYS`: changes full-sync
  observability too; wider blast radius.

## 9. Holistic verdict: targeted fixes, not a rearchitecture

The module is one deep module with a stale docstring and one semantics bug —
not two shallow modules interleaved. Evidence against re-splitting: the old
`diff.py` + `lower.py` pair totalled 552 lines and the merged module is 627
for strictly more behaviour (net −75 lines vs. the pre-#121 split plus its
coordination overhead); `TableDrift.plan()`'s uniform loop is real leverage;
and `lower.py` was deleted in PR #121 precisely because it was a forwarding
layer. A "pure facts + planner" split would only be justified if the planner
became the single policy document — and #6's fix removes the most compelling
policy divergence that would have lived there.

**Recommended work order:**
1. Fix #8 via #6 (declared-projection properties diff) — required for PR #127.
2. Fix the three docstring lies (#1, #3, and the "self-contained" claim in #5).
3. Move `managed_aspects` onto `TableDrift`; make `validate_diff`
   single-argument; drop `desired` from `Rule.evaluate` (#5).
4. Leave alone: the Dimension protocol, isinstance-matching in rules,
   `Changed[T]` for scalar changes, all names.

> **Superseded in part** — see section 10. After this review, the maintainer
> sharpened the complaint from "the module reads badly" to "the model has too
> many concepts". Two further analyses (a four-way design competition and a
> minimal-ontology study) were run against that diagnosis. Items 1–3 above
> stand in every candidate design; item 4's "leave the Dimension protocol
> alone" is now an open decision — the flat-facts model (10.3) deletes it and
> survived adversarial attack.

---

## 10. Ontology study — is the model itself too big?

The maintainer's refined diagnosis: comprehension cost comes from the number
of concepts a reader must learn and relate — aspects, dimensions, entries (in
three encodings), actions, rules, `managed_aspects` — before any line makes
sense. A concept census plus two collapse designs (each adversarially
attacked) tested this.

### 10.1 Concept census

**56 learnable names.** The reader must internalise, among others: the 9-member
`TableAspect` enum, the `Dimension` protocol, 9 dimension classes wired 1:1 to
the enum via ClassVars, 6 bespoke `Column*` entry types, the
`Added/Removed/Changed/Entry` generic family plus `KeyValue`, two union
aliases, the `TableMissing`/`TableDrift` sum, the `Rule` protocol plus 5
rules, and three encodings of "which aspects are governed"
(`metadata_only` → `METADATA_ASPECTS` → `managed_aspects`).

Distinctions adjudicated **essential** (collapsing breaks a hard requirement):
- `TableAspect` (hashable identity token for gating) vs. drift-fact carrier.
- Atomic desired/observed pairing for attribute changes (the `Changed`
  *semantics*, though not necessarily the generic wrapper — see 10.3).
- `TableMissing` vs. `TableDrift` (different plans, different validation).
- `ActionPhase` as the centralised ordering authority.
- Scope-blind diff / scope-aware validation split.

Distinctions adjudicated **accidental**: the `ColumnDrift`/`ForeignKeyDrift`
aliases; `desired` on `Rule.evaluate`; and — critically — the census's claim
that the dimension/entry two-level structure is essential was **refuted** by
the flat-facts attack: the one-failure-per-unmanaged-aspect granularity can be
achieved by deduplicating by aspect at the read site instead of grouping into
containers at the write site.

### 10.2 Design competition (presentation-scoped — ran before the refined diagnosis)

Four designs, each judged by newcomer/Ousterhout/migration lenses:
`single-file-unified` won (115/150) over `per-aspect-vertical` (102),
`facts-with-plan-boundary` (101), `pure-facts-planner` (96). Key rejection
reasoning, still valid regardless of the ontology decision:

- **Pure facts + separate planner**: deleting `actions()` from dimensions
  kills the uniform `TableDrift.plan()` loop and recreates the cross-module
  coordination PR #121 deleted `lower.py` to remove; extension regresses from
  "one class + one line" to "+ a match arm in another file".
- **Per-aspect vertical (11-file package)**: trades scrolling for cross-file
  navigation; the design itself induced a circular import.
- The winner's migration Step 1 is exactly the #8 bug fix (independent,
  ~30 min); Steps 2–3 are the #5 fix and docstring corrections.

The competition never evaluated deleting the dimension layer — that is 10.3.

### 10.3 Flat-facts model — survived adversarial attack

**Shape.** Delete the dimension layer. `TableDrift` holds a flat
`tuple[DriftFact, ...]` plus `managed_aspects`. One closed union of ~15
frozen fact types, each with `aspect: ClassVar[TableAspect]` and `actions()`:
`ColumnAdded`, `ColumnRemoved`, `ColumnDataTypeChanged(desired_type,
observed_type)`, `ColumnNullabilityChanged(desired_nullable,
observed_nullable)`, `ColumnCommentChanged`, `ColumnTagSet`/`ColumnTagUnset`,
`TableCommentChanged`, `PropertyChanged`, `TableTagSet`/`TableTagUnset`,
`PartitioningChanged`, `PrimaryKeyAdded`/`PrimaryKeyRemoved`/
`PrimaryKeyChanged`, `ForeignKeyAdded`/`ForeignKeyRemoved`.

**Deleted outright** (no concept smuggling found by the attack): the
`Dimension` protocol, all 9 dimension classes, the entire
`Added/Removed/Changed/Entry` generic family, `KeyValue`, both union aliases,
and the two-level "find the dimension, then scan its entries" pattern in
rules — rules become single flat isinstance scans. ~56 → ~40 learnable names
and, more importantly, one structural level instead of two.

**How it answers the add/remove-vs-changed question** (raised separately):
set/unset facts where identity is the key (tags — mirroring the upsert
actions); named `desired_*`/`observed_*` field pairs on one atomic fact where
identity persists across an attribute change (type, nullability, comment,
partitioning, PK) — the `Changed` semantics survive, the generic wrapper does
not; pure Added/Removed where identity is the content (FKs, already so).
`SetColumnNullability`-style facts are exactly why full add/remove-only is
impossible: the rules need the change direction atomically.

**Attack verdict: survives — no hard requirement broken.** Natural zero,
metadata-only gating, rich rule messages, `ActionPhase` determinism (plan
order cannot leak from fact order because `ActionPlan` sorts on
construction), `TableMissing` arm, and the #8 properties fix all check out.

**Required repairs before adoption:**
1. `UnmanagedAspectDrift` must dedupe with `dict.fromkeys(...)` (not a set)
   to keep failure order deterministic.
2. The `engine.py` call site update (`validate_diff(run.diff)`) must be in
   the normative diff, not a comment.
3. `PrimaryKeyRemoved` should carry the observed constraint for future
   messages.
4. `managed_aspects` on `TableDrift` must be required (no default), or tests
   constructing `TableDrift()` directly silently get all-aspects-managed.

**Cost:** effectively every test in `test_diff.py` and `test_validation.py`
is rewritten (field renames, type renames, signature changes). Genuine churn,
mechanical in nature.

### 10.4 Rejected: aspect-as-organism collapse

Making each aspect one self-contained unit (enum + class + ClassVar merged)
**failed its attack**: its `UnmanagedDimensionDrift` split the `Rule` protocol
into two calling conventions dispatched by isinstance, with
`evaluate()` raising `NotImplementedError` at runtime — invisible to mypy;
plus the `managed_aspects` silent-default hazard. The attack also found
concept smuggling: the protocol "deletion" reappeared as an implicit two-path
dispatch contract. Repairable, but its collapse is largely cosmetic — the
9-class structure survives under new names.

### 10.5 The decision

Two coherent end-states, sharing identical first steps (the #8 bug fix and
the #5 `managed_aspects`-onto-`TableDrift` fix):

- **(A) Unified current shape** (10.2 winner): keep dimensions; fix bugs,
  vocabulary, and docstrings in place. Lowest churn; ontology stays ~56
  names and two structural levels.
- **(B) Flat facts** (10.3): also delete the dimension layer. ~40 names, one
  level, flat rule scans; large-but-mechanical test churn; four named
  repairs required.

(B) is the only design that addresses the refined complaint (model size) and
it survived attack; (A) is the only option if churn on a freshly rewritten
module outweighs the ontology gain.

### 10.6 Decision and outcome — RESOLVED

**The maintainer chose (B), folded into PR #127.** Implemented on
`feat/metadata-only-deployment`:

- `diff.py` rewritten to the flat model: 17 fact types in one closed
  `DriftFact` union, each with `aspect: ClassVar[TableAspect]` and
  `actions()`. Deleted: the `Dimension` protocol, all 9 dimension classes,
  `Added`/`Removed`/`Changed`/`Entry` generics, `KeyValue`, both union
  aliases. `*Changed` facts carry `desired_*`/`observed_*` field pairs with
  `__post_init__` no-difference guards.
- `TableDrift(facts, managed_aspects)` — `managed_aspects` is required (no
  default, per attack repair #4). `validate_diff(diff)` is single-argument;
  `Rule.evaluate(facts, managed_aspects)`.
- `UnmanagedAspectDrift` (renamed from `UnmanagedDimensionDrift`) dedupes
  per-aspect with `dict.fromkeys` for deterministic failure order (repair #1).
- `PrimaryKeyRemoved` carries `observed_primary_key` (repair #3); the
  `engine.py` call site was updated in the same commit (repair #2).
- The #8 bug is fixed by `_diff_properties` iterating declared keys only;
  regression test:
  `test_metadata_only_sync_succeeds_when_catalog_has_engine_written_properties`.
- Docs updated: architecture explanation, safe-change rules, how-to-add-action,
  how-to-deploy-metadata-only.

Concerns #1–#9 above are all resolved or mooted by the rewrite. This document
is retained as the design record for why the flat model was chosen.
