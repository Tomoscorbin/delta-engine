# Annotations scope on streaming tables — design

Date: 2026-07-30
Status: implemented (2026-07-31)
Supersedes in part: `2026-07-16-streaming-table-tags-design.md`

## Problem

The tags scope on streaming tables (2026-07-16) reasoned that a streaming
table's definition belongs to its pipeline, and concluded that Unity Catalog
tags are "the one aspect durably manageable from outside it". The principle
was right; the line was drawn one aspect too tight.

Comments are documented as alterable on a streaming table from outside its
pipeline:

- `ALTER STREAMING TABLE … ALTER COLUMN c COMMENT '…'` — an explicit member
  of the `column_clause` grammar, with a worked example on the reference
  page.
- `COMMENT ON TABLE` — the reference page says plainly: "To add or alter a
  comment on a streaming table, use `COMMENT ON`."

The line the platform actually draws is not tags-versus-everything. It is
**the defining SQL**: schema, properties, and constraints belong to
`CREATE OR REFRESH`, while tags, comments, masks, row filters, schedules, and
ownership do not. `ALTER STREAMING TABLE`'s clause list is exactly the second
set.

This also settles an anomaly the 2026-07-16 design recorded but could not
explain. That design planned to pin plain `ALTER TABLE … SET TAGS` as
*rejected* on a streaming table and found it *tolerated* instead, and it
treated the docs as unreliable thereafter. They are not. Both the limitation
and its error condition scope themselves to "the schema or properties" — and
tags are neither. Tags being tolerated through the wrong statement is
consistent with the defining-SQL line, not evidence against it.

### Backend facts

Verified against Databricks documentation 2026-07-30; to be pinned live
before the gate ships.

- `ALTER STREAMING TABLE` supports, in full: `{ADD|ALTER} schedule`,
  `DROP SCHEDULE`, `ALTER COLUMN column_clause`, `SET`/`DROP ROW FILTER`,
  `SET`/`UNSET TAGS`, `SET OWNER TO`. Its `column_clause` is
  `{COMMENT | SET MASK | DROP MASK | SET TAGS | UNSET TAGS}`.
- It has no `ADD CONSTRAINT`, no `DROP CONSTRAINT`, and no
  `SET TBLPROPERTIES`.
- `ALTER MATERIALIZED VIEW` has the identical clause list. Two pipeline-owned
  relation kinds, the same line in the same place: this is policy, not an
  omission.
- `CREATE STREAMING TABLE` → Limitations: "`ALTER TABLE` commands are
  disallowed on streaming tables. The definition and properties of the table
  should be altered through the `CREATE OR REFRESH` or `ALTER STREAMING
  TABLE` statement."
- The platform has a dedicated error condition for it —
  `STREAMING_TABLE_OPERATION_NOT_ALLOWED.INVALID_ALTER`: "To alter the schema
  or properties of streaming tables, please use the `CREATE OR REFRESH`
  command."
- Keys on a streaming table are set through its definition, not through
  ALTER. Databricks documents the procedure for an existing one: "To set
  primary keys for an existing streaming table or materialized view, update
  the schema of the streaming table or materialized view in the notebook that
  manages the object. Then, refresh the table to update the Unity Catalog
  object."
- `CREATE OR REFRESH` is fully declarative: "If a refresh command does not
  specify all metadata from the original table creation statement, the
  unspecified metadata is deleted."
- Out-of-band changes that contradict the defining SQL can be undone: "The
  SQL that defines a table or view in a pipeline is re-run on each update.
  This can undo changes you make with an `ALTER` statement."

### A defect this corrects

The 2026-07-16 design asserted that "the information_schema follow-ups (tags,
constraints) run unchanged; streaming tables simply return no constraints."
That was an assumption and it is wrong — `CREATE STREAMING TABLE` accepts
both column and table constraints, and information_schema reports them.

The consequence is live today, before any part of this design lands. A
streaming table carrying a pipeline-declared primary key fails a
`scope="tags"` sync: `_diff_primary_key` treats absence as its own identity,
so a declaration whose `primary_key` is `None` emits `DropPrimaryKey` against
the observed key, `PRIMARY_KEY` is not in `TAG_ASPECTS`, and
`UnmanagedAspectDrift` rejects the diff. The escape hatch exists and needs no
code — declaring `primary_key=["id"]` makes the signatures match, so no
difference is emitted at all — but nothing documents it, and the failure
message ("sync the table fully, or update the declaration to match the live
schema") recommends a full sync that this very engine refuses on a streaming
table. That is the trap this design closes.

## Decisions

1. **Goal**: streaming tables become comments-and-tags territory, not
   tags-only. Comments are a real, documented capability there.
2. **Vocabulary**: a new public scope `"annotations"` manages exactly the
   table comment, column comments, table tags, and column tags. Like every
   scope it is relation-kind-independent — it means the same four aspects
   against an ordinary table, where it is useful in its own right for an
   adopted table whose structure the engine should not touch.
3. **Authority stays honest**: `scope="metadata"` and `scope="full"` are
   still refused against a streaming table, still at zero drift. Metadata
   claims key authority the engine can never exercise there, and the
   2026-07-16 principle stands: a declaration claiming authority the engine
   must never exercise is wrong now, not when drift eventually materialises.
   This is why the capability gets a new scope name rather than a
   kind-dependent reading of an existing one.
4. **Keys are excluded from engine management on a streaming table
   entirely** — not silently ignored. A declaration mirrors the pipeline's
   key to keep the aspect quiet, exactly as it already mirrors the pipeline's
   columns. This is the standing contract ("a restricted scope still declares
   the full table shape"), applied to keys and finally written down.
5. **The check stays streaming-table-specific**, renamed
   `StreamingTableAnnotationsOnly`. No per-kind capability mapping: the
   2026-07-16 design deferred that "until a second real use case exists", and
   widening one kind's permitted set is not a second kind. Materialized views
   would be, and they remain out of scope.
6. **No adapter or domain changes.** The compiler already emits both comment
   statements correctly for either relation kind; the reader already admits
   streaming tables and already reads comments and constraints. The
   capability is vocabulary plus one eligibility check.
7. **Verification**: the documented facts above are pinned in the opt-in
   `tests/live` suite (`databricks_e2e`), alongside unit coverage.

## Design

### Scopes (`application/scopes.py`)

The four scopes form a total order — `tags ⊂ annotations ⊂ metadata ⊂ full` —
so the sets are defined by composition rather than as parallel literals that
can drift apart:

```python
type ScopeName = Literal["full", "metadata", "tags", "annotations"]

TAG_ASPECTS        = frozenset({TABLE_TAGS, COLUMN_TAGS})
COMMENT_ASPECTS    = frozenset({TABLE_COMMENT, COLUMN_COMMENTS})
ANNOTATION_ASPECTS = TAG_ASPECTS | COMMENT_ASPECTS
KEY_ASPECTS        = frozenset({PRIMARY_KEY, FOREIGN_KEYS})
METADATA_ASPECTS   = ANNOTATION_ASPECTS | KEY_ASPECTS
```

`METADATA_ASPECTS` keeps its exact current membership; only its spelling
changes, from a six-member literal to the union that makes the containment
structural. `TAG_ASPECTS` is unchanged and still backs `scope="tags"`, which
stays — it is shipped public API, and "tags only" remains a narrower thing a
caller may genuinely want.

### Validation (`application/validation.py`)

`StreamingTableTagsOnly` becomes `StreamingTableAnnotationsOnly`. The logic is
untouched apart from the permitted set:

```python
if drift.observed.kind is not TableKind.STREAMING_TABLE:
    return ()
if drift.desired.managed_aspects <= ANNOTATION_ASPECTS:
    return ()
```

It stays in `ELIGIBILITY_CHECKS`, keeps its position before
`UnmanagedAspectDrift` (a root defect leads what it causes), keeps firing at
zero drift, and stays unsuppressable via `rules`. The constant is shared with
the `"annotations"` scope for the reason the current docstring gives about
`TAG_ASPECTS`: so the two policies cannot diverge.

The failure message changes to name the new scope and to say what is
manageable rather than only what is not:

> Operation not allowed: this relation is a streaming table, whose
> definition — schema, properties, and keys — is owned by its pipeline. Only
> comments and Unity Catalog tags can be managed on it: declare the table
> with `scope="annotations"` (or `scope="tags"`), or change its definition in
> the owning pipeline.

### Adapters and domain — unchanged, deliberately

Both comment actions already compile to the documented statements for either
relation kind, so the capability needs no compiler work:

- `SetColumnComment` renders `{target.alter_clause} ALTER COLUMN c COMMENT …`,
  which is `ALTER STREAMING TABLE … ALTER COLUMN c COMMENT …` when
  `plan.kind` is `STREAMING_TABLE` — the documented clause, verbatim.
- `SetTableComment` renders `COMMENT ON TABLE … IS …`, which is
  kind-independent and is what the reference prescribes for streaming tables.

The reader is likewise untouched: `_TABLE_KINDS_BY_RELATION_TYPE` already
admits `STREAMING_TABLE`, `DESCRIBE … AS JSON` already supplies both comment
levels, and the information_schema follow-ups already run. `TableKind`,
`ObservedTable.kind`, `TableDrift.observed`, and `ActionPlan.kind` all carry
what is needed already.

One existing compiler behaviour now reaches streaming tables and must be
pinned there: an empty desired column comment compiles to `COMMENT ''` rather
than `UNSET COMMENT`, because SQL warehouses reject the latter and `''`
round-trips as the empty comment the reader observes. Whether that holds
under the `ALTER STREAMING TABLE` prefix is a live question, not an inherited
guarantee.

### Keys on a streaming table

The engine never manages keys on a streaming table under any scope. What a
declaration must do about a key the pipeline declared is mirror it:

```python
DeltaTable(
    "dev", "silver", "clicks",
    columns=[Column("id", Integer(), tags={"pii": "low"})],
    primary_key=["id"],        # mirrors the pipeline's key; never applied
    tags={"owner": "governance"},
    comment="Click events, owned by the ingest pipeline.",
    scope="annotations",
)
```

No `SetPrimaryKey` or `DropPrimaryKey` is emitted, because the signatures
match and `_diff_primary_key` returns nothing. This is not a new mechanism —
it is what the engine already requires for columns under a restricted scope,
and it is why `scope="annotations"` can leave `PRIMARY_KEY` out of its
managed set without going blind to it. If the pipeline later changes the key,
the mirror stops matching, `UnmanagedAspectDrift` fires, and the declaration
must be updated to the new reality. Late, but loud, and the only honest
answer available: the engine cannot reconcile that key and must not pretend
otherwise.

### Error handling

No new failure channels. Read-phase behaviour is unchanged; the renamed check
produces an ordinary `ValidationFailure` in the sync report; backend
rejections still surface as statement-level failures in `ExecutionSummary`.

## Testing

### Unit

- Scopes: `managed_aspects_for("annotations")` returns the four aspects; the
  lattice containments (`tags ⊂ annotations ⊂ metadata ⊂ full`) are asserted
  directly, so a future edit to one set cannot silently break the order.
- Validation, as a scope × kind matrix: against a streaming table,
  `"tags"` and `"annotations"` pass and `"metadata"` and `"full"` fail —
  each at zero drift as well as with drift; against an ordinary table all
  four pass. Comment drift under `"annotations"` on a streaming table is
  admitted and plans.
- Composition, unchanged: an absent streaming table under `"annotations"`
  still fails `MissingTableUnmanaged`; column-structure drift under
  `"annotations"` still fails `UnmanagedAspectDrift`; a misspelled column
  still fails `ColumnSpellingMustMatchCatalog` first.
- The mirroring contract: a streaming table with an observed primary key and
  a declaration that mirrors it produces no key action and passes; one that
  does not mirror it fails `UnmanagedAspectDrift`. This pins the corrected
  fact and the trap it closes.
- Compiler: `SetTableComment` and `SetColumnComment` under
  `TableKind.STREAMING_TABLE` produce `COMMENT ON TABLE …` and
  `ALTER STREAMING TABLE … ALTER COLUMN … COMMENT …`; a dry-run sync against
  an observed streaming table under `"annotations"` produces that statement
  text end to end.

### Live pins (`tests/live`, `databricks_e2e`)

Provisioning is quota-bound to one active DBSQL pipeline, so these join the
existing `streaming_table` xdist group and share a provisioned table wherever
the facts allow.

1. `ALTER STREAMING TABLE … ALTER COLUMN c COMMENT '…'` succeeds, and the
   comment is visible to the engine's reader.
2. `COMMENT ON TABLE <streaming table> IS '…'` succeeds and round-trips.
3. An empty column comment compiles and applies under the streaming-table
   prefix (the `COMMENT ''` behaviour above).
4. Round-trip: an `"annotations"`-scope sync reconciles a table comment, a
   column comment, a tag set, and a tag unset; the resync reports no changes.
5. `scope="metadata"` against a live streaming table is refused with
   `StreamingTableAnnotationsOnly` and plans no SQL — extending the existing
   wider-scope refusal pin rather than replacing it.
6. A streaming table created *with* a primary key: information_schema reports
   it (disproving the 2026-07-16 assumption), an `"annotations"` declaration
   mirroring the key syncs clean, and one omitting it fails
   `UnmanagedAspectDrift`.

Not pinned, deliberately: whether a pipeline refresh reverts an engine-set
comment. Provoking it needs a streaming table whose defining SQL declares a
contradicting comment plus a full refresh cycle, against a one-pipeline
quota, to observe a platform behaviour the docs already state and the engine
cannot mitigate. It is documented as a limitation instead.

## Documentation

- `how-to-deploy-metadata-only.md`: retitle the "Tag a streaming table"
  section to cover annotations, and correct its claim that comments are
  pipeline-owned and unmanageable. Add the key-mirroring requirement — this
  is the first place a reader hits it.
- `reference-safe-change-rules.md`: the law's row and the paragraph at
  lines 87–91, which currently states that "comments and properties stay
  unmanageable on streaming tables deliberately". Properties still are;
  comments no longer are.
- `reference-limitations.md`: the "Streaming tables" row moves from "Tags
  only" to comments and tags, the backend row's "tag-only management"
  wording follows, and the comment-revert caveat is added.
- `explanation-safety-model.md` lines 72–79: streaming tables as annotation
  territory rather than tag territory.
- `how-to-configure-table.md`: the new scope alongside the existing
  tags-only section.
- `api/delta_table.py`: the `scope` docstring gains `"annotations"` and stops
  saying streaming tables are supported under `"tags"` "and only this
  scope".
- `CHANGELOG.md` / release notes: the new scope is a feature; the
  `rule_name` change is breaking and rides a `BREAKING CHANGE:` footer.

## Blast radius

Named because a recent entry in `todo.md` records getting this wrong in both
directions. Enumerated by grepping `StreamingTableTagsOnly`, the scope
vocabulary (`ScopeName`, `managed_aspects_for`, `TAG_ASPECTS`,
`METADATA_ASPECTS`), and `scope="` across `src`, `tests`, and `docs` — the
first draft of this list missed two files, both recovered below.

- `src` (3): `application/scopes.py`, `application/validation.py`,
  `api/delta_table.py`.
- `tests` (6): `application/test_scopes.py`,
  `application/test_validation.py`, `application/test_planning.py`,
  `api/test_delta_table.py`,
  `adapters/databricks/warehouse/test_streaming_table_dry_run.py`,
  `live/test_sql_warehouse_live_streaming_tables.py`. The dry-run file is
  where the end-to-end "planned SQL carries the streaming-table dialect"
  assertion lives, so the new comment statements are pinned there rather
  than in a new file.
- `docs` (6): the five prose files listed under Documentation
  (`how-to-deploy-metadata-only.md`, `reference-safe-change-rules.md`,
  `reference-limitations.md`, `explanation-safety-model.md`,
  `how-to-configure-table.md`), plus `todo/policy-visibility-review.md`,
  whose lines 78–81 state that `StreamingTableTagsOnly` reuses
  `TAG_ASPECTS` — true today and stale after this change. The sixth entry
  under Documentation, the `scope` docstring, is in `api/delta_table.py` and
  is already counted as code; the seventh, `CHANGELOG.md`, is generated from
  commit footers and edited by nobody.
- Checked and needing nothing: `explanation-architecture.md` lines 517–520,
  which say `CLUSTERING` is not a metadata aspect so `scope="metadata"`
  never reconciles it. Still true; a fourth scope does not bear on it.
- Left alone on purpose: `CHANGELOG.md` history; the archived
  `2026-07-16-streaming-table-tags-*` documents; and `todo.md`'s own entries,
  which record what was decided when and should not be retrofitted.

Total: 15 files, 9 of them code.

The rename is breaking: `ValidationFailure.rule_name` is projected by
`to_dict()` under `schema_version: 2`. v0.7.0 is unreleased and already
carries `BREAKING CHANGE:` footers, so the window is open.

`schema_version` stays at 2 (confirmed 2026-07-31). The key and its type do not
change, only one of the values it can take, following the `DiffOperation`
precedent. The rename rides a `BREAKING CHANGE:` footer instead, which is where
a consumer matching on a rule name will see it.

## Out of scope

- Materialized views. They share the clause list exactly and this design
  extends to them by one enum member, one admit-gate entry, and one gate
  condition — deferred until the need is real.
- A per-kind capability mapping (decision 5).
- Managing properties, constraints, schedules, row filters, column masks, or
  ownership on streaming tables. Masks and row filters are alterable
  out-of-band, so they are candidates for a later widening; they are not
  modelled as aspects at all today, which is the real blocker.
- Creating streaming tables. The engine creates ordinary tables only, and a
  streaming table can therefore only ever reach it as a `TableDrift`.
- Views and foreign tables.

## Rejected alternatives

- **Admit `scope="metadata"` by narrowing effective authority per kind**
  (`managed_aspects ∩ manageable(kind)`). Gives the caller the scope name
  they asked for and reuses the unmanaged-drift machinery for key
  differences. Rejected because the narrowing applies to every scope:
  `scope="full"` against an in-sync streaming table would start passing,
  having claimed authority over structure and properties it can never
  exercise. That is precisely what the 2026-07-16 zero-drift principle
  exists to prevent, and trading it away for a scope name is a bad trade.
- **Admit `scope="metadata"` and treat keys as silently inert.** Same
  benefit, worse: key drift goes unreported, and a declaration's
  `primary_key=` would mean nothing on one relation kind and everything on
  another. `primary_key=None` is a positive assertion of absence everywhere
  else in the engine.
- **Widen `scope="tags"` to include comments.** No new name to argue about,
  but it silently changes what `"tags"` manages on ordinary tables — a
  behaviour change disguised as a capability addition.
- **A per-kind capability mapping now** (`MANAGEABLE_ASPECTS_BY_KIND`). It
  deletes the `if kind is not STREAMING_TABLE` branch and would make
  materialized views one line. Rejected as premature: with one restricted
  kind it is a general mechanism named after its only instance, and naming
  it honestly would produce exactly the kind-agnostic name that then rots
  against a streaming-table-specific rule.
- **Emitting keys through plain `ALTER TABLE` on a streaming table**, on the
  strength of the tolerated `SET TAGS` precedent. Rejected: constraints are
  in the defining SQL and tags are not, which is the line the tolerance
  respects rather than breaks. Even if tolerated, `CREATE OR REFRESH` would
  delete an unspecified key on the next update, so the engine would be
  writing state the pipeline silently reverts.

## Risks

- The documented comment facts are unpinned until the live tests land. The
  gate is written after pinning, as in 2026-07-16.
- A comment declared in the pipeline's defining SQL is reverted on refresh,
  so a contested comment re-drifts on every update and each sync sets it
  again. The engine cannot read the pipeline's SQL and so cannot warn;
  documented as a limitation.
- Live provisioning cost is unchanged in kind but grows: more pins against
  the same one-pipeline quota. Sharing a provisioned table across tests, as
  the existing module already does, is the mitigation.
- The key-mirroring requirement is a sharp edge, however well documented. If
  it proves painful in practice, the honest fix is to model the pipeline's
  ownership explicitly rather than to soften the gate.
