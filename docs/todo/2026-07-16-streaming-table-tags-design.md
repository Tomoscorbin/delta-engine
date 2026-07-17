# Tags scope on streaming tables — design

Date: 2026-07-16
Status: approved

## Problem

Databricks documents `ALTER STREAMING TABLE` as the statement for altering a
streaming table. The tags scope (`DeltaTable(scope="tags")`) was built
assuming one ALTER dialect for all tables, and its motivating use case —
managing tags on tables owned elsewhere, e.g. by a streaming pipeline — is
exactly the case the assumption breaks on. (Live verification during
implementation, 2026-07-16, found plain `ALTER TABLE ... SET TAGS` tolerated
on a streaming table at table level; the engine emits the documented dialect
regardless rather than relying on undocumented tolerance.)

Nothing is silently broken today: the reader admits only `MANAGED` and
`EXTERNAL` Delta relations, so a streaming table becomes `ReadFailed` before
any DDL is planned. The wrong assumption produced a dead end, not wrong SQL.
This design turns the dead end into support.

Backend facts (verified against Databricks documentation; to be pinned live):

- `ALTER STREAMING TABLE` supports `SET TAGS`, `UNSET TAGS`, and
  `ALTER COLUMN ... SET TAGS` / `UNSET TAGS`.
- It does not support constraint clauses; `SET TBLPROPERTIES` is not in the
  supported clause list.
- The pipeline definition owns schema, comments, and properties; out-of-band
  changes to those can be reverted on pipeline refresh. Tags are Unity
  Catalog governance metadata and persist. Tags are therefore the one aspect
  durably manageable on a pipeline-owned dataset from outside its pipeline.

## Decisions

1. **Goal**: real support — tags-scope syncs work against streaming tables.
2. **Kinds**: streaming tables only. Materialized views, views, foreign
   tables, and unknown kinds keep failing closed at read.
3. **Policy**: only declarations whose managed aspects are tag aspects may
   target a streaming table. `scope="full"` and `scope="metadata"` against
   one fail validation.
4. **API**: no public API change. The engine discovers the relation kind at
   read time; kind is an observed fact, never a declared one.
5. **Plumbing**: the observed kind reaches the SQL compiler on the objects
   that already flow — the diff carries the observed table it was computed
   against, and the plan carries the kind its actions lower against — so no
   port or engine signature changes. (Revised during review, 2026-07-16:
   the original decision widened `PlanExecutor.compile` to
   `compile(qualified_name, plan, kind)`; the parameter read as bolted on,
   and the engine had to re-derive a fact the diff already knew.)
6. **Verification**: backend facts are pinned in the opt-in `tests/live`
   suite (`databricks_e2e`), alongside unit coverage.

## Design

### Domain model

- New enum `TableKind` in `domain/model`: `TABLE`, `STREAMING_TABLE`.
- `ObservedTable` gains `kind: TableKind` with default `TABLE`. The reader
  always sets it explicitly; the default keeps existing construction sites
  and tests valid.
- `DesiredTable` does not get a kind. The asymmetry is deliberate: kind is
  discovered, not declared.

### Reader (`adapters/databricks/read.py`)

- The admit-gate changes from a set of relation types to a mapping onto
  `TableKind`: `MANAGED → TABLE`, `EXTERNAL → TABLE`,
  `STREAMING_TABLE → STREAMING_TABLE`.
- Everything else — including materialized views — still raises
  `UnsupportedRelationError` (surfaced as `ReadFailed`), including kinds
  Databricks adds in the future. The error message no longer lists
  streaming tables among the unmanageable kinds; materialized views stay
  listed.
- Open fact to pin live before writing the gate: the `type` and `provider`
  values `DESCRIBE ... AS JSON` reports for a streaming table. Whether the
  existing `provider == "delta"` check extends to streaming tables is
  written against the pinned values, not guessed.
- The information_schema follow-ups (tags, constraints) run unchanged;
  streaming tables simply return no constraints.

### Diff (`domain/plan/diff.py`)

- `TableDrift` carries the `observed` table it was computed against,
  alongside the `desired` declaration it already carried: the two endpoints
  the differences separate, available as judging context. Validation reads
  observed facts (the relation kind) off `drift.observed`; the diff states
  facts and does no judging. (Revised during review, 2026-07-16: the first
  shape copied a bespoke `kind: TableKind = TABLE` field onto the drift —
  a forwarded scalar whose default could silently disagree with the table
  it came from.)
- `TableMissing` is unchanged: an absent table has no observed side, and the
  engine only creates ordinary tables.

### Validation (`application/validation.py`)

- New scope-gate peer alongside `UnmanagedAspectDrift` and
  `MissingTableUnmanaged` — unconditional and not suppressable via `rules`.
  Working name: `StreamingTableTagsOnly`.
- Semantics: when `TableDrift.kind` is `STREAMING_TABLE` and the
  declaration's managed aspects are not a subset of the tag aspects,
  produce one failure naming the observed kind and pointing the user at
  `scope="tags"`.
- The gate fires even with zero drift. A full-scope declaration pointed at a
  pipeline-owned relation claims authority the engine must never exercise
  there; that is wrong now, not when drift eventually materialises, and dry
  runs surface it immediately.
- Existing behaviour composes for free: an absent streaming table under
  tags scope still fails `MissingTableUnmanaged` (tags scope does not
  manage existence), and structural drift under tags scope still fails
  `UnmanagedAspectDrift`.

### Planning and the execution port

- `ActionPlan` gains `kind: TableKind = TableKind.TABLE` — the relation kind
  its actions lower against, part of what the plan means: the same actions
  compile to different statements per kind, and `compile` can no longer be
  handed a kind that disagrees with the plan it was built from (previously
  the two travelled as separate call-site arguments). `plan_diff` copies
  `drift.observed.kind` onto the plan; the `TableMissing` arm keeps the
  default, as the engine only creates ordinary tables. The default itself
  is a residual hazard accepted deliberately: `plan_diff` is the one
  boundary constructing plans from diffs, and empty scratch plans need a
  valid default.
- `PlanExecutor.compile(qualified_name, plan)` is unchanged: backends whose
  dialect differs by kind read `plan.kind`. The engine, both executors, and
  the CLI `plan` command need no signature changes. (Revised during review,
  2026-07-16 — see decision 5.)

### SQL compilation (`adapters/databricks/sql/compile.py`)

- The hardcoded `ALTER TABLE {name}` prefix becomes a kind-dispatched
  prefix — `ALTER TABLE` / `ALTER STREAMING TABLE` — applied mechanically
  to every ALTER-family statement via one prefix helper, not a per-action
  matrix.
- The compiler stays policy-free. It does not know that `AddColumn` on a
  streaming table is nonsense; validation made that unreachable. Handed
  one anyway, it would emit the statement and the backend would reject it —
  the existing division of responsibility (adapters compile, validation
  judges safety).
- Under the approved policy, only the four tag statements (table set/unset,
  column set/unset) ever carry the `ALTER STREAMING TABLE` prefix; every
  other ALTER statement keeps compiling against ordinary tables as
  `ALTER TABLE`.
- `CREATE TABLE` and `COMMENT ON` compilation is untouched: creates are
  always ordinary tables, and comment actions cannot reach a streaming
  table past the gate.

### Error handling

No new failure channels. Read-phase: unsupported kinds still become
`ReadFailed`. Validation-phase: the new gate produces an ordinary
`ValidationFailure` in the sync report. Execute-phase: backend rejections
translate into statement-level failures in `ExecutionSummary`, as today.

## Testing

### Unit (local, black-box through existing suites)

- Reader: a describe fixture for a streaming table maps to
  `STREAMING_TABLE`; fixtures for a materialized view and a view still
  read as failed — pinning that materialized views remain fail-closed.
- Validation: tags scope + streaming-table drift passes the gate; `full`
  and `metadata` fail it, including with zero drift; an absent streaming
  table under tags scope still fails `MissingTableUnmanaged`; structural
  drift under tags scope still fails `UnmanagedAspectDrift`.
- Compiler: the four tag statements compile under both prefixes; an
  end-to-end dry-run sync against an observed streaming table produces
  `ALTER STREAMING TABLE` statement text.
- Existing tests keep passing untouched except where `compile()` signatures
  appear.

### Live pins (`tests/live`, `databricks_e2e`, run via the Live workflow)

1. `DESCRIBE ... AS JSON` `type`/`provider` values for a real streaming
   table — the reader gate is written against these.
2. `ALTER STREAMING TABLE` `SET TAGS`, `UNSET TAGS`, and
   `ALTER COLUMN ... SET TAGS` succeed.
3. ~~Plain `ALTER TABLE ... SET TAGS` against a streaming table fails~~ —
   disproven live (2026-07-16): the statement is tolerated at table level,
   so no rejection pin exists; the engine emits the documented dialect and
   relies on nothing being rejected.
4. information_schema tag reads return streaming-table tags.
5. Round-trip: a tags-scope sync against a live streaming table reconciles
   a tag set and a tag unset.

Provisioning uses `CREATE STREAMING TABLE` on the SQL warehouse (implicit
pipeline spin-up; the Live run gets slower), with a teardown drop.

## Documentation

- `how-to-deploy-metadata-only.md`: tags scope works on streaming tables;
  example.
- `reference-safe-change-rules.md`: the new gate, its failure message, and
  why comments and properties stay unmanageable (pipeline refresh reverts
  them).
- `reference-limitations.md`: narrow "streaming tables unsupported" to the
  precise truth — tags-only management; materialized views remain
  unsupported.
- `explanation-safety-model.md`: streaming tables as tags-only territory.
- `scope` docstring in `api/delta_table.py`: the "owned elsewhere (e.g. by
  a streaming pipeline)" example is now literally supported; say so.

## Out of scope

- Materialized views. They share the problem shape (pipeline-owned,
  `ALTER MATERIALIZED VIEW` dialect, same table- and column-level tag
  clauses per the Databricks docs), and this design extends to them by
  adding one enum member, one admit-gate entry, one gate condition, and
  one prefix — deferred until the need is real.
- Managing comments, properties, constraints, schedules, row filters, or
  ownership on streaming tables.
- Creating streaming tables.
- Views and foreign tables.
- A per-kind capability matrix; the tags-only rule is hardcoded policy
  until a second real use case exists.
- Promoting the full statement target (name + kind) onto the plan and
  narrowing the port to `compile(plan)` — parked deliberately (2026-07-16
  review). If taken, take it whole; do not bolt further target facts onto
  the plan piecemeal.

## Rejected alternatives

- **Adapter-only dispatch** (reader admits streaming tables, executor
  re-describes or caches kind; no domain/validation change): validation
  cannot see kind, so a full-scope declaration against a streaming table
  reaches execution and fails as a backend error — the safety model's job
  done by the backend. Blocking non-tag actions inside the adapter would
  mean the adapter deciding safety, which the architecture forbids.
- **Special-cased tag path** (tags-scope declarations bypass the relation
  gate onto a side flow): forks the sync lifecycle and buries a scope
  special-case in the read layer; the fork grows the moment anything else
  is needed on streaming tables.
- **Explicit kind declaration** (`kind=` parameter or a `StreamingTable`
  class): new API surface users must maintain and the engine must verify
  against reality anyway; discovery loses nothing given the tags-only gate.

## Risks

- The `provider` value for streaming tables in `DESCRIBE ... AS JSON` is
  unverified until the live pin lands; the reader gate is written after
  pinning.
- Live provisioning cost: streaming-table creation spins up a managed
  pipeline per Live run (accepted).
- If Databricks later accepts `ALTER TABLE` on streaming tables, the
  dispatch becomes unnecessary but harmless. (Materialised immediately:
  the first live run, 2026-07-16, observed table-level `SET TAGS` tolerated
  via plain `ALTER TABLE`. Decision: keep emitting the documented
  `ALTER STREAMING TABLE` dialect.)
