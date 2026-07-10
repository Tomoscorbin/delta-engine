# Feature roadmap

2026-07-10. Consolidated from two codebase review sweeps. Ordered by impact,
highest first, within four tiers. Effort is rough: S (a day or two), M (about
a week), L (multiple weeks). Items already sketched in `todo.md` are marked;
this document supersedes their prioritisation but not their detail.

## Summary

| #   | Item                                              | Tier | Effort | In todo.md |
| --- | ------------------------------------------------- | ---- | ------ | ---------- |
| 1   | Type widening                                     | 1    | M      | yes        |
| 2   | Column renames (`renamed_from`) + interim guard   | 1    | M      | yes        |
| 3   | CI-grade dry runs: structured report, SQL preview | 1    | M      | partly     |
| 4   | Databricks SQL warehouse adapter (no PySpark)     | 1    | L      | no         |
| 5   | Delta-format / view guard in the reader           | 2    | S      | no         |
| 6   | Identity columns                                  | 2    | M      | no         |
| 7   | Adoption tooling: declaration codegen + names     | 2    | M–L    | partly     |
| 8   | CHECK constraints                                 | 2    | M      | no         |
| 9   | `RELY` on PK/FK constraints                       | 2    | S      | yes        |
| 10  | External-table (LOCATION) policy                  | 3    | S–M    | no         |
| 11  | Schema-level orphan detection                     | 3    | M      | no         |
| 12  | `ignored_properties` escape hatch                 | 3    | S      | yes        |
| 13  | Column defaults and generated columns             | 3    | M      | no         |
| 14  | Declaration-time limit checks                     | 3    | S      | no         |
| 15  | Multi-environment deployment pattern (docs)       | 3    | S      | no         |
| 16  | Transient-failure retry                           | 3    | S      | no         |
| 17  | Metadata read batching                            | 4    | M      | partly     |
| 18  | Plan artifacts (approve-then-apply)               | 4    | L      | no         |
| 19  | Existing gated items (UNIQUE, Char/Varchar, ...)  | 4    | —      | yes        |

Sequencing note: impact order is not build order. Items 5 and 9 are
afternoon-sized and can ship immediately; the SQL-preview half of item 3 is
similarly small. Item 4 is the largest single investment and multiplies the
value of item 3, so plan them together.

---

## Tier 1 — highest impact

### 1. Type widening

**Status.** Shipped 2026-07-10. The shape below was built as described; the
widening matrix and runtime notes live in `reference-safe-change-rules.md`.

**Why.** `ColumnDataTypeChangeNotSupported` blocks every type change with
"recreate the table", and type evolution is the most common real schema change
users hit. Delta supports the safe subset natively via
`delta.enableTypeWidening`. Every hard block is a moment a user considers
abandoning the tool; this converts the most painful one into a managed
operation.

**Shape.** Already scoped in `todo.md`:

- Register `delta.enableTypeWidening` in the property registry. Permit
  `false -> true` only — disabling requires `ALTER TABLE ... DROP FEATURE`
  with history truncation, so `true -> false` is blocked by the same
  `permitted_transitions` mechanism as column mapping.
- New `AlterColumnType` action plus `ALTER TABLE ... ALTER COLUMN ... TYPE`
  SQL compilation.
- A validation rule that permits a `ColumnDataTypeChanged` only when the
  transition is in the widening matrix **and** the declaration states the
  property `true`; everything else keeps the current failure.
- Full Delta widening matrix (decided during execution — the engine is
  Databricks-only, so the Iceberg-compatible subset was needless caution):
  integer widenings, integer→double/decimal, float→double, decimal digit
  growth, date→timestamp_ntz. UniForm-Iceberg tables reject the non-Iceberg
  entries at execution; documented, not modeled.
- Document the runtime minimum; do not gate on it (established policy).

### 2. Column renames: `renamed_from` hint, with an interim guard

**Why.** A rename in the declaration diffs as drop + add. Without column
mapping it is blocked; **with** `columnMapping.mode='name'` declared — which
the engine itself requires for drops — it executes and silently destroys the
column's data. This is the sharpest correctness hazard in the tool, currently
only documented (`reference-limitations.md`).

**Shape.** Two steps, shippable independently:

1. _Interim guard (S)._ A validation rule that fails any diff containing both
   a `ColumnRemoved` and a `ColumnAdded` of the same data type, with a message
   offering the two honest paths: declare the rename (once supported), or
   split the drop and the add across two syncs to state that they are
   unrelated. Over-cautious by design; the two-sync escape hatch keeps false
   positives cheap.
2. _The feature (M)._ A `renamed_from` hint on `Column` (Terraform
   moved-block style). The differ emits a rename when the old name is
   observed and the new one absent; the hint is inert once applied, preserving
   idempotency. New `RenameColumn` action compiling to
   `ALTER TABLE ... RENAME COLUMN`, valid only under column mapping (validation
   rule, same pattern as `ColumnMappingRequiredForDrop`). The same mechanism
   extends later to table renames.

### 3. CI-grade dry runs: structured report projection, SQL preview, drift gate

**Status.** Shipped 2026-07-10, with the shape refined during design: the SQL
preview flows through a new `PlanExecutor.compile` port method onto
`TableRunReport.planned_sql_statements` (no `databricks.py` helper — the
engine compiles once and `execute` runs exactly the previewed statements);
`to_dict()` only, under `schema_version: 1` (no `to_rows`, no Spark lift);
`has_changes` and the `any_failures` → `has_failures` rename shipped
together. See `reference-run-report.md` and `how-to-gate-changes-in-ci.md`.
The sketch below is kept as the original motivation.

**Why.** The workflow that makes tools like this indispensable: PR opens →
`sync(dry_run=True)` runs → the plan renders as a PR comment → merge applies.
Dry run exists; what is missing is everything around it. This item turns
delta-engine from "a library I call" into "the deployment pipeline".

**Shape.** Three deliverables, one theme:

- _Structured projection_ (design already in `todo.md`): backend-free
  `SyncReport.to_rows()` / `to_dict()` yielding per-table records — name,
  status, action count, applied/total, failure summaries — plus optional
  per-action rows. Export `TableRunReport` and the concrete failure types
  from the public API. An optional thin Spark lift in `databricks.py` for
  `display()` / run-history persistence stays at the edge.
- _SQL preview for dry runs (S)._ The most common question a cautious user
  asks is "show me the exact DDL you will run", and today the compiled SQL is
  only visible after execution, truncated to 240 chars. `compile_plan` is
  pure and takes `(qualified_name, plan)` — both on every `TableRunReport`.
  Add a helper in `databricks.py` that renders full statements for a report,
  keeping the compiler at the adapter edge.
- _Drift gate (S)._ `SyncReport.has_changes` — trivially derivable, but it is
  the natural CI assertion ("fail if drift exists") and belongs on the report.

### 4. Databricks SQL warehouse adapter (PySpark-free syncs)

**Why.** Everything the engine executes is SQL; everything it reads has an
`information_schema` / `DESCRIBE` equivalent. A second adapter on
`databricks-sql-connector` (or the Statement Execution REST API) means syncs
and dry runs execute from a plain CI runner against a serverless warehouse —
no Spark session, no cluster spin-up, schema sync as a 30-second GitHub
Actions step. It is also the first real test of the ports architecture the
project was designed around. The largest single unlock on this list.

**Shape.**

- Implement `CatalogStateReader` and `PlanExecutor` per
  `how-to-implement-adapter.md`.
- Replace `spark.catalog.listColumns` / `getTable` / `tableExists` with
  `information_schema.columns` / `tables` queries.
- The real work: a DDL type-string parser that does not depend on
  `pyspark.sql.types.DataType.fromDDL`. Scope it to the types the domain
  models; unknown types keep the existing skip-with-warning behaviour.
- Ship as an optional dependency group (e.g. `delta-engine[sql]`), keeping
  the existing lazy-import structure.

---

## Tier 2 — high impact

### 5. Delta-format and view guard in the reader

**Why.** `tableExists` answers true for views and non-Delta tables, and
nothing downstream checks the format. A view limps to a confusing raw
`ReadFailed` at `DESCRIBE DETAIL`. A Parquet/Iceberg table is worse:
`DESCRIBE DETAIL` succeeds, the engine diffs it as Delta, and can plan
`delta.*` properties, `CLUSTER BY`, and constraint DDL against it, failing (or
partially succeeding) at execution with errors pointing nowhere near the
cause.

**Shape (S).** The `DESCRIBE DETAIL` row is already fetched for every present
table and carries `format`. Check it and return a typed "exists but is not a
Delta table" read failure; catch the view case earlier via the catalog table
type for a clean message. Same protective class as
`ColumnMappingRequiredForDrop`, at the read boundary.

### 6. Identity columns

**Why.** `GENERATED ALWAYS AS IDENTITY` is the standard surrogate-key idiom
on Databricks and is **create-time only** — it cannot be added to an existing
column. That is precisely the case where a declarative tool that owns
`CREATE TABLE` must support the feature: users cannot work around its absence
without abandoning the tool for that table.

**Shape (M).** Model like partitioning: declared at creation, drift blocked.

- An `identity` field on `Column` (or a dedicated declaration type) carrying
  always-vs-default, start, and step.
- Compile into the `CREATE TABLE` column definition.
- Read side: observe identity metadata (verify the best source —
  `information_schema.columns` or `DESCRIBE TABLE EXTENDED`) so drift can be
  stated and blocked rather than invisible.
- Validation: identity change or addition to an existing column is blocked
  with a recreate message, mirroring `PartitioningChangeNotSupported`.

### 7. Adoption tooling: declaration codegen and explicit constraint names

**Why.** The biggest barrier to adopting the tool is an existing estate of
hundreds of tables nobody wants to hand-transcribe. The hard half — parsing
observed state into a typed model — already exists in the reader.

**Shape (M–L).**

- A helper that introspects a table (or a whole schema) and emits
  `DeltaTable(...)` declaration source using public import paths. Terraform's
  `import`, but generating code.
- Prerequisite: explicit PK/FK constraint names on `DeltaTable` (parked in
  `todo.md`) — adopted tables carry pre-existing names the generator must be
  able to reproduce, and it is also the escape hatch for generated-name
  collisions. Remember the `how-to-configure-table.md` doc follow-up recorded
  when this was parked.
- FK declarations reference `DeltaTable` objects, so whole-schema generation
  must emit tables in dependency order and wire the references; single-table
  generation can emit a commented placeholder.

### 8. CHECK constraints

**Why.** The only _enforced_ constraint Databricks has — PK/FK are
informational — so its absence is conspicuous in a tool whose pitch is
declared table contracts.

**Shape (M).** Named constraints; add/drop reconciliation; full-state
semantics like tags (an observed-only CHECK is drift). Delta stores them as
`delta.constraints.<name>` table properties, so observation rides the
existing `DESCRIBE DETAIL` path — but they must be modelled as constraints,
not properties. Known design risk: expression normalisation (the catalog may
rewrite the stored expression), which would churn drift; resolve by comparing
normalised forms, and verify actual storage behaviour on a live workspace
before building.

### 9. `RELY` on PK/FK constraints

**Why.** Without `RELY`, the constraints the engine's whole FK machinery
manages are documentation only; with it the optimizer gets join elimination
and MV optimisations. Smallest effort-to-value ratio on this list.

**Shape (S).** Already sketched in `todo.md`: a `rely: bool` on the
constraint declarations, a `NOT ENFORCED RELY` / `NOT ENFORCED` suffix in the
compiler. Verify whether `RELY` is observable (information_schema or
`DESCRIBE`) — if yes, diff it; if not, treat it as create-time-only and
document that an out-of-band change is invisible. Trusting unverified data is
the user's call, consistent with the runtime-version policy.

---

## Tier 3 — worthwhile, not urgent

### 10. External-table (LOCATION) policy

Nothing models `LOCATION` today: an external Delta table is managed as if the
distinction did not exist, and a `CREATE` from a declaration always produces a
managed table even where the user expected external. Minimum (S): document
the managed-only stance in `reference-limitations.md` and observe
`DESCRIBE DETAIL.location` enough to warn on surprises. Full (M): a
`location` parameter — external creates, location drift blocked. Decide
explicitly; today the scope decision is implicit, and implicit ones bite.

### 11. Schema-level orphan detection

Full-state semantics exist for tags but not for the table inventory: a table
removed from declarations silently stops being managed. A report-only check —
"these tables exist in schema X but are not declared" — completes the
declarative story one level up. Never auto-drop. Cheap read
(`information_schema.tables`); the design question is the API shape (a
separate `check` entry point vs. a sync option scoped to declared schemas).

### 12. `ignored_properties` escape hatch

From `todo.md` (deferred from the property-ownership design). The registry
admission policy is right, but coexistence with other tooling that writes
managed keys is a real wall, and the only current recourse is un-declaring
the table.

### 13. Column defaults and generated columns

Follow-on to identity (item 6), same family. `DEFAULT` is reconcilable in
place (`ALTER COLUMN SET DEFAULT`, requires the `allowColumnDefaults` writer
feature — property registry entry). Generation expressions are create-time
only — same blocked-drift pattern as identity. Do after identity proves the
column-metadata plumbing.

### 14. Declaration-time limit checks

The declaration already validates tag counts, clustering limits, decimal
precision, CDF reserved names, and column-mapping characters. Comment length
and identifier length/character rules are the same class. Verify the actual
Unity Catalog limits against current docs before implementing — do not guess
them (documentation rule).

### 15. Multi-environment deployment pattern (docs)

`DeltaTable` binds its catalog at construction and FKs reference table
_objects_, so dev/staging/prod deployment requires wrapping declarations in
factory functions parameterised by catalog. That is fine — but every real
adopter hits it in week one, so it deserves a how-to page. Revisit API
support only if the pattern proves insufficient.

### 16. Transient-failure retry

One network blip during read fails the table **and blocks all its FK
dependents** for the run. A bounded retry for transient error categories at
the adapter boundary (read and execute) smooths large syncs. Policy lives in
the adapter, never in application code.

---

## Tier 4 — deferred or evidence-gated

### 17. Metadata read batching

Each present table costs ~8 metadata round trips (existence, columns, detail,
comment, table tags, column tags, PK, FKs, inbound FKs); a 500-table estate
pays minutes of wall clock. `todo.md` already gates this on latency evidence —
keep that gate, but expect it to be the first complaint from a large adopter.
The shape when evidence lands: per-schema/per-catalog batched
information_schema queries instead of per-table.

### 18. Plan artifacts (approve-then-apply)

The plan a reviewer approves in a PR dry run is recomputed at apply time; if
the catalog moved in between, what runs is not what was approved. Terraform
solves this with saved plan files plus a staleness check. This is the ceiling
of the CI story (item 3), not its foundation — do not build until the CI
workflow has real users asking for it.

### 19. Existing gated items

Tracked in `todo.md`, deliberately unchanged by this roadmap:

- UNIQUE constraints — wait for the feature to leave Public Preview.
- `Char(n)`/`Varchar(n)` fidelity — needs a migration story for the read-side
  normalisation flip; wait for demand.
- Struct-field nullability — gated on a richer observation source than DDL
  string parsing.
- Backend-agnostic application layer (rules and property registry injected
  from the composition root) — architectural refactor, own PR, not blocking
  any item above.
- Live-workspace verification items (CDF reserved columns, nested
  special-character rules, UC `clusteringColumns`, unique-constraint filter)
  — fold into the next manual walkthrough.
