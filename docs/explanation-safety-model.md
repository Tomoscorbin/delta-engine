---
tags:
  - explanation
---

# The safety model

delta-engine mutates shared production tables, so its default posture is
refusal: it only applies changes it can make safely in place, and everything
else fails with a named rule before any SQL runs. This page explains the four
layers that enforce that, and the scoping concept — managed aspects — that
decides what a declaration is responsible for at all.

## Four layers of protection

| Layer                       | When it runs                                | What it catches                                                                                                                                        |
| --------------------------- | ------------------------------------------- | ------------------------------------------------------------------------------------------------------------------------------------------------------ |
| **Declaration-time checks** | Constructing a `DeltaTable`                 | Declarations that could never succeed — a nullable primary key, a typo'd property key                                                                  |
| **Validation rules**        | Every sync, after diffing live state        | Transitions that are unsafe _from this table's current state_ — tightening nullability, narrowing a type                                               |
| **Managed aspects**         | Every sync, as part of validation           | Drift outside what the declaration manages — never silently reconciled                                                                                 |
| **Dependency blocking**     | Every sync, during resolution and execution | Foreign keys that can't be satisfied — a cycle, a missing or mismatched reference — and tables whose dependency won't reach its desired state this run |

### Declaration-time checks

Some declarations are invalid regardless of catalog state, so they fail
immediately with a `ValueError` when the `DeltaTable` is constructed — long
before a sync, typically at import time. This is deliberate: the earlier an
impossible declaration fails, the closer the error is to the code that caused
it. The full list is in
[safe-change rules](reference-safe-change-rules.md#declaration-time-checks).

### Validation rules

Other changes are only unsafe in context: adding a `NOT NULL` column is fine
on a new table but unsafe on an existing one with rows. These rules run on
every sync, after the engine has read the live table and diffed it against the
declaration. Each rule blocks a specific transition and its failure message
names the rule, the affected column or table, and the way out — usually either
a staged migration (backfill, then tighten outside the engine) or recreating
the table out of band. The full rule table is in
[safe-change rules](reference-safe-change-rules.md).

A validation failure means that table is untouched: no SQL ran, and re-syncing
after fixing the declaration is always safe.

## Managed aspects: what a declaration is responsible for

Every declaration manages a defined set of _aspects_ of its table — column
structure, comments, properties, tags, partitioning, and key constraints. For
catalog state it can represent, the engine reconciles drift in managed aspects
and refuses to proceed when an unmanaged aspect has drifted. It never silently
reconciles something a declaration did not claim responsibility for.

There is one current fail-open limitation at the catalog boundary: an observed
non-partition column whose type the reader cannot parse is skipped with a logged
warning, so drift in that column is invisible to validation. A table with no
mappable columns, or an unsupported partition-column type, fails its read
instead. See [unsupported types](reference-data-types.md#unsupported-types).

There are three public scopes, selected by `DeltaTable`'s `scope` parameter:

| Scope                  | Manages                                                                                 | Use for                                                                                                           |
| ---------------------- | --------------------------------------------------------------------------------------- | ----------------------------------------------------------------------------------------------------------------- |
| `"full"` (the default) | Everything: columns, comments, properties, tags, partitioning, primary and foreign keys | Tables this declaration owns end to end                                                                           |
| `"metadata"`           | Comments, tags, and key constraints only                                                | Rolling out governance metadata with a hard guarantee that no schema change can slip in                           |
| `"tags"`               | Table and column tags only                                                              | Tag governance for tables owned elsewhere — including streaming tables, where tags are the only manageable aspect |

See [how to deploy metadata only](how-to-deploy-metadata-only.md) for the
metadata scope in practice, and [tags](how-to-configure-table.md#manage-tags-only)
for tag-only declarations.

Streaming tables make the scope boundary literal. Their definition is owned by
a pipeline, so the engine reads one for tag governance only: the relation kind
is discovered at read time, and a declaration that manages anything beyond
tags fails validation (`StreamingTableTagsOnly`) before any SQL runs — even
with zero drift. See
[safe-change rules](reference-safe-change-rules.md) for the invariant and
[tag a streaming table](how-to-deploy-metadata-only.md#tag-a-streaming-table)
for the workflow.

## Cross-table dependency blocking

The first three layers protect a table from its own declaration. The fourth
protects it from the rest of the run: a table only executes when every table
its foreign keys reference can be trusted to reach its desired state this
sync.

A table blocked by this layer reports `FOREIGN_KEY_FAILED`, with one of four
reasons in its failure message:

| Failure reason                 | Fires when                                                                          |
| ------------------------------ | ----------------------------------------------------------------------------------- |
| `UNRESOLVABLE_REFERENCE`       | A foreign key references a table that is not part of the sync                       |
| `CYCLE`                        | The table is part of a foreign-key dependency cycle                                 |
| `REFERENCED_COLUMNS_NOT_A_KEY` | The referenced columns are not the referenced table's primary key                   |
| `BLOCKED_BY_FAILED_DEPENDENCY` | A referenced table failed this run — at any of the layers above, or while executing |

The first three are structural problems with the declarations themselves —
validation in spirit, run separately only because they need to see every table
in the sync at once. The last is failure propagation: the declaration is fine,
but the state its foreign keys depend on won't exist, so the table is blocked
rather than executed.

The rule is uniform: if a dependency won't reach its desired state this sync,
its dependents don't run either. Fix the upstream table and re-sync.
[How a sync works](explanation-sync-lifecycle.md) covers where resolution sits
in the phase chain;
[foreign keys](how-to-configure-table.md#foreign-keys) covers the
declaration side.

## Declared, observed, or both: aspect semantics

Within the managed aspects, what counts as drift depends on the aspect. The
differences are intentional:

| Aspect         | Semantics                                                                                                                                                                                                                       |
| -------------- | ------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------- |
| **Properties** | Exact-declaration: only declared keys are compared. Declaring a key with value `None` asserts its absence. A managed key set on the table but missing from the declaration fails validation rather than being ignored or unset. |
| **Tags**       | Full-state: the declaration is the complete tag state. A tag on the table that is not declared is drift and will be removed.                                                                                                    |
| **Comments**   | Always managed: the declared comment (including "no comment") is the desired state, so removing a comment from the declaration clears it on the table.                                                                          |

The property semantics are the most conservative because properties change
engine behaviour — retention, change data feed, column mapping — where an
unintended unset can be destructive. Tags and comments are pure metadata, so
converging on the declaration is always safe. Details and examples:
[properties](how-to-configure-table.md#properties),
[tags](how-to-configure-table.md#tags),
[comments](how-to-configure-table.md#comments).

## When the engine says no

A refused change is not a dead end; the failure message and
[safe-change rules](reference-safe-change-rules.md) name the resolution for
each rule. The patterns are:

- **Stage the migration.** Add the column nullable and backfill. The tighten
  itself is an out-of-band step (`ALTER TABLE ... SET NOT NULL` — the engine
  refuses to run it); once applied, declare `nullable=False` and the next sync
  sees no drift.
- **Recreate out of band.** Type and partitioning changes require a rebuild;
  do it deliberately, then re-sync.
- **Fix the declaration.** If the catalog is right and the declaration is
  stale, update the declaration — the engine treats the declaration as desired
  state, not automatically as correct.

To see what a sync would do before letting it touch anything, use a
[dry run](how-to-preview-changes.md).
