---
tags:
  - explanation
---

# The safety model

delta-engine mutates shared production tables, so its default posture is
rejection: it only applies changes it can make safely in place, and everything
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
and rejects to proceed when an unmanaged aspect has drifted. It never silently
reconciles something a declaration did not claim responsibility for.

The read boundary upholds this by failing closed: a column whose type the
engine cannot model fails that table's read (`READ_FAILED`) rather than being
silently omitted from the observed state, so drift can never hide in a column
the reader could not represent. See
[unsupported types](reference-data-types.md#unsupported-types).

There are four public scopes, selected by `DeltaTable`'s `scope` parameter.
They nest — `tags ⊂ annotations ⊂ metadata ⊂ full` — so narrowing a scope only
ever drops authority:

| Scope                  | Manages                                                                                 | Use for                                                                                                           |
| ---------------------- | --------------------------------------------------------------------------------------- | ----------------------------------------------------------------------------------------------------------------- |
| `"full"` (the default) | Everything: columns, comments, properties, tags, partitioning, primary and foreign keys | Tables this declaration owns end to end                                                                           |
| `"metadata"`           | Comments, tags, and key constraints only                                                | Rolling out governance metadata with a hard guarantee that no schema change can slip in                           |
| `"annotations"`        | Table and column comments, table and column tags                                        | Documenting and tagging a table whose structure and keys belong elsewhere — including streaming tables            |
| `"tags"`               | Table and column tags only                                                              | Tag governance alone, where even a comment would be too much authority to claim                                   |

Inside the engine these names resolve once to a closed `TableScope` value.
That value owns scope policy; the rest of the system asks it whether an aspect
is managed instead of interpreting sets of permissions or constructing new
scope combinations.

See [how to deploy metadata only](how-to-deploy-metadata-only.md) for the
metadata scope in practice, and
[annotations](how-to-configure-table.md#manage-comments-and-tags-only) and
[tags](how-to-configure-table.md#manage-tags-only) for the two restricted
scopes below it.

Streaming tables make the scope boundary literal. Their definition is owned by
a pipeline, and the line the platform draws is the defining SQL: schema,
properties, and keys belong to `CREATE OR REFRESH`, while comments and tags are
alterable from outside it. So the engine reads one for annotation governance:
the relation kind is discovered at read time, and a declaration that manages
anything beyond comments and tags fails validation
(`StreamingTableAnnotationsOnly`) before any SQL runs — even with zero drift.
A declaration claiming authority the engine must never exercise is wrong now,
not when drift eventually materialises, which is why the capability has its own
scope name rather than a kind-dependent reading of `"metadata"`. See
[safe-change rules](reference-safe-change-rules.md) for the invariant and
[annotate a streaming table](how-to-deploy-metadata-only.md#annotate-a-streaming-table)
for the workflow.

## Cross-table dependency blocking

The first three layers protect a table from its own declaration. The fourth
protects it from the rest of the run: a table only executes when every table
its foreign keys reference can be trusted to reach its desired state this
sync.

A table blocked by this layer reports `FOREIGN_KEY_FAILED`, with one of six
reasons in its failure message:

| Failure reason                    | Fires when                                                                                                   |
| --------------------------------- | ------------------------------------------------------------------------------------------------------------ |
| `UNRESOLVABLE_REFERENCE`          | A foreign key references a table that is not part of the sync                                                |
| `CYCLE`                           | The table is part of a foreign-key dependency cycle                                                          |
| `REFERENCED_COLUMNS_NOT_A_KEY`    | The referenced columns are not the referenced table's primary key                                            |
| `REFERENCED_COLUMN_TYPE_MISMATCH` | A foreign-key column's type does not match the referenced column's type on the table registered for the sync |
| `REFERENCED_COLUMN_CASE_MISMATCH` | The referenced columns are that key, but spelled with different case than the registered declaration         |
| `BLOCKED_BY_FAILED_DEPENDENCY`    | A referenced table failed this run — at any of the layers above, or while executing                          |

The first five are structural problems with the declarations themselves —
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

A rejected change is not a dead end; the failure message and
[safe-change rules](reference-safe-change-rules.md) name the resolution for
each rule. The patterns are:

- **Stage the migration.** Add the column nullable and backfill. The tighten
  itself is an out-of-band step (`ALTER TABLE ... SET NOT NULL` — the engine
  rejects to run it); once applied, declare `nullable=False` and the next sync
  sees no drift.
- **Recreate out of band.** Type and partitioning changes require a rebuild;
  do it deliberately, then re-sync.
- **Fix the declaration.** If the catalog is right and the declaration is
  stale, update the declaration — the engine treats the declaration as desired
  state, not automatically as correct.

To see what a sync would do before letting it touch anything, use a
[dry run](how-to-preview-changes.md).
