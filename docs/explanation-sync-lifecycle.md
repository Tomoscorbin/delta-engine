---
tags:
  - explanation
---

# How a sync works

You never tell delta-engine what to change — you tell it what a table should
look like, and each `engine.sync(...)` works out the changes itself. This page
explains what happens between calling `sync` and getting a report back. You do
not need any of it to run your first sync, but it is the mental model behind
every other page: what gets compared, where a change can be rejected, and why
one table's failure does not take down the rest of the run.

## The phases

Every sync runs the same chain of phases over every table in the call:

```text
read → diff → plan → compile → resolve → execute → report
```

| Phase       | Question it answers                          | Outcome                                                            |
| ----------- | -------------------------------------------- | ------------------------------------------------------------------ |
| **Read**    | What does the table look like right now?     | Present (with its observed state), absent, or a read failure       |
| **Diff**    | How does observed state differ from desired? | Direct actions and non-action differences — or no drift            |
| **Plan**    | Is the diff accepted, and in what order?     | A validated action plan, or named validation failures with no plan |
| **Compile** | What exact backend statements apply it?     | The SQL exposed on the report and passed unchanged to execution    |
| **Resolve** | Which tables must be synced before which?    | Tables ordered so foreign-key targets go first                     |
| **Execute** | Apply the compiled statements                | Attempted results, a dependency block, or skipped on a dry run     |

The result is a `SyncReport` with one entry per table. On a real run, if any
table failed, the engine raises `SyncFailedError` with that report attached; a
dry run always returns the report without raising.

The rest of this page looks at the behaviours that fall out of this design.

## Diff produces actions; planning decides safety

The diff phase produces rich, backend-neutral actions directly — for example,
`DropColumn` carries the complete observed column — but never judges whether
executing one is a good idea. Ambiguous or unsupported states remain domain
differences rather than being labelled as blockers; the application default
policy decides to reject them. The planning boundary always validates that
complete diff and returns either an accepted `ActionPlan` or failures; a
rejected result has no plan. An accepted plan carries the qualified table
target and relation kind needed for compilation as well as its ordered actions.

This separation is why rejections are precise: a failed sync names the exact
rule that fired and the column or table it fired on, rather than a generic
"cannot apply changes". The full rule set is in
[safe-change rules](reference-safe-change-rules.md), and
[the safety model](explanation-safety-model.md) explains the reasoning behind
it.

## No SQL runs until a table fully passes

Validation is inseparable from plan construction, and planning happens before
execution, so a rejected table is untouched — there is no plan containing the
safe subset. Within execution, statements run in a deterministic order and a
failure stops that table's remaining statements. Re-running `sync` after
fixing the cause is always the recovery path: the engine re-reads live state
and plans only the drift that still remains.

## One table's failure does not abort the run

Each table carries its own result through the phases. A table that fails an
early phase — say, its read failed — keeps that failure in its report entry
and is skipped by the later phases, while every other table proceeds normally.
You always get a complete report of what happened to every table, then a
single `SyncFailedError` at the end if anything failed.

The exception is foreign-key dependents, which are deliberately not
independent — see the next section.

## Foreign keys order the run — and propagate failure

A foreign key can only be created if the table it references already exists
with its primary key in place. The resolve phase therefore orders tables so
referenced tables are executed before the tables that reference them.

The same dependency logic propagates failure: if a table fails — at any phase,
including mid-execution — every table whose foreign keys depend on it is
blocked rather than executed, reporting `FOREIGN_KEY_FAILED`. The rule is
uniform: if a dependency won't reach its desired state this sync, its
dependents don't run either. Fix the upstream table and re-sync.
[Foreign keys](how-to-configure-table.md#foreign-keys) covers the
declaration side.

## Dry runs stop before execution

`sync(..., dry_run=True)` runs read, diff, accepted/rejected planning,
compilation, and resolve, then skips execution. You get every action and SQL
statement planned from the catalog snapshot it read, plus any read, validation,
or foreign-key failure found before execution. It cannot predict execution-time
Databricks errors. The run makes zero catalog mutations and raises no exception.
See [how to preview changes](how-to-preview-changes.md).

## Where to drill down

- What the engine will refuse and why: [the safety model](explanation-safety-model.md)
- Reading and acting on a failed run: [how to handle sync failures](how-to-handle-sync-failures.md)
- The internals — layers, ports, adapters, planning: [architecture](explanation-architecture.md)
