---
tags:
  - how-to
---

# How to deploy metadata only

`DeltaTable(scope="metadata")` restricts a sync to catalog metadata: table
and column comments, table and column tags, and primary/foreign key constraints.
Column structure, table properties, partitioning, and clustering are read for
context but never changed — a metadata-only sync can never add, drop, or alter
a column.

Use it to roll out governance metadata with a hard guarantee that no schema
change can slip in — for example, applying tags and comments across tables
whose schemas are owned by another team. `scope="metadata"` is a
managed-aspects scope: see
[the safety model](explanation-safety-model.md#managed-aspects-what-a-declaration-is-responsible-for)
for how managed aspects decide what a declaration is responsible for.

## Declare a metadata-only table

```python
from delta_engine.schema import Column, DeltaTable, Integer, String

table = DeltaTable(
    catalog="dev",
    schema="silver",
    name="orders",
    columns=[
        Column("id", Integer(), nullable=False, comment="surrogate key"),
        Column("customer_email", String(), comment="PII",
               tags={"pii": "true"}),
    ],
    primary_key=["id"],
    comment="Customer orders",
    tags={"domain": "sales"},
    scope="metadata",
)
```

The full schema is required. It states the expected shape of the live table —
if the live schema drifts from the declaration, the sync fails before any
metadata is applied.

## What a metadata-only sync does

- **Reconciles** table comment, column comments, table tags, column tags, and
  PK/FK constraints, exactly as a fully managed sync would.
- **Requires** the live schema to match the declaration exactly. Any unmanaged
  aspect (column structure, partitioning, or clustering) that has drifted
  fails the sync at validation (`UnmanagedAspectDrift`) before any SQL
  executes. That includes column *case*: because you must name the live
  columns to declare keys or comments over them, a column declared
  `requestid` against a catalog holding `requestId` fails with
  `ColumnSpellingMustMatchCatalog` naming both spellings. `DESCRIBE TABLE`
  shows the spelling to copy. Catalog properties are the exception: a declaration that does not
  manage properties makes no property assertion at all, so they are never
  compared — declared properties are carried but ignored, and properties on
  the live table (for example those written by a previous fully managed sync)
  are not drift.
- **Cannot create** a missing table. If the table does not exist, the sync
  fails at validation (`MissingTableUnmanaged`) — a metadata-only declaration
  does not manage table existence, so it has no authority to create the table.

Both failures are laws rather than rules, listed in
[safe-change rules](reference-safe-change-rules.md).

## Annotate a streaming table

`scope="annotations"` extends to streaming tables, as does the narrower
`scope="tags"`. A streaming table's definition — schema, properties, and
keys — is owned by its pipeline and belongs to `CREATE OR REFRESH`; comments
and Unity Catalog tags sit outside it, and are exactly what
`ALTER STREAMING TABLE` and `COMMENT ON` reach. The
engine discovers the relation kind when it reads the table (nothing is
declared), compiles column comments and tag changes as `ALTER STREAMING
TABLE`, the table comment as `COMMENT ON TABLE`, and rejects any wider scope:
a `"full"` or `"metadata"` declaration against a streaming table fails
validation (`StreamingTableAnnotationsOnly`) even when nothing has drifted.

```python
from delta_engine.schema import Column, DeltaTable, Integer

clicks = DeltaTable(
    catalog="dev",
    schema="silver",
    name="clicks",
    columns=[Column("id", Integer(), nullable=False, tags={"pii": "low"})],
    primary_key=["id"],        # mirrors the pipeline's key; never applied
    tags={"owner": "governance"},
    comment="Click events, owned by the ingest pipeline.",
    scope="annotations",
)
```

### Mirror the pipeline's keys

A restricted scope still declares the full table shape, and keys are no
exception. If the pipeline's `CREATE STREAMING TABLE` declares a primary key,
the declaration must mirror it — as `primary_key=["id"]` above. The engine
never applies it: the declared and observed keys match, so no difference is
emitted at all. Omitting it is not neutral, because `primary_key=None` is a
positive assertion of absence everywhere else in the engine; the sync would
fail `UnmanagedAspectDrift` on a key this scope cannot manage.

If the pipeline later changes the key, the mirror stops matching and the next
sync fails the same way. That is late but loud, and it is the only honest
answer available: the engine cannot reconcile a key the pipeline owns, and
must not pretend otherwise. Update the declaration to the new reality.

A comment declared in the pipeline's own defining SQL is a different matter:
`CREATE OR REFRESH` is fully declarative, so a refresh can revert a comment
this engine set, and the next sync will set it again. The engine cannot read
the pipeline's SQL and so cannot warn about it. Do not manage a comment from
both places.

Materialized views remain unsupported: a name that resolves to one still
fails its read. See [limitations](reference-limitations.md).

## Mixing scopes in one sync

`scope` is per-table, not per-sync. A single `engine.sync(...)` call can
include fully managed, metadata-scoped, annotations-scoped, and tag-scoped
tables.
