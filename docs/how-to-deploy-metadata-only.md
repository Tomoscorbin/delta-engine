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
  executes. Catalog properties are the exception: a declaration that does not
  manage properties makes no property assertion at all, so they are never
  compared — declared properties are carried but ignored, and properties on
  the live table (for example those written by a previous fully managed sync)
  are not drift.
- **Cannot create** a missing table. If the table does not exist, the sync
  fails at validation (`MissingTableUnmanaged`) — a metadata-only declaration
  does not manage table existence, so it has no authority to create the table.

Both failures are scope invariants, listed in
[safe-change rules](reference-safe-change-rules.md).

## Mixing scopes in one sync

`scope` is per-table, not per-sync. A single `engine.sync(...)` call
can include fully managed, metadata-scoped, and tag-scoped tables.
