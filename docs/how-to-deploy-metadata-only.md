---
tags:
  - how-to
---

# How to deploy metadata only

`DeltaTable(metadata_only=True)` restricts a sync to catalog metadata: table
and column comments, table and column tags, and primary/foreign key constraints.
Column structure, table properties, and partitioning are read for context but
never changed — a metadata-only sync can never add, drop, or alter a column.

Use it to roll out governance metadata with a hard guarantee that no schema
change can slip in.

## Declare a metadata-only table

```python
from delta_engine.schema import Column, DeltaTable, Integer, String

table = DeltaTable(
    catalog="dev",
    schema="silver",
    name="orders",
    columns=[
        Column("id", Integer(), nullable=False, primary_key=True,
               comment="surrogate key"),
        Column("customer_email", String(), comment="PII",
               tags={"pii": "true"}),
    ],
    comment="Customer orders",
    tags={"domain": "sales"},
    metadata_only=True,
)
```

The full schema is required. It states the expected shape of the live table —
if the live schema drifts from the declaration, the sync fails before any
metadata is applied.

## What a metadata-only sync does

- **Reconciles** table comment, column comments, table tags, column tags, and
  PK/FK constraints, exactly as a fully managed sync would.
- **Requires** the live schema to match the declaration exactly. Any unmanaged
  aspect (column structure or partitioning) that has drifted causes the sync
  to fail at validation before any SQL executes. Catalog properties are never
  compared for a metadata-only table — it declares none, and undeclared
  properties (for example those written by a previous fully managed sync) are
  not drift.
- **Cannot create** a missing table. If the table does not exist, the sync
  fails at validation.

## Mixing modes in one sync

`metadata_only` is per-table, not per-sync. A single `engine.sync(...)` call
can include both fully managed and metadata-only tables.
