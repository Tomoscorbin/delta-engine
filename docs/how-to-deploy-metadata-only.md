---
tags:
  - how-to
---

# How to deploy metadata only

`DeltaTable(metadata_only=True)` restricts a sync to catalog metadata: table
and column comments, table and column tags, and primary/foreign key
constraints. Column structure, table properties, and partitioning are read
for context but never changed — a metadata-only sync can never add, drop, or
alter a column. Use it to roll out governance metadata with a hard guarantee
that no schema change slips in.

## Declare a metadata-only table

```python
from delta_engine import Column, DeltaTable, Integer, String

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
    comment="Customer orders, one row per order",
    tags={"domain": "sales"},
    metadata_only=True,
)
```

The full schema is still required. It states the expected shape of the table
and lets the engine fail loudly when metadata targets a column that does not
exist — it is not used to change the schema.

## What a metadata-only sync does

- **Reconciles** table comment, column comments, table tags, column tags, and
  PK/FK constraints — tags full-state (undeclared tags are unset), exactly as
  in a fully managed sync.
- **Ignores** benign schema drift: extra live columns, changed column types,
  loosened nullability, property drift, and partitioning are none of its
  business. A declared column that has drifted in type still gets its comment
  and tags reconciled — metadata DDL targets columns by name.
- **Fails at plan time** when metadata cannot land:
  - the table does not exist (a metadata-only definition cannot create it);
  - a declared column carrying a comment, tags, or key membership is missing
    from the live table;
  - a primary key change targets columns that are nullable in the live table
    (with nullability unmanaged, the constraint would be rejected by
    Databricks at execution).

Each case surfaces as a `VALIDATION_FAILED` status naming the rule
(`MissingTargetTable`, `MissingTargetColumn`,
`UnenforceablePrimaryKeyChange`) before any SQL executes.

## Mixing modes in one sync

`metadata_only` is per table, not per sync. A single `engine.sync(...)` call
can carry fully managed and metadata-only tables together, including foreign
keys between them.
