---
tags:
  - how-to
---

# How to declare a primary key

Declare a primary key by setting `primary_key=True` on one or more `Column` objects. All primary key columns must be `NOT NULL`.

```python
from delta_engine import Column, DeltaTable, Integer, String

orders = DeltaTable(
    catalog="dev",
    schema="silver",
    name="orders",
    columns=[
        Column("order_id", Integer(), nullable=False, primary_key=True),
        Column("customer_id", Integer(), nullable=False),
        Column("status", String()),
    ],
)
```

The engine derives the constraint name as `{table_name}_pk` — `orders_pk` in the example above. This name is determined by the SQL adapter when DDL is emitted; it is not exposed on the table object.

## Composite primary keys

Set `primary_key=True` on multiple columns. The constraint covers them in declaration order.

```python
order_items = DeltaTable(
    catalog="dev",
    schema="silver",
    name="order_items",
    columns=[
        Column("order_id", Integer(), nullable=False, primary_key=True),
        Column("line_number", Integer(), nullable=False, primary_key=True),
        Column("product_id", Integer(), nullable=False),
    ],
)
```

## Drift management

The engine manages primary key drift on existing tables:

| Change | Actions emitted |
|---|---|
| Primary key added | `SetPrimaryKey` |
| Primary key removed | `DropPrimaryKey` |
| Primary key columns changed | `DropPrimaryKey` then `SetPrimaryKey` |
| No change | nothing |

Column order within the key is ignored when detecting drift — `(a, b)` and `(b, a)` are treated as equal.

## Constraints

Databricks primary key constraints are informational, not enforced. They do not prevent duplicate or null values at write time, but they enable query optimizations in Unity Catalog.

Because Databricks rejects a primary key on a nullable column at execution time, the engine validates this before running any SQL. If any primary key column is nullable, `sync` raises `SyncFailedError` with a `PrimaryKeyColumnsNullable` failure. See [reference-safe-change-rules.md](reference-safe-change-rules.md) for all validation rules.
