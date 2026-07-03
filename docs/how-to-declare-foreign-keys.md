---
tags:
  - how-to
---

# How to declare a foreign key

Pass `foreign_keys` to `DeltaTable` with one `ForeignKey` per constraint. Each foreign key names the local columns and the table they reference; the referenced columns are inferred from that table's primary key.

```python
from delta_engine import Column, DeltaTable, ForeignKey, Long, String

customers = DeltaTable(
    catalog="dev",
    schema="silver",
    name="customers",
    columns=[
        Column("id", Long(), nullable=False, primary_key=True),
        Column("name", String()),
    ],
)

orders = DeltaTable(
    catalog="dev",
    schema="silver",
    name="orders",
    columns=[
        Column("order_id", Long(), nullable=False, primary_key=True),
        Column("customer_id", Long(), nullable=False),
        Column("status", String()),
    ],
    foreign_keys=[
        ForeignKey(
            local_columns=("customer_id",),
            references=customers,
        ),
    ],
)
```

The engine derives the constraint name as `{table_name}_{local_columns}_fk` — `orders_customer_id_fk` above. This name is internal; `constraint_name` is not part of the public API.

## Self-referential foreign keys

Use the `Self` sentinel when a table references itself:

```python
from delta_engine import Self

employees = DeltaTable(
    catalog="dev",
    schema="silver",
    name="employees",
    columns=[
        Column("id", Long(), nullable=False, primary_key=True),
        Column("manager_id", Long()),
    ],
    foreign_keys=[
        ForeignKey(local_columns=("manager_id",), references=Self),
    ],
)
```

## Composite foreign keys

For a composite primary key, list `local_columns` in the referenced table's primary-key declaration order. The referenced columns are inferred one-to-one in that same order.

```python
customer_accounts = DeltaTable(
    catalog="dev",
    schema="silver",
    name="customer_accounts",
    columns=[
        Column("tenant_id", Long(), nullable=False, primary_key=True),
        Column("id", Long(), nullable=False, primary_key=True),
    ],
)

order_lines = DeltaTable(
    catalog="dev",
    schema="silver",
    name="order_lines",
    columns=[
        Column("order_line_id", Long(), nullable=False, primary_key=True),
        Column("tenant_id", Long(), nullable=False),
        Column("customer_id", Long(), nullable=False),
    ],
    foreign_keys=[
        ForeignKey(
            local_columns=("tenant_id", "customer_id"),  # aligns with customer_accounts PK (tenant_id, id)
            references=customer_accounts,
        ),
    ],
)
```

## Dependency ordering

The engine syncs a referenced table before the tables that depend on it. Declare `orders` and `customers` in any order — the engine reorders them so `customers` exists before `orders` adds its foreign key.

A foreign key that references the table it belongs to is allowed. The engine creates the table first, then adds the constraint.

## All-or-nothing across dependencies

A foreign key fails its whole table when:

- It references a table missing from the registry (`UNRESOLVABLE_REFERENCE`).
- It forms a dependency cycle with other tables (`CYCLE`).
- The table it references won't reach its desired state this sync, for any reason (`BLOCKED_BY_FAILED_DEPENDENCY`).

The third case is transitive. If `customers` fails validation, `orders` won't execute either — and any table that references `orders` is blocked in turn. A dependency that won't build blocks every table downstream of it.

A blocked table reports `FOREIGN_KEY_FAILED`. See [how-to-handle-sync-failures.md](how-to-handle-sync-failures.md) for reading the failure report.

## Drift management

The engine matches foreign keys by content — local columns, referenced table, and referenced columns — not by constraint name.

| Change | Actions emitted |
|---|---|
| Foreign key added | `SetForeignKey` |
| Foreign key removed | `DropForeignKey` |
| Foreign key changed | `DropForeignKey` then `SetForeignKey` |
| Same foreign key, different constraint name | nothing |
| No change | nothing |

Matching by content keeps syncs idempotent: a foreign key created outside this engine, under a name the engine wouldn't derive, produces no actions as long as its columns and referenced table match the declaration.

## Constraints

Databricks foreign key constraints are informational, not enforced. They do not block inserts that violate referential integrity, but they enable query optimizations and document intent in Unity Catalog.

The referenced table needs a matching primary or unique key for Databricks to accept the constraint at execution time.
