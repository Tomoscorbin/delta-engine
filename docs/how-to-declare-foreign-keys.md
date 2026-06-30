---
tags:
  - how-to
---

# How to declare a foreign key

Pass `foreign_keys` to `DeltaTable` with one `ForeignKey` per constraint. Each foreign key names the local columns, the fully qualified table it references, and the referenced columns.

```python
from delta_engine import Column, DeltaTable, ForeignKey, Integer, String

orders = DeltaTable(
    catalog="dev",
    schema="silver",
    name="orders",
    columns=[
        Column("order_id", Integer(), nullable=False, primary_key=True),
        Column("customer_id", Integer(), nullable=False),
        Column("status", String()),
    ],
    foreign_keys=[
        ForeignKey(
            local_columns=("customer_id",),
            references="dev.silver.customers",
            referenced_columns=("id",),
        ),
    ],
)
```

The engine derives the constraint name as `{table_name}_{local_columns}_fk` — `orders_customer_id_fk` above. To set the name yourself, pass `constraint_name`.

## Composite foreign keys

List the local and referenced columns in matching order. The first local column maps to the first referenced column, and so on.

```python
ForeignKey(
    local_columns=("tenant_id", "customer_id"),
    references="dev.silver.customers",
    referenced_columns=("tenant_id", "id"),
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
