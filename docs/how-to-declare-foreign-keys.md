---
tags:
  - how-to
---

# How to declare a foreign key

Pass `foreign_keys` to `DeltaTable` with one `ForeignKey` per constraint. Each foreign key names the local columns and the table they reference.

The preferred form is to reference another `DeltaTable`. When the referenced table has a primary key, the engine infers the referenced columns from that primary key:

```python
from delta_engine import Column, DeltaTable, ForeignKey, Integer, String

customers = DeltaTable(
    catalog="dev",
    schema="silver",
    name="customers",
    columns=[
        Column("id", Integer(), nullable=False, primary_key=True),
        Column("name", String()),
    ],
)

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
            references=customers,  # referenced_columns inferred as ("id",)
        ),
    ],
)
```

The engine derives the physical constraint name internally as `{table_name}_{local_columns}_fk` — `orders_customer_id_fk` above. The public `ForeignKey` declaration does not accept `constraint_name`; explicit constraint naming is not currently part of the public API.

## Referencing by name

Use a fully qualified string when the referenced table object is not available, such as for a forward reference or a declaration split across modules. A string reference must include `referenced_columns` because a name alone carries no primary-key metadata:

```python
ForeignKey(
    local_columns=("customer_id",),
    references="dev.silver.customers",
    referenced_columns=("id",),
)
```

A `QualifiedName` value can be used the same way, also with explicit `referenced_columns`.

## Composite foreign keys

List the local and referenced columns in matching order. The first local column maps to the first referenced column, and so on.

```python
ForeignKey(
    local_columns=("tenant_id", "customer_id"),
    references="dev.silver.customers",
    referenced_columns=("tenant_id", "id"),
)
```

If you pass a referenced `DeltaTable` with a composite primary key and omit `referenced_columns`, the full primary-key column tuple is inferred in declaration order.

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

The referenced table needs a matching primary or unique key for Databricks to accept the constraint at execution time. delta-engine currently validates foreign keys against referenced primary keys declared in the registry.
