---
tags:
  - how-to
---

# How to configure a table

`DeltaTable` is the single declaration for everything about a table — its
columns, the metadata that documents and governs it, its keys, and how it is
partitioned. This page is the practical reference for configuring each aspect.

| Aspect            | What it configures                                    | Where                                                                                 |
| ----------------- | ----------------------------------------------------- | ------------------------------------------------------------------------------------- |
| Columns and types | The table's shape and column data types               | [Getting started](tutorial-getting-started.md), [data types](reference-data-types.md) |
| Properties        | Delta/Spark table behaviour (retention, CDF, mapping) | [Table properties](how-to-configure-properties.md)                                    |
| Tags              | Unity Catalog governance tags                         | [Tags](#tags), below                                                                  |
| Comments          | Table and column documentation                        | [Comments](#comments), below                                                          |
| Primary keys      | The table's primary key                               | [Primary keys](#primary-keys), below                                                  |
| Foreign keys      | Cross-table references and sync ordering              | [Foreign keys](#foreign-keys), below                                                  |
| Partitioning      | Partition columns, fixed at creation                  | [Partitioning](#partitioning), below                                                  |

Every aspect except properties is covered here in full. Properties has its own
page for now and will move into this one as the documentation grows.

## Tags

`DeltaTable` accepts a `tags` dict of Unity Catalog tag keys to string values. Tags are a Unity Catalog governance feature — separate from table properties: they are stored in the Unity Catalog metastore (not the Delta log), applied with `ALTER TABLE ... SET TAGS`, and read back from `information_schema.table_tags`. Use them for classification, ownership, cost attribution, and discovery.

### Declare tags

```python
from delta_engine.schema import Column, DeltaTable, String

table = DeltaTable(
    catalog="dev",
    schema="silver",
    name="events",
    columns=[Column("id", String())],
    tags={
        "env": "prod",
        "domain": "sales",
        "cost_centre": "data-eng",
    },
)
```

Tag keys are free-form strings — there is no enum allowlist (unlike properties). Keys are **case-sensitive**: `env` and `Env` are distinct tags.

### Reconciliation is full-state

The engine owns the complete set of tags on a table. On each sync it:

- **sets** any declared tag that is missing from the catalog or has a different value, and
- **unsets** any tag found on the table that is _not_ in your declaration.

This means a tag applied outside delta-engine (in the Databricks UI, by another job, or by a tag policy) **will be removed** on the next sync unless you also declare it. This is deliberate — it keeps the table's tags exactly as declared — but declare every tag you want to keep.

> This differs from table properties, which are declared-subset: properties set out-of-band are left untouched.

### Requirements

Tags require Unity Catalog on Databricks Runtime 13.3 LTS or later, and the `APPLY TAG` privilege on the table (plus `USE SCHEMA` / `USE CATALOG`). On non-Unity-Catalog environments the engine observes no tags and emits no tag changes.

Databricks limits: up to 50 tags per table; keys up to 256 characters and values up to 1,000 characters; tag keys cannot contain `. , - = / :` or leading/trailing spaces. Delta-engine enforces the 50-tag limit and the 1,000-character value limit at declaration time; declarations that violate these limits fail immediately with a `ValueError`.

### Column tags

Tags can also be declared on individual columns. Pass a `tags` dict to a
`Column`:

```python
from delta_engine.schema import Column, DeltaTable, String

table = DeltaTable(
    catalog="dev",
    schema="silver",
    name="events",
    columns=[
        Column("id", String()),
        Column(
            "email",
            String(),
            tags={"pii": "true", "classification": "restricted"},
        ),
    ],
)
```

Column tags follow the **same full-state reconciliation** as table tags: on each
sync the engine sets any declared tag that is missing or has a different value,
and unsets any tag found on the column that is not declared. A column tag applied
out-of-band (Databricks UI, another job, an automated classifier) is removed on
the next sync unless it is also declared.

As with table tags, keys are **case-sensitive** (`PII` and `pii` are distinct).

### Requirements and limits

Column tags require Unity Catalog on Databricks Runtime 13.3 LTS or later and the
`APPLY TAG` privilege. Databricks limits: up to 50 tags per column, at most 1,000
column tags per table across all columns, keys up to 256 characters and values up
to 1,000 characters, and tag keys cannot contain `. , - = / :` or leading/trailing
spaces. Delta-engine enforces the 50-tags-per-column limit and the 1,000-character
value limit at declaration time; violations fail immediately with a `ValueError`.

## Comments

Comments document a table and its columns in the catalog, where they show up in
the Unity Catalog UI and `DESCRIBE` output. As with tags, the declaration is the
source of truth: whatever comment it states — including no comment — is what the
table gets.

### Declare comments

Pass `comment` to `DeltaTable` for the table and to `Column` for each column:

```python
from delta_engine.schema import Column, DeltaTable, Integer, String

orders = DeltaTable(
    catalog="dev",
    schema="silver",
    name="orders",
    comment="One row per customer order.",
    columns=[
        Column("id", Integer(), nullable=False, comment="Surrogate key."),
        Column("customer_email", String(), comment="PII - masked downstream."),
    ],
)
```

Syncing applies any comment that differs from the live table.

### Removing a comment

Comments follow the declaration exactly, in both directions. A column declared
without a comment (the default is the empty string) asserts that the column has
no comment — so removing a comment from the declaration clears it on the table
at the next sync, and a comment added to the table outside the declaration is
drift that the sync overwrites.

## Primary keys

Declare a primary key by setting `primary_key=True` on one or more columns.
Every primary key column must be non-nullable.

```python
from delta_engine.schema import Column, DeltaTable, Integer, String

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

The engine names the constraint `{table}_pk` — `orders_pk` above. The name is
generated when the `DeltaTable` is lowered to the domain model, then carried as
data through diffing and SQL generation rather than re-derived; it is not
exposed on the table object.

### Composite primary keys

Set `primary_key=True` on several columns. The constraint covers them in
declaration order.

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

### Why primary key columns must be non-nullable

A primary key identifies a row, so a nullable key column is not a well-formed
table definition — and Databricks rejects a nullable primary key at execution
time regardless. The engine enforces this early: declaring `primary_key=True`
on a nullable column raises `ValueError` when the `DeltaTable` is constructed,
before any sync runs.

### Constraints are informational

Databricks primary and foreign key constraints are _informational, not
enforced_: they do not prevent duplicate or null values at write time. Unity
Catalog uses them to document intent and to enable query optimizations such as
eliminating provably redundant joins. Declaring one tells Databricks the key
holds; the engine does not, and cannot, make Databricks validate it.

### Drift

The engine detects primary key drift by comparing the _column set_, not the
constraint name. The generated `{table}_pk` name is used only to emit the DDL;
it never participates in the comparison. So a primary key already on the table
with the same columns under a different name — one created by hand or by another
tool — is not drift and produces no action, which keeps repeated syncs
idempotent.

| Change                       | Actions emitted                       |
| ---------------------------- | ------------------------------------- |
| Primary key added            | `SetPrimaryKey`                       |
| Primary key removed          | `DropPrimaryKey`                      |
| Primary key columns changed  | `DropPrimaryKey` then `SetPrimaryKey` |
| Same columns, any name/order | nothing                               |

Column order within the key is ignored too — `(a, b)` and `(b, a)` are treated
as equal.

Key-constraint support depends on your Databricks environment. The engine does
not preflight Databricks Runtime or Delta table protocol compatibility; if
Databricks rejects the constraint DDL, `sync` reports an `EXECUTION_FAILED`
table with the original error. See
{ref}`runtime-and-delta-feature-compatibility`.

## Foreign keys

Pass `foreign_keys` with one `ForeignKey` per constraint. Each names the local
columns and the table they reference; the referenced columns are inferred from
that table's primary key.

```python
from delta_engine.schema import Column, DeltaTable, ForeignKey, Long, String

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
        ForeignKey(local_columns=["customer_id"], references=customers),
    ],
)
```

Referencing the target `DeltaTable` object — rather than a dotted table name —
is what lets the engine infer the referenced columns from that table's primary
key, and keeps the reference valid if the target is renamed. The constraint
name is generated at lowering as `{table}_{local_columns}_fk`
(`orders_customer_id_fk` above); it is internal and not part of the public API.

Each local column's data type must match its referenced primary-key column's
type. A mismatch raises `ValueError` when the `DeltaTable` is constructed,
before any sync runs.

### Self-referential foreign keys

Use the `Self` sentinel when a table references itself:

```python
from delta_engine.schema import Self

employees = DeltaTable(
    catalog="dev",
    schema="silver",
    name="employees",
    columns=[
        Column("id", Long(), nullable=False, primary_key=True),
        Column("manager_id", Long()),
    ],
    foreign_keys=[
        ForeignKey(local_columns=["manager_id"], references=Self),
    ],
)
```

### Composite foreign keys

For a composite primary key, list `local_columns` in the referenced table's
primary-key declaration order. The referenced columns are inferred one-to-one
in that same order.

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
            # aligns with customer_accounts PK (tenant_id, id)
            local_columns=["tenant_id", "customer_id"],
            references=customer_accounts,
        ),
    ],
)
```

### Dependency ordering

A foreign key can only be added once its referenced table exists with the
matching key in place. The engine therefore syncs a referenced table before the
tables that reference it, so you can declare tables in any order. A foreign key
into the table's own primary key is allowed — the engine creates the table,
then adds the constraint.

The same dependency logic propagates failure: a referenced table that won't
reach its desired state this sync blocks every table downstream of it, which
report `FOREIGN_KEY_FAILED`. That cross-table blocking is part of the safety
model — see
[the safety model](explanation-safety-model.md#cross-table-dependency-blocking)
for the failure reasons and [how to handle sync failures](how-to-handle-sync-failures.md)
for reading the report.

### Drift

The engine matches foreign keys by content — local columns, referenced table,
and referenced columns — not by constraint name.

| Change                                      | Actions emitted                       |
| ------------------------------------------- | ------------------------------------- |
| Foreign key added                           | `SetForeignKey`                       |
| Foreign key removed                         | `DropForeignKey`                      |
| Foreign key changed                         | `DropForeignKey` then `SetForeignKey` |
| Same foreign key, different constraint name | nothing                               |
| No change                                   | nothing                               |

Matching by content keeps syncs idempotent: a foreign key created outside this
engine, under a name the engine would not derive, produces no actions as long
as its columns and referenced table match the declaration.

### Constraints are informational

Like primary keys, Databricks foreign keys are informational, not enforced:
they do not block inserts that violate referential integrity. The referenced
table needs a matching primary or unique key for Databricks to accept the
constraint at execution time. Support, including unique constraints used as
referenced keys, depends on your Databricks environment; a rejected constraint
surfaces as an `EXECUTION_FAILED` table with the original error. See
{ref}`runtime-and-delta-feature-compatibility`.

## Partitioning

Pass `partitioned_by` to set the columns a table is partitioned by when it is
created:

```python
from delta_engine.schema import Column, Date, DeltaTable, String

events = DeltaTable(
    catalog="dev",
    schema="silver",
    name="events",
    columns=[
        Column("event_date", Date()),
        Column("event_type", String()),
        Column("payload", String()),
    ],
    partitioned_by=["event_date"],
)
```

Every name in `partitioned_by` must also appear in `columns`. Partition columns
are still regular columns: `partitioned_by` names them, and `columns` defines
their types and other metadata.

### Partitioning is fixed at creation

Partitioning can only be set when the table is created. Partition columns
determine how the table's data files are physically laid out on storage, and
Delta Lake has no `ALTER TABLE` that repartitions an existing table in place.
Changing the partition columns means rewriting every data file into the new
layout — a full table rewrite (for example `REPLACE TABLE ... PARTITIONED BY`,
or an overwrite with a new partitioning), not the in-place DDL delta-engine
issues.

Because that rewrite is a data operation outside the engine's remit, declaring
a different `partitioned_by` for an existing table fails validation before any
SQL runs. To change partitioning, rewrite the table out of band, then re-sync
against the new layout.

Partition columns also cannot be of complex type (`Array`, `Map`, `Struct`,
`Variant`), and a table cannot be partitioned by every column; both are
rejected when the `DeltaTable` is constructed. See
[safe-change rules](reference-safe-change-rules.md) for the full set of changes
the engine rejects.
