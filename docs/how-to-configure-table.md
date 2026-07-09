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
| Properties        | Delta/Spark table behaviour (retention, CDF, mapping) | [Properties](#properties), below                                                      |
| Tags              | Unity Catalog governance tags                         | [Tags](#tags), below                                                                  |
| Comments          | Table and column documentation                        | [Comments](#comments), below                                                          |
| Primary keys      | The table's primary key                               | [Primary keys](#primary-keys), below                                                  |
| Foreign keys      | Cross-table references and sync ordering              | [Foreign keys](#foreign-keys), below                                                  |
| Partitioning      | Partition columns, fixed at creation                  | [Partitioning](#partitioning), below                                                  |

## Properties

Properties are Delta/Spark `TBLPROPERTIES` that control table behaviour —
retention, change data feed, column mapping. The engine manages them by
**exact declaration**: your declaration is the complete list of the properties
you manage on that table.

- A key declared **with a value** is reconciled — set when absent, corrected
  when the catalog value differs.
- A key declared **as `None`** is asserted absent — removed from the table
  if present.
- A **managed key present on the table but missing from your declaration**
  fails the sync with a message naming the key and its current value:
  declare it to manage it, or declare it `None` to remove it.
- **Any other key is ignored.** Databricks writes many properties
  autonomously (`delta.enableRowTracking`, compression codecs, internal
  counters); the engine never compares or touches keys it does not manage,
  so platform behaviour and runtime upgrades cannot fail your syncs.

This is stricter than the tag and comment model on purpose: properties change
engine behaviour, where an unintended removal can be destructive, so the engine
only ever touches a key you named.

### The managed keys

| `Property` member                 | Delta table property key             | Restrictions                          |
| --------------------------------- | ------------------------------------ | ------------------------------------- |
| `CHANGE_DATA_FEED`                | `delta.enableChangeDataFeed`         | none                                  |
| `DELETED_FILE_RETENTION_DURATION` | `delta.deletedFileRetentionDuration` | none                                  |
| `LOG_RETENTION_DURATION`          | `delta.logRetentionDuration`         | none                                  |
| `DATA_SKIPPING_NUM_INDEXED_COLS`  | `delta.dataSkippingNumIndexedCols`   | none                                  |
| `COLUMN_MAPPING_MODE`             | `delta.columnMapping.mode`           | only `none → name`; cannot be removed |

Passing a key outside this set raises `ValueError` at `DeltaTable`
construction (for `None` assertions too). This prevents typos from silently
doing nothing.

Deletion vectors (`delta.enableDeletionVectors`) are deliberately **not**
managed: Databricks enables them automatically on new tables, so the engine
leaves that key entirely to the platform. The managed set is kept small on the
same principle — keys Databricks writes for itself stay out of it — and grows
only in documented releases.

### Value validation

Declared values are validated at `DeltaTable` construction, before a first
write can ever reach the catalog. Each managed key has an expected format:

| `Property` member                 | Expected value                                 |
| --------------------------------- | ---------------------------------------------- |
| `CHANGE_DATA_FEED`                | lowercase `true` or `false`                    |
| `DELETED_FILE_RETENTION_DURATION` | `interval <n> <unit>`, e.g. `interval 7 days`  |
| `LOG_RETENTION_DURATION`          | `interval <n> <unit>`, e.g. `interval 30 days` |
| `DATA_SKIPPING_NUM_INDEXED_COLS`  | an integer `>= -1` (`-1` indexes all columns)  |
| `COLUMN_MAPPING_MODE`             | `none` or `name`                               |

A value outside its key's format raises `ValueError` naming the key, the
rejected value, and the expected format. Booleans must be lowercase because
the catalog stores `true`/`false`; any other casing (`"True"`, `"yes"`)
would re-diff as drift on every sync even though the underlying value never
changes.

A key declared `None` asserts absence, not a value, so it is exempt from
this check.

Retention durations accept a single `interval <n> <unit>` term only.
Compound intervals such as `interval 1 hour 30 minutes` are rejected at
declaration even though the catalog itself accepts them; declare a
single-unit equivalent instead (e.g. `interval 90 minutes`). One canonical
spelling keeps the declared and observed values comparable, so an unchanged
property never re-diffs as drift.

### Declaring and removing properties

```python
from delta_engine.schema import Column, DeltaTable, Integer, Property

orders = DeltaTable(
    catalog="prod",
    schema="sales",
    name="orders",
    columns=[Column("id", Integer(), nullable=False, primary_key=True)],
    properties={
        Property.CHANGE_DATA_FEED: "true",          # ensure it is set
        Property.LOG_RETENTION_DURATION: None,       # ensure it is absent
    },
)
```

Removing a line from `properties` does **not** remove the property from the
table — it stops managing it, and the next sync fails loud asking you to
decide (declare a value, or declare `None`). Nothing is ever removed
implicitly.

The engine emits the requested table-property DDL but does not verify
whether your Databricks Runtime or Delta table protocol supports each
feature. If you enable change data feed on a runtime that cannot support
it, Databricks rejects the statement and `sync` reports an
`EXECUTION_FAILED` table with the original error. See
{ref}`runtime-and-delta-feature-compatibility`.

### Column mapping and dropping columns

Delta only permits `ALTER TABLE ... DROP COLUMN` when
`delta.columnMapping.mode` is `name`. Declare it on any table whose columns
may be dropped:

```python
properties={Property.COLUMN_MAPPING_MODE: "name"}
```

A sync that drops a column without this declaration fails at validation
(`ColumnMappingRequiredForDrop`) naming the property. Declaring it in the
same sync as the drop is safe — properties are set before columns are
dropped.

Two operations on this key are blocked at validation because the table
protocol upgrade is permanent: changing `name` back to `none`, and
declaring it `None` (a removal is a transition to absence, judged by the
same `PropertyTransitionNotSupported` rule). Once a table has column
mapping, its declaration must carry `Property.COLUMN_MAPPING_MODE: "name"`.

### When something else writes a managed key

Two platform mechanisms can write managed keys without your action:
Databricks' Automatic Upgrades service (writes properties onto enrolled
Unity Catalog managed tables) and admin session defaults
(`spark.databricks.delta.properties.defaults.*`, stamped onto new tables at
creation). If either writes a managed key onto your table, the next sync
fails loud with `PropertyMustBeDeclared` naming the key — add the line and
carry on. The engine never reacts silently to keys it did not set.

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

### Streaming tables: manage tags only

Use `StreamingTable` when the table is owned by a streaming pipeline but you
still want delta-engine to reconcile Unity Catalog tags. It accepts the same
shape as `DeltaTable`, but only table tags and column tags are managed:
columns, comments, properties, partitioning, primary keys, and foreign keys are
never changed by this declaration.

```python
from delta_engine.schema import Column, String, StreamingTable

events = StreamingTable(
    catalog="dev",
    schema="silver",
    name="streaming_events",
    columns=[
        Column("id", String()),
        Column("email", String(), tags={"pii": "true"}),
    ],
    tags={"domain": "events"},
)
```

The live table must already exist. If a non-tag aspect drifts from the
declaration, validation fails before any tag SQL runs; update the declaration
to match the live table or use a fully managed `DeltaTable`.

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
(`orders_customer_id_fk` above). The name cannot be chosen, and drift matching
never depends on it — a foreign key created outside the engine under a
different name still matches by content.

Generated names join local columns with underscores, so two foreign keys over
different columns can derive the same name — `("a", "b_c")` and `("a_b", "c")`
both derive `orders_a_b_c_fk`. A within-table collision is rejected when the
`DeltaTable` is constructed; rename a local column so the names differ.
Databricks scopes constraint names to the schema, so a generated name can also
collide with a constraint on _another_ table — that case is not checked and
fails at execution.

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

The referenced table must live in the same catalog as the table declaring the
key. Unity Catalog's information_schema is per-catalog, so the engine could
create a cross-catalog constraint but never observe it afterwards — every
later sync would re-plan and fail. A cross-catalog `references` is therefore
rejected when the `DeltaTable` is constructed.

The same dependency logic propagates failure: a referenced table that won't
reach its desired state this sync blocks every table downstream of it, which
report `FOREIGN_KEY_FAILED`. That cross-table blocking is part of the safety
model — see
[the safety model](explanation-safety-model.md#cross-table-dependency-blocking)
for the failure reasons and [how to handle sync failures](how-to-handle-sync-failures.md)
for reading the report.

Every table a foreign key references must be registered in the same
`sync(...)` call — including a parent that already exists in the catalog with
no drift. A foreign key to an unregistered table fails resolution with
`UNRESOLVABLE_REFERENCE`: the engine only trusts a parent it is also
reconciling, so a stale or drifted parent blocks its dependents rather than
being silently assumed correct.

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

The order of names in `partitioned_by` is significant, and independent of the
order columns appear in `columns`. It sets the order Delta nests partition
directories in — `partitioned_by=["region", "event_date"]` nests `region` above
`event_date` on storage — so `["region", "event_date"]` and
`["event_date", "region"]` describe different physical layouts, not the same set
of partition columns. Because partitioning is fixed at creation (below),
reordering the list on an existing table reads as a partitioning change and
fails validation, so declare the nesting order you want up front.

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
