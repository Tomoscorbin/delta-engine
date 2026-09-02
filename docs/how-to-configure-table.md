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
| Clustering        | Liquid clustering keys, reconciled in place           | [Clustering](#clustering), below                                                      |

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

| `TableProperty` member                 | Delta table property key             | Restrictions                          |
| --------------------------------- | ------------------------------------ | ------------------------------------- |
| `CHANGE_DATA_FEED`                | `delta.enableChangeDataFeed`         | none                                  |
| `DELETED_FILE_RETENTION_DURATION` | `delta.deletedFileRetentionDuration` | none                                  |
| `LOG_RETENTION_DURATION`          | `delta.logRetentionDuration`         | none                                  |
| `DATA_SKIPPING_NUM_INDEXED_COLS`  | `delta.dataSkippingNumIndexedCols`   | none                                  |
| `COLUMN_MAPPING_MODE`             | `delta.columnMapping.mode`           | only `none → name`; cannot be removed |
| `TYPE_WIDENING`                   | `delta.enableTypeWidening`           | none                                  |

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

| `TableProperty` member                 | Expected value                                 |
| --------------------------------- | ---------------------------------------------- |
| `CHANGE_DATA_FEED`                | lowercase `true` or `false`                    |
| `DELETED_FILE_RETENTION_DURATION` | `interval <n> <unit>`, e.g. `interval 7 days`  |
| `LOG_RETENTION_DURATION`          | `interval <n> <unit>`, e.g. `interval 30 days` |
| `DATA_SKIPPING_NUM_INDEXED_COLS`  | an integer `>= -1` (`-1` indexes all columns)  |
| `COLUMN_MAPPING_MODE`             | `none` or `name`                               |
| `TYPE_WIDENING`                   | lowercase `true` or `false`                    |

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
from delta_engine.schema import Column, DeltaTable, Integer, TableProperty

orders = DeltaTable(
    catalog="prod",
    schema="sales",
    name="orders",
    columns=[Column("id", Integer(), nullable=False)],
    primary_key=["id"],
    properties={
        TableProperty.CHANGE_DATA_FEED: "true",          # ensure it is set
        TableProperty.LOG_RETENTION_DURATION: None,       # ensure it is absent
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
properties={TableProperty.COLUMN_MAPPING_MODE: "name"}
```

A sync that drops a column without this declaration fails at validation
(`ColumnMappingRequiredForDrop`) naming the property. Declaring it in the
same sync as the drop is safe — properties are set before columns are
dropped.

Two operations on this key are blocked at validation: changing `name` back
to `none`, and declaring it `None` (a removal is a transition to absence,
judged by the same `PropertyTransitionNotSupported` rule). Databricks can
remove column mapping, but doing so rewrites every data file and conflicts
with concurrent writes, so the engine rejects it as an in-place change —
the same class of operation as a partitioning change. Once a table has
column mapping, its declaration must carry
`TableProperty.COLUMN_MAPPING_MODE: "name"`; remove the feature out of band if
you truly need to.

### Renaming a column

To rename a column, keep it in the declaration under its new name and add a
`renamed_from` hint pointing at the old name:

```python
from delta_engine.schema import Column, DeltaTable, Integer, String

customers = DeltaTable(
    "dev",
    "silver",
    "customers",
    columns=[
        Column("id", Integer(), nullable=False),
        Column("customer_name", String(), renamed_from="customer_nm"),
    ],
    properties={"delta.columnMapping.mode": "name"},
)
```

The engine renames the column in place with `ALTER TABLE ... RENAME COLUMN`,
preserving its data — as opposed to editing the name directly, which reads as
a drop plus an add and would destroy the column's data. Renaming requires
`delta.columnMapping.mode='name'`; a hint without it is rejected when the
`DeltaTable` is constructed (the requirement is visible at declaration time,
so it fails early rather than at sync).

The hint applies exactly once — when the old name is observed and the new one
is not. After the rename, the old name is gone, so the hint matches nothing
and the sync is a no-op: it is safe to keep as history (remove it if you later
narrow the declaration's `scope`), and the same declaration deploys correctly
into a fresh catalog. If both the old and new names exist on the table the
rename cannot apply and the sync fails (`AmbiguousColumnRename`); if the old
column should instead be dropped, remove the hint and drop it in its own sync.

A primary or foreign key involving the column is replaced across the rename:
the plan drops the key, renames the column, then re-adds the declared key, so
every statement the engine runs is stated in the plan. (Databricks would drop
those keys implicitly during `RENAME COLUMN`; the engine does not rely on
that.) Keys on Databricks are informational, so if a later statement fails the
only exposure is a missing key until the next successful sync restores it. If
**another** table's foreign key references the renamed primary key, sync that
table without the foreign key first — an inbound reference blocks the change.
Partitioning and clustering metadata follow the mapped column's identity, so
renaming a layout key needs no separate layout change.

Change any dependent CHECK constraint or generated-column expression before
renaming: the engine does not model those dependencies, so Databricks rejects
the rename at execution if one remains
(`DELTA_CONSTRAINT_DEPENDENT_COLUMN_CHANGE`). The engine cannot rename struct
fields — `renamed_from` applies only to top-level columns, although
Databricks itself can rename nested fields.

### When something else writes a managed key

Two platform mechanisms can write managed keys without your action:
Databricks' Automatic Upgrades service (writes properties onto enrolled
Unity Catalog managed tables) and admin session defaults
(`spark.databricks.delta.properties.defaults.*`, stamped onto new tables at
creation). If either writes a managed key onto your table, the next sync
fails loud with `PropertyMustBeDeclared` naming the key — add the line and
carry on. The engine never reacts silently to keys it did not set.

### Type widening

Declaring `delta.enableTypeWidening='true'` allows a sync to widen a column's
type in place:

```python
properties={TableProperty.TYPE_WIDENING: "true"}
```

The widenings Delta can apply in place are supported:

| From            | To                                                    |
| --------------- | ----------------------------------------------------- |
| `Byte`          | `Short`, `Integer`, `Long`, `Double`, `Decimal`       |
| `Short`         | `Integer`, `Long`, `Double`, `Decimal`                |
| `Integer`       | `Long`, `Double`, `Decimal`                           |
| `Long`          | `Decimal`                                             |
| `Float`         | `Double`                                              |
| `Decimal(p, s)` | `Decimal(p′, s′)` with `s′ ≥ s` and `p′ − s′ ≥ p − s` |
| `Date`          | `TimestampNtz`                                        |

A `Decimal` target must keep room for every source value: at least ten
integer digits (`p − s ≥ 10`) when widening `Byte`, `Short`, or `Integer`,
and at least twenty when widening `Long`. The same principle governs
decimal-to-decimal changes — scale may grow only when precision grows with
it, so the integer digits never shrink.

A widening without the property declared fails validation
(`TypeWideningRequiredForTypeChange`) naming the property; declaring it in
the same sync as the widen is safe — properties are set before column types
change. Any other type change fails validation
(`NonWideningColumnTypeChange`); recreate the table out of band to make it.

Tables using UniForm with Iceberg compatibility reject the widenings Iceberg
cannot read — integer types to `Decimal` or `Double`, decimal scale growth,
and `Date` to `TimestampNtz`. The engine does not model UniForm, so on such
a table these widenings fail at execution with the original Databricks
error.

Type widening requires Databricks Runtime 15.4 LTS or later for Spark
workloads. Databricks manages SQL warehouse versions separately. Delta-engine
does not preflight feature availability, so using it in an environment that
does not support it fails at execution with the original Databricks error.
Note that enabling type widening adds the `typeWidening` protocol feature to
the table permanently: declaring the property `false` (or `None`) later stops
further widenings but does not remove the feature — that requires
`ALTER TABLE ... DROP FEATURE`, which is outside this engine's scope.

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

Tag keys are strings rather than members of an engine enum (unlike properties),
but Unity Catalog still applies the character and length restrictions below.
Keys are **case-sensitive**: `env` and `Env` are distinct tags.

### Reconciliation is full-state

The engine owns the complete set of tags on a table. On each sync it:

- **sets** any declared tag that is missing from the catalog or has a different value, and
- **unsets** any tag found on the table that is _not_ in your declaration.

This means a tag applied outside delta-engine (in the Databricks UI, by another job, or by a tag policy) **will be removed** on the next sync unless you also declare it. This is deliberate — it keeps the table's tags exactly as declared — but declare every tag you want to keep.

> This differs from table properties. A supported property found on the table
> but omitted from the declaration fails validation; properties outside the
> engine's supported set are ignored.

### Requirements

Tags require Unity Catalog and either a Databricks SQL warehouse or Databricks
Runtime 13.3 LTS or later. The principal needs `APPLY TAG` on the table plus
`USE SCHEMA` and `USE CATALOG`; applying a governed tag also requires `ASSIGN`
on that tag. Both delta-engine backends require Unity Catalog for reads.

Databricks limits each table to 50 tags, tag keys to 256 characters, and tag
values to 256 characters. Tag keys cannot contain `. , - = / :`, and tag keys
or values cannot have leading or trailing spaces. Delta-engine enforces all of
this at declaration time: a declaration that violates any of these limits
raises `ValueError` when the `DeltaTable` is constructed, before any SQL runs.

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

### Manage tags only

Use `scope="tags"` when the table is owned elsewhere — for example by a
streaming pipeline — but you still want delta-engine to reconcile Unity
Catalog tags. The declaration carries the same table shape as any other, but
only table tags and column tags are managed: columns, comments, properties,
partitioning, primary keys, and foreign keys are never changed.

```python
from delta_engine.schema import Column, DeltaTable, String

events = DeltaTable(
    catalog="dev",
    schema="silver",
    name="streaming_events",
    columns=[
        Column("id", String()),
        Column("email", String(), tags={"pii": "true"}),
    ],
    tags={"domain": "events"},
    scope="tags",
)
```

A table that does not exist yet is deferred with a warning — the declaration
cannot create it, so the sync reports it as `DEFERRED` (neither changed nor
failed) and applies the tags once something else has created it. If a non-tag
aspect drifts from the declaration, validation fails before any tag SQL runs;
update the declaration to match the live table or use the full scope.
Properties are the exception: a restricted scope never compares them, so live
table properties cannot fail the sync.

### Manage comments and tags only

Use `scope="annotations"` when the table's structure belongs to someone else
but its catalog documentation should still be governed here. It manages the
table comment, column comments, table tags, and column tags — a superset of
`"tags"` and a subset of `"metadata"`, which adds key constraints on top.

```python
from delta_engine.schema import Column, DeltaTable, String

events = DeltaTable(
    catalog="dev",
    schema="silver",
    name="streaming_events",
    columns=[
        Column("id", String(), comment="Event identifier"),
        Column("email", String(), comment="Contact address", tags={"pii": "true"}),
    ],
    comment="Raw events, owned by the ingest pipeline.",
    tags={"domain": "events"},
    scope="annotations",
)
```

Streaming tables are supported under `"annotations"` and `"tags"`, and no
wider scope: the engine discovers the relation kind at read time, compiles
column comments and tags with the `ALTER STREAMING TABLE` dialect and the
table comment with `COMMENT ON TABLE`, and rejects a scope claiming schema,
properties, or keys. A key the owning pipeline declared must be mirrored in
the declaration. See
[annotate a streaming table](how-to-deploy-metadata-only.md#annotate-a-streaming-table).

### Requirements and limits

Column tags require Unity Catalog and either a Databricks SQL warehouse or
Databricks Runtime 13.3 LTS or later, plus the `APPLY TAG` privilege; governed
tags also require `ASSIGN`. Databricks limits each column to 50 tags, each
table to 1,000 column tags in total, tag keys and values to 256 characters,
forbids `. , - = / :` in tag keys, and forbids leading or trailing spaces in
keys and values. Delta-engine enforces all of this at declaration time: a
declaration that violates any of these limits raises `ValueError` when the
`DeltaTable` is constructed, before any SQL runs.

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

Declare a primary key by passing `primary_key` to `DeltaTable` — a list of
column names. Column order is not part of the key's meaning: the engine stores
and renders the columns in a canonical (case-insensitive sorted) order, so the
order you declare them in does not matter. Every primary key column must be
non-nullable.

```python
from delta_engine.schema import Column, DeltaTable, Integer, String

orders = DeltaTable(
    catalog="dev",
    schema="silver",
    name="orders",
    columns=[
        Column("order_id", Integer(), nullable=False),
        Column("customer_id", Integer(), nullable=False),
        Column("status", String()),
    ],
    primary_key=["order_id"],
)
```

Because no physical name is supplied above, the engine omits the name from the
SQL and Databricks generates one. On later syncs, any observed primary key over
the same columns satisfies this declaration regardless of its physical name.

### Choose a primary-key name

Pass `primary_key_name` to request a physical name when the key is created:

```python
orders = DeltaTable(
    catalog="dev",
    schema="silver",
    name="orders",
    columns=[Column("order_id", Integer(), nullable=False)],
    primary_key=["order_id"],
    primary_key_name="orders_business_key",
)
```

An explicit name must accompany `primary_key`. It is a creation preference,
not ongoing managed state: if the live key already has the same columns under
another name, the engine accepts it. Changing only `primary_key_name` therefore
does not rename or recreate an existing key.

Constraint names share one case-insensitive namespace across all tables and
constraint kinds in a schema. Choose an explicit name that is unique across
that whole schema. `DeltaTable.primary_key_name` returns the explicitly
declared name, or `None` when the name is left to Databricks (including when a
primary key exists); `DeltaTable.primary_key` continues to return the tuple of
key columns.

### Composite primary keys

List several column names in `primary_key`. Their order does not matter — the
key covers the same set of columns however you order them, and the engine
renders them in a canonical sorted order.

```python
order_items = DeltaTable(
    catalog="dev",
    schema="silver",
    name="order_items",
    columns=[
        Column("order_id", Integer(), nullable=False),
        Column("line_number", Integer(), nullable=False),
        Column("product_id", Integer(), nullable=False),
    ],
    primary_key=["order_id", "line_number"],
)
```

### Why primary key columns must be non-nullable

A primary key identifies a row, so a nullable key column is not a well-formed
table definition — and Databricks rejects a nullable primary key at execution
time regardless. The engine enforces this early: naming a nullable column in
`primary_key` raises `ValueError` when the `DeltaTable` is constructed,
before any sync runs.

### Constraints are informational

Databricks primary and foreign key constraints are _informational, not
enforced_: they do not prevent duplicate or invalid references at write time.
The engine does not specify `RELY`, so Databricks records its default `NORELY`
form. These constraints document relationships in Unity Catalog but are not
trusted for optimizer rewrites such as join elimination. Those optimizations
require `RELY`, which delta-engine does not currently model. If you add `RELY`
out of band, verify the data first; Databricks trusts the assertion, and a later
engine plan that drops and re-adds the key restores the default `NORELY` form.

### Drift

The engine compares primary keys by their _column set_. The physical name is
not part of drift; `primary_key_name` is used only when creating the key.

| Change                                                | Actions emitted                       |
| ----------------------------------------------------- | ------------------------------------- |
| Primary key added                                     | `SetPrimaryKey`                       |
| Primary key removed                                   | `DropPrimaryKey`                      |
| Primary key columns changed                           | `DropPrimaryKey` then `SetPrimaryKey` |
| Same columns, any requested or observed name          | nothing                               |

Column order within the key is ignored too — `(a, b)` and `(b, a)` are treated
as equal.

Key-constraint support depends on your Databricks environment. The engine does
not preflight Databricks Runtime or Delta table protocol compatibility; if
Databricks rejects the constraint DDL, `sync` reports an `EXECUTION_FAILED`
table with the original error. See
{ref}`runtime-and-delta-feature-compatibility`.

## Foreign keys

Pass `foreign_keys` with one `ForeignKey` per constraint. For a
single-column parent key, `columns` can be the local column name. For a
same-name composite key, it can be a sequence of local names. Use an explicit
`{local: referenced}` mapping when local and parent names differ.

```python
from delta_engine.schema import Column, DeltaTable, ForeignKey, Long, String

customers = DeltaTable(
    catalog="dev",
    schema="silver",
    name="customers",
    columns=[
        Column("id", Long(), nullable=False),
        Column("name", String()),
    ],
    primary_key=["id"],
)

orders = DeltaTable(
    catalog="dev",
    schema="silver",
    name="orders",
    columns=[
        Column("order_id", Long(), nullable=False),
        Column("customer_id", Long(), nullable=False),
        Column("status", String()),
    ],
    primary_key=["order_id"],
    foreign_keys=[
        ForeignKey(
            columns="customer_id",
            references=customers,
            name="orders_customer_fk",
        ),
    ],
)
```

Referencing the target `DeltaTable` object — rather than a dotted table name —
lets the engine resolve the declaration against that table's primary key,
validate the resulting column pairs, and capture the target's qualified name.
Set `name` to request a physical name when the constraint is created. When it
is omitted, the engine emits unnamed foreign-key SQL and Databricks allocates a
schema-unique name. On later syncs, any observed foreign key with the same local
columns, referenced table, and referenced columns satisfies the declaration,
regardless of its physical name. Explicit names must be schema-unique at
creation; a collision is reported as an execution failure from Databricks.

Each local column's data type must match its referenced primary-key column's
type. A mismatch raises `ValueError` when the `DeltaTable` is constructed,
before any sync runs. That check uses the exact parent object passed to
`ForeignKey(references=...)`. If the same sync registers a different
`DeltaTable` instance with the same qualified name but different key types, the
resolver repeats the type check against that registered instance and fails the
table with `REFERENCED_COLUMN_TYPE_MISMATCH`, blocking its dependents.
Register the same parent object used by the foreign-key declaration so the
mismatch surfaces at construction rather than at resolution.

### Referencing a table by name

`references` also accepts a dotted name string, for when the parent's
`DeltaTable` object is not importable — another team owns it, or importing it
would create a circular import:

```python
orders = DeltaTable(
    catalog="dev",
    schema="silver",
    name="orders",
    columns=[
        Column("order_id", Long(), nullable=False),
        Column("customer_id", Long(), nullable=False),
    ],
    primary_key=["order_id"],
    foreign_keys=[
        ForeignKey(columns={"customer_id": "id"}, references="dev.silver.customers"),
    ],
)
```

The full `"catalog.schema.table"` name is required; fewer parts are rejected.
The catalog must be the owning table's catalog — a cross-catalog name fails
construction exactly as a cross-catalog object reference does. When the
catalog varies by environment, build the reference from the same value the
table uses: `references=f"{catalog}.silver.customers"`.

A name carries no primary key to resolve column shorthands against, so a name
reference requires the explicit `{local: referenced}` mapping form. The
primary-key and column-type checks an object reference runs at construction
happen at sync time instead, when the resolver judges the registered parent —
the referenced table must be registered in the same sync either way. The
mapping's referenced column spelling must match the parent's declared spelling
exactly; a case difference is reported at sync as
`REFERENCED_COLUMN_CASE_MISMATCH`.

### Self-referential foreign keys

Use the `Self` sentinel when a table references itself:

```python
from delta_engine.schema import Self

employees = DeltaTable(
    catalog="dev",
    schema="silver",
    name="employees",
    columns=[
        Column("id", Long(), nullable=False),
        Column("manager_id", Long()),
    ],
    primary_key=["id"],
    foreign_keys=[
        ForeignKey(columns="manager_id", references=Self),
    ],
)
```

### Composite foreign keys

For a composite primary key whose local columns have the same names, pass the
local names in any order and the engine pairs them by name. When names differ,
map each local column to the primary-key column it references.

```python
customer_accounts = DeltaTable(
    catalog="dev",
    schema="silver",
    name="customer_accounts",
    columns=[
        Column("tenant_id", Long(), nullable=False),
        Column("id", Long(), nullable=False),
    ],
    primary_key=["tenant_id", "id"],
)

order_lines = DeltaTable(
    catalog="dev",
    schema="silver",
    name="order_lines",
    columns=[
        Column("order_line_id", Long(), nullable=False),
        Column("tenant_id", Long(), nullable=False),
        Column("customer_id", Long(), nullable=False),
    ],
    primary_key=["order_line_id"],
    foreign_keys=[
        ForeignKey(
            columns={"tenant_id": "tenant_id", "customer_id": "id"},
            references=customer_accounts,
        ),
    ],
)
```

String, sequence, and mapping input is copied when the `ForeignKey` declaration
is constructed. Identity is case-insensitive, and sequence and mapping order
never matters. When the constraint is attached to its `DeltaTable`, its local
and referenced names use the actual `Column.name` from each table. A composite
sequence is accepted only when its identifier keys match the parent key
exactly; otherwise an explicit mapping is required. A mapping that does not
cover the referenced table's primary key exactly fails when the owning
`DeltaTable` is constructed.

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

The same per-catalog visibility limits the inbound direction: a cross-catalog
foreign key created with raw SQL that references one of this sync's primary
keys is invisible to `PrimaryKeyReferencedByForeignKeys`, so changing that key
fails at execution rather than validation — see
[safe-change rules](reference-safe-change-rules.md).

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

The engine compares foreign keys by definition: local columns, referenced
table, and referenced columns. The physical name is not part of drift; `name`
is a creation preference whose spelling is preserved for SQL and previews.

| Change                                                | Actions emitted                       |
| ----------------------------------------------------- | ------------------------------------- |
| Foreign key added                                     | `SetForeignKey`                       |
| Foreign key removed                                   | `DropForeignKey`                      |
| Foreign key definition changed                        | `DropForeignKey` then `SetForeignKey` |
| Same definition, any requested or observed name       | nothing                               |
| No change                                             | nothing                               |

### Constraints are informational

Like primary keys, Databricks foreign keys are informational, not enforced:
they do not block inserts that violate referential integrity. Current
Databricks versions can target a primary key or a supported unique constraint,
but delta-engine declares and resolves primary keys only. It cannot declare a
`UNIQUE` constraint or register one as a foreign-key target. A constraint that
Databricks rejects for runtime or protocol compatibility surfaces as an
`EXECUTION_FAILED` table with the original error. See
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

### Partitioning is create-only in delta-engine

Delta-engine sets partitioning only when it creates a table. Changing one
partition specification to another requires a data rewrite, which is outside
the engine's DDL-only remit. Databricks SQL and Databricks Runtime 18.1 and
above also provide `REPLACE PARTITIONED BY WITH CLUSTER BY` to convert a
partitioned Delta table to liquid clustering, but delta-engine does not model
that layout-strategy conversion.

Declaring a different `partitioned_by` for an existing table therefore fails
validation before any SQL runs. Rewrite the table or perform a supported
partition-to-clustering conversion out of band, then update the declaration and
re-sync against the resulting layout.

Partition columns also cannot be of complex type (`Array`, `Map`, `Struct`,
`Variant`), and a table cannot be partitioned by every column; both are
rejected when the `DeltaTable` is constructed. See
[safe-change rules](reference-safe-change-rules.md) for the full set of changes
the engine rejects.

## Clustering

Declare Delta liquid clustering keys with the `clustered_by` argument — a
table-level list of column names, the same shape as `partitioned_by`:

```python
from delta_engine.schema import Column, DeltaTable, String

events = DeltaTable(
    catalog="dev",
    schema="silver",
    name="events",
    columns=[
        Column("region", String()),
        Column("event_type", String()),
    ],
    clustered_by=["region"],
)
```

`DeltaTable.clustered_by` exposes the declared tuple of clustering column
names, in declaration order. Key order does not matter — Delta clusters by the
key set — so reordering the keys is never treated as drift.

A table cannot declare both `partitioned_by` and `clustered_by` — Delta
supports one physical layout strategy per table — and a declaration is
limited to four clustering keys. Both are rejected when the `DeltaTable` is
constructed. See
[limitations](reference-limitations.md) for the unsupported key types.

### Clustering is reconciled in place

Unlike partitioning, clustering keys are not fixed at creation. The engine
changes the key set with `ALTER TABLE ... CLUSTER BY (...)` (or `CLUSTER BY
NONE` to remove clustering) whenever the declaration changes — no table
recreation required.

The `ALTER` is a metadata change: it sets the _target_ clustering keys but
does not rewrite existing data, so it stays cheap regardless of table size.
Liquid clustering still lays data out physically — it co-locates rows within
files rather than in partition directories — but existing files keep their
old clustering until a later `OPTIMIZE` (or `OPTIMIZE FULL` to recluster the
whole table) rewrites them. This is why partitioning is blocked while
clustering is not: changing partition columns would mean physically rewriting
every data file up front. See
[safe-change rules](reference-safe-change-rules.md) for the full contrast.

### Drift

The engine compares clustering keys by _set_, not by order: declaring the
same keys in a different order is not drift and plans nothing. This mirrors
primary keys, and is unlike partitioning, where order is a physical layout
decision.
