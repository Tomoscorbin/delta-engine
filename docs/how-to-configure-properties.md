---
tags:
  - how-to
---

# How to configure table properties

The engine manages properties by **exact declaration**: your declaration is
the complete list of the properties you manage on that table.

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

## The managed keys

| `Property` member | Delta table property key | Restrictions |
|---|---|---|
| `CHANGE_DATA_FEED` | `delta.enableChangeDataFeed` | none |
| `DELETED_FILE_RETENTION_DURATION` | `delta.deletedFileRetentionDuration` | none |
| `LOG_RETENTION_DURATION` | `delta.logRetentionDuration` | none |
| `DATA_SKIPPING_NUM_INDEXED_COLS` | `delta.dataSkippingNumIndexedCols` | none |
| `COLUMN_MAPPING_MODE` | `delta.columnMapping.mode` | only `none → name`; cannot be removed |

Passing a key outside this set raises `ValueError` at `DeltaTable`
construction (for `None` assertions too). This prevents typos from silently
doing nothing.

Deletion vectors (`delta.enableDeletionVectors`) are deliberately **not**
managed: Databricks enables them automatically on new tables, so the engine
leaves that key entirely to the platform.

## Declaring and removing properties

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

## Column mapping and dropping columns

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

## Migrating from older engine versions

Engine versions before this one injected `delta.columnMapping.mode='name'`
into every fully-managed table. Those tables carry the key in the catalog,
so their first sync after upgrading fails with `PropertyMustBeDeclared`.
The fix is one line per declaration:

```python
properties={Property.COLUMN_MAPPING_MODE: "name"}
```

Tables synced only with `metadata_only=True` are unaffected (their
properties are never compared). Old deletion-vectors residue from the
pre-2026-07 default is also unaffected — that key is no longer managed.

## When something else writes a managed key

Two platform mechanisms can write managed keys without your action:
Databricks' Automatic Upgrades service (writes properties onto enrolled
Unity Catalog managed tables) and admin session defaults
(`spark.databricks.delta.properties.defaults.*`, stamped onto new tables at
creation). If either writes a managed key onto your table, the next sync
fails loud with `PropertyMustBeDeclared` naming the key — add the line and
carry on. The engine never reacts silently to keys it did not set.

## Adding keys to the managed set

Growing the managed set is a breaking change: tables carrying the new key
undeclared start failing loud on upgrade. Before a key is added, a fresh
table is created on a current Databricks Runtime and its `DESCRIBE DETAIL`
properties inspected — platform-auto-written keys are not added. Additions
are called out in release notes.
