---
tags:
  - how-to
---

# How to configure table properties

`DeltaTable` accepts a `properties` dict that maps `Property` enum members to string values. The engine reconciles only the keys you declare; properties set out-of-band are left untouched.

## Default properties

Every `DeltaTable` applies one default unless you override it:

| Property | Default value |
|---|---|
| `Property.COLUMN_MAPPING_MODE` | `"name"` |

`COLUMN_MAPPING_MODE=name` is required for column drops to work. Override it to `none` only if you never drop columns.

Deletion vectors are intentionally **not** defaulted here — current Databricks runtimes enable them automatically. The engine reconciles only the properties you declare, so leaving `ENABLE_DELETION_VECTORS` undeclared lets the runtime own it. Declare it explicitly if you need to pin a specific value.

## Override a default

```python
from delta_engine import Column, DeltaTable, Property, String

table = DeltaTable(
    catalog="dev",
    schema="silver",
    name="events",
    columns=[Column("id", String())],
    properties={
        Property.COLUMN_MAPPING_MODE: "none",
    },
)
```

## Set additional properties

```python
from delta_engine import Column, DeltaTable, Property, String

table = DeltaTable(
    catalog="dev",
    schema="silver",
    name="events",
    columns=[Column("id", String())],
    properties={
        Property.CHANGE_DATA_FEED: "true",
        Property.DELETED_FILE_RETENTION_DURATION: "interval 30 days",
        Property.LOG_RETENTION_DURATION: "interval 30 days",
    },
)
```

## Available properties

| `Property` member | Delta table property key |
|---|---|
| `ENABLE_DELETION_VECTORS` | `delta.enableDeletionVectors` |
| `COLUMN_MAPPING_MODE` | `delta.columnMapping.mode` |
| `CHANGE_DATA_FEED` | `delta.enableChangeDataFeed` |
| `DELETED_FILE_RETENTION_DURATION` | `delta.deletedFileRetentionDuration` |
| `LOG_RETENTION_DURATION` | `delta.logRetentionDuration` |
| `DATA_SKIPPING_NUM_INDEXED_COLS` | `delta.dataSkippingNumIndexedCols` |

Passing a key not in this enum raises `ValueError` at `DeltaTable` construction. This prevents typos from silently doing nothing.
