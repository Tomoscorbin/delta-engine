---
tags:
  - how-to
---

# How to declare partitioned tables

Pass `partitioned_by` to `DeltaTable` to set partition columns at creation time.

## Declare a partitioned table

```python
from delta_engine import Column, Date, DeltaTable, String

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

## Constraints

- Every name in `partitioned_by` must appear in `columns`. `DeltaTable` raises `ValueError` at construction otherwise.
- All names must be lowercase. `DeltaTable` enforces lowercase on all column names.
- Partition columns must be listed as regular columns too — `partitioned_by` names them; `columns` defines them.

## Partitioning is fixed at creation

Partitioning cannot be changed once a table exists. Declaring a different `partitioned_by` on an existing table raises `SyncFailedError` at validation — no SQL runs. To change partitioning, drop and recreate the table out of band, then re-sync.

See [reference-safe-change-rules.md](reference-safe-change-rules.md) for the full list of changes the engine rejects at validation.
