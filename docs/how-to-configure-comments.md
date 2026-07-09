---
tags:
  - how-to
---

# How to configure comments

Comments document a table and its columns in the catalog, where they show up
in the Unity Catalog UI and `DESCRIBE` output. The declaration is the source
of truth: whatever comment it states (including no comment) is what the table
gets.

## Declare comments

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

## Removing a comment

Comments follow the declaration exactly, in both directions. A column declared
without a comment (the default is the empty string) asserts that the column
has no comment — so removing a comment from the declaration clears it on the
table at the next sync, and a comment added to the table outside the
declaration is drift that the sync overwrites.

## Comments in metadata-only syncs

Comments are catalog metadata, so a
[metadata-only declaration](how-to-deploy-metadata-only.md) manages them
exactly as a fully managed one does. This makes `metadata_only=True` the way
to roll out documentation across tables whose schemas are owned elsewhere.
