---
tags:
  - how-to
---

# How to configure tags and comments

Tags and comments are the two pieces of pure catalog metadata delta-engine
manages. Neither changes table data or Delta behaviour, both live in the
catalog, and for both the declaration is the complete source of truth. Because
they are metadata only, they are exactly what a
[metadata-only sync](how-to-deploy-metadata-only.md) manages — use that mode to
roll out governance and documentation across tables whose schemas are owned
elsewhere.

To change table behaviour instead (deletion vectors, change data feed,
retention), use [table properties](how-to-configure-properties.md), which are
`TBLPROPERTIES` with different, exact-declaration semantics.

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
