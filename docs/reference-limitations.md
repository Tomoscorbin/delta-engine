---
tags:
  - reference
---

# Capabilities and limitations

This page summarises what delta-engine can and cannot manage. Each row links to
the page with the detail.

## Platform

| Requirement           | Supported                                                                                                                                                                                                                                                                                                                                                              |
| --------------------- | ---------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------- |
| Backend               | Delta Lake tables on Databricks with Unity Catalog — the supported target today; the reader reads managed and external Delta tables, plus Delta streaming tables for tag-only management, and any other relation a registered name resolves to (view, materialized view, foreign table, non-Delta format) fails its read ([architecture](explanation-architecture.md)) |
| Python                | 3.12 or later                                                                                                                                                                                                                                                                                                                                                          |
| PySpark               | Needed only for the Spark backend; the SQL warehouse backend needs none. Declaring and planning are pure Python either way ([installation](installation.md))                                                                                                                                                                                                           |
| Reads (both backends) | Unity Catalog only — every read is one `DESCRIBE TABLE EXTENDED … AS JSON` plus `information_schema` for tags, primary and foreign keys, and inbound foreign keys; a `hive_metastore` or other non-UC table is not readable and surfaces as a read failure on both the Spark and SQL warehouse backends ([installation](installation.md))                              |

## Identifier handling

Declared identifiers must currently satisfy `name == name.casefold()`; they are
rejected rather than normalized, while reader adapters casefold observed names.
That is stricter than ordinary lowercase for some Unicode text. The engine also
does not fully prevalidate Unity Catalog's length and character rules for
catalog, schema, and table names, so quoting an accepted declaration does not
guarantee that Databricks will accept it. Prefer simple lowercase object names
until the identifier policy is aligned; column names that require special
characters still need column mapping as described in [column mapping and
dropping columns](how-to-configure-table.md#column-mapping-and-dropping-columns).

## What a sync manages

| Aspect                    |    Managed    | Notes                                                                                                                                                                                                                     |
| ------------------------- | :-----------: | ------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------- |
| Create table              |       ✓       | Missing tables are created from the declaration                                                                                                                                                                           |
| Add column                |       ✓       | Must be nullable on an existing table ([rules](reference-safe-change-rules.md))                                                                                                                                           |
| Drop column               |       ✓       | Requires `delta.columnMapping.mode='name'` declared ([properties](how-to-configure-table.md#properties))                                                                                                                  |
| Loosen nullability        |       ✓       | `NOT NULL` → nullable is applied                                                                                                                                                                                          |
| Tighten nullability       |       ✗       | Blocked — backfill first, then tighten ([rules](reference-safe-change-rules.md))                                                                                                                                          |
| Change column type        | Widening only | Safe widenings (e.g. `Integer` → `Long`) apply in place with `delta.enableTypeWidening='true'` declared; anything else is blocked ([type widening](how-to-configure-table.md#type-widening))                              |
| Rename column             |       ✓       | Declare `renamed_from` on the new column; requires `delta.columnMapping.mode='name'`. Editing a name directly (without the hint) is a drop plus an add ([renaming a column](how-to-configure-table.md#renaming-a-column)) |
| Rename partition column   |       ✓       | Column mapping preserves the partition column's physical identity, so its layout metadata follows the rename ([rules](reference-safe-change-rules.md))                                                                    |
| Table and column comments |       ✓       | Always managed; an empty declaration clears the comment ([comments](how-to-configure-table.md#comments))                                                                                                                  |
| Table properties          |       ✓       | Six managed `delta.*` keys; other keys are rejected at declaration ([properties](how-to-configure-table.md#properties))                                                                                                   |
| Table and column tags     |       ✓       | Full-state: undeclared tags are removed ([tags](how-to-configure-table.md#tags))                                                                                                                                          |
| Primary keys              |       ✓       | Declared at table level ([primary keys](how-to-configure-table.md#primary-keys))                                                                                                                                          |
| Foreign keys              |       ✓       | Must target the referenced table's primary key; orders the sync; names are engine-generated and cannot be chosen ([foreign keys](how-to-configure-table.md#foreign-keys))                                                 |
| Partitioning              |  Create only  | Fixed after creation; changes are blocked ([rules](reference-safe-change-rules.md))                                                                                                                                       |
| Clustering                |       ✓       | Liquid clustering keys are reconciled in place, unlike partitioning ([clustering](how-to-configure-table.md#clustering))                                                                                                  |
| Metadata-only scope       |       ✓       | `scope="metadata"` restricts a sync to comments, tags, and keys ([guide](how-to-deploy-metadata-only.md))                                                                                                                 |
| Tag-only scope            |       ✓       | `scope="tags"` restricts a sync to table and column tags ([tags](how-to-configure-table.md#manage-tags-only))                                                                                                             |
| Streaming tables          |   Tags only   | Discovered at read time; only `scope="tags"` declarations may target one — schema, comments, and properties belong to the owning pipeline ([guide](how-to-deploy-metadata-only.md#tag-a-streaming-table))                 |
| Dry run                   |       ✓       | Full plan and validation, zero mutations ([guide](how-to-preview-changes.md))                                                                                                                                             |

## Outside the model

These features are not modeled at all: the engine never reads, creates,
changes, or drops them, and they produce no drift.

| Not modeled                                                       | Meaning                                                                                                                                                                                        |
| ----------------------------------------------------------------- | ---------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------- |
| CHECK constraints                                                 | Cannot be declared; a constraint that references a renamed column must be changed before the rename                                                                                            |
| Key constraint options (`RELY`, `MATCH`, `ON UPDATE`/`ON DELETE`) | Keys are created with Databricks defaults (`NOT ENFORCED NORELY`); option drift is invisible, and an out-of-band `RELY` is lost when a primary-key change drops and re-adds the key            |
| `UNIQUE` constraints                                              | Cannot be declared or used as a registered foreign-key target, even on Databricks versions that support them                                                                                   |
| Identity and generated columns                                    | Generation expressions are invisible; one that references a renamed column must be changed before the rename                                                                                   |
| Views and materialized views                                      | Unsupported; a registered name that resolves to a view or materialized view fails its read rather than being planned against (streaming tables, by contrast, are read for tag-only management) |
| External table creation                                           | Existing external Delta tables are read and reconciled like managed ones, but the engine creates managed tables only: a location cannot be declared, and an absent table is created managed    |
| Grants, row filters, column masks                                 | Governance beyond comments and tags is out of scope                                                                                                                                            |
| Data                                                              | The engine runs DDL only; it never reads, writes, or backfills rows                                                                                                                            |

## Type support

The full matrix is in [data types](reference-data-types.md). The limitations
in brief:

| Limitation               | Behaviour                                                                                                                                                     |
| ------------------------ | ------------------------------------------------------------------------------------------------------------------------------------------------------------- |
| Unsupported Spark types  | Non-partition columns are left unmanaged with a warning, so their drift is invisible; an unsupported partition type or wholly unmappable table fails its read |
| `CHAR(n)` / `VARCHAR(n)` | Treated as `String`; the length bound is not modeled and never altered                                                                                        |
| Struct fields            | Structs change as a whole: any field change is a blocked column type change                                                                                   |
| `Decimal` precision      | Maximum 38, enforced at declaration                                                                                                                           |

## Clustering limits

Liquid clustering ([clustering](how-to-configure-table.md#clustering)) has
its own set of declaration-time and execution-time limits, distinct from
partitioning:

| Limitation               | Behaviour                                                                                                                                                      |
| ------------------------ | -------------------------------------------------------------------------------------------------------------------------------------------------------------- |
| Key count                | At most four `clustered_by` keys per table, rejected at declaration                                                                                            |
| Mutual exclusivity       | A table cannot declare both `partitioned_by` and `clustered_by`, rejected at declaration                                                                       |
| Unsupported key types    | `Array`, `Map`, `Struct`, and `Variant` columns cannot be clustering keys, rejected at declaration                                                             |
| Nested struct-field keys | Clustering by a field inside a `Struct` column is not supported by the declaration — only top-level columns can be named in `clustered_by`                     |
| Stats and column order   | Databricks only collects the file statistics clustering relies on for a table's first 32 columns; a clustering key outside that range gets no skipping benefit |
| Runtime compatibility    | Liquid clustering requires Databricks Runtime 13.3 LTS or later; delta-engine does not preflight this — see [runtime features](#runtime-features)              |

## Concurrent catalog changes

A sync is not transactional across its read, plan, and execute phases. Table
creation compiles as a plain `CREATE TABLE`: if another writer creates the same
name after the reader observed it missing, that statement errors and the table
is reported as an execution failure rather than a false success. The next sync
reads the table that actually exists and reports any resulting drift. Avoid
concurrent creators for the same qualified table name.

## Runtime features

delta-engine does not preflight Databricks Runtime or Delta protocol versions.
Declaring a feature the workspace or table protocol does not support — key
constraints, tags, change data feed — fails at execution with the original
Databricks error. See
[runtime and Delta feature compatibility](how-to-handle-sync-failures.md#runtime-and-delta-feature-compatibility).

Reading a table relies on `DESCRIBE TABLE EXTENDED … AS JSON`, which needs
Databricks Runtime 16.2 or later, or any SQL warehouse. Primary and foreign keys
are then read from `information_schema`; because key constraints are available
from Databricks Runtime 13.3 (GA 15.2) — below the AS JSON read floor — any
runtime new enough to read a table can also observe its keys. As with every
other runtime feature, delta-engine documents this floor rather than
preflighting it — an unsupported runtime surfaces as a read failure.
