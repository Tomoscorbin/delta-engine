---
tags:
  - reference
---

# Capabilities and limitations

This page summarises what delta-engine can and cannot manage. Each row links to
the page with the detail.

## Platform

| Requirement | Supported                                                                                                                                                |
| ----------- | -------------------------------------------------------------------------------------------------------------------------------------------------------- |
| Backend     | Delta Lake tables on Databricks with Unity Catalog — the only adapter today; the core is designed for more ([architecture](explanation-architecture.md)) |
| Python      | 3.12 or later                                                                                                                                            |
| PySpark     | Only needed to run syncs; declaring and planning are pure Python ([installation](installation.md))                                                       |

## What a sync manages

| Aspect                    |   Managed   | Notes                                                                                                                                                                     |
| ------------------------- | :---------: | ------------------------------------------------------------------------------------------------------------------------------------------------------------------------- |
| Create table              |      ✓      | Missing tables are created from the declaration                                                                                                                           |
| Add column                |      ✓      | Must be nullable on an existing table ([rules](reference-safe-change-rules.md))                                                                                           |
| Drop column               |      ✓      | Requires `delta.columnMapping.mode='name'` declared ([properties](how-to-configure-table.md#properties))                                                                  |
| Loosen nullability        |      ✓      | `NOT NULL` → nullable is applied                                                                                                                                          |
| Tighten nullability       |      ✗      | Blocked — backfill first, then tighten ([rules](reference-safe-change-rules.md))                                                                                          |
| Change column type        |      ✗      | Blocked — recreate the table out of band ([rules](reference-safe-change-rules.md))                                                                                        |
| Rename column             |      ✗      | Not detected: a rename in the declaration is planned as a drop plus an add of an empty column                                                                             |
| Table and column comments |      ✓      | Always managed; an empty declaration clears the comment ([comments](how-to-configure-table.md#comments))                                                                  |
| Table properties          |      ✓      | Five managed `delta.*` keys; other keys are rejected at declaration ([properties](how-to-configure-table.md#properties))                                                  |
| Table and column tags     |      ✓      | Full-state: undeclared tags are removed ([tags](how-to-configure-table.md#tags))                                                                                          |
| Primary keys              |      ✓      | Declared per column ([primary keys](how-to-configure-table.md#primary-keys))                                                                                              |
| Foreign keys              |      ✓      | Must target the referenced table's primary key; orders the sync; names are engine-generated and cannot be chosen ([foreign keys](how-to-configure-table.md#foreign-keys)) |
| Partitioning              | Create only | Fixed after creation; changes are blocked ([rules](reference-safe-change-rules.md))                                                                                       |
| Metadata-only scope       |      ✓      | `scope="metadata"` restricts a sync to comments, tags, and keys ([guide](how-to-deploy-metadata-only.md))                                                                 |
| Tag-only scope            |      ✓      | `scope="tags"` restricts a sync to table and column tags ([tags](how-to-configure-table.md#manage-tags-only))                                                             |
| Dry run                   |      ✓      | Full plan and validation, zero mutations ([guide](how-to-preview-changes.md))                                                                                             |

## Outside the model

These features are not modeled at all: the engine never reads, creates,
changes, or drops them, and they produce no drift.

| Not modeled                                                       | Meaning                                                                                                                                                                             |
| ----------------------------------------------------------------- | ----------------------------------------------------------------------------------------------------------------------------------------------------------------------------------- |
| CHECK constraints                                                 | Existing ones are left untouched and cannot be declared                                                                                                                             |
| Key constraint options (`RELY`, `MATCH`, `ON UPDATE`/`ON DELETE`) | Keys are created with Databricks defaults (`NOT ENFORCED NORELY`); option drift is invisible, and an out-of-band `RELY` is lost when a primary-key change drops and re-adds the key |
| Identity and generated columns                                    | A column's generation expression is invisible to the engine                                                                                                                         |
| Liquid clustering                                                 | Only Hive-style `PARTITIONED BY` is modeled                                                                                                                                         |
| Views and materialized views                                      | Only Delta tables are managed                                                                                                                                                       |
| Grants, row filters, column masks                                 | Governance beyond comments and tags is out of scope                                                                                                                                 |
| Data                                                              | The engine runs DDL only; it never reads, writes, or backfills rows                                                                                                                 |

## Type support

The full matrix is in [data types](reference-data-types.md). The limitations
in brief:

| Limitation               | Behaviour                                                                                         |
| ------------------------ | ------------------------------------------------------------------------------------------------- |
| Unsupported Spark types  | Columns of e.g. `VOID`, `INTERVAL`, or geospatial types are left unmanaged, with a logged warning |
| `CHAR(n)` / `VARCHAR(n)` | Treated as `String`; the length bound is not modeled and never altered                            |
| Struct fields            | Structs change as a whole: any field change is a blocked column type change                       |
| `Decimal` precision      | Maximum 38, enforced at declaration                                                               |

## Runtime features

delta-engine does not preflight Databricks Runtime or Delta protocol versions.
Declaring a feature the workspace or table protocol does not support — key
constraints, tags, change data feed — fails at execution with the original
Databricks error. See
[runtime and Delta feature compatibility](how-to-handle-sync-failures.md#runtime-and-delta-feature-compatibility).
