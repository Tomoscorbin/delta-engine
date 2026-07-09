# Databricks notebook source
# MAGIC %md
# MAGIC # delta-engine walkthrough
# MAGIC
# MAGIC This notebook is both a worked example of delta-engine and a manual
# MAGIC integration test. It walks the full lifecycle — **define → sync → change →
# MAGIC resync** — and proves every sync by inspecting the live Unity Catalog and
# MAGIC asserting the outcome. The `assert` statements are the test suite; this
# MAGIC cluster is the test runner.
# MAGIC
# MAGIC **Requirements:** Databricks Runtime 13.3 LTS or later, Unity Catalog
# MAGIC enabled (for primary keys, foreign keys, and tags), and the `APPLY TAG`
# MAGIC privilege on the target schema. Set the `catalog` and `schema` widgets to a
# MAGIC sandbox you can create and drop tables in.

# COMMAND ----------

# MAGIC %md
# MAGIC ## 1. Setup

# COMMAND ----------
import sys

import pyspark.sql.types as T

from delta_engine import SyncFailedError, SyncReport, TableRunStatus, render_diff, render_report
from delta_engine.databricks import build_engine, configure_logging
from delta_engine.schema import (
    Column,
    Date,
    Decimal,
    DeltaTable,
    Double,
    ForeignKey,
    Long,
    Property,
    String,
    Struct,
    StructField,
    Timestamp,
)

# COMMAND ----------

dbutils.widgets.text("catalog", "main")
dbutils.widgets.text("schema", "delta_engine_demo")

CATALOG = dbutils.widgets.get("catalog")
SCHEMA = dbutils.widgets.get("schema")

DEMO_TABLES = ("customers", "orders", "events", "line_items")

print(f"Target: {CATALOG}.{SCHEMA}")

# COMMAND ----------

# Route logs to stdout so they share one ordered stream with print() — otherwise
# stdout and stderr flush independently and log lines surface after later prints.
configure_logging(stream=sys.stdout)

# Clean slate: drop every table this notebook may create.
for _table in DEMO_TABLES:
    spark.sql(f"DROP TABLE IF EXISTS {CATALOG}.{SCHEMA}.{_table}")

# COMMAND ----------

# MAGIC %md
# MAGIC ### Verification helpers
# MAGIC
# MAGIC `CatalogInspector` reads live table state back from Unity Catalog using the
# MAGIC same surfaces the engine's own reader uses, so assertions reflect real
# MAGIC catalog state. It lives in `catalog_inspector.py` beside this notebook.

# COMMAND ----------

from catalog_inspector import CatalogInspector

inspector = CatalogInspector(CATALOG, SCHEMA)

# COMMAND ----------

# MAGIC %md
# MAGIC Every sync below prints its report the same way, so a small helper keeps the
# MAGIC output consistent and the sync cells focused on the change under test.


# COMMAND ----------
def show_report(report: SyncReport) -> None:
    """Print a sync report and its diff under labelled section headers."""
    print(render_report(report))
    print()
    print(render_diff(report))


# COMMAND ----------

# MAGIC %md
# MAGIC ## 2. Define and first sync
# MAGIC
# MAGIC **Goal**
# MAGIC
# MAGIC Define two tables — `customers` and `orders` — and sync them. `orders` has a
# MAGIC foreign key to `customers` and is declared **first** in the sync call, on
# MAGIC purpose. The `customers` baseline is deliberately plain (no tags, no extra
# MAGIC properties, no foreign key) so later steps can add them.
# MAGIC
# MAGIC **Outcome**
# MAGIC
# MAGIC The engine reorders by dependency, so `customers` is created before `orders`
# MAGIC adds its foreign key. Both tables land with the expected columns,
# MAGIC partitioning, and constraints.

# COMMAND ----------

customers = DeltaTable(
    catalog=CATALOG,
    schema=SCHEMA,
    name="customers",
    columns=[
        Column("id", Long(), nullable=False),
        Column("name", String()),
        Column("legacy_code", String(), comment="Retired identifier, dropped in step 4a"),
        Column("status", String(), nullable=False),
    ],
    primary_key=["id"],
    comment="Customer master table",
)

orders = DeltaTable(
    catalog=CATALOG,
    schema=SCHEMA,
    name="orders",
    columns=[
        Column("order_id", Long(), nullable=False),
        Column("customer_id", Long(), nullable=False),
        Column("order_date", Date()),
    ],
    primary_key=["order_id"],
    partitioned_by=["order_date"],
    foreign_keys=[
        ForeignKey(
            local_columns=("customer_id",),
            references=customers,
        )
    ],
)

tables = [orders, customers]  # passed orders-first on purpose

engine = build_engine(spark)
report = engine.sync(*tables)
show_report(report)

# COMMAND ----------

# MAGIC %md
# MAGIC **Verify** both tables landed with the expected shape, partitioning, and
# MAGIC constraints.

# COMMAND ----------

assert report.any_failures is False, "first sync should fully succeed"

assert spark.catalog.tableExists(inspector.fqname("customers"))
assert spark.catalog.tableExists(inspector.fqname("orders"))

customer_fields = inspector.fields_of("customers")
assert set(customer_fields) == {"id", "name", "legacy_code", "status"}
assert customer_fields["id"].nullable is False
assert customer_fields["status"].nullable is False
assert inspector.has_primary_key("customers")

assert inspector.partitions_of("orders") == ("order_date",)
assert inspector.has_primary_key("orders")
assert inspector.has_foreign_key("orders")

print("Step 2 verified: both tables created with expected schema and constraints.")
inspector.display_schema("customers")
inspector.display_schema("orders")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 3. Idempotent resync
# MAGIC
# MAGIC **Goal**
# MAGIC
# MAGIC Snapshot the live `customers` schema, sync the exact same tables a second
# MAGIC time with the catalog already matching the declaration, then compare.
# MAGIC
# MAGIC **Outcome**
# MAGIC
# MAGIC A true no-op: the engine plans and executes nothing (every table's
# MAGIC `execution` is `None`), and the live catalog schema is byte-for-byte
# MAGIC identical before and after.

# COMMAND ----------

schema_before = spark.table(inspector.fqname("customers")).schema

report = engine.sync(*tables)
show_report(report)

# COMMAND ----------

# MAGIC %md
# MAGIC **Verify** the engine ran nothing (`execution is None`) *and* the live
# MAGIC catalog is unchanged — the resync neither planned nor mutated anything.

# COMMAND ----------

assert report.any_failures is False
assert all(table_report.execution is None for table_report in report), (
    "a matching resync must execute nothing"
)

# The engine planned nothing; confirm the live catalog is genuinely untouched too.
assert spark.table(inspector.fqname("customers")).schema == schema_before
assert inspector.has_primary_key("customers")
assert inspector.partitions_of("orders") == ("order_date",)
assert inspector.has_foreign_key("orders")

print("Step 3 verified: resync was a true no-op; live catalog unchanged.")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 4. Safe schema evolution & drift management
# MAGIC
# MAGIC **Goal**
# MAGIC
# MAGIC Evolve the live `customers` table through a series of changes — adding and
# MAGIC dropping columns, loosening nullability, setting and unsetting tags, and
# MAGIC demonstrating property visibility — batched where practical.
# MAGIC
# MAGIC **Outcome**
# MAGIC
# MAGIC Each step re-declares the **full** desired `customers` state and the engine
# MAGIC reconciles the live table to match.

# COMMAND ----------

# MAGIC %md
# MAGIC ### 4a. Batched heterogeneous change
# MAGIC
# MAGIC **Goal**
# MAGIC
# MAGIC Add `email` (with a `pii` column tag), drop `legacy_code`, loosen `status`
# MAGIC to nullable, set two table tags, set two properties, and change the table
# MAGIC and a column comment — all declared at once.
# MAGIC
# MAGIC Dropping a column requires `delta.columnMapping.mode = name`. The engine
# MAGIC enforces this at validation: declare the property on any table whose columns
# MAGIC may be dropped.
# MAGIC
# MAGIC **Outcome**
# MAGIC
# MAGIC The engine plans every independent change together and applies them in a
# MAGIC single sync.

# COMMAND ----------

customers = DeltaTable(
    catalog=CATALOG,
    schema=SCHEMA,
    name="customers",
    columns=[
        Column("id", Long(), nullable=False),
        Column("name", String(), comment="Full legal name"),  # <-- added column comment
        Column(
            "email", String(), tags={"pii": "true"}
        ),  # <-- added with column tag (legacy_code dropped by omission)
        Column("status", String()),  # <-- loosened from NOT NULL to nullable
    ],
    primary_key=["id"],
    comment="Customer master table (with contact details)",  # <-- added table comment
    tags={"domain": "sales", "owner": "data-eng"},  # <-- set 2 table tags
    properties={
        Property.COLUMN_MAPPING_MODE: "name",  # <-- required to drop a column
        Property.CHANGE_DATA_FEED: "true",  # <-- set a property
    },
)

report = engine.sync(customers, orders)
show_report(report)

# COMMAND ----------

# MAGIC %md
# MAGIC **Verify** the batched change: new column present with its column tag, old
# MAGIC column gone, `status` now nullable, table tags and properties set, comments
# MAGIC updated.

# COMMAND ----------

assert report.any_failures is False

customer_fields = inspector.fields_of("customers")
assert "email" in customer_fields and customer_fields["email"].nullable is True
assert "legacy_code" not in customer_fields
assert customer_fields["status"].nullable is True
assert inspector.column_tags_of("customers")[("email", "pii")] == "true"
assert inspector.tags_of("customers") == {"domain": "sales", "owner": "data-eng"}
assert inspector.properties_of("customers").get(Property.COLUMN_MAPPING_MODE) == "name"
assert inspector.properties_of("customers").get(Property.CHANGE_DATA_FEED) == "true"
assert inspector.table_comment("customers") == "Customer master table (with contact details)"
assert inspector.column_comment("customers", "name") == "Full legal name"
print("Step 4a verified: batched add/drop/nullability/tags/properties/comments applied together.")
inspector.display_schema("customers")
inspector.display_tags("customers")
inspector.display_properties("customers")

# COMMAND ----------

# MAGIC %md
# MAGIC ### 4b. Prove tag unset (table and column)
# MAGIC
# MAGIC **Goal**
# MAGIC
# MAGIC Drop the `owner` table tag and the `pii` column tag from the declaration
# MAGIC and resync.
# MAGIC
# MAGIC **Outcome**
# MAGIC
# MAGIC Tags are **full-state** at both levels: the declaration is the complete set,
# MAGIC so any tag you stop declaring is removed. Only `domain` remains on the table;
# MAGIC `email` loses its `pii` tag. Properties behave the opposite way — step 4c
# MAGIC shows platform-managed properties are invisible to the engine.

# COMMAND ----------

customers = DeltaTable(
    catalog=CATALOG,
    schema=SCHEMA,
    name="customers",
    columns=[
        Column("id", Long(), nullable=False),
        Column("name", String(), comment="Full legal name"),
        Column("email", String()),  # <-- pii column tag dropped
        Column("status", String()),
    ],
    primary_key=["id"],
    comment="Customer master table (with contact details)",
    tags={"domain": "sales"},  # <-- "owner" table tag dropped
    properties={
        Property.COLUMN_MAPPING_MODE: "name",
        Property.CHANGE_DATA_FEED: "true",
    },
)

report = engine.sync(customers, orders)
show_report(report)

# COMMAND ----------

# MAGIC %md
# MAGIC **Verify** the `owner` table tag and `pii` column tag are both gone.

# COMMAND ----------

assert report.any_failures is False
assert inspector.tags_of("customers") == {"domain": "sales"}
assert ("email", "pii") not in inspector.column_tags_of("customers")
print("Step 4b verified: undeclared table and column tags removed by full-state reconciliation.")
inspector.display_tags("customers")

# COMMAND ----------

# MAGIC %md
# MAGIC ### 4c. Platform-managed properties are invisible to the engine
# MAGIC
# MAGIC **Goal**
# MAGIC
# MAGIC Read a property Databricks writes autonomously — `delta.enableRowTracking` —
# MAGIC then resync `customers` without declaring it.
# MAGIC
# MAGIC **Outcome**
# MAGIC
# MAGIC The property is untouched. Unlike tags (step 4c), the engine does not treat
# MAGIC `properties` as the complete set: it manages only the keys in its registry
# MAGIC and leaves everything else invisible. This matters because Databricks writes
# MAGIC many `delta.*` properties autonomously — a sync must never strip them.

# COMMAND ----------

# Read the platform-written value before the sync.
platform_props = inspector.properties_of("customers")
row_tracking = platform_props.get("delta.enableRowTracking")
assert row_tracking is not None, "Databricks should have written delta.enableRowTracking"
print(f"Platform-managed delta.enableRowTracking = {row_tracking!r} (not declared by us)")

# Re-sync with the same declaration — delta.enableRowTracking is intentionally absent.
customers = DeltaTable(
    catalog=CATALOG,
    schema=SCHEMA,
    name="customers",
    columns=[
        Column("id", Long(), nullable=False),
        Column("name", String(), comment="Full legal name"),
        Column("email", String()),
        Column("status", String()),
    ],
    primary_key=["id"],
    comment="Customer master table (with contact details)",
    tags={"domain": "sales"},
    properties={
        Property.COLUMN_MAPPING_MODE: "name",
        Property.CHANGE_DATA_FEED: "true",
    },
)

report = engine.sync(customers, orders)
show_report(report)

# COMMAND ----------

# MAGIC %md
# MAGIC **Verify** the platform-managed property survived the sync unchanged.

# COMMAND ----------

assert report.any_failures is False
assert inspector.properties_of("customers").get("delta.enableRowTracking") == row_tracking
print("Step 4c verified: platform-managed property left untouched by sync.")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 5. Validation blocks unsafe structural changes
# MAGIC
# MAGIC **Goal**
# MAGIC
# MAGIC Attempt four changes that would silently break data or require a rewrite,
# MAGIC against a dedicated, foreign-key-free `events` table. The foreign-key-free
# MAGIC table keeps each failure a clean `VALIDATION_FAILED` (a foreign-key failure
# MAGIC would otherwise take precedence and mask it).
# MAGIC
# MAGIC **Outcome**
# MAGIC
# MAGIC The engine refuses each change. Validation runs *before* any SQL executes, so
# MAGIC the live table is left untouched every time. Each failed report is printed —
# MAGIC read the `STATUS` column and summary footer to see that nothing was applied.

# COMMAND ----------


def define_events(*, columns, partitioned_by=("event_date",), primary_key=("event_id",)):
    """Build the events table from a column list (baseline plus one variation)."""
    return DeltaTable(
        catalog=CATALOG,
        schema=SCHEMA,
        name="events",
        columns=columns,
        partitioned_by=partitioned_by,
        primary_key=primary_key,
        comment="Raw events",
    )


def sync_expecting_failure(*tables: DeltaTable) -> SyncReport:
    """Sync tables that must fail, print the failed report, and return it."""
    try:
        engine.sync(*tables)
    except SyncFailedError as error:
        show_report(error.report)
        return error.report
    raise AssertionError("expected the sync to fail")


events_baseline_columns = [
    Column("event_id", Long(), nullable=False),
    Column("event_date", Date()),
    Column("amount", Decimal(12, 2)),
    Column("created_at", Timestamp()),
]

report = engine.sync(define_events(columns=events_baseline_columns))
show_report(report)

assert report.any_failures is False
assert inspector.partitions_of("events") == ("event_date",)
print("events baseline created.")

# COMMAND ----------

# MAGIC %md
# MAGIC ### 5a. Adding a NOT NULL column
# MAGIC
# MAGIC **Goal**
# MAGIC
# MAGIC Try to add a new `NOT NULL` column to a table that already has rows.
# MAGIC
# MAGIC **Outcome**
# MAGIC
# MAGIC Blocked: existing rows would have no value for it. The safe path is to add it
# MAGIC as nullable, backfill, then tighten outside the engine.

# COMMAND ----------

# <-- new NOT NULL column added to a table that already has rows
new_column = Column("region", String(), nullable=False)

report = sync_expecting_failure(define_events(columns=[*events_baseline_columns, new_column]))

# The report shows the change was planned but blocked at validation — nothing ran.
[events_report] = report.table_reports
assert events_report.status is TableRunStatus.VALIDATION_FAILED
assert "region" not in inspector.fields_of("events")
print("Step 5a verified: NOT NULL add blocked, table untouched.")

# COMMAND ----------

# MAGIC %md
# MAGIC ### 5b. Tightening a column to NOT NULL
# MAGIC
# MAGIC **Goal**
# MAGIC
# MAGIC Try to tighten an existing nullable column (`event_date`) to `NOT NULL`.
# MAGIC
# MAGIC **Outcome**
# MAGIC
# MAGIC Blocked: existing nulls would violate the new constraint. Backfill and tighten
# MAGIC out of band instead.

# COMMAND ----------

report = sync_expecting_failure(
    define_events(
        columns=[
            Column("event_id", Long(), nullable=False),
            Column("event_date", Date(), nullable=False),  # <-- tightened to NOT NULL
            Column("amount", Decimal(12, 2)),
            Column("created_at", Timestamp()),
        ]
    )
)

[events_report] = report.table_reports
assert events_report.status is TableRunStatus.VALIDATION_FAILED
assert inspector.fields_of("events")["event_date"].nullable is True
print("Step 5b verified: tightening blocked, event_date still nullable.")

# COMMAND ----------

# MAGIC %md
# MAGIC ### 5c. Changing a column's data type
# MAGIC
# MAGIC **Goal**
# MAGIC
# MAGIC Try to change `amount` from `Decimal(12, 2)` to `Double`.
# MAGIC
# MAGIC **Outcome**
# MAGIC
# MAGIC Blocked: type changes can corrupt or fail to cast existing data. Recreate the
# MAGIC table out of band if a type must change.

# COMMAND ----------

report = sync_expecting_failure(
    define_events(
        columns=[
            Column("event_id", Long(), nullable=False),
            Column("event_date", Date()),
            Column("amount", Double()),  # <-- was Decimal(12, 2)
            Column("created_at", Timestamp()),
        ]
    )
)

[events_report] = report.table_reports
assert events_report.status is TableRunStatus.VALIDATION_FAILED
assert isinstance(inspector.fields_of("events")["amount"].dataType, T.DecimalType)
print("Step 5c verified: type change blocked, amount still Decimal.")

# COMMAND ----------

# MAGIC %md
# MAGIC ### 5d. Changing the partitioning
# MAGIC
# MAGIC **Goal**
# MAGIC
# MAGIC Try to drop the partitioning from the `events` table.
# MAGIC
# MAGIC **Outcome**
# MAGIC
# MAGIC Blocked: partitioning is fixed at creation and changing it requires a rewrite,
# MAGIC so the engine refuses to alter it in place.

# COMMAND ----------

# <-- partitioning dropped (was partitioned by event_date)
report = sync_expecting_failure(define_events(columns=events_baseline_columns, partitioned_by=()))

[events_report] = report.table_reports
assert events_report.status is TableRunStatus.VALIDATION_FAILED
assert inspector.partitions_of("events") == ("event_date",)
print("Step 5d verified: partitioning change blocked, partitions unchanged.")

# COMMAND ----------

# MAGIC %md
# MAGIC ### 5e. A foreign key that cannot be resolved
# MAGIC
# MAGIC **Goal**
# MAGIC
# MAGIC Sync `line_items`, which references `products` — a table that is not passed
# MAGIC to the sync call.
# MAGIC
# MAGIC **Outcome**
# MAGIC
# MAGIC The sync fails with `FOREIGN_KEY_FAILED`. The engine resolves foreign keys
# MAGIC before executing, so `line_items` is never created.

# COMMAND ----------

# products is defined but never passed to the sync — the reference is intentionally unresolvable.
products = DeltaTable(
    catalog=CATALOG,
    schema=SCHEMA,
    name="products",
    columns=[
        Column("product_id", Long(), nullable=False),
    ],
    primary_key=["product_id"],
)

line_items = DeltaTable(
    catalog=CATALOG,
    schema=SCHEMA,
    name="line_items",
    columns=[
        Column("line_item_id", Long(), nullable=False),
        Column("product_id", Long(), nullable=False),
    ],
    primary_key=["line_item_id"],
    foreign_keys=[
        ForeignKey(
            local_columns=("product_id",),
            references=products,  # <-- never passed to the sync
        )
    ],
)

report = sync_expecting_failure(line_items)

[line_items_report] = report.table_reports
assert line_items_report.status is TableRunStatus.FOREIGN_KEY_FAILED
assert spark.catalog.tableExists(inspector.fqname("line_items")) is False
print("Step 5e verified: unresolved foreign key blocked, no table created.")

# COMMAND ----------

# MAGIC %md
# MAGIC ### 5f. Dropping a primary key that other tables reference
# MAGIC
# MAGIC **Goal**
# MAGIC
# MAGIC Stop declaring the `customers` primary key while the live `orders` table
# MAGIC still holds a foreign key to it. `orders` is deliberately **not** passed to
# MAGIC the sync: the engine must protect the key from referencing tables it was
# MAGIC never told about.
# MAGIC
# MAGIC **Outcome**
# MAGIC
# MAGIC Blocked: `DROP PRIMARY KEY` is RESTRICT by default in Unity Catalog — it
# MAGIC fails while any foreign key references the key. The engine reads the
# MAGIC inbound references from `information_schema` when it observes the table,
# MAGIC so validation refuses before any SQL runs, and the failure message names
# MAGIC the blocking constraint (`orders_customer_id_fk`) and the tables to sync
# MAGIC first.

# COMMAND ----------

# Same declaration as the live table, except id is no longer a primary key.
customers_without_pk = DeltaTable(
    catalog=CATALOG,
    schema=SCHEMA,
    name="customers",
    columns=[
        Column("id", Long(), nullable=False),  # <-- no longer listed in primary_key=[...]
        Column("name", String(), comment="Full legal name"),
        Column("email", String()),
        Column("status", String()),
    ],
    comment="Customer master table (with contact details)",
    tags={"domain": "sales"},
    properties={
        Property.COLUMN_MAPPING_MODE: "name",
        Property.CHANGE_DATA_FEED: "true",
    },
)

report = sync_expecting_failure(customers_without_pk)  # <-- orders not passed

# COMMAND ----------

[customers_report] = report.table_reports
assert customers_report.status is TableRunStatus.VALIDATION_FAILED
assert inspector.has_primary_key("customers")
assert inspector.has_foreign_key("orders")
print("Step 5f verified: primary key drop blocked while orders' foreign key references it.")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 6. Construction-time guards
# MAGIC
# MAGIC **Goal**
# MAGIC
# MAGIC Try to *define* five malformed tables. Everything above failed at **sync**
# MAGIC time, inside the engine; these fail earlier, at **construction** time, when
# MAGIC the `DeltaTable` is built. Each example is the smallest table that trips one
# MAGIC guard.
# MAGIC
# MAGIC **Outcome**
# MAGIC
# MAGIC Each `DeltaTable(...)` raises `ValueError` before it exists — a malformed
# MAGIC definition never reaches the engine or the catalog.

# COMMAND ----------

# MAGIC %md
# MAGIC ### 6a. A nullable primary key
# MAGIC
# MAGIC **Goal**
# MAGIC
# MAGIC Declare a primary key column that is nullable (columns are nullable unless
# MAGIC `nullable=False` is set).
# MAGIC
# MAGIC **Outcome**
# MAGIC
# MAGIC Rejected: a primary key column must be `NOT NULL`.

# COMMAND ----------

DeltaTable(
    catalog=CATALOG,
    schema=SCHEMA,
    name="bad_pk",
    columns=[Column("id", Long())],  # <-- nullable (no NOT NULL)
    primary_key=["id"],
)

# COMMAND ----------

# MAGIC %md
# MAGIC ### 6b. A foreign key on a column that does not exist
# MAGIC
# MAGIC **Goal**
# MAGIC
# MAGIC Declare a foreign key whose local column is not one of the table's columns.
# MAGIC
# MAGIC **Outcome**
# MAGIC
# MAGIC Rejected: every foreign key local column must exist on the table.

# COMMAND ----------

DeltaTable(
    catalog=CATALOG,
    schema=SCHEMA,
    name="bad_fk_column",
    columns=[Column("id", Long(), nullable=False)],
    primary_key=["id"],
    foreign_keys=[
        ForeignKey(
            local_columns=("missing_id",),  # <-- no such column on this table
            references=customers,
        )
    ],
)

# COMMAND ----------

# MAGIC %md
# MAGIC ### 6c. A foreign key referencing a table with no primary key
# MAGIC
# MAGIC **Goal**
# MAGIC
# MAGIC Reference a `DeltaTable` that has no primary key. Referenced columns are
# MAGIC inferred from the referenced table's primary key, so there is nothing to
# MAGIC infer from — the engine rejects this at construction time.
# MAGIC
# MAGIC **Outcome**
# MAGIC
# MAGIC Rejected: referenced table declares no primary key, so referenced columns
# MAGIC cannot be inferred.

# COMMAND ----------

no_pk_table = DeltaTable(
    catalog=CATALOG,
    schema=SCHEMA,
    name="no_pk",
    columns=[Column("id", Long())],  # <-- no primary_key declared
)
DeltaTable(
    catalog=CATALOG,
    schema=SCHEMA,
    name="bad_fk_no_pk",
    columns=[Column("id", Long(), nullable=False)],
    primary_key=["id"],
    foreign_keys=[
        ForeignKey(
            local_columns=("id",),
            references=no_pk_table,  # <-- no primary key to infer from
        )
    ],
)

# COMMAND ----------

# MAGIC %md
# MAGIC ### 6d. CDF enabled with a reserved column name
# MAGIC
# MAGIC **Goal**
# MAGIC
# MAGIC Declare a table with `delta.enableChangeDataFeed = "true"` that also has a
# MAGIC column named `_change_type` — one of the three column names that Change Data
# MAGIC Feed reserves for its own output (`_change_type`, `_commit_version`,
# MAGIC `_commit_timestamp`).
# MAGIC
# MAGIC **Outcome**
# MAGIC
# MAGIC Rejected: the engine refuses to build the declaration because the column name
# MAGIC conflicts with a CDF reserved name. Rename the column or omit CDF.

# COMMAND ----------

DeltaTable(
    catalog=CATALOG,
    schema=SCHEMA,
    name="bad_cdf_reserved_col",
    columns=[
        Column("id", Long(), nullable=False),
        Column("_change_type", String()),  # <-- reserved by CDF
    ],
    primary_key=["id"],
    properties={Property.CHANGE_DATA_FEED: "true"},
)

# COMMAND ----------

# MAGIC %md
# MAGIC ### 6e. Special characters in a struct field name without column mapping
# MAGIC
# MAGIC **Goal**
# MAGIC
# MAGIC Declare a table with a `Struct` column whose field name contains a space —
# MAGIC one of the characters Delta only permits when column mapping is enabled.
# MAGIC Column mapping is not declared here.
# MAGIC
# MAGIC **Outcome**
# MAGIC
# MAGIC Rejected: the engine detects that the nested field path (`payload.order id`)
# MAGIC contains a character that requires `delta.columnMapping.mode = "name"`.
# MAGIC Either rename the field or add the column-mapping property.

# COMMAND ----------

DeltaTable(
    catalog=CATALOG,
    schema=SCHEMA,
    name="bad_struct_field_name",
    columns=[
        Column("id", Long(), nullable=False),
        Column(
            "payload",
            Struct([StructField("order id", String())]),  # <-- space in field name
        ),
    ],
    primary_key=["id"],
)

# COMMAND ----------

# MAGIC %md
# MAGIC ## 7. Metadata-only sync
# MAGIC
# MAGIC **Goal**
# MAGIC
# MAGIC Sync only the catalog metadata — comments, tags, and key constraints — of a
# MAGIC table that is owned by another process and must not have its columns or
# MAGIC properties touched. Two failure cases follow the happy path: attempting to
# MAGIC create a missing table and declaring structural drift the engine cannot resolve.
# MAGIC
# MAGIC **Outcome**
# MAGIC
# MAGIC With `metadata_only=True`, the engine updates metadata and ignores column
# MAGIC and property state entirely (7a). Attempting to create a table that does not
# MAGIC exist is blocked — column structure management is required for creation (7b).
# MAGIC Declaring columns that do not match the live table is also blocked — structural
# MAGIC drift on a metadata-only table is a scope violation the caller must resolve out
# MAGIC of band (7c).

# COMMAND ----------

# MAGIC %md
# MAGIC ### 7a. Metadata-only update

# COMMAND ----------

# metadata_only=True restricts the sync to comments, tags, and key constraints.
# Columns and properties are still declared (the engine validates structural
# alignment) but never compared or changed.
customers_meta = DeltaTable(
    catalog=CATALOG,
    schema=SCHEMA,
    name="customers",
    columns=[
        Column("id", Long(), nullable=False),
        Column("name", String(), comment="Full legal name"),
        Column("email", String()),
        Column("status", String()),
    ],
    primary_key=["id"],
    comment="Customer master table (managed externally)",  # <-- comment changed
    tags={"domain": "sales"},
    properties={
        Property.COLUMN_MAPPING_MODE: "name",
        Property.CHANGE_DATA_FEED: "true",
    },
    metadata_only=True,  # <-- restricts sync to metadata aspects only
)

report = engine.sync(customers_meta)
show_report(report)

# COMMAND ----------

# MAGIC %md
# MAGIC **Verify** the comment changed but the column schema is untouched.

# COMMAND ----------

assert report.any_failures is False
assert inspector.table_comment("customers") == "Customer master table (managed externally)"
assert set(inspector.fields_of("customers")) == {"id", "name", "email", "status"}
print("Step 7a verified: metadata-only sync updated the comment; columns untouched.")

# COMMAND ----------

# MAGIC %md
# MAGIC ### 7b. A metadata-only declaration cannot create a missing table
# MAGIC
# MAGIC **Goal**
# MAGIC
# MAGIC Attempt to sync a `metadata_only` declaration for a table that does not yet
# MAGIC exist in the catalog.
# MAGIC
# MAGIC **Outcome**
# MAGIC
# MAGIC Blocked: creating a table requires managing column structure. A metadata-only
# MAGIC declaration explicitly opts out of that, so the engine refuses rather than
# MAGIC creating a structureless table.

# COMMAND ----------

new_table_meta_only = DeltaTable(
    catalog=CATALOG,
    schema=SCHEMA,
    name="never_created",
    columns=[
        Column("id", Long(), nullable=False),
    ],
    primary_key=["id"],
    metadata_only=True,  # <-- cannot create; no column structure management
)

# COMMAND ----------
# assert table does not exist
assert not spark.catalog.tableExists(inspector.fqname("never_created"))

report = sync_expecting_failure(new_table_meta_only)

# COMMAND ----------

[report_entry] = report.table_reports
assert report_entry.status is TableRunStatus.VALIDATION_FAILED
assert spark.catalog.tableExists(inspector.fqname("never_created")) is False
print("Step 7b verified: metadata-only declaration blocked from creating a missing table.")

# COMMAND ----------

# MAGIC %md
# MAGIC ### 7c. Structural drift on a metadata-only table
# MAGIC
# MAGIC **Goal**
# MAGIC
# MAGIC Declare a column that doesn't exist on the live table in a `metadata_only`
# MAGIC declaration and attempt to sync.
# MAGIC
# MAGIC **Outcome**
# MAGIC
# MAGIC Blocked: structural drift on a metadata-only table is a scope violation —
# MAGIC the declaration promises not to manage columns, so any mismatch is an error
# MAGIC the caller must resolve out of band.

# COMMAND ----------

customers_meta_drifted = DeltaTable(
    catalog=CATALOG,
    schema=SCHEMA,
    name="customers",
    columns=[
        Column("id", Long(), nullable=False),
        Column("name", String(), comment="Full legal name"),
        Column("email", String()),
        Column("status", String()),
        Column("ghost", String()),  # <-- does not exist on the live table
    ],
    primary_key=["id"],
    comment="Customer master table (managed externally)",
    tags={"domain": "sales"},
    properties={
        Property.COLUMN_MAPPING_MODE: "name",
        Property.CHANGE_DATA_FEED: "true",
    },
    metadata_only=True,  # <-- restricts sync to metadata aspects only
)

report = sync_expecting_failure(customers_meta_drifted)

# COMMAND ----------

[customers_report] = report.table_reports
assert customers_report.status is TableRunStatus.VALIDATION_FAILED
assert "ghost" not in inspector.fields_of("customers")
print("Step 7c verified: structural drift on metadata-only table blocked.")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 8. Preview with a dry run, then commit
# MAGIC
# MAGIC **Goal**
# MAGIC
# MAGIC Add a nullable `phone` column and preview it with a dry run, then sync the
# MAGIC same tables for real.
# MAGIC
# MAGIC **Outcome**
# MAGIC
# MAGIC The dry run plans and validates but executes nothing, so the catalog is
# MAGIC unchanged; the real sync then applies the previewed change. This is the
# MAGIC standard way to preview a change before committing it.

# COMMAND ----------

customers = DeltaTable(
    catalog=CATALOG,
    schema=SCHEMA,
    name="customers",
    columns=[
        Column("id", Long(), nullable=False),
        Column("name", String(), comment="Full legal name"),
        Column("email", String()),
        Column("status", String()),
        Column("phone", String()),  # <-- added
    ],
    primary_key=["id"],
    comment="Customer master table (with contact details)",
    tags={"domain": "sales"},
    properties={
        Property.COLUMN_MAPPING_MODE: "name",
        Property.CHANGE_DATA_FEED: "true",
    },
)

preview = engine.sync(customers, orders, dry_run=True)
show_report(preview)

# COMMAND ----------

# MAGIC %md
# MAGIC **Verify** the dry run executed nothing and `phone` is not yet in the table.

# COMMAND ----------

assert all(table_report.execution is None for table_report in preview)
assert "phone" not in inspector.fields_of("customers")
print("Dry run verified: plan shown, nothing executed.")

# COMMAND ----------

# MAGIC %md
# MAGIC **Commit** the previewed change with a real sync.

# COMMAND ----------

report = engine.sync(customers, orders)
show_report(report)

# COMMAND ----------

assert report.any_failures is False
assert "phone" in inspector.fields_of("customers")
print("Step 8 verified: dry-run previewed the change; real sync applied it.")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 9. Teardown
# MAGIC
# MAGIC **Goal**
# MAGIC
# MAGIC Drop the demo tables this notebook created.
# MAGIC
# MAGIC **Outcome**
# MAGIC
# MAGIC The demo tables are gone. The schema is left in place — the engine never
# MAGIC owned it, and dropping it is out of scope. The guard examples above created
# MAGIC nothing to clean up.

# COMMAND ----------

for _table in DEMO_TABLES:
    spark.sql(f"DROP TABLE IF EXISTS {CATALOG}.{SCHEMA}.{_table}")
print("Teardown complete: demo tables dropped.")
