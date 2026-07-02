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

from delta_engine import (
    Column,
    Date,
    Decimal,
    DeltaTable,
    Double,
    ForeignKey,
    Long,
    Property,
    Registry,
    String,
    SyncFailedError,
    SyncReport,
    TableRunStatus,
    Timestamp,
    build_databricks_engine,
    configure_logging,
)

# COMMAND ----------

dbutils.widgets.text("catalog", "main")
dbutils.widgets.text("schema", "delta_engine_demo")

CATALOG = dbutils.widgets.get("catalog")
SCHEMA = dbutils.widgets.get("schema")

DEMO_TABLES = ("customers", "orders", "regions", "events", "line_items")

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
    print("\n\n---------------- Sync Report ----------------")
    print(report)
    print("\n\n---------------- Diff ----------------")
    print(report.diff())


# COMMAND ----------

# MAGIC %md
# MAGIC ## 2. Define and first sync
# MAGIC
# MAGIC **Goal**
# MAGIC
# MAGIC Define two tables — `customers` and `orders` — and sync them. `orders` has a
# MAGIC foreign key to `customers` and is declared **first** in the registry, on
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
        Column("id", Long(), nullable=False, primary_key=True),
        Column("name", String()),
        Column("legacy_code", String(), comment="Retired identifier, dropped in step 4a"),
        Column("status", String(), nullable=False),
    ],
    comment="Customer master table",
)

orders = DeltaTable(
    catalog=CATALOG,
    schema=SCHEMA,
    name="orders",
    columns=[
        Column("order_id", Long(), nullable=False, primary_key=True),
        Column("customer_id", Long(), nullable=False),
        Column("order_date", Date()),
    ],
    partitioned_by=["order_date"],
    foreign_keys=[
        ForeignKey(
            local_columns=("customer_id",),
            references=f"{CATALOG}.{SCHEMA}.customers",
            referenced_columns=("id",),
        )
    ],
)

registry = Registry()
registry.register(orders, customers)  # declared orders-first on purpose

engine = build_databricks_engine(spark)
report = engine.sync(registry)
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
# MAGIC Snapshot the live `customers` schema, sync the exact same registry a second
# MAGIC time with the catalog already matching the declaration, then compare.
# MAGIC
# MAGIC **Outcome**
# MAGIC
# MAGIC A true no-op: the engine plans and executes nothing (every table's
# MAGIC `execution` is `None`), and the live catalog schema is byte-for-byte
# MAGIC identical before and after.

# COMMAND ----------

schema_before = spark.table(inspector.fqname("customers")).schema

report = engine.sync(registry)
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
# MAGIC Evolve the live `customers` table through a series of small changes — adding
# MAGIC and dropping columns, loosening nullability, setting and unsetting tags, and
# MAGIC adding a foreign key — a single change per step.
# MAGIC
# MAGIC **Outcome**
# MAGIC
# MAGIC Each step re-declares the **full** desired `customers` state, changing only
# MAGIC the item under demonstration, and the engine reconciles the live table to
# MAGIC match.

# COMMAND ----------

# MAGIC %md
# MAGIC ### 4a. Batched heterogeneous change
# MAGIC
# MAGIC **Goal**
# MAGIC
# MAGIC Add `email`, drop `legacy_code`, set two tags, set a property, and change the
# MAGIC table and a column comment — all declared at once.
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
        Column("id", Long(), nullable=False, primary_key=True),
        Column("name", String(), comment="Full legal name"),  # <-- added column comment
        Column("email", String()),  # <-- added (legacy_code dropped by omission)
        Column("status", String(), nullable=False),
    ],
    comment="Customer master table (with contact details)",  # <-- added table comment
    tags={"domain": "sales", "owner": "data-eng"},   # <-- set 2 tags
    properties={Property.CHANGE_DATA_FEED: "true"},  # <-- set a property
)

registry = Registry()
registry.register(customers, orders)
report = engine.sync(registry)
show_report(report)

# COMMAND ----------

# MAGIC %md
# MAGIC **Verify** the batched change: new column present, old column gone, tags and
# MAGIC property set, comments updated.

# COMMAND ----------

assert report.any_failures is False

customer_fields = inspector.fields_of("customers")
assert "email" in customer_fields and customer_fields["email"].nullable is True
assert "legacy_code" not in customer_fields
assert inspector.tags_of("customers") == {"domain": "sales", "owner": "data-eng"}
assert inspector.properties_of("customers").get("delta.enableChangeDataFeed") == "true"
assert inspector.table_comment("customers") == "Customer master table (with contact details)"
assert inspector.column_comment("customers", "name") == "Full legal name"
print("Step 4a verified: batched add/drop/tags/property/comments applied together.")
inspector.display_schema("customers")
inspector.display_tags("customers")
inspector.display_properties("customers")

# COMMAND ----------

# MAGIC %md
# MAGIC ### 4b. Loosen nullability
# MAGIC
# MAGIC **Goal**
# MAGIC
# MAGIC Relax `status` from `NOT NULL` to nullable.
# MAGIC
# MAGIC **Outcome**
# MAGIC
# MAGIC The engine *allows* loosening and applies it. Step 5 shows the reverse
# MAGIC (tightening) is blocked — same rule, both directions.

# COMMAND ----------

customers = DeltaTable(
    catalog=CATALOG,
    schema=SCHEMA,
    name="customers",
    columns=[
        Column("id", Long(), nullable=False, primary_key=True),
        Column("name", String(), comment="Full legal name"),
        Column("email", String()),
        Column("status", String()),  # <-- was NOT NULL; now nullable
    ],
    comment="Customer master table (with contact details)",
    tags={"domain": "sales", "owner": "data-eng"},
    properties={Property.CHANGE_DATA_FEED: "true"},
)

registry = Registry()
registry.register(customers, orders)
report = engine.sync(registry)
show_report(report)

# COMMAND ----------

# MAGIC %md
# MAGIC **Verify** `status` is now nullable.

# COMMAND ----------

assert report.any_failures is False
assert inspector.fields_of("customers")["status"].nullable is True
print("Step 4b verified: nullability loosened (NOT NULL dropped).")
inspector.display_schema("customers")

# COMMAND ----------

# MAGIC %md
# MAGIC ### 4c. Prove tag unset
# MAGIC
# MAGIC **Goal**
# MAGIC
# MAGIC Drop the `owner` tag from the declaration and resync.
# MAGIC
# MAGIC **Outcome**
# MAGIC
# MAGIC Tags are **full-state**: the declaration is the complete set, so a tag you
# MAGIC stop declaring is removed. Only `domain` remains. Properties behave the
# MAGIC opposite way — step 4f shows undeclared properties are left alone.

# COMMAND ----------

customers = DeltaTable(
    catalog=CATALOG,
    schema=SCHEMA,
    name="customers",
    columns=[
        Column("id", Long(), nullable=False, primary_key=True),
        Column("name", String(), comment="Full legal name"),
        Column("email", String()),
        Column("status", String()),
    ],
    comment="Customer master table (with contact details)",
    tags={"domain": "sales"},  # <-- "owner" dropped
    properties={Property.CHANGE_DATA_FEED: "true"},
)

registry = Registry()
registry.register(customers, orders)
report = engine.sync(registry)
show_report(report)

# COMMAND ----------

# MAGIC %md
# MAGIC **Verify** the `owner` tag is gone and only `domain` remains.

# COMMAND ----------

assert report.any_failures is False
assert inspector.tags_of("customers") == {"domain": "sales"}
print("Step 4c verified: undeclared tag removed by full-state reconciliation.")
inspector.display_tags("customers")

# COMMAND ----------

# MAGIC %md
# MAGIC ### 4d. Column tags — set then unset
# MAGIC
# MAGIC **Goal**
# MAGIC
# MAGIC Add a column tag to `email` (`pii=true`) and sync, then re-declare `email`
# MAGIC without the tag and resync.
# MAGIC
# MAGIC **Outcome**
# MAGIC
# MAGIC The tag lands on the first sync and is removed on the second: column-tag
# MAGIC reconciliation is **full-state**, so an absent declaration drops the tag.
# MAGIC This is the column-level analogue of step 4c.

# COMMAND ----------

customers = DeltaTable(
    catalog=CATALOG,
    schema=SCHEMA,
    name="customers",
    columns=[
        Column("id", Long(), nullable=False, primary_key=True),
        Column("name", String(), comment="Full legal name"),
        Column("email", String(), tags={"pii": "true"}),  # <-- column tag added
        Column("status", String()),
    ],
    comment="Customer master table (with contact details)",
    tags={"domain": "sales"},
    properties={Property.CHANGE_DATA_FEED: "true"},
)

registry = Registry()
registry.register(customers, orders)
report = engine.sync(registry)
show_report(report)

# COMMAND ----------

assert report.any_failures is False
assert inspector.column_tags_of("customers")[("email", "pii")] == "true"
print("Column tag set: customers.email pii=true")

# COMMAND ----------

customers = DeltaTable(
    catalog=CATALOG,
    schema=SCHEMA,
    name="customers",
    columns=[
        Column("id", Long(), nullable=False, primary_key=True),
        Column("name", String(), comment="Full legal name"),
        Column("email", String()),  # <-- column tag removed
        Column("status", String()),
    ],
    comment="Customer master table (with contact details)",
    tags={"domain": "sales"},
    properties={Property.CHANGE_DATA_FEED: "true"},
)

registry = Registry()
registry.register(customers, orders)
report = engine.sync(registry)
show_report(report)

# COMMAND ----------

assert report.any_failures is False
assert ("email", "pii") not in inspector.column_tags_of("customers")
print("Column tag unset by full-state reconciliation: customers.email pii removed")

# COMMAND ----------

# MAGIC %md
# MAGIC ### 4e. Foreign-key drift on an existing table
# MAGIC
# MAGIC **Goal**
# MAGIC
# MAGIC Create a `regions` dimension, then add a `region_id` column and a foreign key
# MAGIC from the already-existing `customers` table to it.
# MAGIC
# MAGIC **Outcome**
# MAGIC
# MAGIC The engine adds the column and the foreign key to a live table. This is drift
# MAGIC management on an existing table, distinct from step 2's create-time foreign
# MAGIC key ordering.

# COMMAND ----------

regions = DeltaTable(
    catalog=CATALOG,
    schema=SCHEMA,
    name="regions",
    columns=[
        Column("region_id", Long(), nullable=False, primary_key=True),
        Column("region_name", String()),
    ],
    comment="Region dimension",
)

registry = Registry()
registry.register(regions)
report = engine.sync(registry)
show_report(report)
assert report.any_failures is False
assert spark.catalog.tableExists(inspector.fqname("regions"))

# COMMAND ----------

customers = DeltaTable(
    catalog=CATALOG,
    schema=SCHEMA,
    name="customers",
    columns=[
        Column("id", Long(), nullable=False, primary_key=True),
        Column("name", String(), comment="Full legal name"),
        Column("email", String()),
        Column("status", String()),
        Column("region_id", Long()),  # <-- added
    ],
    comment="Customer master table (with contact details)",
    tags={"domain": "sales"},
    properties={Property.CHANGE_DATA_FEED: "true"},
    foreign_keys=[  # <-- foreign key added to the existing table
        ForeignKey(
            local_columns=("region_id",),
            references=f"{CATALOG}.{SCHEMA}.regions",
            referenced_columns=("region_id",),
        )
    ],
)

registry = Registry()
registry.register(customers, orders, regions)
report = engine.sync(registry)
show_report(report)

# COMMAND ----------

# MAGIC %md
# MAGIC **Verify** `customers` gained the column and the foreign key.

# COMMAND ----------

assert report.any_failures is False
customer_fields = inspector.fields_of("customers")
assert "region_id" in customer_fields and customer_fields["region_id"].nullable is True
assert inspector.has_foreign_key("customers")
print("Step 4e verified: foreign key added to an existing table.")
# COMMAND ----------

# MAGIC %md
# MAGIC ### 4f. Undeclared properties are left alone
# MAGIC
# MAGIC **Goal**
# MAGIC
# MAGIC Set a property directly with raw SQL — `delta.logRetentionDuration`, which
# MAGIC the `customers` declaration never mentions — then resync `customers` with
# MAGIC that same declaration.
# MAGIC
# MAGIC **Outcome**
# MAGIC
# MAGIC The property survives the sync. Unlike tags (step 4c), the engine does not
# MAGIC treat your `properties` as the complete set: it manages only the keys you
# MAGIC declare and leaves every other property untouched. This matters because
# MAGIC Delta tables carry many system-managed `delta.*` properties — a sync must
# MAGIC not strip the ones you did not list.

# COMMAND ----------

spark.sql(
    f"ALTER TABLE {inspector.fqname('customers')}"
    f" SET TBLPROPERTIES ('delta.logRetentionDuration' = 'interval 7 days')"
)

# Re-declare customers exactly as in step 4e (logRetentionDuration is NOT declared).
customers = DeltaTable(
    catalog=CATALOG,
    schema=SCHEMA,
    name="customers",
    columns=[
        Column("id", Long(), nullable=False, primary_key=True),
        Column("name", String(), comment="Full legal name"),
        Column("email", String()),
        Column("status", String()),
        Column("region_id", Long()),
    ],
    comment="Customer master table (with contact details)",
    tags={"domain": "sales"},
    properties={Property.CHANGE_DATA_FEED: "true"},
    foreign_keys=[
        ForeignKey(
            local_columns=("region_id",),
            references=f"{CATALOG}.{SCHEMA}.regions",
            referenced_columns=("region_id",),
        )
    ],
)

registry = Registry()
registry.register(customers, orders, regions)
report = engine.sync(registry)
show_report(report)

# COMMAND ----------

# MAGIC %md
# MAGIC **Verify** the out-of-band property is untouched.

# COMMAND ----------

assert report.any_failures is False
assert inspector.properties_of("customers").get("delta.logRetentionDuration") == "interval 7 days"
print("Step 4f verified: undeclared property left untouched.")
# COMMAND ----------

# MAGIC %md
# MAGIC ## 5. Validation blocks unsafe changes
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


def define_events(*, columns, partitioned_by=("event_date",)):
    """Build the events table from a column list (baseline plus one variation)."""
    return DeltaTable(
        catalog=CATALOG,
        schema=SCHEMA,
        name="events",
        columns=columns,
        partitioned_by=partitioned_by,
        comment="Raw events",
    )


def sync_expecting_failure(registry: Registry) -> SyncReport:
    """Sync a registry that must fail, print the failed report, and return it."""
    try:
        engine.sync(registry)
    except SyncFailedError as error:
        show_report(error.report)
        return error.report
    raise AssertionError("expected the sync to fail")


events_baseline_columns = [
    Column("event_id", Long(), nullable=False, primary_key=True),
    Column("event_date", Date()),
    Column("amount", Decimal(12, 2)),
    Column("created_at", Timestamp()),
]

registry = Registry()
registry.register(define_events(columns=events_baseline_columns))
report = engine.sync(registry)
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

registry = Registry()
registry.register(define_events(columns=[*events_baseline_columns, new_column]))
report = sync_expecting_failure(registry)

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

registry = Registry()
registry.register(
    define_events(
        columns=[
            Column("event_id", Long(), nullable=False, primary_key=True),
            Column("event_date", Date(), nullable=False),  # <-- tightened to NOT NULL
            Column("amount", Decimal(12, 2)),
            Column("created_at", Timestamp()),
        ]
    )
)
report = sync_expecting_failure(registry)

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

registry = Registry()
registry.register(
    define_events(
        columns=[
            Column("event_id", Long(), nullable=False, primary_key=True),
            Column("event_date", Date()),
            Column("amount", Double()),  # <-- was Decimal(12, 2)
            Column("created_at", Timestamp()),
        ]
    )
)
report = sync_expecting_failure(registry)

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

registry = Registry()
# <-- partitioning dropped (was partitioned by event_date)
registry.register(define_events(columns=events_baseline_columns, partitioned_by=()))
report = sync_expecting_failure(registry)

[events_report] = report.table_reports
assert events_report.status is TableRunStatus.VALIDATION_FAILED
assert inspector.partitions_of("events") == ("event_date",)
print("Step 5d verified: partitioning change blocked, partitions unchanged.")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 6. A foreign key that cannot be resolved
# MAGIC
# MAGIC **Goal**
# MAGIC
# MAGIC Sync `line_items`, which references `products` — a table that is not in the
# MAGIC registry.
# MAGIC
# MAGIC **Outcome**
# MAGIC
# MAGIC The sync fails with `FOREIGN_KEY_FAILED`. The engine resolves foreign keys
# MAGIC before executing, so `line_items` is never created.

# COMMAND ----------

line_items = DeltaTable(
    catalog=CATALOG,
    schema=SCHEMA,
    name="line_items",
    columns=[
        Column("line_item_id", Long(), nullable=False, primary_key=True),
        Column("product_id", Long(), nullable=False),
    ],
    foreign_keys=[
        ForeignKey(
            local_columns=("product_id",),
            references=f"{CATALOG}.{SCHEMA}.products",  # <-- never registered
            referenced_columns=("product_id",),
        )
    ],
)

registry = Registry()
registry.register(line_items)
report = sync_expecting_failure(registry)

[line_items_report] = report.table_reports
assert line_items_report.status is TableRunStatus.FOREIGN_KEY_FAILED
assert spark.catalog.tableExists(inspector.fqname("line_items")) is False
print("Step 6 verified: unresolved foreign key blocked, no table created.")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 7. Preview with a dry run, then commit
# MAGIC
# MAGIC **Goal**
# MAGIC
# MAGIC Add a nullable `phone` column and preview it with a dry run, then sync the
# MAGIC same registry for real.
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
        Column("id", Long(), nullable=False, primary_key=True),
        Column("name", String(), comment="Full legal name"),
        Column("email", String()),
        Column("status", String()),
        Column("region_id", Long()),
        Column("phone", String()),  # <-- added
    ],
    comment="Customer master table (with contact details)",
    tags={"domain": "sales"},
    properties={Property.CHANGE_DATA_FEED: "true"},
    foreign_keys=[
        ForeignKey(
            local_columns=("region_id",),
            references=f"{CATALOG}.{SCHEMA}.regions",
            referenced_columns=("region_id",),
        )
    ],
)

registry = Registry()
registry.register(customers, orders, regions)

preview = engine.sync(registry, dry_run=True)
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
# MAGIC ## 8. Construction-time guards
# MAGIC
# MAGIC **Goal**
# MAGIC
# MAGIC Try to *define* three malformed tables. Everything above failed at **sync**
# MAGIC time, inside the engine; these fail earlier, at **construction** time, when
# MAGIC the `DeltaTable` is built. Each example is the smallest table that trips one
# MAGIC guard.
# MAGIC
# MAGIC **Outcome**
# MAGIC
# MAGIC Each `DeltaTable(...)` raises `ValueError` before it exists — a malformed
# MAGIC definition never reaches a registry, the engine, or the catalog.

# COMMAND ----------

# MAGIC %md
# MAGIC ### 8a. A nullable primary key
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

try:
    DeltaTable(
        catalog=CATALOG,
        schema=SCHEMA,
        name="bad_pk",
        columns=[Column("id", Long(), primary_key=True)],  # <-- nullable primary key
    )
    raise AssertionError("expected ValueError")
except ValueError as error:
    print(error)

# COMMAND ----------

# MAGIC %md
# MAGIC ### 8b. A foreign key on a column that does not exist
# MAGIC
# MAGIC **Goal**
# MAGIC
# MAGIC Declare a foreign key whose local column is not one of the table's columns.
# MAGIC
# MAGIC **Outcome**
# MAGIC
# MAGIC Rejected: every foreign key local column must exist on the table.

# COMMAND ----------

try:
    DeltaTable(
        catalog=CATALOG,
        schema=SCHEMA,
        name="bad_fk_column",
        columns=[Column("id", Long(), nullable=False, primary_key=True)],
        foreign_keys=[
            ForeignKey(
                local_columns=("missing_id",),  # <-- no such column on this table
                references=f"{CATALOG}.{SCHEMA}.customers",
                referenced_columns=("id",),
            )
        ],
    )
    raise AssertionError("expected ValueError")
except ValueError as error:
    print(error)

# COMMAND ----------

# MAGIC %md
# MAGIC ### 8c. A foreign key reference that is not fully qualified
# MAGIC
# MAGIC **Goal**
# MAGIC
# MAGIC Build a `ForeignKey` whose `references` is a bare table name rather than a
# MAGIC `catalog.schema.table` name. This guard lives on `ForeignKey` itself, so it
# MAGIC trips before a `DeltaTable` is even involved.
# MAGIC
# MAGIC **Outcome**
# MAGIC
# MAGIC Rejected: `references` must be a fully qualified `catalog.schema.table` name.

# COMMAND ----------

try:
    ForeignKey(
        local_columns=("customer_id",),
        references="customers",  # <-- not catalog.schema.table
        referenced_columns=("id",),
    )
    raise AssertionError("expected ValueError")
except ValueError as error:
    print(error)

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
