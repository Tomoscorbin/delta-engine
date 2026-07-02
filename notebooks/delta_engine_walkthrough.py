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
# MAGIC ## Act 0 — Setup
# MAGIC
# MAGIC Read the target location from widgets, enable coloured logging, drop any
# MAGIC leftover demo tables for a clean slate, and ensure the schema exists. The
# MAGIC engine manages *tables*, not schemas, so we create the schema ourselves.

# COMMAND ----------
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

configure_logging()

# Clean slate: drop every table this notebook may create, then ensure the schema.
for _table in DEMO_TABLES:
    spark.sql(f"DROP TABLE IF EXISTS {CATALOG}.{SCHEMA}.{_table}")

# COMMAND ----------

# MAGIC %md
# MAGIC ### Verification helpers
# MAGIC
# MAGIC `CatalogInspector` reads live table state back from Unity Catalog using the
# MAGIC same surfaces the engine's own reader uses, so assertions reflect real
# MAGIC catalog state. It lives in `catalog_inspector.py` beside this notebook —
# MAGIC import it as a module so the notebook itself stays focused on the syncs.

# COMMAND ----------

from catalog_inspector import CatalogInspector

inspector = CatalogInspector(CATALOG, SCHEMA)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Act 1 — Define and first sync
# MAGIC
# MAGIC Define two tables and sync them. `orders` has a foreign key to `customers`
# MAGIC and is declared **first** in the registry — the engine reorders by
# MAGIC dependency, so `customers` is created before `orders` adds its foreign key.
# MAGIC The `customers` baseline is deliberately plain (no tags, no extra
# MAGIC properties, no foreign key) so Act 3 can add them.

# COMMAND ----------

customers = DeltaTable(
    catalog=CATALOG,
    schema=SCHEMA,
    name="customers",
    columns=[
        Column("id", Long(), nullable=False, primary_key=True),
        Column("name", String()),
        Column("legacy_code", String(), comment="Retired identifier, dropped in Act 3"),
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
print(report)
print(report.diff())

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

print("Act 1 verified: both tables created with expected schema and constraints.")
inspector.display_schema("customers")
inspector.display_schema("orders")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Act 2 — Idempotent resync
# MAGIC
# MAGIC Syncing the same registry again is a true no-op: the engine executes
# MAGIC nothing when the catalog already matches the declaration.

# COMMAND ----------

report = engine.sync(registry)
print(report)
print(report.diff())

# COMMAND ----------

# MAGIC %md
# MAGIC **Verify** no actions ran — `execution is None` for every table.

# COMMAND ----------

assert report.any_failures is False
assert all(table_report.execution is None for table_report in report), (
    "a matching resync must execute nothing"
)
print("Act 2 verified: resync was a true no-op.")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Act 3 — Safe schema evolution & drift management
# MAGIC
# MAGIC Some changes batch into a single sync; others need their own. **Batched:**
# MAGIC independent, one-directional changes (add a column, drop another, set tags
# MAGIC and properties) plan together — the common case. **Own sync:** behaviours
# MAGIC that must react to state a prior sync established — you cannot *unset* a tag
# MAGIC in the same sync that *sets* it, nor prove properties are *declared-subset*
# MAGIC until one exists out-of-band. Each step re-declares the **full** desired
# MAGIC `customers` state, changing only the item under demonstration.

# COMMAND ----------

# MAGIC %md
# MAGIC ### Act 3a — Batched heterogeneous change (one sync)
# MAGIC
# MAGIC Add `email`, drop `legacy_code`, set two tags, set a property, and change the
# MAGIC table and a column comment — all in one sync.

# COMMAND ----------

customers = DeltaTable(
    catalog=CATALOG,
    schema=SCHEMA,
    name="customers",
    columns=[
        Column("id", Long(), nullable=False, primary_key=True),
        Column("name", String(), comment="Full legal name"),
        Column("email", String()),
        Column("status", String(), nullable=False),
    ],
    comment="Customer master table (with contact details)",
    tags={"domain": "sales", "owner": "data-eng"},
    properties={Property.CHANGE_DATA_FEED: "true"},
)

registry = Registry()
registry.register(customers, orders)
report = engine.sync(registry)
print(report)
print(report.diff())

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
print("Act 3a verified: batched add/drop/tags/property/comments applied together.")
inspector.display_schema("customers")
inspector.display_tags("customers")
inspector.display_properties("customers")

# COMMAND ----------

# MAGIC %md
# MAGIC ### Act 3b — Loosen nullability (own sync)
# MAGIC
# MAGIC Relax `status` from `NOT NULL` to nullable. The engine *allows* loosening;
# MAGIC Act 4 shows the reverse (tightening) is blocked. Same rule, both directions.

# COMMAND ----------

customers = DeltaTable(
    catalog=CATALOG,
    schema=SCHEMA,
    name="customers",
    columns=[
        Column("id", Long(), nullable=False, primary_key=True),
        Column("name", String(), comment="Full legal name"),
        Column("email", String()),
        Column("status", String()),  # was NOT NULL; now nullable
    ],
    comment="Customer master table (with contact details)",
    tags={"domain": "sales", "owner": "data-eng"},
    properties={Property.CHANGE_DATA_FEED: "true"},
)

registry = Registry()
registry.register(customers, orders)
report = engine.sync(registry)
print(report)
print(report.diff())

# COMMAND ----------

# MAGIC %md
# MAGIC **Verify** `status` is now nullable.

# COMMAND ----------

assert report.any_failures is False
assert inspector.fields_of("customers")["status"].nullable is True
print("Act 3b verified: nullability loosened (NOT NULL dropped).")
inspector.display_schema("customers")

# COMMAND ----------

# MAGIC %md
# MAGIC ### Act 3c — Prove tag unset (own sync)
# MAGIC
# MAGIC Drop the `owner` tag from the declaration. Tag reconciliation is
# MAGIC **full-state**: a tag no longer declared is removed. This proves the claim
# MAGIC from Act 3a — contrast with properties (declared-subset), proven in Act 3e.

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
    tags={"domain": "sales"},  # dropped "owner"
    properties={Property.CHANGE_DATA_FEED: "true"},
)

registry = Registry()
registry.register(customers, orders)
report = engine.sync(registry)
print(report)
print(report.diff())

# COMMAND ----------

# MAGIC %md
# MAGIC **Verify** the `owner` tag is gone and only `domain` remains.

# COMMAND ----------

assert report.any_failures is False
assert inspector.tags_of("customers") == {"domain": "sales"}
print("Act 3c verified: undeclared tag removed by full-state reconciliation.")
inspector.display_tags("customers")

# COMMAND ----------

# MAGIC %md
# MAGIC ### Act 3d — Column tags — set then unset (full-state)
# MAGIC
# MAGIC Add a column tag to `email` (`pii=true`), sync, and assert it landed. Then
# MAGIC re-declare without the tag and resync — column-tag reconciliation is
# MAGIC **full-state**, so the absent declaration removes the tag. This is the
# MAGIC column-level analogue of Act 3c.

# COMMAND ----------

customers = DeltaTable(
    catalog=CATALOG,
    schema=SCHEMA,
    name="customers",
    columns=[
        Column("id", Long(), nullable=False, primary_key=True),
        Column("name", String(), comment="Full legal name"),
        Column("email", String(), tags={"pii": "true"}),
        Column("status", String()),
    ],
    comment="Customer master table (with contact details)",
    tags={"domain": "sales"},
    properties={Property.CHANGE_DATA_FEED: "true"},
)

registry = Registry()
registry.register(customers, orders)
report = engine.sync(registry)
print(report)
print(report.diff())

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
        Column("email", String()),  # tag removed from declaration
        Column("status", String()),
    ],
    comment="Customer master table (with contact details)",
    tags={"domain": "sales"},
    properties={Property.CHANGE_DATA_FEED: "true"},
)

registry = Registry()
registry.register(customers, orders)
report = engine.sync(registry)
print(report)
print(report.diff())

assert report.any_failures is False
assert ("email", "pii") not in inspector.column_tags_of("customers")
print("Column tag unset by full-state reconciliation: customers.email pii removed")

# COMMAND ----------

# MAGIC %md
# MAGIC ### Act 3e — Foreign-key drift on an existing table (own sync)
# MAGIC
# MAGIC Create a `regions` dimension, then add a `region_id` column and a foreign
# MAGIC key to `customers` — which already exists. This is drift management on a
# MAGIC live table, distinct from Act 1's create-time foreign key ordering.

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
print(report)
print(report.diff())
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
print(report)
print(report.diff())

# COMMAND ----------

# MAGIC %md
# MAGIC **Verify** `customers` gained the column and the foreign key.

# COMMAND ----------

assert report.any_failures is False
customer_fields = inspector.fields_of("customers")
assert "region_id" in customer_fields and customer_fields["region_id"].nullable is True
assert inspector.has_foreign_key("customers")
print("Act 3e verified: foreign key added to an existing table.")
# COMMAND ----------

# MAGIC %md
# MAGIC ### Act 3f — Properties are declared-subset (own sync)
# MAGIC
# MAGIC Set a property **out-of-band**, then resync `customers` without declaring
# MAGIC it. The engine manages only declared property keys, so the out-of-band
# MAGIC property survives. This is the deliberate contrast with full-state tags.

# COMMAND ----------

spark.sql(
    f"ALTER TABLE {inspector.fqname('customers')}"
    f" SET TBLPROPERTIES ('delta.logRetentionDuration' = 'interval 7 days')"
)

# Re-declare customers exactly as in Act 3e (logRetentionDuration is NOT declared).
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
print(report)
print(report.diff())

# COMMAND ----------

# MAGIC %md
# MAGIC **Verify** the out-of-band property is untouched.

# COMMAND ----------

assert report.any_failures is False
assert inspector.properties_of("customers").get("delta.logRetentionDuration") == "interval 7 days"
print("Act 3f verified: undeclared property left untouched (declared-subset).")
# COMMAND ----------

# MAGIC %md
# MAGIC ## Act 4 — Validation blocks unsafe changes
# MAGIC
# MAGIC The engine refuses changes that would silently break data or require a
# MAGIC rewrite. Each block runs *before* any SQL executes, so the live table is
# MAGIC left untouched. We use a dedicated, foreign-key-free `events` table so each
# MAGIC failure is a clean `VALIDATION_FAILED` (a foreign-key failure would
# MAGIC otherwise take precedence and mask it).

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


events_baseline_columns = [
    Column("event_id", Long(), nullable=False, primary_key=True),
    Column("event_date", Date()),
    Column("amount", Decimal(12, 2)),
    Column("created_at", Timestamp()),
]

registry = Registry()
registry.register(define_events(columns=events_baseline_columns))
report = engine.sync(registry)
print(report)
print(report.diff())

assert report.any_failures is False
assert inspector.partitions_of("events") == ("event_date",)
print("events baseline created.")

# COMMAND ----------

# MAGIC %md
# MAGIC ### Block 1 — Adding a NOT NULL column
# MAGIC
# MAGIC A new `NOT NULL` column has no value for existing rows. Add it as nullable,
# MAGIC backfill, then tighten outside the engine.

# COMMAND ----------

try:
    registry = Registry()
    registry.register(
        define_events(
            columns=[*events_baseline_columns, Column("region", String(), nullable=False)]
        )
    )
    engine.sync(registry)
    raise AssertionError("expected SyncFailedError")
except SyncFailedError as error:
    [table_report] = error.report.table_reports
    print(table_report.status.value)
    for failure in table_report.all_failures:
        print("\n".join(failure.format_lines()))
    assert table_report.status is TableRunStatus.VALIDATION_FAILED
    assert table_report.all_failures

# Verify nothing was applied.
assert "region" not in inspector.fields_of("events")
print("Block 1 verified: NOT NULL add blocked, table untouched.")

# COMMAND ----------

# MAGIC %md
# MAGIC ### Block 2 — Tightening a column to NOT NULL
# MAGIC
# MAGIC Existing nulls would violate the new constraint. Backfill and tighten
# MAGIC out of band instead.

# COMMAND ----------

try:
    registry = Registry()
    registry.register(
        define_events(
            columns=[
                Column("event_id", Long(), nullable=False, primary_key=True),
                Column("event_date", Date(), nullable=False),  # tightened
                Column("amount", Decimal(12, 2)),
                Column("created_at", Timestamp()),
            ]
        )
    )
    engine.sync(registry)
    raise AssertionError("expected SyncFailedError")
except SyncFailedError as error:
    [table_report] = error.report.table_reports
    print(table_report.status.value)
    for failure in table_report.all_failures:
        print("\n".join(failure.format_lines()))
    assert table_report.status is TableRunStatus.VALIDATION_FAILED
    assert table_report.all_failures

# Verify nothing was applied.
assert inspector.fields_of("events")["event_date"].nullable is True
print("Block 2 verified: tightening blocked, event_date still nullable.")

# COMMAND ----------

# MAGIC %md
# MAGIC ### Block 3 — Changing a column's data type
# MAGIC
# MAGIC Type changes can corrupt or fail to cast existing data. Recreate the table
# MAGIC out of band if a type must change.

# COMMAND ----------

try:
    registry = Registry()
    registry.register(
        define_events(
            columns=[
                Column("event_id", Long(), nullable=False, primary_key=True),
                Column("event_date", Date()),
                Column("amount", Double()),  # was Decimal(12, 2)
                Column("created_at", Timestamp()),
            ]
        )
    )
    engine.sync(registry)
    raise AssertionError("expected SyncFailedError")
except SyncFailedError as error:
    [table_report] = error.report.table_reports
    print(table_report.status.value)
    for failure in table_report.all_failures:
        print("\n".join(failure.format_lines()))
    assert table_report.status is TableRunStatus.VALIDATION_FAILED
    assert table_report.all_failures

# Verify nothing was applied — amount is still a decimal.
assert isinstance(inspector.fields_of("events")["amount"].dataType, T.DecimalType)
print("Block 3 verified: type change blocked, amount still Decimal.")

# COMMAND ----------

# MAGIC %md
# MAGIC ### Block 4 — Changing the partitioning
# MAGIC
# MAGIC Partitioning is fixed at creation; changing it requires a rewrite. The
# MAGIC engine refuses to alter it in place.

# COMMAND ----------

try:
    registry = Registry()
    registry.register(define_events(columns=events_baseline_columns, partitioned_by=()))
    engine.sync(registry)
    raise AssertionError("expected SyncFailedError")
except SyncFailedError as error:
    [table_report] = error.report.table_reports
    print(table_report.status.value)
    for failure in table_report.all_failures:
        print("\n".join(failure.format_lines()))
    assert table_report.status is TableRunStatus.VALIDATION_FAILED
    assert table_report.all_failures

# Verify nothing was applied — still partitioned by event_date.
assert inspector.partitions_of("events") == ("event_date",)
print("Block 4 verified: partitioning change blocked, partitions unchanged.")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Act 5 — A foreign key that cannot be resolved
# MAGIC
# MAGIC `line_items` references `products`, which is not in the registry. The engine
# MAGIC resolves foreign keys before executing, so the table is never created.

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
            references=f"{CATALOG}.{SCHEMA}.products",  # never registered
            referenced_columns=("product_id",),
        )
    ],
)

try:
    registry = Registry()
    registry.register(line_items)
    engine.sync(registry)
    raise AssertionError("expected SyncFailedError")
except SyncFailedError as error:
    [table_report] = error.report.table_reports
    print(table_report.status.value)
    for failure in table_report.all_failures:
        print("\n".join(failure.format_lines()))
    assert table_report.status is TableRunStatus.FOREIGN_KEY_FAILED
    assert table_report.all_failures

# Verify nothing was applied — the table was never created.
assert spark.catalog.tableExists(inspector.fqname("line_items")) is False
print("Act 5 verified: unresolved foreign key blocked, no table created.")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Act 6 — Preview with a dry run, then commit
# MAGIC
# MAGIC A dry run plans and validates but executes nothing — the standard way to
# MAGIC preview a change before committing it. We add a nullable `phone` column,
# MAGIC preview it, confirm the catalog is unchanged, then sync for real.

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
        Column("phone", String()),  # new
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
print(preview)
print(preview.diff())

# COMMAND ----------

# MAGIC %md
# MAGIC **Verify** the dry run executed nothing and `phone` is not yet in the table.

# COMMAND ----------

assert all(table_report.execution is None for table_report in preview)
assert "phone" not in inspector.fields_of("customers")
print("Dry run verified: plan shown, nothing executed.")

# COMMAND ----------

# MAGIC %md
# MAGIC Now commit the same registry for real.

# COMMAND ----------

report = engine.sync(registry)
print(report)
print(report.diff())

assert report.any_failures is False
assert "phone" in inspector.fields_of("customers")
print("Act 6 verified: previewed change committed.")
inspector.display_schema("customers")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Act 7 — Teardown
# MAGIC
# MAGIC Drop the demo tables. The schema is left in place — the engine never owned
# MAGIC it, and dropping it is out of scope.

# COMMAND ----------

for _table in DEMO_TABLES:
    spark.sql(f"DROP TABLE IF EXISTS {CATALOG}.{SCHEMA}.{_table}")
print("Teardown complete: demo tables dropped.")
