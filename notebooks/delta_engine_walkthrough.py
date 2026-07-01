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
spark.sql(f"CREATE SCHEMA IF NOT EXISTS {CATALOG}.{SCHEMA}")

# COMMAND ----------

# MAGIC %md
# MAGIC ### Verification helpers
# MAGIC
# MAGIC These read the live catalog with the same surfaces the engine's own reader
# MAGIC uses, so assertions reflect real Unity Catalog state. Defined once here and
# MAGIC reused by every verification cell.

# COMMAND ----------

def fqname(table):
    """Fully qualified name for a demo table."""
    return f"{CATALOG}.{SCHEMA}.{table}"


def fields_of(table):
    """Map of column name to StructField for the live table."""
    return {field.name: field for field in spark.table(fqname(table)).schema.fields}


def partitions_of(table):
    """Tuple of partition column names, in catalog order."""
    columns = spark.catalog.listColumns(fqname(table))
    return tuple(column.name for column in columns if column.isPartition)


def properties_of(table):
    """Delta table properties as a plain dict (from DESCRIBE DETAIL)."""
    row = spark.sql(f"DESCRIBE DETAIL {fqname(table)}").first()
    return dict(row["properties"]) if row else {}


def tags_of(table):
    """Unity Catalog tags as a plain dict, read from information_schema."""
    rows = spark.sql(
        f"SELECT tag_name, tag_value FROM {CATALOG}.information_schema.table_tags"
        f" WHERE schema_name = '{SCHEMA}' AND table_name = '{table}'"
    ).collect()
    return {row.tag_name: row.tag_value for row in rows}


def has_primary_key(table):
    """Return True if the live table has a primary key constraint."""
    rows = spark.sql(
        f"SELECT 1 FROM {CATALOG}.information_schema.table_constraints"
        f" WHERE table_schema = '{SCHEMA}' AND table_name = '{table}'"
        f" AND constraint_type = 'PRIMARY KEY'"
    ).collect()
    return len(rows) > 0


def has_foreign_key(table):
    """Return True if the live table is the child of a foreign key constraint."""
    rows = spark.sql(
        f"SELECT 1 FROM {CATALOG}.information_schema.referential_constraints AS rc"
        f" JOIN {CATALOG}.information_schema.key_column_usage AS kcu"
        f" USING (constraint_catalog, constraint_schema, constraint_name)"
        f" WHERE kcu.table_schema = '{SCHEMA}' AND kcu.table_name = '{table}'"
    ).collect()
    return len(rows) > 0


def table_comment(table):
    """Return the live table comment, or empty string."""
    return spark.catalog.getTable(fqname(table)).description or ""


def column_comment(table, column):
    """Return the live comment on one column, or empty string."""
    return fields_of(table)[column].metadata.get("comment", "")


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

assert spark.catalog.tableExists(fqname("customers"))
assert spark.catalog.tableExists(fqname("orders"))

customer_fields = fields_of("customers")
assert set(customer_fields) == {"id", "name", "legacy_code", "status"}
assert customer_fields["id"].nullable is False
assert customer_fields["status"].nullable is False
assert has_primary_key("customers")

assert partitions_of("orders") == ("order_date",)
assert has_primary_key("orders")
assert has_foreign_key("orders")

print("Act 1 verified: both tables created with expected schema and constraints.")

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

customer_fields = fields_of("customers")
assert "email" in customer_fields and customer_fields["email"].nullable is True
assert "legacy_code" not in customer_fields
assert tags_of("customers") == {"domain": "sales", "owner": "data-eng"}
assert properties_of("customers").get("delta.enableChangeDataFeed") == "true"
assert table_comment("customers") == "Customer master table (with contact details)"
assert column_comment("customers", "name") == "Full legal name"
print("Act 3a verified: batched add/drop/tags/property/comments applied together.")

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
assert fields_of("customers")["status"].nullable is True
print("Act 3b verified: nullability loosened (NOT NULL dropped).")

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
assert tags_of("customers") == {"domain": "sales"}
print("Act 3c verified: undeclared tag removed by full-state reconciliation.")

# COMMAND ----------

# MAGIC %md
# MAGIC ### Act 3d — Foreign-key drift on an existing table (own sync)
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
assert spark.catalog.tableExists(fqname("regions"))

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
customer_fields = fields_of("customers")
assert "region_id" in customer_fields and customer_fields["region_id"].nullable is True
assert has_foreign_key("customers")
print("Act 3d verified: foreign key added to an existing table.")

# COMMAND ----------

# MAGIC %md
# MAGIC ### Act 3e — Properties are declared-subset (own sync)
# MAGIC
# MAGIC Set a property **out-of-band**, then resync `customers` without declaring
# MAGIC it. The engine manages only declared property keys, so the out-of-band
# MAGIC property survives. This is the deliberate contrast with full-state tags.

# COMMAND ----------

spark.sql(
    f"ALTER TABLE {fqname('customers')}"
    f" SET TBLPROPERTIES ('delta.logRetentionDuration' = 'interval 7 days')"
)

# Re-declare customers exactly as in Act 3d (logRetentionDuration is NOT declared).
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
assert properties_of("customers").get("delta.logRetentionDuration") == "interval 7 days"
print("Act 3e verified: undeclared property left untouched (declared-subset).")

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
assert partitions_of("events") == ("event_date",)
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
assert "region" not in fields_of("events")
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
assert fields_of("events")["event_date"].nullable is True
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
assert isinstance(fields_of("events")["amount"].dataType, T.DecimalType)
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
assert partitions_of("events") == ("event_date",)
print("Block 4 verified: partitioning change blocked, partitions unchanged.")
