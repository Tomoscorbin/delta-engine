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
