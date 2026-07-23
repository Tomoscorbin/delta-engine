# Databricks notebook source
# MAGIC %md
# MAGIC # delta-engine release smoke test
# MAGIC
# MAGIC Install the candidate wheel before running this notebook:
# MAGIC
# MAGIC ```python
# MAGIC %pip install --force-reinstall /Workspace/path/to/delta_engine-X.Y.Z.whl
# MAGIC dbutils.library.restartPython()
# MAGIC ```
# MAGIC
# MAGIC Set `catalog` and `schema` to a disposable Unity Catalog namespace. The
# MAGIC notebook creates one uniquely named table and drops it in `finally`.

# COMMAND ----------

from uuid import uuid4

import delta_engine
from delta_engine.databricks import build_spark_engine
from delta_engine.schema import Column, DeltaTable, Integer, String

# COMMAND ----------

dbutils.widgets.text("catalog", "main")
dbutils.widgets.text("schema", "delta_engine_release_test")

CATALOG = dbutils.widgets.get("catalog")
SCHEMA = dbutils.widgets.get("schema")
TABLE_NAME = f"release_smoke_{uuid4().hex[:8]}"
QUALIFIED_NAME = f"`{CATALOG}`.`{SCHEMA}`.`{TABLE_NAME}`"

runtime = spark.sql("SELECT current_version() AS version").first()["version"]
print(f"delta-engine: {delta_engine.__version__}")
print(f"Databricks version: {runtime}")
print(f"Target: {QUALIFIED_NAME}")

# COMMAND ----------

engine = build_spark_engine(spark)
initial = DeltaTable(
    catalog=CATALOG,
    schema=SCHEMA,
    name=TABLE_NAME,
    columns=[
        Column("id", Integer(), nullable=False),
        Column("name", String()),
    ],
    comment="release smoke: initial",
)
updated = DeltaTable(
    catalog=CATALOG,
    schema=SCHEMA,
    name=TABLE_NAME,
    columns=[
        Column("id", Integer(), nullable=False),
        Column("name", String()),
    ],
    comment="release smoke: updated",
)

try:
    created = engine.sync(initial)
    assert created.has_failures is False
    assert created.has_changes is True

    unchanged = engine.sync(initial)
    assert unchanged.has_failures is False
    assert unchanged.has_changes is False

    altered = engine.sync(updated)
    assert altered.has_failures is False
    assert altered.has_changes is True

    converged = engine.sync(updated)
    assert converged.has_failures is False
    assert converged.has_changes is False

    print("PASS: create, no-op, safe alteration, and convergence succeeded")
finally:
    spark.sql(f"DROP TABLE IF EXISTS {QUALIFIED_NAME}")
    print(f"Cleaned up {QUALIFIED_NAME}")
