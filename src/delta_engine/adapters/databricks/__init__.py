"""
Databricks adapter package.

The shared, PySpark-free SQL core lives in ``sql`` (DDL compilation, identifier
quoting, information_schema queries, type rendering). The Spark backend lives
in ``spark``. ``log_config`` is shared by both. The public user-facing entry
points live in :mod:`delta_engine.databricks`. This ``__init__`` stays empty so
importing the package never pulls PySpark.
"""
