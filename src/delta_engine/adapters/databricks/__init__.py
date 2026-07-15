"""
Databricks adapter package.

Two backends live here: ``spark`` syncs through an active Spark session, and
``warehouse`` syncs through a Databricks SQL warehouse connection over
``databricks-sql-connector``. Both build on shared modules: ``sql`` is the
PySpark-free SQL core (DDL compilation, identifier quoting, information_schema
queries, type rendering); ``read`` is the observed-table assembly both readers
build state through and ``execution`` the statement-execution loop both
executors run compiled SQL through; ``errors`` names failed exceptions,
preferring the Java class when py4j wraps a JVM exception; and
``log_config`` is the shared colored-logging setup.
The public user-facing entry points live in :mod:`delta_engine.databricks`.
This ``__init__`` stays empty so importing the package never pulls PySpark
or the SQL connector.
"""
