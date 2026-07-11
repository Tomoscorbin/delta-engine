"""
Databricks adapter package.

Two backends live here: ``spark`` syncs through an active Spark session, and
``warehouse`` syncs through a Databricks SQL warehouse connection over
``databricks-sql-connector``. Both build on shared modules: ``sql`` is the
PySpark-free SQL core (DDL compilation, identifier quoting, information_schema
queries, type rendering); ``errors`` is the generic backend-exception
translation, refined per backend (Spark layers its own py4j unwrapping on
top); ``execution`` is the statement-execution loop both executors run
compiled SQL through; and ``log_config`` is the shared colored-logging setup.
The public user-facing entry points live in :mod:`delta_engine.databricks`.
This ``__init__`` stays empty so importing the package never pulls PySpark
or the SQL connector.
"""
