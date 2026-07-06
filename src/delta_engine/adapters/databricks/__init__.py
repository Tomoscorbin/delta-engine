"""
Databricks/Spark adapter package.

Import concrete modules directly (``factory``, ``log_config``, ``reader``,
``executor``); the public user-facing entry points live in
:mod:`delta_engine.databricks`. This ``__init__`` stays empty so importing
the package never pulls PySpark.
"""
