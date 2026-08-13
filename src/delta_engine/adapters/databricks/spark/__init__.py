"""
Spark backend for the Databricks adapter.

Everything PySpark-coupled lives here: the reader, executor, engine factory,
and native schema converter. Import concrete modules directly
(``factory``, ``reader``, ``executor``, ``schema``); the public user-facing
entry points live in :mod:`delta_engine.databricks`. This ``__init__`` stays
empty so importing the package never pulls PySpark.
"""
