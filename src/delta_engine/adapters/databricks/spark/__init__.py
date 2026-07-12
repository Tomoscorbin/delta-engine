"""
Spark backend for the Databricks adapter.

Everything PySpark-coupled lives here: the reader, the executor, and the
engine factory. Import concrete modules directly
(``factory``, ``reader``, ``executor``); the public user-facing
entry points live in :mod:`delta_engine.databricks`. This ``__init__`` stays
empty so importing the package never pulls PySpark.
"""
