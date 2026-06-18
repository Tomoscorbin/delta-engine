"""
delta-engine: declarative schema management for Delta Lake tables.

This is the curated entry point. The common workflow imports from here directly::

    from delta_engine import DeltaTable, Column, Integer, Registry, build_databricks_engine

The schema and engine surface (defining tables, building a registry, the engine
and its result types) is pyspark-free and eagerly available. ``build_databricks_engine``
and ``configure_logging`` are exposed here too but imported lazily, so
``import delta_engine`` never requires pyspark -- tables can be defined and
planned without a Spark install.
"""

from typing import TYPE_CHECKING

from delta_engine.application import (
    Engine,
    Failure,
    Registry,
    SyncFailedError,
    SyncReport,
)
from delta_engine.schema import (
    Array,
    Boolean,
    Column,
    Date,
    Decimal,
    DeltaTable,
    Double,
    Float,
    Integer,
    Long,
    Map,
    String,
    Timestamp,
)

# Lazily-exposed names. These live in the Databricks adapter, which imports
# pyspark; resolving them here on demand keeps the eager surface above
# importable without a Spark install. See __getattr__ below (PEP 562).
_LAZY_EXPORTS = frozenset({"build_databricks_engine", "configure_logging"})

if TYPE_CHECKING:  # let type checkers / IDEs see the lazy names statically
    from delta_engine.adapters.databricks import build_databricks_engine, configure_logging

__all__ = [
    "Array",
    "Boolean",
    "Column",
    "Date",
    "Decimal",
    "DeltaTable",
    "Double",
    "Engine",
    "Failure",
    "Float",
    "Integer",
    "Long",
    "Map",
    "Registry",
    "String",
    "SyncFailedError",
    "SyncReport",
    "Timestamp",
    "build_databricks_engine",
    "configure_logging",
]


def __getattr__(name: str):
    """Resolve the pyspark-bound exports on first access (PEP 562)."""
    if name in _LAZY_EXPORTS:
        from delta_engine.adapters import databricks

        return getattr(databricks, name)
    raise AttributeError(f"module {__name__!r} has no attribute {name!r}")
