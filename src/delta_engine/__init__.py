"""
delta-engine: declarative schema management for Delta Lake tables.

This is the curated compatibility entry point. The preferred user imports are::

    from delta_engine.schema import DeltaTable, Column, Integer
    from delta_engine.databricks import build_engine
    from delta_engine import Engine

The schema and engine surface (defining tables, the engine and its result types)
is still available here for compatibility. ``build_databricks_engine`` and
``configure_logging`` are exposed here too but imported lazily, so ``import
delta_engine`` never requires pyspark -- tables can be defined and planned
without a Spark install.
"""

from typing import TYPE_CHECKING

from delta_engine.api import (
    Array,
    Boolean,
    Column,
    Date,
    Decimal,
    DeltaTable,
    Double,
    Float,
    ForeignKey,
    Integer,
    Long,
    Map,
    Property,
    Self,
    String,
    Timestamp,
)
from delta_engine.application import (
    Engine,
    Failure,
    SyncFailedError,
    SyncReport,
    TableRunStatus,
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
    "ForeignKey",
    "Integer",
    "Long",
    "Map",
    "Property",
    "Self",
    "String",
    "SyncFailedError",
    "SyncReport",
    "TableRunStatus",
    "Timestamp",
    "build_databricks_engine",
    "configure_logging",
]


def __getattr__(name: str) -> object:
    """Resolve the pyspark-bound exports on first access (PEP 562)."""
    if name in _LAZY_EXPORTS:
        from delta_engine.adapters import databricks

        return getattr(databricks, name)
    raise AttributeError(f"module {__name__!r} has no attribute {name!r}")
