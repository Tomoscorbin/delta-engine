"""
delta-engine: declarative schema management for Delta Lake tables.

This is the curated runtime entry point. The preferred user imports are::

    from delta_engine.schema import DeltaTable, Column, Integer
    from delta_engine.databricks import build_engine
    from delta_engine import Engine

Only backend-neutral runtime types live here. Schema declarations belong in
``delta_engine.schema`` and Databricks helpers belong in ``delta_engine.databricks``.
"""

from delta_engine.application import (
    Engine,
    Failure,
    SyncFailedError,
    SyncReport,
    TableRunStatus,
)

__all__ = [
    "Engine",
    "Failure",
    "SyncFailedError",
    "SyncReport",
    "TableRunStatus",
]
