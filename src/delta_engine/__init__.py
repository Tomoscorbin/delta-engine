"""
delta-engine: declarative schema management for Delta Lake tables.

This is the curated runtime entry point. The preferred user imports are::

    from delta_engine.schema import DeltaTable, Column, Integer
    from delta_engine.databricks import build_spark_engine
    from delta_engine import Engine

Only backend-neutral runtime types live here. Schema declarations belong in
``delta_engine.schema`` and Databricks helpers belong in ``delta_engine.databricks``.
"""

from importlib.metadata import PackageNotFoundError, version as _version

from delta_engine.application import (
    DuplicateTableDefinitionError,
    Engine,
    ExecutionFailure,
    Failure,
    FailurePhase,
    ForeignKeyFailure,
    ReadFailure,
    SyncFailedError,
    SyncReport,
    TableRunReport,
    TableRunStatus,
    ValidationFailure,
    render_diff,
    render_report,
)

try:
    __version__ = _version("delta-engine")
except PackageNotFoundError:  # running from a source tree that is not installed
    __version__ = "0.0.0"

# `__version__` is package metadata, not part of the runtime API surface, so it
# is intentionally not advertised in `__all__`; access it as `delta_engine.__version__`.
__all__ = [
    "DuplicateTableDefinitionError",
    "Engine",
    "ExecutionFailure",
    "Failure",
    "FailurePhase",
    "ForeignKeyFailure",
    "ReadFailure",
    "SyncFailedError",
    "SyncReport",
    "TableRunReport",
    "TableRunStatus",
    "ValidationFailure",
    "render_diff",
    "render_report",
]
