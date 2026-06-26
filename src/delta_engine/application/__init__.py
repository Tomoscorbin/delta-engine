"""
Application layer: the engine and the public outcome types.

These are the names a library consumer depends on -- register tables in a
`Registry`, construct an `Engine`, call `sync`, and handle the `SyncReport` it
returns or the `SyncFailedError` it raises (whose failures render via
`Failure.format_line`). The per-phase result types (`CatalogState`,
`ExecutionSummary`, `TableRunReport`, ...) remain internal.
"""

from delta_engine.application.engine import Engine
from delta_engine.application.errors import SyncFailedError
from delta_engine.application.registry import Registry
from delta_engine.application.results import Failure, SyncReport, TableRunStatus

__all__ = ["Engine", "Failure", "Registry", "SyncFailedError", "SyncReport", "TableRunStatus"]
