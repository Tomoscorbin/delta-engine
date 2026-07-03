"""
Application layer: the engine and the public outcome types.

These are the names a library consumer depends on -- pass tables to an `Engine`
and call `sync`, and handle the `SyncReport` it returns or the `SyncFailedError`
it raises (whose failures render via `Failure.format_line`). The per-phase
result types (`CatalogState`, `ExecutionSummary`, `TableRunReport`, ...) remain
internal.
"""

from delta_engine.application.engine import Engine
from delta_engine.application.errors import SyncFailedError
from delta_engine.application.failures import Failure
from delta_engine.application.report import SyncReport, TableRunStatus

__all__ = ["Engine", "Failure", "SyncFailedError", "SyncReport", "TableRunStatus"]
