"""
Application layer: the engine and outcome types.

The root ``delta_engine`` package re-exports this runtime surface for library
users. The per-phase result types (`CatalogState`, `ExecutionSummary`,
`TableRunReport`, ...) remain internal.
"""

from delta_engine.application.engine import Engine
from delta_engine.application.errors import SyncFailedError
from delta_engine.application.failures import Failure
from delta_engine.application.report import SyncReport, TableRunStatus

__all__ = ["Engine", "Failure", "SyncFailedError", "SyncReport", "TableRunStatus"]
