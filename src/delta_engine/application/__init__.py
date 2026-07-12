"""
Application layer: the engine and outcome types.

The root ``delta_engine`` package re-exports this runtime surface for library
users. The per-phase result types (`CatalogState`, `ExecutionSummary`, ...)
remain internal, but `TableRunReport` and the concrete failure types are
public so callers can annotate and inspect the objects a `SyncReport` yields.
"""

from delta_engine.application.engine import Engine
from delta_engine.application.errors import DuplicateTableDefinitionError, SyncFailedError
from delta_engine.application.failures import (
    ExecutionFailure,
    Failure,
    FailurePhase,
    ForeignKeyFailure,
    ReadFailure,
    ValidationFailure,
)
from delta_engine.application.rendering import render_diff, render_planned_sql, render_report
from delta_engine.application.report import SyncReport, TableRunReport, TableRunStatus

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
    "render_planned_sql",
    "render_report",
]
