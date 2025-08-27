"""Application ports (adapter interfaces). No business logic here."""

from __future__ import annotations

from typing import Protocol, Callable, Iterable, Any, runtime_checkable

from delta_engine.application.results import TableExecutionReport
from delta_engine.domain.model import (
    ObservedTable,
    QualifiedName,
)
from delta_engine.domain.plan.actions import ActionPlan


@runtime_checkable
class CatalogStateReader(Protocol):
    """Reads current catalog state."""

    def fetch_state(self, qualified_name: QualifiedName) -> ObservedTable | None:
        ...


@runtime_checkable
class PlanExecutor(Protocol):
    """Executes an action plan against a backing engine."""

    def execute(self, plan: ActionPlan) -> TableExecutionReport:
        """Run the plan and return the execution outcome."""
        ...


@runtime_checkable
class TableObject(Protocol):
    catalog: str | None
    schema: str | None
    name: str
    columns: Iterable[Any]

@runtime_checkable
class ColumnObject(Protocol):
    name: str
    data_type: Any
    is_nullable: bool
