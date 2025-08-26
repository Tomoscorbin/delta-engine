"""Application ports (adapter interfaces). No business logic here."""

from __future__ import annotations

from typing import Protocol, runtime_checkable

from tabula.application.results import ExecutionReport
from tabula.domain.model import (
    ObservedTable,
    QualifiedName,
)
from tabula.domain.plan.actions import ActionPlan


@runtime_checkable
class CatalogStateReader(Protocol):
    """Reads current catalog state."""

    def fetch_state(self, qualified_name: QualifiedName) -> ObservedTable | None:
        ...


@runtime_checkable
class PlanExecutor(Protocol):
    """Executes an action plan against a backing engine."""

    def execute(self, plan: ActionPlan) -> ExecutionReport:
        """Run the plan and return the execution outcome."""
        ...
