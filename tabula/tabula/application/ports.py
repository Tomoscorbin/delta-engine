"""Application ports (adapter interfaces). No business logic here."""

from __future__ import annotations

from typing import Protocol, runtime_checkable

from tabula.application.results import ExecutionOutcome
from tabula.domain.model import (
    ObservedTable,
    QualifiedName,
)
from tabula.domain.plan.actions import ActionPlan


@runtime_checkable
class CatalogReader(Protocol):
    """Reads current catalog state."""

    def fetch_state(self, qualified_name: QualifiedName) -> ObservedTable | None:
        """Return observed table state for the given name."""


@runtime_checkable
class PlanExecutor(Protocol):
    """Executes an action plan against a backing engine."""

    def execute(self, plan: ActionPlan) -> ExecutionOutcome:
        """Run the plan and return the execution outcome."""
