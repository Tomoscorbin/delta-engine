"""Application ports / adapter interfaces."""

from __future__ import annotations

from typing import Protocol, runtime_checkable

from delta_engine.application.results import CatalogState, ExecutionSummary
from delta_engine.domain.model import QualifiedName
from delta_engine.domain.plan.actions import ActionPlan


@runtime_checkable
class CatalogStateReader(Protocol):
    """Reads current catalog state."""

    def fetch_state(self, qualified_name: QualifiedName) -> CatalogState:
        """
        Return the table's current state: present, absent, or unreadable.

        Args:
            qualified_name: Fully qualified object name to look up.

        """
        ...


@runtime_checkable
class PlanExecutor(Protocol):
    """Executes an action plan against a backing engine."""

    def execute(self, target: QualifiedName, plan: ActionPlan) -> ExecutionSummary:
        """Run the plan against ``target`` and return the execution outcome."""
        ...
