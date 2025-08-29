"""Application ports (adapter interfaces). No business logic here."""

from __future__ import annotations

from collections.abc import Iterable
from typing import Any, Protocol, runtime_checkable

from delta_engine.application.results import CatalogReadResult, ExecutionResult
from delta_engine.domain.model import (
    QualifiedName,
)
from delta_engine.domain.plan.actions import ActionPlan


@runtime_checkable
class CatalogStateReader(Protocol):
    """Reads current catalog state."""

    def fetch_state(self, qualified_name: QualifiedName) -> CatalogReadResult:
        """Return the observed definition for ``qualified_name`` or ``None``.

        Args:
            qualified_name: Fully qualified object name to look up.

        Returns:
            The observed table definition if it exists, otherwise ``None``.

        """
        ...


@runtime_checkable
class PlanExecutor(Protocol):
    """Executes an action plan against a backing engine."""

    def execute(self, plan: ActionPlan) -> ExecutionResult:
        """Run the plan and return the execution outcome."""
        ...


@runtime_checkable
class TableObject(Protocol):
    """Lightweight table specification accepted by the registry."""

    catalog: str | None
    schema: str | None
    name: str
    columns: Iterable[Any]


@runtime_checkable
class ColumnObject(Protocol):
    """Lightweight column specification accepted by the registry."""

    name: str
    data_type: Any
    is_nullable: bool
