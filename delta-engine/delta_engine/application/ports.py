"""Application ports / adapter interfaces."""

from __future__ import annotations

from collections.abc import Iterable, Mapping
from typing import Any, Protocol, runtime_checkable

from delta_engine.application.results import ExecutionResult, ReadResult
from delta_engine.domain.model import QualifiedName
from delta_engine.domain.plan.actions import ActionPlan


@runtime_checkable
class CatalogStateReader(Protocol):
    """Reads current catalog state."""

    def fetch_state(self, qualified_name: QualifiedName) -> ReadResult:
        """
        Return the result of an attempted catalog read against a table.

        Args:
            qualified_name: Fully qualified object name to look up.

        """
        ...


@runtime_checkable
class PlanExecutor(Protocol):
    """Executes an action plan against a backing engine."""

    def execute(self, plan: ActionPlan) -> tuple[ExecutionResult, ...]:
        """Run the plan and return the execution outcome."""
        ...


@runtime_checkable
class TableObject(Protocol):
    """Lightweight table specification accepted by the registry."""

    catalog: str
    schema: str
    name: str
    columns: Iterable[ColumnObject]
    properties: Mapping[str, str] | None


@runtime_checkable
class ColumnObject(Protocol):
    """Lightweight column specification accepted by the registry."""

    name: str
    data_type: Any
    is_nullable: bool
