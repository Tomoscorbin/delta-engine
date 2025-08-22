"""Application ports (adapter interfaces). No business logic here."""

from __future__ import annotations

from typing import Protocol, runtime_checkable

from tabula.application.results import ExecutionOutcome
from tabula.domain.plan.actions import ActionPlan
from tabula.domain.model import (
    QualifiedName,
    ObservedTable,
)


@runtime_checkable
class CatalogReader(Protocol):
    """Reads current catalog state. Must be side-effect free and consistent for a single call."""

    def fetch_state(self, qualified_name: QualifiedName) -> ObservedTable | None: ...


@runtime_checkable
class PlanExecutor(Protocol):
    """
    Executes an ActionPlan against a backing engine (e.g., Delta).
    Policy:
    - Must be idempotent per action (safe to re-run).
    - Returns ExecutionOutcome; truthiness reflects success.
    - Should not raise for partial failures; surface them in the outcome.
      (Application layer may raise based on outcome.)
    """

    def execute(self, plan: ActionPlan) -> ExecutionOutcome: ...
