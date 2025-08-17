from __future__ import annotations
from typing import Optional, Protocol, runtime_checkable
from tabula.domain.model.full_name import FullName
from tabula.domain.model.table_state import TableState
from tabula.domain.model.actions import ActionPlan
from tabula.application.results import ExecutionOutcome

@runtime_checkable
class CatalogReader(Protocol):
    def fetch_state(self, full_name: FullName) -> Optional[TableState]:
        """Return TableState, or None if the table does not exist."""

@runtime_checkable
class PlanExecutor(Protocol):
    def execute(self, plan: ActionPlan) -> ExecutionOutcome:
        """Execute actions in order; should be idempotent where possible."""
