"""Immutable result types for planning and execution."""

from __future__ import annotations
from dataclasses import dataclass
from typing import Mapping, Tuple
from tabula.domain.model.actions import ActionPlan

@dataclass(frozen=True, slots=True)
class PlanPreview:
    """
    Preview of a plan:
    - plan: the action plan
    - is_noop: True when no actions are required
    - summary_counts: e.g., {'create_table': 1, 'add_column': 2}
    - summary_text: human text, e.g., 'add_column=2 create_table=1'
    """
    plan: ActionPlan
    is_noop: bool
    summary_counts: Mapping[str, int]
    summary_text: str

@dataclass(frozen=True, slots=True)
class ExecutionOutcome:
    """Adapter-level outcome returned by executors. Truthy on success."""
    success: bool
    messages: Tuple[str, ...] = ()
    executed_count: int = 0
    executed_sql: Tuple[str, ...] = ()
    durations_ms: Tuple[float, ...] = ()

    def __bool__(self) -> bool:
        return self.success

@dataclass(frozen=True, slots=True)
class ExecutionResult:
    """
    Application-level result after executing a plan successfully.
    For failures, the application raises ExecutionFailed.
    """
    plan: ActionPlan
    messages: Tuple[str, ...] = ()
    executed_count: int = 0
