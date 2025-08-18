"""Immutable result types for planning and execution."""

from __future__ import annotations

from collections.abc import Mapping
from dataclasses import dataclass

from tabula.domain.plan.actions import ActionPlan


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
    total_actions: int


@dataclass(frozen=True, slots=True)
class ExecutionOutcome:
    """Adapter-level outcome returned by executors. Truthy on success."""

    success: bool
    messages: tuple[str, ...] = ()
    executed_count: int = 0
    executed_sql: tuple[str, ...] = ()
    durations_ms: tuple[float, ...] = ()

    def __bool__(self) -> bool:
        return self.success


@dataclass(frozen=True, slots=True)
class ExecutionResult:
    """
    Application-level result after executing a plan successfully.
    For failures, the application raises ExecutionFailed.
    """

    plan: ActionPlan
    messages: tuple[str, ...] = ()
    executed_count: int = 0
