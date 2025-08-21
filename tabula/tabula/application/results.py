"""Immutable result types for planning and execution."""

from __future__ import annotations

from collections.abc import Mapping
from dataclasses import dataclass

from tabula.domain.plan.actions import ActionPlan


@dataclass(frozen=True, slots=True)
class PlanPreview:
    """Preview of an action plan."""

    plan: ActionPlan
    is_noop: bool
    summary_counts: Mapping[str, int]
    total_actions: int

    def __len__(self) -> int:
        return self.total_actions

    @property
    def summary_text(self) -> str:
        """
        Human-readable summary like:
        'add_column=2 create_table=1' (keys sorted ascending).
        """
        if not self.summary_counts:
            return ""
        parts = [f"{k}={self.summary_counts[k]}" for k in sorted(self.summary_counts)]
        return " ".join(parts)


@dataclass(frozen=True, slots=True)
class ExecutionOutcome:
    """Adapter-level outcome returned by executors. Truthy on success."""

    success: bool
    messages: tuple[str, ...] = ()
    executed_count: int = 0
    executed_sql: tuple[str, ...] = ()

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
