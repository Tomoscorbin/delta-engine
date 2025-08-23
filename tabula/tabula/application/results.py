from __future__ import annotations

"""Result objects returned by application orchestration."""

from collections.abc import Mapping
from dataclasses import dataclass

from tabula.domain.plan.actions import ActionPlan


@dataclass(frozen=True, slots=True)
class PlanPreview:
    """Summary of a planned set of actions.

    Attributes:
        plan: Ordered action plan.
        is_noop: Whether the plan has no actions.
        summary_counts: Mapping of action type name to counts.
        total_actions: Total number of actions.
    """

    plan: ActionPlan
    is_noop: bool
    summary_counts: Mapping[str, int]
    total_actions: int

    def __bool__(self) -> int:
        return not self.is_noop

    @property
    def summary_text(self) -> str:
        """Human-readable summary of action counts."""

        if not self.summary_counts:
            return ""
        parts = [f"{k}={self.summary_counts[k]}" for k in sorted(self.summary_counts)]
        return " ".join(parts)


@dataclass(frozen=True, slots=True)
class ExecutionOutcome:
    """Outcome returned by a plan executor.

    Attributes:
        success: Whether execution completed without errors.
        messages: Messages reported by the executor.
        executed_count: Number of statements successfully executed.
        executed_sql: SQL statements that were run.
    """

    success: bool
    messages: tuple[str, ...] = ()
    executed_count: int = 0
    executed_sql: tuple[str, ...] = ()

    def __post_init__(self) -> None:
        object.__setattr__(self, "messages", tuple(self.messages))
        object.__setattr__(self, "executed_sql", tuple(self.executed_sql))

    def __bool__(self) -> bool:
        return self.success


@dataclass(frozen=True, slots=True)
class ExecutionResult:
    """Result returned by the ``plan_then_execute`` orchestration."""

    plan: ActionPlan
    messages: tuple[str, ...] = ()
    executed_count: int = 0

    def __post_init__(self) -> None:
        object.__setattr__(self, "messages", tuple(self.messages))
