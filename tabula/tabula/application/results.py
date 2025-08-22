from __future__ import annotations
from collections.abc import Mapping
from dataclasses import dataclass
from tabula.domain.plan.actions import ActionPlan

@dataclass(frozen=True, slots=True)
class PlanPreview:
    plan: ActionPlan
    is_noop: bool
    summary_counts: Mapping[str, int]
    total_actions: int

    def __len__(self) -> int:
        return self.total_actions

    @property
    def summary_text(self) -> str:
        if not self.summary_counts:
            return ""
        parts = [f"{k}={self.summary_counts[k]}" for k in sorted(self.summary_counts)]
        return " ".join(parts)


@dataclass(frozen=True, slots=True)
class ExecutionOutcome:
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
    plan: ActionPlan
    messages: tuple[str, ...] = ()
    executed_count: int = 0

    def __post_init__(self) -> None:
        object.__setattr__(self, "messages", tuple(self.messages))
