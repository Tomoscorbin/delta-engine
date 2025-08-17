from __future__ import annotations
from dataclasses import dataclass
from typing import Tuple
from tabula.domain.model.actions import ActionPlan

@dataclass(frozen=True, slots=True)
class PlanPreview:
    plan: ActionPlan
    is_noop: bool
    summary: str  # e.g., "create=1 add=2 drop=0"

@dataclass(frozen=True, slots=True)
class ExecutionResult:
    plan: ActionPlan
    success: bool
    messages: Tuple[str, ...]

@dataclass(frozen=True, slots=True)
class ExecutionOutcome:
    success: bool
    messages: Tuple[str, ...] = ()
    executed_count: int = 0  # number of actions executed