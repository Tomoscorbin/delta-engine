from __future__ import annotations
from dataclasses import dataclass, asdict
from enum import StrEnum
from typing import Mapping, Iterable
from datetime import datetime
from delta_engine.domain.plan import ActionPlan


# ---- Enums ------------------------------------------------------------------

class ActionStatus(StrEnum):
    OK = "OK"
    FAILED = "FAILED"
    SKIPPED_DEPENDENCY = "SKIPPED_DEPENDENCY"
    NOOP = "NOOP"

class ValidationStatus(StrEnum):
    PASSED = "PASSED"
    FAILED = "FAILED"

# ---- Records ----------------------------------------------------------------

@dataclass(frozen=True, slots=True)
class ValidationFailure:
    rule_name: str
    message: str

@dataclass(frozen=True, slots=True)
class ExecutionFailure:
    fully_qualified_name: str
    action_index: int
    exception_type: str
    message: str
    statement_preview: str

@dataclass(frozen=True, slots=True)
class ActionResult:
    name: str
    action_index: int
    status: ActionStatus
    statement_preview: str
    failure: ExecutionFailure | None = None

@dataclass(frozen=True, slots=True)
class TableExecutionReport:
    fully_qualified_name: str
    total_actions: int
    results: tuple[ActionResult, ...]

    @property
    def status(self) -> ActionStatus:
        if any(r.status == ActionStatus.FAILED for r in self.results):
            return ActionStatus.FAILED
        if all(r.status == ActionStatus.NOOP for r in self.results) or self.total_actions == 0:
            return ActionStatus.NOOP
        return ActionStatus.OK
    
@dataclass(frozen=True, slots=True)
class TableValidationReport:
    fully_qualified_name: str
    failures: tuple[ValidationFailure, ...]

    @property
    def status(self) -> ValidationStatus:
        return ValidationStatus.FAILED if self.failures else ValidationStatus.PASSED


@dataclass(frozen=True, slots=True)
class RunReport:
    run_id: str
    started_at: str
    ended_at: str
    executions: Mapping[str, TableExecutionReport]

    def any_failures(self) -> bool:
        return any(t.status == ActionStatus.FAILED for t in self.executions.values())

    def failures(self) -> list[ExecutionFailure]:
        out: list[ExecutionFailure] = []
        for t in self.executions.values():
            for r in t.results:
                if r.failure:
                    out.append(r.failure)
        return out

    
@dataclass(frozen=True, slots=True)
class ValidationRunReport:
    run_id: str
    validations: Mapping[str, TableValidationReport]

    def any_failures(self) -> bool:
        return any(t.status == ValidationStatus.FAILED for t in self.validations.values())

    def failures(self) -> list[ValidationFailure]:
        out: list[ValidationFailure] = []
        for t in self.validations.values():
            out.extend(t.failures)
        return out


@dataclass(frozen=True, slots=True)
class PlanPreview:
    """Per-table, ordered plan summary (also carries validation failures)."""

    plan: ActionPlan
    is_noop: bool
    summary_counts: Mapping[str, int]
    total_actions: int
    failures: tuple[ValidationFailure, ...] = ()

    @property
    def summary(self) -> str:
        if not self.summary_counts:
            return ""
        parts = [f"{k}={self.summary_counts[k]}" for k in sorted(self.summary_counts)]
        return " ".join(parts)
