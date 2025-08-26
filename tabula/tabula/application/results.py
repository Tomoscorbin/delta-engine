from __future__ import annotations

from collections.abc import Mapping
from dataclasses import dataclass, field

from tabula.application.validation import ValidationFailure
from tabula.domain.plan import ActionPlan


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


@dataclass(frozen=True, slots=True)
class ExecutionReport:
    """Per-table execution report from the executor."""

    fully_qualified_name: str
    messages: tuple[str, ...] = ()
    executed_sql: tuple[str, ...] = ()
    executed_count: int = 0
    failed_sql: tuple[str, ...] = ()
    failed_count: int = 0

    @property
    def has_failures(self) -> bool:
        return self.failed_count > 0


@dataclass(frozen=True, slots=True)
class SyncPreview:
    """Engine dry-run: what would be done, grouped by table."""

    previews: tuple[PlanPreview, ...] = ()
    failures_by_table: Mapping[str, tuple[ValidationFailure, ...]] = field(default_factory=dict)


@dataclass(frozen=True, slots=True)
class SyncReport:
    """Engine real-run: what actually ran."""

    previews: tuple[PlanPreview, ...]
    executions: tuple[ExecutionReport, ...]
