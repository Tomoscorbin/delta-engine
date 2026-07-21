"""Interpret the canonical phase outcomes retained for one table run."""

from dataclasses import dataclass
from typing import assert_never

from delta_engine.application.dependency_resolution import ResolutionFailed, TableResolution
from delta_engine.application.failures import Failure, ForeignKeyFailure, ReadFailure
from delta_engine.application.planning import PlanningFailed, PlanningResult
from delta_engine.application.ports import ExecutionSummary, ReadResult


@dataclass(frozen=True, slots=True)
class ExecutionBlockedByDependency:
    """Execution was not attempted because a referenced table failed during execution."""

    failures: tuple[ForeignKeyFailure, ...]

    def __post_init__(self) -> None:
        object.__setattr__(self, "failures", tuple(self.failures))
        if not self.failures:
            raise ValueError("ExecutionBlockedByDependency requires at least one failure")


type ExecutionOutcome = ExecutionSummary | ExecutionBlockedByDependency


def collect_failures(
    *,
    read: ReadResult,
    planning: PlanningResult | None,
    resolution: TableResolution | None,
    execution: ExecutionOutcome | None,
) -> tuple[Failure, ...]:
    """Flatten each phase's canonical outcome into report order."""
    failures: list[Failure] = []

    if isinstance(read, ReadFailure):
        failures.append(read)

    if isinstance(planning, PlanningFailed):
        failures.extend(planning.failures)

    if isinstance(resolution, ResolutionFailed):
        failures.extend(resolution.failures)

    match execution:
        case ExecutionSummary() as summary:
            failures.extend(summary.failures)
        case ExecutionBlockedByDependency(failures=blocked):
            failures.extend(blocked)
        case None:
            pass
        case _ as unreachable:
            assert_never(unreachable)

    return tuple(failures)
