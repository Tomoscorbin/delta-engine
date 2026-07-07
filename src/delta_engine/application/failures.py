"""
Failure vocabulary for engine runs.

Every way a sync can fail, as one closed family: each failure knows the phase
that produced it (`FailurePhase`) and renders itself as display lines. Reports
derive a table's status from the earliest failing phase, so the family stays
together rather than being scattered across its producers.
"""

from abc import ABC, abstractmethod
from dataclasses import dataclass
from enum import IntEnum, StrEnum
from typing import ClassVar

from delta_engine.domain.model import QualifiedName


class FailurePhase(IntEnum):
    """The sync phase that produced a failure. Ordered so the earliest wins."""

    READ = 1
    VALIDATION = 2
    FOREIGN_KEY = 3
    EXECUTION = 4


class ForeignKeyFailureReason(StrEnum):
    """Why a foreign key constraint could not be applied, failing its whole table."""

    CYCLE = "CYCLE"
    UNRESOLVABLE_REFERENCE = "UNRESOLVABLE_REFERENCE"
    BLOCKED_BY_FAILED_DEPENDENCY = "BLOCKED_BY_FAILED_DEPENDENCY"
    REFERENCED_COLUMNS_NOT_A_KEY = "REFERENCED_COLUMNS_NOT_A_KEY"

    @property
    def detail(self) -> str:
        """Human-readable reason clause for a failure message."""
        match self:
            case ForeignKeyFailureReason.CYCLE:
                return "it is part of a foreign key dependency cycle"
            case ForeignKeyFailureReason.UNRESOLVABLE_REFERENCE:
                return "it references a table that is not registered"
            case ForeignKeyFailureReason.BLOCKED_BY_FAILED_DEPENDENCY:
                return "it references a table that failed to sync"
            case ForeignKeyFailureReason.REFERENCED_COLUMNS_NOT_A_KEY:
                return "its referenced columns are not the primary key of the referenced table"


# ---------- Failure value objects ----------


class Failure(ABC):
    """A failure that can render itself as display lines, tagged with its phase."""

    phase: ClassVar[FailurePhase]

    @abstractmethod
    def format_lines(self) -> tuple[str, ...]:
        """Return one or more human-readable lines describing this failure."""
        ...

    @abstractmethod
    def headline(self) -> str:
        """Return a compact one-line summary without the detail message, for the report grid."""
        ...


@dataclass(frozen=True, slots=True)
class ReadFailure(Failure):
    """Failure reading current catalog state for a table."""

    phase: ClassVar[FailurePhase] = FailurePhase.READ
    exception_type: str
    message: str

    def format_lines(self) -> tuple[str, ...]:
        return (f"Read error: {self.exception_type} - {self.message}",)

    def headline(self) -> str:
        return f"Read error: {self.exception_type}"


@dataclass(frozen=True, slots=True)
class ValidationFailure(Failure):
    """Description of a validation rule failure."""

    phase: ClassVar[FailurePhase] = FailurePhase.VALIDATION
    rule_name: str
    message: str

    def format_lines(self) -> tuple[str, ...]:
        return (f"Validation failed: {self.rule_name} - {self.message}",)

    def headline(self) -> str:
        return f"Validation failed: {self.rule_name}"


@dataclass(frozen=True, slots=True)
class ExecutionFailure(Failure):
    """Details about a failed action execution."""

    phase: ClassVar[FailurePhase] = FailurePhase.EXECUTION
    action_index: int
    exception_type: str
    message: str
    statement_preview: str

    def format_lines(self) -> tuple[str, ...]:
        return (
            f"Execution failed at action {self.action_index}: "
            f"{self.exception_type} - {self.message}",
            f"    SQL preview: {self.statement_preview}",
        )

    def headline(self) -> str:
        return f"Execution failed at action {self.action_index}: {self.exception_type}"


@dataclass(frozen=True, slots=True)
class ForeignKeyFailure(Failure):
    """A foreign key constraint that could not be applied, failing its whole table."""

    phase: ClassVar[FailurePhase] = FailurePhase.FOREIGN_KEY
    table: QualifiedName
    local_columns: tuple[str, ...]
    references: QualifiedName
    reason: ForeignKeyFailureReason

    def format_lines(self) -> tuple[str, ...]:
        columns = ", ".join(self.local_columns)
        return (
            f"Foreign key ({columns}) → {self.references} on {self.table} was not applied: "
            f"{self.reason.detail}.",
        )

    def headline(self) -> str:
        columns = ", ".join(self.local_columns)
        return f"Foreign key ({columns}) → {self.references} not applied"
