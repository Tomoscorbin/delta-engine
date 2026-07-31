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
from typing import ClassVar, Final, assert_never

from delta_engine.domain.model import QualifiedName


class FailurePhase(IntEnum):
    """The sync phase that produced a failure. Ordered so the earliest wins."""

    FOREIGN_KEY = 1
    READ = 2
    PLANNING = 3
    EXECUTION = 4


class ForeignKeyFailureReason(StrEnum):
    """Why a foreign key constraint could not be applied, failing its whole table."""

    CYCLE = "CYCLE"
    UNRESOLVABLE_REFERENCE = "UNRESOLVABLE_REFERENCE"
    BLOCKED_BY_FAILED_DEPENDENCY = "BLOCKED_BY_FAILED_DEPENDENCY"
    REFERENCED_COLUMNS_NOT_A_KEY = "REFERENCED_COLUMNS_NOT_A_KEY"
    REFERENCED_COLUMN_TYPE_MISMATCH = "REFERENCED_COLUMN_TYPE_MISMATCH"
    REFERENCED_COLUMN_CASE_MISMATCH = "REFERENCED_COLUMN_CASE_MISMATCH"

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
            case ForeignKeyFailureReason.REFERENCED_COLUMN_TYPE_MISMATCH:
                return (
                    "its column types do not match the registered referenced table's column types"
                )
            case ForeignKeyFailureReason.REFERENCED_COLUMN_CASE_MISMATCH:
                return (
                    "its referenced columns are spelled differently from the"
                    " registered referenced table's declaration"
                )
            case _ as unreachable:
                assert_never(unreachable)


# ---------- Failure value objects ----------

# Failure messages are recorded in full (a backend stack trace can run to
# hundreds of lines); rendered reports show only this many leading lines.
# The complete text stays on the failure's ``message`` field.
_MESSAGE_HEAD_LINES: Final = 5


def _message_head(message: str) -> str:
    """Return the first lines of a failure message, bounded for display."""
    return "\n".join(message.splitlines()[:_MESSAGE_HEAD_LINES])


class Failure(ABC):
    """A failure that can render itself as display lines, tagged with its phase."""

    phase: ClassVar[FailurePhase]

    def __init_subclass__(cls, **kwargs: object) -> None:
        # A report derives a table's status from the earliest failing phase, so
        # a failure without one fails there rather than where it was written.
        super().__init_subclass__(**kwargs)
        if not hasattr(cls, "phase"):
            raise TypeError(f"{cls.__name__} must declare the phase that produces it")

    @abstractmethod
    def format_lines(self) -> tuple[str, ...]:
        """
        Return the human-readable lines describing this failure.

        Lines carry no indentation: how deeply a report nests them is the
        renderer's decision, and the machine projection joins them flat.
        """
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
        return (f"Read error: {self.exception_type} - {_message_head(self.message)}",)

    def headline(self) -> str:
        return f"Read error: {self.exception_type}"


@dataclass(frozen=True, slots=True)
class ValidationFailure(Failure):
    """
    Description of a validation rule failure.

    ``details`` are the individual differences behind a summary judgment, for
    a rule whose message names a whole aspect rather than one column. They are
    separate lines rather than newlines inside ``message`` so the report
    renderer owns their indentation, as it already does for the SQL line of an
    execution failure; a rule that embeds its own layout composes wrongly
    wherever the failure is nested.
    """

    phase: ClassVar[FailurePhase] = FailurePhase.PLANNING
    rule_name: str
    message: str
    details: tuple[str, ...] = ()

    def format_lines(self) -> tuple[str, ...]:
        return (f"Validation failed: {self.rule_name} - {self.message}", *self.details)

    def headline(self) -> str:
        return f"Validation failed: {self.rule_name}"


@dataclass(frozen=True, slots=True)
class ExecutionFailure(Failure):
    """Details about a statement that failed while executing."""

    phase: ClassVar[FailurePhase] = FailurePhase.EXECUTION
    statement_index: int
    exception_type: str
    message: str
    statement: str

    def format_lines(self) -> tuple[str, ...]:
        return (
            f"Execution failed at statement {self.statement_index}: "
            f"{self.exception_type} - {_message_head(self.message)}",
            f"SQL: {self.statement}",
        )

    def headline(self) -> str:
        return f"Execution failed at statement {self.statement_index}: {self.exception_type}"


@dataclass(frozen=True, slots=True)
class ForeignKeyFailure(Failure):
    """A foreign key constraint that could not be applied, failing its whole table."""

    phase: ClassVar[FailurePhase] = FailurePhase.FOREIGN_KEY
    table: QualifiedName
    local_columns: tuple[str, ...]
    references: QualifiedName
    reason: ForeignKeyFailureReason

    @property
    def _constraint(self) -> str:
        """
        How a message names the constraint that failed.

        A table may carry several foreign keys, so the local columns and the
        referenced table are what identify one to a reader — the constraint
        name is generated and means nothing to them.
        """
        return f"({', '.join(self.local_columns)}) → {self.references}"

    def format_lines(self) -> tuple[str, ...]:
        return (
            f"Foreign key {self._constraint} on {self.table} was not applied: "
            f"{self.reason.detail}.",
        )

    def headline(self) -> str:
        return f"Foreign key {self._constraint} not applied"
