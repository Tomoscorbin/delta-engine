"""
Failure vocabulary for engine runs.

A failure is a recorded reason a table did not converge — a frozen value in
the table's run, never raised (it is not an ``Exception``). ``ReadFailure``
and ``ExecutionFailure`` are born when the engine catches the corresponding
adapter error; ``ValidationFailure`` and ``ForeignKeyFailure`` are born as
values from pure judgment — no exception is ever involved. Each failure
knows the phase that produced it (`FailurePhase`) and renders itself for
display: ``format_lines`` returns the human-readable lines, carrying no
indentation — how deeply a report nests them is the renderer's decision, and
the machine projection joins them flat — and ``headline`` is the compact
one-line summary for the report grid. Reports derive a table's status from
the earliest failing phase, so the family stays together rather than being
scattered across its producers.
"""

from dataclasses import dataclass, field
from enum import IntEnum, StrEnum
from typing import ClassVar, Final, assert_never

from delta_engine.domain.collection_types import ListOrTuple
from delta_engine.domain.model import QualifiedName

# Failure messages are recorded in full (a backend stack trace can run to
# hundreds of lines); rendered reports show only this many leading lines.
# The complete text stays on the failure's ``message`` field.
_MESSAGE_HEAD_LINES: Final = 5


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


def _message_head(message: str) -> str:
    """Return the first lines of a failure message, bounded for display."""
    return "\n".join(message.splitlines()[:_MESSAGE_HEAD_LINES])


@dataclass(frozen=True, slots=True)
class ReadFailure:
    """Failure reading current catalog state for a table."""

    phase: ClassVar[FailurePhase] = FailurePhase.READ
    exception_type: str
    message: str

    def format_lines(self) -> tuple[str, ...]:
        return (f"Read error: {self.exception_type} - {_message_head(self.message)}",)

    def headline(self) -> str:
        return f"Read error: {self.exception_type}"


@dataclass(frozen=True, slots=True)
class ValidationFailure:
    """
    Description of a validation rule failure.

    ``subject`` is what the failure is about — a column, property key, or
    aspect — used by the compact headline in the report grid. Rules that judge
    the table as a whole leave it empty.
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
    subject: str = field(default="", kw_only=True)
    details: ListOrTuple[str] = ()

    def __post_init__(self) -> None:
        object.__setattr__(self, "details", tuple(self.details))

    def format_lines(self) -> tuple[str, ...]:
        return (f"Validation failed: {self.rule_name} - {self.message}", *self.details)

    def headline(self) -> str:
        subject = f" ({self.subject})" if self.subject else ""
        return f"Validation failed: {self.rule_name}{subject}"


@dataclass(frozen=True, slots=True)
class ExecutionFailure:
    """Details about a statement that failed while executing."""

    phase: ClassVar[FailurePhase] = FailurePhase.EXECUTION
    statement_index: int
    exception_type: str
    message: str
    statement: str

    def format_lines(self) -> tuple[str, ...]:
        return (
            f"Execution failed at statement {self.statement_number}: "
            f"{self.exception_type} - {_message_head(self.message)}",
            f"SQL: {self.statement}",
        )

    def headline(self) -> str:
        return f"Execution failed at statement {self.statement_number}: {self.exception_type}"

    @property
    def statement_number(self) -> int:
        """
        The failing statement's one-based display position.

        ``statement_index`` remains zero-based so it indexes the compiled
        statements. Only the reader-facing number shifts, making "statement
        3" agree with a report-grid progress count of "2/3".
        """
        return self.statement_index + 1


@dataclass(frozen=True, slots=True)
class ForeignKeyFailure:
    """A foreign key constraint that could not be applied, failing its whole table."""

    phase: ClassVar[FailurePhase] = FailurePhase.FOREIGN_KEY
    table: QualifiedName
    local_columns: ListOrTuple[str]
    references: QualifiedName
    reason: ForeignKeyFailureReason

    def __post_init__(self) -> None:
        object.__setattr__(self, "local_columns", tuple(self.local_columns))

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


type Failure = ReadFailure | ValidationFailure | ExecutionFailure | ForeignKeyFailure
