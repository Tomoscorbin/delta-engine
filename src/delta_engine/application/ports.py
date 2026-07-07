"""
Application ports: adapter contracts and the message types they exchange.

Each boundary is defined in full here — the vocabulary an adapter returns
(``CatalogState`` for reads, ``ExecutionSummary`` for execution) sits beside
the Protocol that requires it, so an adapter author reads one file to learn
the whole contract.
"""

from dataclasses import dataclass
from typing import Protocol

from delta_engine.application.failures import ExecutionFailure, ReadFailure
from delta_engine.domain.model import ObservedTable, QualifiedName
from delta_engine.domain.plan import ActionPlan

# ---------- CatalogState ----------


@dataclass(frozen=True, slots=True)
class TablePresent:
    """The catalog holds a live table; ``table`` is its observed schema."""

    table: ObservedTable


@dataclass(frozen=True, slots=True)
class TableAbsent:
    """The catalog confirmed the table does not exist; the engine will create it."""


@dataclass(frozen=True, slots=True)
class ReadFailed:
    """A catalog read that raised before any state could be determined."""

    failure: ReadFailure


# The three answers a catalog can give about a table: it is there, it is not
# there, or it could not be read.
CatalogState = TablePresent | TableAbsent | ReadFailed


class CatalogStateReader(Protocol):
    """
    Reads the current catalog state for a single table.

    The boundary every adapter must honour: ``fetch_state`` is **total**. A read
    that cannot determine state -- a backend error, an unmappable schema, a
    permissions failure -- is returned as ``ReadFailed``, never raised. The
    engine reads many tables in one run and branches on the returned state rather
    than guarding each call, so an exception escaping here would abort the whole
    sync instead of failing the one table. Implementations contain their own
    failure modes.
    """

    def fetch_state(self, qualified_name: QualifiedName) -> CatalogState:
        """
        Return the table's current state: present, absent, or unreadable.

        Total: returns ``ReadFailed`` on any error rather than raising.

        Args:
            qualified_name: Fully qualified object name to look up.

        """
        ...


# ---------- ExecutionResult ----------


@dataclass(frozen=True, slots=True)
class ExecutionSucceeded:
    """A single plan action that executed without error."""

    action: str
    action_index: int
    statement_preview: str


@dataclass(frozen=True, slots=True)
class ExecutionFailed:
    """A single plan action that raised while executing."""

    action: str
    failure: ExecutionFailure


# An executed action either succeeds or fails. The split makes "succeeded but
# carries a failure" (and "failed but carries none") unrepresentable, so no
# runtime invariant guard is needed.
ExecutionResult = ExecutionSucceeded | ExecutionFailed


@dataclass(frozen=True, slots=True)
class ExecutionSummary:
    """
    The outcome of running a whole action plan.

    Mirrors :class:`ValidationResult`: a frozen container over the phase's raw
    results that answers ``failed`` and exposes its ``failures``. It owns the
    single pass that separates failed actions from successful ones, so callers
    read a property instead of re-deriving the split with ``isinstance``.
    """

    results: tuple[ExecutionResult, ...] = ()

    @property
    def failed(self) -> bool:
        """True when any action in the plan failed."""
        return any(isinstance(result, ExecutionFailed) for result in self.results)

    @property
    def failures(self) -> tuple[ExecutionFailure, ...]:
        """The failure detail from each failed action, in execution order."""
        return tuple(
            result.failure for result in self.results if isinstance(result, ExecutionFailed)
        )

    @property
    def failed_count(self) -> int:
        """How many of the plan's actions failed."""
        return len(self.failures)


class PlanExecutor(Protocol):
    """
    Executes an action plan against a backing engine.

    Like :class:`CatalogStateReader`, the boundary is **total**: a statement that
    fails is captured in the returned ``ExecutionSummary`` (which records both the
    actions that succeeded and the one that failed), not raised. The engine
    records the summary on the table's report and moves on, so a failure executing
    one table does not abort the others.
    """

    def execute(self, qualified_name: QualifiedName, plan: ActionPlan) -> ExecutionSummary:
        """
        Run the plan against ``qualified_name`` and return the execution outcome.

        Total: failures are captured in the returned ``ExecutionSummary`` rather
        than raised.
        """
        ...
