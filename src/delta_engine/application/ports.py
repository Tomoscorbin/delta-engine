"""
Application ports: the engine's boundary contracts and the message types they exchange.

Each boundary is defined in full here — the vocabulary an adapter returns
(``CatalogState`` for reads, ``ExecutionSummary`` for execution) sits beside
the Protocol that requires it, so an adapter author reads one file to learn
the whole contract. ``DesiredTableSource`` is the inbound counterpart: the
contract a user-facing declaration satisfies to enter a sync.
"""

from dataclasses import dataclass
from typing import Protocol

from delta_engine.application.failures import ExecutionFailure, ReadFailure
from delta_engine.domain.model import DesiredTable, ObservedTable, QualifiedName
from delta_engine.domain.plan import ActionPlan

# ---------- DesiredTableSource ----------


class DesiredTableSource(Protocol):
    """A user-facing table specification that can produce a domain table."""

    def to_desired_table(self) -> DesiredTable:
        """Return the domain :class:`DesiredTable` for this specification."""
        ...


# ---------- CatalogState ----------


@dataclass(frozen=True, slots=True)
class TablePresent:
    """The catalog holds a live table; ``table`` is its observed schema."""

    table: ObservedTable


@dataclass(frozen=True, slots=True)
class TableAbsent:
    """The catalog confirmed the table does not exist; the engine will create it."""


# A catalog state is known only when the table was found or its absence was
# confirmed. Failure to determine either state crosses the port as ReadError.
type CatalogState = TablePresent | TableAbsent

# The persistent outcome retained by a table run. The adapter never constructs
# ReadFailure; the engine creates it when it catches ReadError.
type ReadResult = CatalogState | ReadFailure


class CatalogStateReader(Protocol):
    """
    Reads the current catalog state for a single table.

    Returning normally means the adapter determined that the table is present
    or absent. A backend error, unmappable schema, or permissions failure is
    translated into :class:`delta_engine.application.errors.ReadError`. The
    engine catches that specific error per table and records a
    :class:`ReadFailure`; unexpected adapter errors propagate.
    """

    def fetch_state(self, qualified_name: QualifiedName) -> CatalogState:
        """
        Return the table's current state: present or absent.

        Args:
            qualified_name: Fully qualified object name to look up.

        Raises:
            ReadError: The adapter could not determine the catalog state and
                translated its backend-specific exception.

        """
        ...


# ---------- ExecutionResult ----------


@dataclass(frozen=True, slots=True)
class ExecutionSucceeded:
    """
    A single statement that executed without error.

    ``statement_index`` is the statement's position in the run and
    ``statement`` is its SQL exactly as executed; neither is rendered by the
    engine's own reports — they are carried for callers that inspect
    ``ExecutionSummary.results`` directly.
    """

    statement_index: int
    statement: str


# A completed statement is either recorded as successful or as the execution
# failure itself.
type ExecutionResult = ExecutionSucceeded | ExecutionFailure


@dataclass(frozen=True, slots=True)
class ExecutionSummary:
    """
    The outcome of running a plan's compiled statements.

    Mirrors :class:`ValidationResult`: a frozen container over the phase's raw
    results that answers ``failed`` and exposes its ``failures``. It owns the
    single pass that separates failed statements from successful ones, so
    callers read a property instead of re-deriving the split with
    ``isinstance``.
    """

    results: tuple[ExecutionResult, ...] = ()

    def __post_init__(self) -> None:
        object.__setattr__(self, "results", tuple(self.results))

        failure_seen = False
        for expected_index, result in enumerate(self.results):
            if result.statement_index != expected_index:
                raise ValueError("Execution result indexes must be contiguous and start at zero")
            if failure_seen:
                raise ValueError("An execution failure must be the final result")
            failure_seen = isinstance(result, ExecutionFailure)

    @property
    def failed(self) -> bool:
        """True when any statement failed."""
        return any(isinstance(result, ExecutionFailure) for result in self.results)

    @property
    def failures(self) -> tuple[ExecutionFailure, ...]:
        """The failure detail from each failed statement, in execution order."""
        return tuple(result for result in self.results if isinstance(result, ExecutionFailure))

    @property
    def failed_count(self) -> int:
        """How many statements failed."""
        return len(self.failures)

    @property
    def applied_count(self) -> int:
        """How many statements ran successfully."""
        return len(self.results) - self.failed_count


class PlanExecutor(Protocol):
    """
    Compile a plan and execute individual statements against a backing engine.

    Execution is a two-stage boundary: ``compile`` turns a domain plan into the
    backend statements it lowers to, and ``execute`` attempts one of those
    statements. The engine compiles once per invocation: a dry run exposes those
    statements, while a real run passes that invocation's same statements to
    ``execute`` one at a time.

    The application owns statement ordering, result construction, and stopping
    after the first failure. An adapter contains its backend's exception types
    and translates an expected execution failure into
    :class:`delta_engine.application.errors.ExecutionError`.
    The engine catches that specific exception and records an
    :class:`ExecutionFailure`; unexpected programming errors still propagate.
    """

    def compile(self, qualified_name: QualifiedName, plan: ActionPlan) -> tuple[str, ...]:
        """
        Return the statements that apply ``plan``, in execution order.

        The plan carries the relation kind its actions lower against
        (``plan.kind``) — backends whose statements differ by kind read it
        from the plan.

        The ordering is the plan's own deterministic order, which is the order
        the application passes statements to ``execute``. An empty plan compiles
        to no statements.

        Pure and side-effect free: the engine calls this on every run -- dry or
        real -- to record the SQL on the table's report. Unlike ``execute``,
        this is not a total boundary: compiling a validated plan cannot fail
        against a backend, so an exception here is a programming error and
        propagates.
        """
        ...

    def execute(self, statement: str) -> None:
        """
        Execute one statement produced by :meth:`compile`.

        Returning normally means the statement succeeded. The application adds
        the statement and its sequence index to the execution summary.

        Args:
            statement: The backend statement to execute verbatim.

        Raises:
            ExecutionError: The backend could not execute the statement. The
                adapter must translate backend-specific exceptions into this
                type.

        """
        ...
