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
from delta_engine.domain.plan.actions import Action

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
class CompiledAction:
    """One domain action paired with the single statement that applies it."""

    action: Action
    statement: str

    def __post_init__(self) -> None:
        if not self.statement.strip():
            raise ValueError(
                f"{type(self.action).__name__} compiled to an empty statement"
            )


@dataclass(frozen=True, slots=True)
class CompiledPlan:
    """An accepted action plan paired exactly with its compiled statements."""

    plan: ActionPlan
    compiled_actions: tuple[CompiledAction, ...]

    def __post_init__(self) -> None:
        compiled_actions = tuple(self.compiled_actions)
        object.__setattr__(self, "compiled_actions", compiled_actions)
        source_actions = tuple(compiled.action for compiled in compiled_actions)

        if source_actions != self.plan.actions:
            raise ValueError("Compiled actions must correspond exactly to plan actions")

    @property
    def statements(self) -> tuple[str, ...]:
        """Statements in the source plan's execution order."""
        return tuple(item.statement for item in self.compiled_actions)


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

    Results must cover the compiled statements in order. A successful history
    covers the complete plan; a failed history is the prefix ending at the
    first failed statement. Therefore ``failures`` holds at most one.
    """

    compiled_plan: CompiledPlan
    results: tuple[ExecutionResult, ...] = ()

    def __post_init__(self) -> None:
        """Reject result histories that the engine's execution loop cannot produce."""
        results = tuple(self.results)
        object.__setattr__(self, "results", results)
        statements = self.compiled_plan.statements
        execution_failed = False
        for expected_index, result in enumerate(results):
            if result.statement_index != expected_index:
                raise ValueError("Execution result indexes must be contiguous")
            if expected_index >= len(statements) or result.statement != statements[expected_index]:
                raise ValueError("Execution results must match the compiled statement prefix")
            if isinstance(result, ExecutionFailure):
                if expected_index != len(results) - 1:
                    raise ValueError("Execution must stop at its first failure")
                execution_failed = True

        if not execution_failed and len(results) != len(statements):
            raise ValueError("Successful execution must cover the complete compiled plan")

    @property
    def failures(self) -> tuple[ExecutionFailure, ...]:
        """The failure detail from each failed statement, in execution order."""
        return tuple(result for result in self.results if isinstance(result, ExecutionFailure))

    @property
    def applied_count(self) -> int:
        """How many statements ran successfully."""
        return len(self.results) - len(self.failures)


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

    def compile(self, plan: ActionPlan) -> CompiledPlan:
        """
        Return ``plan`` paired exactly with its statements in execution order.

        The plan carries the qualified table target and relation kind its
        actions lower against (``plan.target`` and ``plan.kind``). Backends
        read both from the plan rather than accepting parallel context.

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
