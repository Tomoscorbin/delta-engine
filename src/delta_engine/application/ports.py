"""
Application ports: the engine's boundary contracts and the message types they exchange.

Each boundary is defined in full here — the vocabulary an adapter returns
(``CatalogState`` for reads, ``ExecutionResult`` for execution) sits beside
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
class CompiledPlan:
    """An accepted action plan and one compiled statement per action, in order."""

    plan: ActionPlan
    statements: tuple[str, ...]

    def __post_init__(self) -> None:
        if not isinstance(self.statements, tuple):
            raise TypeError("CompiledPlan statements must be a tuple")

        if len(self.statements) != len(self.plan.actions):
            raise ValueError("Compiled statements must correspond exactly to plan actions")

        for action, statement in zip(self.plan.actions, self.statements, strict=True):
            if not statement.strip():
                raise ValueError(f"{type(action).__name__} compiled to an empty statement")


@dataclass(frozen=True, slots=True)
class ExecutionResult:
    """
    The recorded outcome of executing one table's compiled plan.

    Execution applies statements in compiled order and stops at the first
    failure, so the complete history is an applied prefix plus at most one
    failure sitting immediately after it: ``applied_count`` says how far the
    prefix ran, and ``failure`` is the statement that stopped it, ``None``
    when the whole plan applied. The statements themselves live on
    ``compiled_plan`` — nothing derivable is restated here.
    """

    compiled_plan: CompiledPlan
    applied_count: int = 0
    failure: ExecutionFailure | None = None

    def __post_init__(self) -> None:
        """Reject histories the engine's execution loop cannot produce."""
        statements = self.compiled_plan.statements
        attempted = self.applied_count + (0 if self.failure is None else 1)
        if not 0 <= self.applied_count or attempted > len(statements):
            raise ValueError("Execution history must lie within the compiled plan")
        if self.failure is None:
            if self.applied_count != len(statements):
                raise ValueError("Successful execution must cover the complete compiled plan")
            return
        if self.failure.statement_index != self.applied_count:
            raise ValueError("Execution must stop at its first failure")
        if self.failure.statement != statements[self.applied_count]:
            raise ValueError("Execution failure must carry its compiled statement")

    @property
    def failures(self) -> tuple[ExecutionFailure, ...]:
        """The failure that stopped execution, as the tuple reports flatten."""
        return () if self.failure is None else (self.failure,)


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
        the statement and its sequence index to the execution result.

        Args:
            statement: The backend statement to execute verbatim.

        Raises:
            ExecutionError: The backend could not execute the statement. The
                adapter must translate backend-specific exceptions into this
                type.

        """
        ...
