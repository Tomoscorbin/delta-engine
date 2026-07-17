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


@dataclass(frozen=True, slots=True)
class ReadFailed:
    """A catalog read that raised before any state could be determined."""

    failure: ReadFailure


# The three answers a catalog can give about a table: it is there, it is not
# there, or it could not be read.
type CatalogState = TablePresent | TableAbsent | ReadFailed


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
    """
    A single statement that executed without error.

    ``statement_index`` is the statement's position in the run and
    ``statement`` is its SQL exactly as executed; neither is rendered by the
    engine's own reports — they are carried for callers that inspect
    ``ExecutionSummary.results`` directly.
    """

    statement_index: int
    statement: str


@dataclass(frozen=True, slots=True)
class ExecutionFailed:
    """A single statement that raised while executing."""

    failure: ExecutionFailure


# An executed statement either succeeds or fails. The split makes "succeeded but
# carries a failure" (and "failed but carries none") unrepresentable, so no
# runtime invariant guard is needed.
type ExecutionResult = ExecutionSucceeded | ExecutionFailed


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

    @property
    def failed(self) -> bool:
        """True when any statement failed."""
        return any(isinstance(result, ExecutionFailed) for result in self.results)

    @property
    def failures(self) -> tuple[ExecutionFailure, ...]:
        """The failure detail from each failed statement, in execution order."""
        return tuple(
            result.failure for result in self.results if isinstance(result, ExecutionFailed)
        )

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
    Compiles a plan into statements and runs them against a backing engine.

    Execution is a two-stage boundary: ``compile`` turns a domain plan into the
    backend statements it lowers to, and ``execute`` runs those statements. The
    engine compiles once per invocation: a dry run exposes those statements,
    while a real run passes that invocation's same statements to ``execute``.

    Like :class:`CatalogStateReader`, ``execute`` is **total**: a statement that
    fails is captured in the returned ``ExecutionSummary`` (which records both the
    statements that succeeded and the one that failed), not raised. The engine
    records the summary on the table's report and moves on, so a failure executing
    one table does not abort the others.
    """

    def compile(self, qualified_name: QualifiedName, plan: ActionPlan) -> tuple[str, ...]:
        """
        Return the statements that apply ``plan``, in execution order.

        The plan carries the relation kind its actions lower against
        (``plan.kind``) — backends whose statements differ by kind read it
        from the plan.

        The ordering is the plan's own deterministic order, which is the order
        ``execute`` runs the statements. An empty plan compiles to no statements.

        Pure and side-effect free: the engine calls this on every run -- dry or
        real -- to record the SQL on the table's report. Unlike ``execute``,
        this is not a total boundary: compiling a validated plan cannot fail
        against a backend, so an exception here is a programming error and
        propagates.
        """
        ...

    def execute(self, statements: tuple[str, ...]) -> ExecutionSummary:
        """
        Run ``statements`` (from :meth:`compile`) in order.

        Total: failures are captured in the returned ``ExecutionSummary`` rather
        than raised. The statements are the complete unit of work — the table
        they target is already baked into each statement by ``compile``.
        """
        ...
