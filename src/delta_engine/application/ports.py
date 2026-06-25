"""Application ports / adapter interfaces."""

from __future__ import annotations

from typing import Protocol, runtime_checkable

from delta_engine.application.results import CatalogState, ExecutionSummary
from delta_engine.domain.model import QualifiedName
from delta_engine.domain.plan.actions import ActionPlan


@runtime_checkable
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


@runtime_checkable
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
