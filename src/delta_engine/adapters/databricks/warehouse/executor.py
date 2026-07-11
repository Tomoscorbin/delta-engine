"""
Execute compiled statements on a Databricks SQL warehouse and capture results.

Compiles an `ActionPlan` to SQL via the shared compiler — byte-for-byte the
same statements the Spark backend runs, so dry-run previews are
backend-independent — and runs them through the shared stop-on-first-failure
loop with a warehouse cursor as the runner and the generic exception
translation (the connector raises a plain Python exception hierarchy, so the
class name is already the informative fact).
"""

from __future__ import annotations

from typing import TYPE_CHECKING

from delta_engine.adapters.databricks.errors import translate_exception
from delta_engine.adapters.databricks.execution import execute_statements
from delta_engine.adapters.databricks.sql import compile_plan
from delta_engine.application.ports import ExecutionSummary
from delta_engine.domain.model import QualifiedName
from delta_engine.domain.plan import ActionPlan

if TYPE_CHECKING:
    from databricks.sql.client import Connection


class WarehouseExecutor:
    """Plan executor that compiles plans to SQL and runs them on a SQL warehouse."""

    def __init__(self, connection: Connection) -> None:
        self._connection = connection

    def compile(self, qualified_name: QualifiedName, plan: ActionPlan) -> tuple[str, ...]:
        """
        Compile ``plan`` to its SQL statements in execution order.

        Does not touch the warehouse.
        """
        return compile_plan(qualified_name, plan)

    def execute(self, statements: tuple[str, ...]) -> ExecutionSummary:
        """Execute each statement in order via the shared stop-on-first-failure loop."""
        with self._connection.cursor() as cursor:
            return execute_statements(cursor.execute, statements, translate_exception)
