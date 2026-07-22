"""Compile plans and execute individual statements through a SQL warehouse."""

from __future__ import annotations

import contextlib
from typing import TYPE_CHECKING

from delta_engine.adapters.databricks.execution import execute_statement
from delta_engine.adapters.databricks.sql import compile_plan
from delta_engine.domain.plan import ActionPlan

if TYPE_CHECKING:
    from databricks.sql.client import Connection, Cursor


class WarehouseExecutor:
    """Plan executor that compiles plans to SQL and runs them on a SQL warehouse."""

    def __init__(self, connection: Connection) -> None:
        self._connection = connection

    def compile(self, plan: ActionPlan) -> tuple[str, ...]:
        """
        Compile ``plan`` to its SQL statements in execution order.

        Does not touch the warehouse.
        """
        return compile_plan(plan)

    def execute(self, statement: str) -> None:
        """
        Execute one statement and contain the warehouse cursor lifecycle.

        Cursor acquisition happens inside the translated callable, so a closed
        or expired connection raises the same application error as a failure
        from ``cursor.execute``. A close failure is suppressed so it cannot
        replace the outcome of the statement itself.
        """
        cursor: Cursor | None = None

        def run(statement: str) -> None:
            nonlocal cursor
            cursor = self._connection.cursor()
            cursor.execute(statement)

        try:
            execute_statement(run, statement)
        finally:
            if cursor is not None:
                with contextlib.suppress(Exception):
                    cursor.close()
