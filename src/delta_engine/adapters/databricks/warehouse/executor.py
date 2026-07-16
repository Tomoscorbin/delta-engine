"""
Execute compiled statements on a Databricks SQL warehouse and capture results.

Compiles an `ActionPlan` to SQL via the shared compiler — byte-for-byte the
same statements the Spark backend runs, so dry-run previews are
backend-independent — and runs them through the shared stop-on-first-failure
loop with a warehouse cursor as the runner.
"""

from __future__ import annotations

import contextlib
from typing import TYPE_CHECKING

from delta_engine.adapters.databricks.execution import execute_statements
from delta_engine.adapters.databricks.sql import compile_plan
from delta_engine.application.ports import ExecutionSummary
from delta_engine.domain.model import QualifiedName, TableKind
from delta_engine.domain.plan import ActionPlan

if TYPE_CHECKING:
    from databricks.sql.client import Connection, Cursor


class WarehouseExecutor:
    """Plan executor that compiles plans to SQL and runs them on a SQL warehouse."""

    def __init__(self, connection: Connection) -> None:
        self._connection = connection

    def compile(
        self, qualified_name: QualifiedName, plan: ActionPlan, kind: TableKind
    ) -> tuple[str, ...]:
        """
        Compile ``plan`` to its SQL statements in execution order.

        Does not touch the warehouse.
        """
        return compile_plan(qualified_name, plan, kind)

    def execute(self, statements: tuple[str, ...]) -> ExecutionSummary:
        """
        Execute each statement in order via the shared stop-on-first-failure loop.

        The cursor is acquired lazily, on the first statement, so the loop's
        totality covers the whole cursor lifecycle as the port requires: a
        connection that cannot produce a cursor (closed by the caller, or a
        session that died after an earlier table's plan) is recorded as the
        first statement's failure — the same outage one call later, inside
        ``cursor.execute``, is already recorded that way. A close failure
        after the loop is suppressed so it cannot discard the summary of
        statements that actually ran.
        """
        cursor: Cursor | None = None

        def run(statement: str) -> None:
            nonlocal cursor
            if cursor is None:
                cursor = self._connection.cursor()
            cursor.execute(statement)

        try:
            return execute_statements(run, statements)
        finally:
            if cursor is not None:
                with contextlib.suppress(Exception):
                    cursor.close()
