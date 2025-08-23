from __future__ import annotations

"""Execution adapter for Databricks Unity Catalog."""

from collections.abc import Callable, Sequence
from dataclasses import dataclass
from typing import Literal

from tabula.adapters.databricks.sql.compile import compile_plan
from tabula.adapters.databricks.sql.dialects import SPARK_SQL, SqlDialect
from tabula.application.results import ExecutionOutcome
from tabula.domain.plan.actions import ActionPlan

RunSql = Callable[[str], None]
OnError = Literal["stop", "continue"]


@dataclass(frozen=True, slots=True)
class UCExecutor:
    """Compile an action plan to SQL and execute statements sequentially.

    Attributes:
        run_sql: Callable used to execute a single SQL statement.
        on_error: Policy for error handling ("stop" or "continue").
        dry_run: If True, log statements without executing them.
        dialect: SQL dialect used during compilation.
    """

    run_sql: RunSql
    on_error: OnError = "stop"
    dry_run: bool = False
    dialect: SqlDialect = SPARK_SQL

    def execute(self, plan: ActionPlan) -> ExecutionOutcome:
        """Compile and run the given action plan.

        Args:
            plan: Ordered actions to perform.

        Returns:
            Outcome of the execution including messages and executed SQL.
        """

        statements: Sequence[str] = compile_plan(plan, dialect=self.dialect)

        executed_sql: list[str] = []
        messages: list[str] = []
        attempted_failure = False
        executed_count = 0

        for index, statement in enumerate(statements, start=1):
            if self.dry_run:
                executed_sql.append(statement)
                messages.append(f"DRY RUN {index}: {statement}")
                continue

            try:
                self.run_sql(statement)
                executed_sql.append(statement)
                messages.append(f"OK {index}")
                executed_count += 1
            except Exception as exc:  # noqa: BLE001
                messages.append(f"ERROR {index}: {type(exc).__name__}: {exc}")
                attempted_failure = True
                if self.on_error == "stop":
                    break

        return ExecutionOutcome(
            success=not attempted_failure,
            messages=tuple(messages),
            executed_count=executed_count,
            executed_sql=tuple(executed_sql),
        )
