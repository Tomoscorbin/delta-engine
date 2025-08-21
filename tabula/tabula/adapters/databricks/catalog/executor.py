from __future__ import annotations

from dataclasses import dataclass
from typing import Callable, Literal, Sequence

from tabula.adapters.databricks.sql.compile import compile_plan
from tabula.adapters.databricks.sql.dialects import SqlDialect, SPARK_SQL
from tabula.application.results import ExecutionOutcome
from tabula.domain.plan.actions import ActionPlan


RunSql = Callable[[str], None]
OnError = Literal["stop", "continue"]


@dataclass(frozen=True, slots=True)
class UCExecutor:
    """
    Minimal executor: compile -> run sequentially.

    - on_error:
        'stop'     -> stop at first error (default)
        'continue' -> attempt all statements; success=False if any failed
    - dry_run: do not execute, just echo would-be SQL statements
    - dialect: SQL dialect for compilation (defaults to Spark/Databricks)
    """

    run_sql: RunSql
    on_error: OnError = "stop"
    dry_run: bool = False
    dialect: SqlDialect = SPARK_SQL

    def execute(self, plan: ActionPlan) -> ExecutionOutcome:
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
            except Exception as exc:
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
