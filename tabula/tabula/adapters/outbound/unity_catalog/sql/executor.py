from __future__ import annotations
from dataclasses import dataclass
from typing import Callable, List, Tuple
from tabula.application.results import ExecutionOutcome
from tabula.domain.model.actions import ActionPlan
from .compile import compile_plan

RunSQL = Callable[[str], None]  # e.g., spark.sql

@dataclass(frozen=True, slots=True)
class SqlPlanExecutor:
    run_sql: RunSQL  # callable that executes a single SQL statement

    def execute(self, plan: ActionPlan) -> ExecutionOutcome:
        statements: Tuple[str, ...] = compile_plan(plan)
        executed: List[str] = []
        for index, sql in enumerate(statements):
            try:
                self.run_sql(sql)
                executed.append(sql)
            except Exception as exc:
                # Report partial progress and the exact failing statement
                return ExecutionOutcome(
                    success=False,
                    messages=tuple(executed) + (
                        f"ERROR at statement {index}: {sql}",
                        f"{type(exc).__name__}: {exc}",
                    ),
                    executed_count=len(executed),
                )
        return ExecutionOutcome(
            success=True,
            messages=tuple(executed),
            executed_count=len(executed),
        )
