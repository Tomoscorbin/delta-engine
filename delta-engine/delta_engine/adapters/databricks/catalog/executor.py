from __future__ import annotations

from collections.abc import Sequence
from dataclasses import dataclass

from pyspark.sql import SparkSession

from delta_engine.adapters.databricks.catalog.compile import compile_plan
from delta_engine.application.results import ExecutionReport
from delta_engine.domain.plan.actions import ActionPlan


@dataclass(frozen=True, slots=True)
class DatabricksExecutor:
    spark: SparkSession

    def execute(self, plan: ActionPlan) -> ExecutionReport:
        statements: Sequence[str] = compile_plan(plan)
        executed_sql: list[str] = []
        failed_sql: list[str] = []
        messages: list[str] = []

        for i, sql in enumerate(statements, 1):
            try:
                self.spark.sql(sql)
                executed_sql.append(sql)
                messages.append(f"OK {i}")
            except Exception as exc:
                failed_sql.append(sql)
                messages.append(f"ERROR {i}: {type(exc).__name__}: {exc}")

        return ExecutionReport(
            messages=tuple(messages),
            executed_sql=tuple(executed_sql),
            executed_count=len(executed_sql),
            failed_sql=tuple(failed_sql),
            failed_count=len(failed_sql),
        )
