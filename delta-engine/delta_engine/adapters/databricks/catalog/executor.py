from datetime import datetime
from dataclasses import dataclass
from collections.abc import Sequence
from pyspark.sql import SparkSession
from delta_engine.application.results import TableExecutionReport
from delta_engine.adapters.databricks.catalog.compile import compile_plan
from delta_engine.domain.plan.actions import ActionPlan
from delta_engine.application.results import sql_preview, ExecutionFailure, ActionResult, ActionStatus



@dataclass(frozen=True, slots=True)
class DatabricksExecutor:
    spark: SparkSession

    def execute_table(self, plan: ActionPlan) -> TableExecutionReport:
        statements: Sequence[str] = compile_plan(plan)
        started = datetime.utcnow().isoformat()
        results: list[ActionResult] = []

        for index, stmt in enumerate(statements, start=1):
            p = sql_preview(stmt)
            action_started = datetime.utcnow().isoformat()
            try:
                if not stmt.strip():
                    results.append(ActionResult(index, ActionStatus.NOOP, action_started, datetime.utcnow().isoformat(), p))
                    continue
                self.spark.sql(stmt)
                results.append(ActionResult(index, ActionStatus.OK, action_started, datetime.utcnow().isoformat(), p))
            except Exception as exc:
                failure = ExecutionFailure(
                    fully_qualified_name=str(plan.target),
                    action_index=index,
                    exception_type=type(exc).__name__,
                    message="\n".join((str(exc) or type(exc).__name__).splitlines()[:5]),
                    statement_preview=p,
                )
                results.append(ActionResult(index, ActionStatus.FAILED, action_started, datetime.utcnow().isoformat(), p, failure))

        return TableExecutionReport(
            fully_qualified_name=str(plan.target),
            total_actions=len(statements),
            results=tuple(results),
            started_at=started,
            ended_at=datetime.utcnow().isoformat(),
        )
