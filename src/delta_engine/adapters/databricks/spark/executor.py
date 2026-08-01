"""Compile plans and execute individual statements through Spark."""

from delta_engine.adapters.databricks.execution import execute_statement
from delta_engine.adapters.databricks.spark._runner import SparkSqlRunner
from delta_engine.adapters.databricks.sql import compile_plan
from delta_engine.application.ports import CompiledPlan
from delta_engine.domain.plan import ActionPlan


class SparkExecutor:
    """Plan executor that compiles plans to SQL and runs them via a Spark session."""

    def __init__(self, runner: SparkSqlRunner) -> None:
        self._runner = runner

    def compile(self, plan: ActionPlan) -> CompiledPlan:
        """Compile ``plan`` to its SQL statements in execution order, without touching Spark."""
        return compile_plan(plan)

    def execute(self, statement: str) -> None:
        """Execute one statement, translating Spark failures at the shared boundary."""
        execute_statement(self._runner.run, statement)
