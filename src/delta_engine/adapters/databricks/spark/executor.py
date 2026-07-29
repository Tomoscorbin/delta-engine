"""Compile plans and execute individual statements through Spark."""

from delta_engine.adapters.databricks.execution import execute_statement
from delta_engine.adapters.databricks.spark._runner import SparkSqlRunner
from delta_engine.adapters.databricks.sql import compile_plan
from delta_engine.application.ports import CatalogSpellings
from delta_engine.domain.plan import ActionPlan


class SparkExecutor:
    """Plan executor that compiles plans to SQL and runs them via a Spark session."""

    def __init__(self, runner: SparkSqlRunner) -> None:
        self._runner = runner

    def compile(self, plan: ActionPlan, spellings: CatalogSpellings) -> tuple[str, ...]:
        """
        Compile ``plan`` to its SQL statements in execution order.

        Statements are rendered in the catalog's column spellings, without
        touching Spark.
        """
        return compile_plan(plan, spellings)

    def execute(self, statement: str) -> None:
        """Execute one statement, translating Spark failures at the shared boundary."""
        execute_statement(self._runner.run, statement)
