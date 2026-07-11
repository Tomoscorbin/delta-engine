"""
Execute compiled statements on Databricks/Spark and capture results.

Compiles an `ActionPlan` to SQL statements via the shared compiler, then runs
them through the shared stop-on-first-failure loop with Spark as the runner
and py4j-aware exception type naming.
"""

from pyspark.sql import SparkSession

from delta_engine.adapters.databricks.execution import execute_statements
from delta_engine.adapters.databricks.spark.errors import exception_type_name
from delta_engine.adapters.databricks.sql import compile_plan
from delta_engine.application.ports import ExecutionSummary
from delta_engine.domain.model import QualifiedName
from delta_engine.domain.plan import ActionPlan


class SparkExecutor:
    """Plan executor that compiles plans to SQL and runs them via a Spark session."""

    def __init__(self, spark: SparkSession) -> None:
        self.spark = spark

    def compile(self, qualified_name: QualifiedName, plan: ActionPlan) -> tuple[str, ...]:
        """Compile ``plan`` to its SQL statements in execution order, without touching Spark."""
        return compile_plan(qualified_name, plan)

    def execute(self, statements: tuple[str, ...]) -> ExecutionSummary:
        """Execute each statement in order via the shared stop-on-first-failure loop."""
        return execute_statements(self.spark.sql, statements, exception_type_name)
