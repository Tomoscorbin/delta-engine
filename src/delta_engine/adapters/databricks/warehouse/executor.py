"""Compile plans and execute individual statements through a SQL warehouse."""

from delta_engine.adapters.databricks.execution import execute_statement
from delta_engine.adapters.databricks.sql import compile_plan
from delta_engine.adapters.databricks.warehouse._runner import WarehouseSqlRunner
from delta_engine.application.ports import CatalogSpellings
from delta_engine.domain.plan import ActionPlan


class WarehouseExecutor:
    """Plan executor that compiles plans to SQL and runs them on a SQL warehouse."""

    def __init__(self, runner: WarehouseSqlRunner) -> None:
        self._runner = runner

    def compile(self, plan: ActionPlan, spellings: CatalogSpellings) -> tuple[str, ...]:
        """
        Compile ``plan`` to its SQL statements in execution order.

        Statements are rendered in the catalog's column spellings. Does not
        touch the warehouse.
        """
        return compile_plan(plan, spellings)

    def execute(self, statement: str) -> None:
        """
        Execute one statement through the backend-private runner.

        Cursor acquisition and execution happen inside the translated callable,
        so both failures become the same application error. The runner owns
        cursor cleanup without replacing the statement outcome.
        """
        execute_statement(self._runner.run, statement)
