from __future__ import annotations

import logging
import time
from dataclasses import dataclass
from typing import Callable, Literal, Sequence

from tabula.adapters.databricks.sql.compile import compile_plan
from tabula.application.results import ExecutionOutcome
from tabula.domain.plan.actions import ActionPlan

RunSql = Callable[[str], None]
OnError = Literal["stop", "continue"]

LOGGER = logging.getLogger("tabula.databricks.catalog.executor")


@dataclass(frozen=True, slots=True)
class CatalogPlanExecutor:
    """
    Execute compiled catalog DDL statements sequentially.

    - on_error:
        'stop'     -> stop at first error (default)
        'continue' -> attempt all statements; success=False if any failed
    - dry_run: do not execute, just return would-be SQL statements
    - logger: optional logger (defaults to module logger)
    """

    run_sql: RunSql
    on_error: OnError = "stop"
    dry_run: bool = False
    logger: logging.Logger | None = None

    def execute(self, plan: ActionPlan) -> ExecutionOutcome:
        statements: Sequence[str] = compile_plan(plan)
        logger = self.logger or LOGGER

        executed_sql: list[str] = []
        messages: list[str] = []
        durations_ms: list[float] = []
        attempted_failure = False
        executed_count = 0

        for index, statement in enumerate(statements, start=1):
            logger.debug("SQL[%s/%s] %s", index, len(statements), statement)

            if self.dry_run:
                executed_sql.append(statement)
                messages.append(f"DRY RUN {index}: {statement}")
                durations_ms.append(0.0)
                continue

            start = time.perf_counter()
            try:
                self.run_sql(statement)
                elapsed_ms = (time.perf_counter() - start) * 1000.0
                executed_sql.append(statement)
                durations_ms.append(elapsed_ms)
                messages.append(f"OK {index} in {elapsed_ms:.1f} ms")
                executed_count += 1
            except Exception as exc:
                elapsed_ms = (time.perf_counter() - start) * 1000.0
                durations_ms.append(elapsed_ms)
                error_message = (
                    f"ERROR {index} after {elapsed_ms:.1f} ms: "
                    f"{type(exc).__name__}: {exc}"
                )
                messages.append(error_message)
                logger.error(error_message)
                attempted_failure = True
                if self.on_error == "stop":
                    break
                # else continue with remaining statements

        success = not attempted_failure
        return ExecutionOutcome(
            success=success,
            messages=tuple(messages),
            executed_count=executed_count,
            executed_sql=tuple(executed_sql),
            durations_ms=tuple(durations_ms),
        )
