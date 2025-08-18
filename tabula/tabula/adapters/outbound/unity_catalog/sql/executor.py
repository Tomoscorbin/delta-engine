"""Sequential SQL executor for UC/Delta with policy, logging, and basic metrics."""

from __future__ import annotations
import logging
import time
from dataclasses import dataclass
from typing import Callable, List, Literal, Tuple

from tabula.application.results import ExecutionOutcome
from tabula.domain.model.actions import ActionPlan
from tabula.adapters.outbound.unity_catalog.sql.compile import compile_plan

RunSQL = Callable[[str], None]
OnError = Literal["stop", "continue"]

LOGGER = logging.getLogger("tabula.uc.sql")

@dataclass(frozen=True, slots=True)
class SqlPlanExecutor:
    """
    Execute a compiled UC plan statement-by-statement.

    - on_error:
        'stop'     -> stop at first error (default)
        'continue' -> attempt all statements; success=False if any failed
    - dry_run: do not execute, just return would-be SQL statements
    - logger: optional logger (defaults to module logger)
    """
    run_sql: RunSQL
    on_error: OnError = "stop"
    dry_run: bool = False
    logger: logging.Logger | None = None

    def execute(self, plan: ActionPlan) -> ExecutionOutcome:
        statements: Tuple[str, ...] = compile_plan(plan)
        log = self.logger or LOGGER

        executed_sql: List[str] = []
        messages: List[str] = []
        durations_ms: List[float] = []
        any_failure = False

        for idx, sql in enumerate(statements, start=1):
            log.debug("SQL[%s/%s] %s", idx, len(statements), sql)

            if self.dry_run:
                executed_sql.append(sql)
                messages.append(f"DRY RUN {idx}: {sql}")
                durations_ms.append(0.0)
                continue

            start = time.perf_counter()
            try:
                self.run_sql(sql)
                elapsed_ms = (time.perf_counter() - start) * 1000.0
                executed_sql.append(sql)
                durations_ms.append(elapsed_ms)
                messages.append(f"OK {idx} in {elapsed_ms:.1f} ms")
            except Exception as exc:
                any_failure = True
                elapsed_ms = (time.perf_counter() - start) * 1000.0
                durations_ms.append(elapsed_ms)  # attempted
                err_msg = f"ERROR {idx} after {elapsed_ms:.1f} ms: {type(exc).__name__}: {exc}"
                messages.append(err_msg)
                log.error(err_msg)
                if self.on_error == "stop":
                    break
                # else continue

        success = not any_failure
        return ExecutionOutcome(
            success=success,
            messages=tuple(messages),
            executed_count=len(executed_sql),
            executed_sql=tuple(executed_sql),
            durations_ms=tuple(durations_ms),
        )
