"""
High-level orchestration of planning, validation, and execution.

`Engine.sync` runs a chain of phases, each a pure transform over the per-table
runs — one `TableRunReport` is born per table in the read phase and accretes
its plan, failures, and execution as the chain proceeds. If any table fails,
`SyncFailedError` is raised with a formatted summary.

The five phases, each taking the runs and returning them:
  1. Read     — fetch current catalog state; birth one run per table
  2. Plan     — compute action plans from desired vs observed state
  3. Validate — check every plan against rules; append per-table failures
  4. Resolve  — order runs by FK dependency; append FK failures and
                propagate blocking to dependents
  5. Execute  — run the plan of every run with no failures and a non-empty plan

Running `resolve()` after validation means a table that fails validation
blocks its FK dependents with BLOCKED_BY_FAILED_DEPENDENCY, not just tables
with FK-structural failures (CYCLE / UNRESOLVABLE_REFERENCE). The rule is
uniform: if a dependency won't reach desired state this sync, its dependents
don't execute either.

A table that fails an early phase carries that failure forward on its run and
is skipped by execution, so all tables are attempted and the report is always
complete.
"""

from __future__ import annotations

from dataclasses import replace
from datetime import UTC, datetime
import logging

from delta_engine.application.dependency_resolution import resolve
from delta_engine.application.errors import SyncFailedError
from delta_engine.application.ports import CatalogStateReader, PlanExecutor
from delta_engine.application.registry import Registry
from delta_engine.application.results import (
    ReadFailed,
    SyncReport,
    TablePresent,
    TableRunReport,
)
from delta_engine.application.validation import validate_plan
from delta_engine.domain.model.table import DesiredTable
from delta_engine.domain.plan.actions import ActionPlan
from delta_engine.domain.plan.differ import compute_plan

logger = logging.getLogger(__name__)


class Engine:
    """
    High-level orchestrator to plan, validate, and execute changes.

    The engine coordinates reading current state from a catalog, computing a
    plan to reach desired state, validating that plan, resolving FK dependencies
    with full failure context, and executing passing plans using the provided
    adapter implementations.
    """

    def __init__(
        self,
        reader: CatalogStateReader,
        executor: PlanExecutor,
    ) -> None:
        """Initialize the engine with the catalog adapters it orchestrates."""
        self.reader = reader
        self.executor = executor

    def sync(self, registry: Registry, *, dry_run: bool = False) -> SyncReport:
        """
        Synchronize all registered tables to their desired state.

        Runs the phases as a chain, each transforming the per-table runs:
        read → plan → validate → resolve → execute. Each ``TableRunReport``
        is born in the read phase and accretes its plan, failures, and
        execution as later phases run.

        A table that fails an early phase carries that failure forward and is
        skipped by execution; its partial run is still included in the report.

        Args:
            registry: The tables to synchronize.
            dry_run: When True, run read → plan → validate → resolve but skip
                execution (zero catalog mutations). Every run's ``execution``
                stays ``None`` while its ``plan`` still records the actions that
                would be applied, and the report is returned instead of raising
                ``SyncFailedError`` even when a table would fail.

        Returns:
            The aggregate :class:`SyncReport` for the run.

        Raises:
            SyncFailedError: On a real run (``dry_run=False``), if any table
                fails to read, validate, or execute. The report is available on
                the exception's ``report`` attribute. A dry run never raises.

        """
        run_started = datetime.now(UTC)
        logger.info("Starting sync for %d table(s)", len(registry))

        runs = self._read(tuple(registry))
        runs = self._plan(runs)
        runs = self._validate(runs)
        runs = self._resolve(runs)
        runs = self._execute(runs, dry_run=dry_run)

        report = SyncReport(
            started_at=run_started,
            ended_at=datetime.now(UTC),
            table_reports=runs,
        )
        if not dry_run and report.any_failures:
            raise SyncFailedError(report)
        self._log_outcome(report, dry_run=dry_run)
        return report

    def _read(self, tables: tuple[DesiredTable, ...]) -> tuple[TableRunReport, ...]:
        """Fetch current catalog state for every table, birthing one run each."""
        runs: list[TableRunReport] = []
        for table in tables:
            qualified_name = table.qualified_name
            state = self.reader.fetch_state(qualified_name)
            if isinstance(state, ReadFailed):
                logger.error(
                    "Read failed for %s: %s - %s",
                    qualified_name,
                    state.failure.exception_type,
                    state.failure.message,
                )
                runs.append(
                    TableRunReport(
                        qualified_name=qualified_name,
                        desired=table,
                        read=state,
                        failures=(state.failure,),
                    )
                )
            else:
                logger.info(
                    "Read state for %s: %s",
                    qualified_name,
                    "present" if isinstance(state, TablePresent) else "absent",
                )
                runs.append(
                    TableRunReport(qualified_name=qualified_name, desired=table, read=state)
                )
        return tuple(runs)

    def _plan(self, runs: tuple[TableRunReport, ...]) -> tuple[TableRunReport, ...]:
        """Compute an action plan for each run; read-failed runs get an empty plan."""
        planned: list[TableRunReport] = []
        for run in runs:
            if isinstance(run.read, ReadFailed):
                planned.append(replace(run, plan=ActionPlan()))
                continue
            observed = run.read.table if isinstance(run.read, TablePresent) else None
            plan = compute_plan(desired=run.desired, observed=observed)
            logger.info("Planned %d action(s) for %s", len(plan), run.qualified_name)
            planned.append(replace(run, plan=plan))
        return tuple(planned)

    def _validate(self, runs: tuple[TableRunReport, ...]) -> tuple[TableRunReport, ...]:
        """Validate every run's plan, appending any validation failures."""
        validated: list[TableRunReport] = []
        for run in runs:
            result = validate_plan(run.plan)
            if result.failed:
                logger.error(
                    "Validation failed for %s (%d failure(s))",
                    run.qualified_name,
                    len(result.failures),
                )
            else:
                logger.info("Validation passed for %s", run.qualified_name)
            validated.append(replace(run, failures=run.failures + tuple(result.failures)))
        return tuple(validated)

    def _resolve(self, runs: tuple[TableRunReport, ...]) -> tuple[TableRunReport, ...]:
        """
        Order runs by FK dependency and fold in FK failures.

        Runs that already carry a failure (read or validation) seed the
        blocked set, so their FK dependents are blocked with
        BLOCKED_BY_FAILED_DEPENDENCY. Returns the runs in dependency-first order.
        """
        blocked = frozenset(run.qualified_name for run in runs if run.failures)
        result = resolve(tuple(run.desired for run in runs), blocked=blocked)
        by_name = {run.qualified_name: run for run in runs}
        return tuple(
            replace(
                by_name[name],
                failures=by_name[name].failures + result.fk_failures.get(name, ()),
            )
            for name in result.ordered_names
        )

    def _execute(
        self, runs: tuple[TableRunReport, ...], *, dry_run: bool
    ) -> tuple[TableRunReport, ...]:
        """
        Execute the plan of every run with no failures and a non-empty plan.

        Skipped runs pass through unchanged. Execution failures are appended to
        the run's ``failures`` and the summary is set on ``execution``. A dry run
        executes nothing and returns the runs unchanged.
        """
        if dry_run:
            return runs
        executed: list[TableRunReport] = []
        for run in runs:
            if run.failures or not run.plan:
                executed.append(run)
                continue
            summary = self.executor.execute(run.qualified_name, run.plan)
            logger.info(
                "Executed %d action(s) for %s (%d failed)",
                len(summary.results),
                run.qualified_name,
                summary.failed_count,
            )
            executed.append(
                replace(run, execution=summary, failures=run.failures + summary.failures)
            )
        return tuple(executed)

    def _log_outcome(self, report: SyncReport, *, dry_run: bool) -> None:
        """Log the run outcome (dry-run notice or success line)."""
        if dry_run:
            logger.info(
                "Dry run complete for %d table(s); no changes were applied",
                len(report.table_reports),
            )
        else:
            logger.info("Sync completed successfully for %d table(s)", len(report.table_reports))
