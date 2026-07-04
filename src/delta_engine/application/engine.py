"""
High-level orchestration of planning, validation, and execution.

`Engine.sync` runs a chain of phases, each a pure transform over the per-table
runs — one `TableRunReport` is born per table in the read phase and accretes
its plan, failures, and execution as the chain proceeds. On a real run, if any
table fails, `SyncFailedError` is raised with a formatted summary.

The six phases, each taking the runs and returning them:
  1. Read     — fetch current catalog state; birth one run per table
  2. Diff     — compute the desired-observed diff; states the facts
  3. Validate — check every diff against rules; append per-table failures
  4. Plan     — lower every diff into its action plan
  5. Resolve  — order runs by FK dependency; append FK failures and
                propagate blocking to dependents
  6. Execute  — run the plan of every run with no failures and a non-empty plan

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

from datetime import UTC, datetime
import logging

from delta_engine.application.dependency_resolution import resolve
from delta_engine.application.desired_tables import DesiredTableSource, prepare_desired_tables
from delta_engine.application.errors import SyncFailedError
from delta_engine.application.failures import Failure
from delta_engine.application.ports import (
    CatalogState,
    CatalogStateReader,
    ExecutionSummary,
    PlanExecutor,
    ReadFailed,
    TablePresent,
)
from delta_engine.application.report import (
    SyncReport,
    TableRunReport,
)
from delta_engine.application.validation import validate_diff
from delta_engine.domain.model import QualifiedName
from delta_engine.domain.model.table import DesiredTable
from delta_engine.domain.plan.actions import ActionPlan
from delta_engine.domain.plan.diff import TableDiff, diff_table

logger = logging.getLogger(__name__)


class _TableRun:
    """
    Mutable scratch pad threaded through the sync phases.

    Born in the read phase, it accretes its diff, plan, failures, and execution
    as the phase chain proceeds, then is frozen into a public
    :class:`TableRunReport` once complete. Kept private to the engine so the
    published report stays immutable while the phases mutate in place.
    """

    def __init__(
        self,
        qualified_name: QualifiedName,
        desired: DesiredTable,
        read: CatalogState,
    ) -> None:
        self.qualified_name = qualified_name
        self.desired = desired
        self.read = read
        self.plan = ActionPlan()
        self.diff: TableDiff | None = None
        self.failures: list[Failure] = []
        self.execution: ExecutionSummary | None = None

    def to_report(self) -> TableRunReport:
        """Freeze this run into its public, immutable report."""
        return TableRunReport(
            qualified_name=self.qualified_name,
            desired=self.desired,
            read=self.read,
            plan=self.plan,
            failures=tuple(self.failures),
            execution=self.execution,
        )


class Engine:
    """
    High-level orchestrator to plan, validate, and execute changes.

    The engine coordinates reading current state from a catalog, computing a
    diff of desired vs observed state, validating that diff, lowering it to an
    action plan, resolving FK dependencies with full failure context, and
    executing passing plans using the provided adapter implementations.
    """

    def __init__(
        self,
        reader: CatalogStateReader,
        executor: PlanExecutor,
    ) -> None:
        """Initialize the engine with the catalog adapters it orchestrates."""
        self.reader = reader
        self.executor = executor

    def sync(self, *tables: DesiredTableSource, dry_run: bool = False) -> SyncReport:
        """
        Synchronize all registered tables to their desired state.

        Runs the phases as a chain, each transforming the per-table runs:
        read → diff → validate → plan → resolve → execute. Each
        ``TableRunReport`` is born in the read phase and accretes its diff,
        plan, failures, and execution as later phases run.

        A table that fails an early phase carries that failure forward and is
        skipped by execution; its partial run is still included in the report.

        Args:
            *tables: The table specifications to synchronize. Duplicate
                qualified names raise ``ValueError`` before any phase runs.
            dry_run: When True, run read → diff → validate → plan → resolve
                but skip execution (zero catalog mutations). Every run's
                ``execution`` stays ``None`` while its ``plan`` still records
                the actions that would be applied, and the report is returned
                instead of raising ``SyncFailedError`` even when a table would
                fail.

        Returns:
            The aggregate :class:`SyncReport` for the run.

        Raises:
            SyncFailedError: On a real run (``dry_run=False``), if any table
                fails to read, validate, resolve foreign keys, or execute. The
                report is available on the exception's ``report`` attribute. A
                dry run never raises.

        """
        run_started = datetime.now(UTC)
        desired = prepare_desired_tables(*tables)
        logger.info("Starting sync for %d table(s)", len(desired))

        runs = self._read(desired)
        runs = self._diff(runs)
        runs = self._validate(runs)
        runs = self._plan(runs)
        runs = self._resolve(runs)
        runs = self._execute(runs, dry_run=dry_run)

        report = SyncReport(
            started_at=run_started,
            ended_at=datetime.now(UTC),
            table_reports=tuple(run.to_report() for run in runs),
        )

        if not dry_run and report.any_failures:
            raise SyncFailedError(report)

        if dry_run:
            logger.info(
                "Dry run complete for %d table(s); no changes were applied",
                len(report.table_reports),
            )
        else:
            logger.info("Sync completed successfully for %d table(s)", len(report.table_reports))

        return report

    def _read(self, tables: tuple[DesiredTable, ...]) -> tuple[_TableRun, ...]:
        """Fetch current catalog state for every table, birthing one run each."""
        runs: list[_TableRun] = []
        for table in tables:
            qualified_name = table.qualified_name
            state = self.reader.fetch_state(qualified_name)
            run = _TableRun(qualified_name=qualified_name, desired=table, read=state)
            if isinstance(state, ReadFailed):
                logger.error(
                    "Read failed for %s: %s - %s",
                    qualified_name,
                    state.failure.exception_type,
                    state.failure.message,
                )
                run.failures.append(state.failure)
            else:
                logger.info(
                    "Read state for %s: %s",
                    qualified_name,
                    "present" if isinstance(state, TablePresent) else "absent",
                )
            runs.append(run)
        return tuple(runs)

    def _diff(self, runs: tuple[_TableRun, ...]) -> tuple[_TableRun, ...]:
        """Compute the desired-observed diff for each run; read-failed runs carry no diff."""
        for run in runs:
            if isinstance(run.read, ReadFailed):
                continue
            observed = run.read.table if isinstance(run.read, TablePresent) else None
            run.diff = diff_table(desired=run.desired, observed=observed)
        return runs

    def _validate(self, runs: tuple[_TableRun, ...]) -> tuple[_TableRun, ...]:
        """Validate every run's diff, appending any validation failures."""
        for run in runs:
            if run.diff is None:
                continue
            result = validate_diff(run.diff)
            if result.failed:
                logger.error(
                    "Validation failed for %s (%d failure(s))",
                    run.qualified_name,
                    len(result.failures),
                )
            else:
                logger.info("Validation passed for %s", run.qualified_name)
            run.failures.extend(result.failures)
        return runs

    def _plan(self, runs: tuple[_TableRun, ...]) -> tuple[_TableRun, ...]:
        """Build the action plan for each run by delegating to the diff."""
        for run in runs:
            if run.diff is None:
                continue
            run.plan = run.diff.plan()
            logger.info("Planned %d action(s) for %s", len(run.plan), run.qualified_name)
        return runs

    def _resolve(self, runs: tuple[_TableRun, ...]) -> tuple[_TableRun, ...]:
        """
        Order runs by FK dependency and fold in FK failures.

        Runs that already carry a failure (read or validation) seed the
        blocked set, so their FK dependents are blocked with
        BLOCKED_BY_FAILED_DEPENDENCY. Returns the runs in dependency-first order.
        """
        blocked = {run.qualified_name for run in runs if run.failures}
        result = resolve(tuple(run.desired for run in runs), blocked=blocked)
        by_name = {run.qualified_name: run for run in runs}
        for name, fk_failures in result.fk_failures.items():
            if fk_failures:
                logger.error(
                    "Foreign key resolution failed for %s (%d failure(s))",
                    name,
                    len(fk_failures),
                )
                by_name[name].failures.extend(fk_failures)
        return tuple(by_name[name] for name in result.ordered_names)

    def _execute(self, runs: tuple[_TableRun, ...], *, dry_run: bool) -> tuple[_TableRun, ...]:
        """
        Execute the plan of every run with no failures and a non-empty plan.

        Skipped runs pass through unchanged. Execution failures are appended to
        the run's ``failures`` and the summary is set on ``execution``. A dry run
        executes nothing and returns the runs unchanged.
        """
        if dry_run:
            return runs
        for run in runs:
            if run.failures or not run.plan:
                continue
            summary = self.executor.execute(run.qualified_name, run.plan)
            logger.info(
                "Executed %d action(s) for %s (%d failed)",
                len(summary.results),
                run.qualified_name,
                summary.failed_count,
            )
            run.execution = summary
            run.failures.extend(summary.failures)
        return runs
