"""
High-level orchestration of planning and execution.

`Engine.sync` runs a chain of phases, each a pass over the per-table
runs — one `TableRunReport` is born per table in the read phase and accretes
its plan, SQL, failures, and execution as the chain proceeds. On a real run, if
any table fails, `SyncFailedError` is raised with a formatted summary.

The six phases, each taking the runs and returning them:
  1. Read     — fetch current catalog state; birth one run per table
  2. Diff     — compute direct actions and non-action differences
  3. Plan     — validate each diff, then accept a plan or append failures
  4. Compile  — lower every accepted plan to its exact backend statements
  5. Resolve  — order runs by FK dependency; append FK failures and
                propagate blocking to dependents
  6. Execute  — run the plan of every run with no failures and a non-empty
                plan, blocking FK dependents of runs that fail mid-execution

Compilation deliberately precedes resolution. An accepted plan is a
table-local fact, so it receives the exact SQL exposed on the report before
cross-table dependency checks decide whether it may execute. A later FK failure
therefore does not erase a valid preview or force compilation to understand
resolution failures.

Running `resolve()` after validation means a table that fails validation
blocks its FK dependents with BLOCKED_BY_FAILED_DEPENDENCY, not just tables
with FK-structural failures (CYCLE / UNRESOLVABLE_REFERENCE). Execution
applies the same rule as it walks the dependency-ordered runs: a run whose
dependency fails during execution is blocked rather than executed. The rule
is uniform: if a dependency won't reach desired state this sync, its
dependents don't execute either.

A table that fails an early phase carries that failure forward on its run and
is skipped by execution, so all tables are attempted and the report is always
complete.
"""

from dataclasses import dataclass, field
from datetime import UTC, datetime
import logging
from typing import assert_never

from delta_engine.application.dependency_resolution import (
    ResolutionFailed,
    ResolutionSucceeded,
    TableResolution,
    resolve,
)
from delta_engine.application.errors import DuplicateTableDefinitionError, SyncFailedError
from delta_engine.application.failures import Failure
from delta_engine.application.planning import PlanningFailed, PlanningSucceeded, plan_diff
from delta_engine.application.ports import (
    CatalogState,
    CatalogStateReader,
    DesiredTableSource,
    ExecutionSummary,
    PlanExecutor,
    ReadFailed,
    TableAbsent,
    TablePresent,
)
from delta_engine.application.report import (
    SyncReport,
    TableRunReport,
)
from delta_engine.domain.model import DesiredTable, ObservedTable, QualifiedName
from delta_engine.domain.plan import ActionPlan, TableDiff, diff_table

logger = logging.getLogger(__name__)


def prepare_desired_tables(*tables: DesiredTableSource) -> tuple[DesiredTable, ...]:
    """
    Lower table specifications into domain tables for the phase chain.

    Converts each source via ``to_desired_table()``, rejects duplicate
    qualified names, and returns the tables in deterministic qualified-name
    order so a sync's report and execution order never depend on the order
    tables were passed. Passing no tables yields an empty tuple.

    Public so drivers such as the CLI can run the same duplicate check
    before acquiring a backend connection; the rule lives only here.

    Raises:
        DuplicateTableDefinitionError: If two sources share a qualified name.

    """
    desired_by_name: dict[str, DesiredTable] = {}
    for source in tables:
        desired = source.to_desired_table()
        key = str(desired.qualified_name)
        if key in desired_by_name:
            raise DuplicateTableDefinitionError(desired.qualified_name)
        desired_by_name[key] = desired
    return tuple(desired_by_name[key] for key in sorted(desired_by_name))


@dataclass(slots=True)
class _TableRun:
    """
    Mutable scratch pad threaded through the sync phases.

    Born in the read phase, it accretes its diff, plan, compiled SQL,
    resolution, failures, and execution as the phase chain proceeds, then is
    frozen into a public :class:`TableRunReport` once complete. Kept private to
    the engine so the published report stays immutable while the phases mutate
    in place.
    """

    qualified_name: QualifiedName
    desired: DesiredTable
    read: CatalogState
    plan: ActionPlan = field(default_factory=ActionPlan)
    planned_sql_statements: tuple[str, ...] = ()
    diff: TableDiff | None = None
    failures: list[Failure] = field(default_factory=list)
    resolution: TableResolution | None = None
    execution: ExecutionSummary | None = None

    @property
    def has_failures(self) -> bool:
        """True when any completed phase has failed for this table."""
        return bool(self.failures)

    def to_report(self) -> TableRunReport:
        """Freeze this run into its public, immutable report."""
        return TableRunReport(
            qualified_name=self.qualified_name,
            desired=self.desired,
            read=self.read,
            plan=self.plan,
            planned_sql_statements=self.planned_sql_statements,
            failures=tuple(self.failures),
            execution=self.execution,
        )


class Engine:
    """
    High-level orchestrator to plan and execute changes.

    The engine coordinates reading current state from a catalog, computing a
    diff of desired vs observed state, accepting or rejecting that diff at the
    validated planning boundary, resolving FK dependencies with full failure
    context, and executing accepted plans using the provided adapters.
    """

    def __init__(
        self,
        reader: CatalogStateReader,
        executor: PlanExecutor,
    ) -> None:
        self.reader = reader
        self.executor = executor

    def sync(self, *tables: DesiredTableSource, dry_run: bool = False) -> SyncReport:
        """
        Synchronize all registered tables to their desired state.

        Runs the phases as a chain, each transforming the per-table runs:
        read → diff → plan → compile → resolve → execute. Each
        ``TableRunReport`` is born in the read phase and accretes its diff,
        plan, compiled SQL, failures, and execution as later phases run.

        A table that fails an early phase carries that failure forward and is
        skipped by execution; its partial run is still included in the report.

        Args:
            *tables: The table specifications to synchronize. Duplicate
                qualified names raise ``DuplicateTableDefinitionError`` before
                any phase runs.
            dry_run: When True, run read → diff → plan → compile → resolve
                but skip execution (zero catalog mutations). Every run's
                ``execution`` stays ``None`` while its ``plan`` still records
                the actions compiled from the observed snapshot, and the report
                is returned instead of raising ``SyncFailedError`` when a
                pre-execution phase fails.

        Returns:
            The aggregate :class:`SyncReport` for the run.

        Raises:
            DuplicateTableDefinitionError: If two table specifications have
                the same qualified name. No phase has run when this is raised.
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
        runs = self._plan(runs)
        runs = self._compile(runs)
        runs = self._resolve(runs)

        if not dry_run:
            runs = self._execute(runs)

        report = SyncReport(
            started_at=run_started,
            ended_at=datetime.now(UTC),
            table_reports=tuple(run.to_report() for run in runs),
            dry_run=dry_run,
        )

        if not dry_run and report.has_failures:
            raise SyncFailedError(report)

        logger.info(
            "%s completed successfully for %d table(s)",
            "Dry run" if dry_run else "Sync",
            len(report.table_reports),
        )

        return report

    def _read(self, tables: tuple[DesiredTable, ...]) -> tuple[_TableRun, ...]:
        """Fetch current catalog state for every table, birthing one run each."""
        runs: list[_TableRun] = []
        for table in tables:
            qualified_name = table.qualified_name
            state = self.reader.fetch_state(qualified_name)
            run = _TableRun(qualified_name=qualified_name, desired=table, read=state)

            match state:
                case ReadFailed(failure=failure):
                    run.failures.append(failure)
                    logger.error(
                        "Read failed for %s: %s - %s",
                        qualified_name,
                        failure.exception_type,
                        failure.message,
                    )
                case TablePresent():
                    logger.info("Table present: %s", qualified_name)
                case TableAbsent():
                    logger.info("Table absent: %s", qualified_name)
                case _ as unreachable:
                    assert_never(unreachable)

            runs.append(run)
        return tuple(runs)

    def _diff(self, runs: tuple[_TableRun, ...]) -> tuple[_TableRun, ...]:
        """Compute the desired-observed diff for each run; read-failed runs carry no diff."""
        for run in runs:
            observed: ObservedTable | None
            match run.read:
                case ReadFailed():
                    continue
                case TablePresent(table=table):
                    observed = table
                case TableAbsent():
                    observed = None

            run.diff = diff_table(desired=run.desired, observed=observed)
        return runs

    def _plan(self, runs: tuple[_TableRun, ...]) -> tuple[_TableRun, ...]:
        """
        Accept or reject each diff according to the default planning policy.

        Rejected runs retain an empty plan and carry validation failures;
        accepted runs carry the validated action plan into compilation.
        """
        for run in runs:
            if run.diff is None:
                continue

            match plan_diff(run.diff):
                case PlanningFailed(failures=failures):
                    run.failures.extend(failures)
                    logger.error(
                        "Planning failed for %s",
                        run.qualified_name,
                    )
                case PlanningSucceeded(plan=plan):
                    run.plan = plan
                    logger.info("Planned %d action(s) for %s", len(run.plan), run.qualified_name)
                case _ as unreachable:
                    assert_never(unreachable)
        return runs

    def _compile(self, runs: tuple[_TableRun, ...]) -> tuple[_TableRun, ...]:
        """
        Lower every accepted plan to the exact statements exposed and executed.

        Compilation is a distinct backend boundary after planning: a dry run
        reports these statements, while a real run passes the same tuple to
        execution. Runs rejected by an earlier phase carry no compiled SQL.
        """
        for run in runs:
            if run.diff is None or run.has_failures:
                continue

            run.planned_sql_statements = self.executor.compile(run.qualified_name, run.plan)
            logger.info(
                "Compiled %d statement(s) for %s",
                len(run.planned_sql_statements),
                run.qualified_name,
            )
        return runs

    def _resolve(self, runs: tuple[_TableRun, ...]) -> tuple[_TableRun, ...]:
        """
        Order runs by FK dependency and fold in FK failures.

        Runs that already carry a failure (read or planning) seed the
        failed-name set, so their FK dependents are blocked with
        BLOCKED_BY_FAILED_DEPENDENCY. Returns the runs in dependency-first order.
        """
        failed_names = {run.qualified_name for run in runs if run.has_failures}
        ordered_resolutions = resolve(
            tables=tuple(run.desired for run in runs),
            failed_names=failed_names,
        )
        runs_by_name = {run.qualified_name: run for run in runs}
        ordered_runs: list[_TableRun] = []

        for resolution in ordered_resolutions:
            match resolution:
                case ResolutionSucceeded(qualified_name=name):
                    run = runs_by_name[name]
                    run.resolution = resolution
                    ordered_runs.append(run)
                case ResolutionFailed(qualified_name=name, failures=failures):
                    run = runs_by_name[name]
                    run.resolution = resolution
                    run.failures.extend(failures)
                    ordered_runs.append(run)
                    logger.error("Foreign key resolution failed for %s", name)
                case _ as unreachable:
                    assert_never(unreachable)

        return tuple(ordered_runs)

    def _execute(self, runs: tuple[_TableRun, ...]) -> tuple[_TableRun, ...]:
        """
        Execute the plan of every run with no failures and a non-empty plan.

        Walks the resolved runs in dependency-first order, tracking tables that
        fail during execution. A run whose resolved dependency references one is
        blocked with BLOCKED_BY_FAILED_DEPENDENCY instead of executed, so an
        execution failure in a parent blocks its FK dependents in the same
        sync — even dependents with no work of their own. A run with an empty
        plan and no blocking failures is skipped and counts as a healthy
        parent. Execution failures are appended to the run's ``failures`` and
        the summary is set on ``execution``.
        """
        failed_during_execution: set[QualifiedName] = set()

        for run in runs:
            if run.has_failures:
                continue

            resolution = run.resolution
            if not isinstance(resolution, ResolutionSucceeded):
                raise RuntimeError(
                    f"Executable table was not successfully resolved: {run.qualified_name}"
                )

            dependency_failures = tuple(
                dependency.blocked_failure
                for dependency in resolution.dependencies
                if dependency.referenced_table in failed_during_execution
            )
            if dependency_failures:
                run.failures.extend(dependency_failures)
                failed_during_execution.add(run.qualified_name)
                logger.error(
                    "Execution blocked for %s (%d foreign key failure(s))",
                    run.qualified_name,
                    len(dependency_failures),
                )
                continue

            # A no-op table must still be blocked when its dependency failed.
            if not run.plan:
                continue

            summary = self.executor.execute(run.planned_sql_statements)
            run.execution = summary
            run.failures.extend(summary.failures)

            if summary.failed:
                failed_during_execution.add(run.qualified_name)

            logger.info(
                "Executed %d statement(s) for %s (%d failed)",
                len(summary.results),
                run.qualified_name,
                summary.failed_count,
            )

        return runs
