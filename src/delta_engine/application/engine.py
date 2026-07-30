"""
High-level orchestration of planning and execution.

`Engine.sync` threads one private table run through eight phases. Each run
retains the canonical outcome of every completed phase; failures, status, and
public report views are derived from those outcomes rather than copied into
parallel fields. On a real run, if any table fails, `SyncFailedError` is raised
with a formatted summary.

The phases are:
  1. Lower    — lower the declaration set into domain tables, deduplicated
  2. Resolve  — birth one run per table, FK-dependency-first, carrying its
                dependency edges and structural verdicts
  3. Read     — fetch the current catalog state of every declared table
  4. Diff     — compute direct actions and non-action differences in place
  5. Plan     — retain an accepted plan of the complete diff, or the
                rejected planning outcome
  6. Compile  — retain the exact backend statements for every accepted plan
  7. Gate     — block dependents of failed tables (dry and real runs)
  8. Execute  — retain attempted statement results (real runs only)

The declaration set is judged before the world is consulted: resolution reads
only declarations, so a run is born knowing its position, its edges, and its
structural verdicts, and no later phase has to narrow a union to reach them.

Resolution is a pure structural judgment of the declarations against each
other (CYCLE, UNRESOLVABLE_REFERENCE, ...). Whether a table is *blocked*
by another table's failure is decided later, at the execution gate: walking
the dependency-ordered runs, a run whose resolved dependency will not reach
desired state this sync — a failed read, a rejected plan, a failed
resolution, an execution failure, or a block of its own — is blocked with
BLOCKED_BY_FAILED_DEPENDENCY rather than executed, so the block propagates
along FK chains. The rule is uniform: if a dependency won't reach desired
state this sync, its dependents don't execute either. A dry run applies the
same gate without attempting statements, so blocking stays visible in
previews.

A table that fails an early phase carries that failure forward on its run and
is skipped by execution, so all tables are attempted and the report is always
complete.
"""

from dataclasses import dataclass
from datetime import UTC, datetime
import logging
from typing import assert_never

from delta_engine.application.errors import (
    DuplicateTableDefinitionError,
    ExecutionError,
    ReadError,
    SyncFailedError,
)
from delta_engine.application.failures import (
    ExecutionFailure,
    ReadFailure,
)
from delta_engine.application.planning import (
    PlanningFailed,
    PlanningResult,
    PlanningSucceeded,
    plan_diff,
)
from delta_engine.application.ports import (
    CatalogStateReader,
    DesiredTableSource,
    ExecutionResult,
    ExecutionSucceeded,
    ExecutionSummary,
    PlanExecutor,
    ReadResult,
    TableAbsent,
    TablePresent,
)
from delta_engine.application.relationships import TableResolution, resolve
from delta_engine.application.report import (
    ExecutionBlockedByDependency,
    ExecutionOutcome,
    SyncReport,
    TableRunReport,
)
from delta_engine.domain.model import (
    DesiredTable,
    QualifiedName,
)
from delta_engine.domain.plan import (
    ActionPlan,
    TableDiff,
    diff_table,
)

logger = logging.getLogger(__name__)


def lower_desired_tables(*tables: DesiredTableSource) -> tuple[DesiredTable, ...]:
    """
    Lower table specifications into domain tables for the phase chain.

    Converts each source via ``to_desired_table()``, rejects duplicate
    qualified names, and returns the tables in deterministic qualified-name
    order so a sync's report and execution order never depend on the order
    tables were passed. Passing no tables yields an empty tuple. The system
    lowers at both edges: specifications into domain tables here, and plans
    into SQL at translation.

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

    Born in the resolve phase, in dependency order, carrying its resolution —
    which holds its static facts: the declaration, its dependency edges, and
    its structural verdicts. Its read, diff, planning outcome, compiled SQL,
    and execution outcome accrete as the worldly phases run, each written
    once, then the run is frozen into a public :class:`TableRunReport`. Kept
    private to the engine so the published report stays immutable while the
    phases mutate in place.
    """

    resolution: TableResolution
    read: ReadResult | None = None
    diff: TableDiff | None = None
    planning: PlanningResult | None = None
    planned_sql_statements: tuple[str, ...] = ()
    execution: ExecutionOutcome | None = None

    @property
    def desired(self) -> DesiredTable:
        return self.resolution.desired

    @property
    def qualified_name(self) -> QualifiedName:
        return self.resolution.qualified_name

    @property
    def plan(self) -> ActionPlan | None:
        match self.planning:
            case PlanningSucceeded(plan=plan):
                return plan
            case PlanningFailed() | None:
                return None
            case _ as unreachable:
                assert_never(unreachable)

    @property
    def has_failures(self) -> bool:
        """True when any completed phase has failed for this table."""
        return (
            isinstance(self.read, ReadFailure)
            or isinstance(self.planning, PlanningFailed)
            or bool(self.resolution.structural_failures)
            or isinstance(self.execution, ExecutionBlockedByDependency)
            or (isinstance(self.execution, ExecutionSummary) and bool(self.execution.failures))
        )

    def to_report(self) -> TableRunReport:
        """Freeze this run into its public, immutable report."""
        if self.read is None:
            raise RuntimeError(f"Run was frozen before its read completed: {self.qualified_name}")
        return TableRunReport(
            read=self.read,
            planning=self.planning,
            planned_sql_statements=self.planned_sql_statements,
            resolution=self.resolution,
            execution_outcome=self.execution,
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

        Runs lower → resolve → read → diff → plan → compile → gate → execute.
        Resolution births one private run per table in dependency-first order
        carrying its static facts, the worldly phases enrich the runs in
        place, the gate blocks dependents of failed tables, and the completed
        outcomes are finally frozen into ``TableRunReport`` values.

        A table that fails an early phase carries that failure forward and is
        skipped by execution; its partial run is still included in the report.

        Args:
            *tables: The table specifications to synchronize. Duplicate
                qualified names raise ``DuplicateTableDefinitionError`` before
                any phase runs.
            dry_run: When True, run every phase but attempt no statements
                (zero catalog mutations). The execution gate still records
                dependency blocking, so a dependent of a failed table reports
                BLOCKED_BY_FAILED_DEPENDENCY in the preview; no run retains
                attempted statement results, while its ``plan`` still records
                the actions compiled from the observed snapshot. The report is
                returned instead of raising ``SyncFailedError`` when a table
                fails.

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
        desired = lower_desired_tables(*tables)
        logger.info("Starting sync for %d table(s)", len(desired))

        runs = self._resolve(desired)
        self._read(runs)
        self._diff(runs)
        self._plan(runs)
        self._compile(runs)
        self._gate(runs)
        if not dry_run:
            self._execute(runs)

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

    def _read(self, runs: tuple[_TableRun, ...]) -> None:
        """Fetch the current catalog state of every run's table."""
        for run in runs:
            read: ReadResult
            try:
                read = self.reader.fetch_state(run.qualified_name)
            except ReadError as error:
                read = ReadFailure(
                    exception_type=error.exception_type,
                    message=str(error),
                )

            run.read = read

            match read:
                case ReadFailure() as failure:
                    logger.error(
                        "Read failed for %s: %s - %s",
                        run.qualified_name,
                        failure.exception_type,
                        failure.message,
                    )
                case TablePresent():
                    logger.info("Table present: %s", run.qualified_name)
                case TableAbsent():
                    logger.info("Table absent: %s", run.qualified_name)
                case _ as unreachable:
                    assert_never(unreachable)

    def _diff(self, runs: tuple[_TableRun, ...]) -> None:
        """Diff each readable run; read-failed runs carry no diff."""
        for run in runs:
            match run.read:
                case ReadFailure() | None:
                    continue
                case TablePresent(table=observed_table):
                    run.diff = diff_table(run.desired, observed_table)
                case TableAbsent():
                    run.diff = diff_table(run.desired, None)
                case _ as unreachable:
                    assert_never(unreachable)

    def _plan(self, runs: tuple[_TableRun, ...]) -> None:
        """
        Accept or reject each diff according to the default planning policy.

        Each diff is judged as one complete stream. Rejected runs retain
        ``PlanningFailed``; accepted runs retain ``PlanningSucceeded`` with the
        validated action plan.
        """
        for run in runs:
            if run.diff is None:
                continue

            planning = plan_diff(run.diff)
            run.planning = planning

            match planning:
                case PlanningFailed():
                    logger.error("Planning failed for %s", run.qualified_name)
                case PlanningSucceeded(plan=plan):
                    logger.info("Planned %d action(s) for %s", len(plan), run.qualified_name)
                case _ as unreachable:
                    assert_never(unreachable)

    def _compile(self, runs: tuple[_TableRun, ...]) -> None:
        """
        Lower every accepted plan to the exact statements exposed and executed.

        Compilation is a distinct backend boundary after planning: a dry run
        reports these statements, while a real run passes the same tuple to
        execution. Runs rejected by an earlier phase carry no compiled SQL.
        """
        for run in runs:
            plan = run.plan
            if plan is None:
                continue

            run.planned_sql_statements = self.executor.compile(
                plan,
            )
            logger.info(
                "Compiled %d statement(s) for %s",
                len(run.planned_sql_statements),
                run.qualified_name,
            )

    def _resolve(self, tables: tuple[DesiredTable, ...]) -> tuple[_TableRun, ...]:
        """
        Birth one run per table, dependency-first, with its resolution outcome.

        Resolution is structural: it judges the declarations against each
        other, before any catalog state is fetched. Whether a table is
        *blocked* by another's failure is decided later, by the gating walk in
        ``_gate``. Every run carries a resolution from birth, so no later
        phase has to consider a half-resolved table.
        """
        runs: list[_TableRun] = []

        for resolution in resolve(tables):
            runs.append(_TableRun(resolution=resolution))

            if resolution.structural_failures:
                logger.error("Foreign key resolution failed for %s", resolution.qualified_name)

        return tuple(runs)

    def _gate(self, runs: tuple[_TableRun, ...]) -> None:
        """
        Block every run whose dependency already failed an earlier phase.

        Walks the resolved runs in dependency-first order, seeding the
        will-not-converge set with every run that failed to read, resolve, or
        plan, and recording ExecutionBlockedByDependency on each of their
        dependents — which then blocks its own dependents, so the block
        propagates along FK chains in one pass. The gate runs on dry and real
        runs alike: blocking is part of the preview, and execution afterwards
        only has to react to failures that arise while statements run.
        """
        failed: set[QualifiedName] = {run.qualified_name for run in runs if run.has_failures}

        for run in runs:
            if run.has_failures:
                continue

            blocking_failures = run.resolution.blocked_by(failed)
            if blocking_failures:
                run.execution = ExecutionBlockedByDependency(blocking_failures)
                failed.add(run.qualified_name)
                logger.error(
                    "Execution blocked for %s (%d foreign key failure(s))",
                    run.qualified_name,
                    len(blocking_failures),
                )

    def _execute(self, runs: tuple[_TableRun, ...]) -> None:
        """
        Execute the plan of every gated run with no failures and a non-empty plan.

        The gate has already blocked dependents of tables that failed an
        earlier phase, so this walk reacts only to failures that arise while
        statements run: a run whose resolved dependency fails mid-execution is
        blocked with BLOCKED_BY_FAILED_DEPENDENCY instead of executed — even a
        dependent with no work of its own. A run with an empty plan and no
        blocking failures is skipped and counts as a healthy parent. The
        execution outcome remains the sole source of execution or runtime
        dependency failures.
        """
        failed_during_execution: set[QualifiedName] = set()

        for run in runs:
            if run.has_failures:
                continue

            blocking_failures = run.resolution.blocked_by(failed_during_execution)
            if blocking_failures:
                run.execution = ExecutionBlockedByDependency(blocking_failures)
                failed_during_execution.add(run.qualified_name)
                logger.error(
                    "Execution blocked for %s (%d foreign key failure(s))",
                    run.qualified_name,
                    len(blocking_failures),
                )
                continue

            # A no-op table must still be blocked when its dependency failed.
            plan = run.plan
            if plan is None:
                raise RuntimeError(f"Executable table was not planned: {run.qualified_name}")
            if not plan:
                continue

            summary = self._execute_statements(run.planned_sql_statements)
            run.execution = summary

            if summary.failures:
                failed_during_execution.add(run.qualified_name)

            logger.info(
                "Executed %d statement(s) for %s (%d failed)",
                len(summary.results),
                run.qualified_name,
                len(summary.failures),
            )

    def _execute_statements(self, statements: tuple[str, ...]) -> ExecutionSummary:
        """Execute statements in order and stop after the first expected failure."""
        results: list[ExecutionResult] = []
        for index, statement in enumerate(statements):
            try:
                self.executor.execute(statement)
            except ExecutionError as error:
                results.append(
                    ExecutionFailure(
                        statement_index=index,
                        statement=statement,
                        exception_type=error.exception_type,
                        message=str(error),
                    )
                )
                break

            results.append(
                ExecutionSucceeded(
                    statement_index=index,
                    statement=statement,
                )
            )

        return ExecutionSummary(tuple(results))
