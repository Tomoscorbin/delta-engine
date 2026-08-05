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
  7. Execute  — retain attempted statement results (real runs only)
  8. Account  — freeze the runs into a report that derives dependency blocking

The declaration set is judged before the world is consulted: resolution reads
only declarations, so a run is born knowing its position, its edges, and its
structural verdicts, and no later phase has to narrow a union to reach them.

Resolution is a pure structural judgment of the declarations against each
other (CYCLE, UNRESOLVABLE_REFERENCE, ...). Whether a table is *blocked* by
another table's failure is nobody's outcome to record: it is the derived
consequence of the dependency edges and the run's other fates, worked out once
at accounting. One rule states it — a table did not converge if it has
failures of its own or a dependency did not — and execution and accounting
each fold that same rule over the dependency-ordered runs: execution to decide
what not to attempt, accounting to say why. Because it is derived rather than
recorded, blocking is equally visible in a dry run, which attempts nothing.

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
    accepted_plan,
    plan_diff,
)
from delta_engine.application.ports import (
    CatalogStateReader,
    CompiledPlan,
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
    compiled: CompiledPlan | None = None
    execution: ExecutionSummary | None = None

    @property
    def desired(self) -> DesiredTable:
        return self.resolution.desired

    @property
    def qualified_name(self) -> QualifiedName:
        return self.resolution.qualified_name

    @property
    def plan(self) -> ActionPlan | None:
        return accepted_plan(self.planning)

    @property
    def has_failures(self) -> bool:
        """
        True when a completed phase failed this table on its own account.

        Own faults only: being blocked by another table is not a fault of this
        run, and is derived at accounting rather than recorded here.
        """
        return (
            isinstance(self.read, ReadFailure)
            or isinstance(self.planning, PlanningFailed)
            or bool(self.resolution.structural_failures)
            or (self.execution is not None and bool(self.execution.failures))
        )

    def to_report(self) -> TableRunReport:
        """Freeze this run into its public, immutable report."""
        if self.read is None:
            raise RuntimeError(f"Run was frozen before its read completed: {self.qualified_name}")

        return TableRunReport(
            read=self.read,
            planning=self.planning,
            compiled=self.compiled,
            resolution=self.resolution,
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

        Runs lower → resolve → read → diff → plan → compile → execute →
        account. Resolution births one private run per table in
        dependency-first order carrying its static facts, the worldly phases
        enrich the runs in place, and assembly freezes the completed outcomes
        into ``TableRunReport`` values, deriving dependency blocking from the
        edges as it goes.

        A table that fails an early phase carries that failure forward and is
        skipped by execution; its partial run is still included in the report.

        Args:
            *tables: The table specifications to synchronize. Duplicate
                qualified names raise ``DuplicateTableDefinitionError`` before
                any phase runs.
            dry_run: When True, stop after compile and attempt no statements
                (zero catalog mutations). No run retains attempted statement
                results, while its ``plan`` still records the actions compiled
                from the observed snapshot. Blocking is derived rather than
                executed, so a dependent of a failed table still reports
                BLOCKED_BY_FAILED_DEPENDENCY in the preview. The report is
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
        if not dry_run:
            self._execute(runs)

        report = SyncReport.assemble(
            started_at=run_started,
            ended_at=datetime.now(UTC),
            table_reports=[run.to_report() for run in runs],
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
        reports the compiled plan, while a real run passes that same value to
        execution. Runs rejected by an earlier phase carry no compiled SQL.
        """
        for run in runs:
            match run.planning:
                case PlanningSucceeded(plan=plan):
                    run.compiled = self.executor.compile(plan)

                    logger.info(  # TODO: should this log live on executor.compile()?
                        "Compiled %d statement(s) for %s",
                        len(
                            run.compiled.statements
                        ),  # TODO: should we put a __len__ on CompiledPlan?
                        run.qualified_name,
                    )
                case PlanningFailed() | None:  # TODO: why can this be None?
                    continue

    def _resolve(self, tables: tuple[DesiredTable, ...]) -> tuple[_TableRun, ...]:
        """
        Birth one run per table, dependency-first, with its resolution outcome.

        Resolution is structural: it judges the declarations against each
        other, before any catalog state is fetched. Whether a table is
        *blocked* by another's failure is not decided here but derived from
        these edges once the run's other fates are known. Every run carries a
        resolution from birth, so no later phase has to consider a
        half-resolved table.
        """
        runs: list[_TableRun] = []

        for resolution in resolve(tables):
            runs.append(_TableRun(resolution=resolution))

            if resolution.structural_failures:
                logger.error("Foreign key resolution failed for %s", resolution.qualified_name)

        return tuple(runs)

    def _execute(self, runs: tuple[_TableRun, ...]) -> None:
        """
        Execute every convergent run's compiled plan, skipping dependents of failure.

        One walk in dependency order applies the single blocking rule: a run
        with failures of its own, or with a dependency that will not converge,
        joins the not-converged set and is skipped. Nothing is recorded on the
        run either way — the account derives blocking from the same edges — so
        the two arms differ only in what they can say about the skip. Compiled
        plan emptiness gates only statement execution: a no-op run still joins
        the set through the same rule when its dependency failed, so blocking
        propagates through tables with no work of their own.
        """
        not_converged: set[QualifiedName] = set()

        for run in runs:
            if run.has_failures:
                not_converged.add(run.qualified_name)
                continue

            blocking_failures = run.resolution.blocked_by(not_converged)
            if blocking_failures:
                not_converged.add(run.qualified_name)
                logger.error(
                    "Execution blocked for %s (%d foreign key failure(s))",
                    run.qualified_name,
                    len(blocking_failures),
                )
                continue

            compiled = run.compiled
            if compiled is None:
                raise RuntimeError(f"Executable table was not compiled: {run.qualified_name}")
            if not compiled.compiled_actions:
                continue

            summary = self._execute_compiled(compiled)
            run.execution = summary

            if summary.failures:
                not_converged.add(run.qualified_name)

            logger.info(
                "Executed %d statement(s) for %s (%d failed)",
                len(summary.results),
                run.qualified_name,
                len(summary.failures),
            )

    def _execute_compiled(self, compiled: CompiledPlan) -> ExecutionSummary:
        """Execute statements in order and stop after the first expected failure."""
        results: list[ExecutionResult] = []
        for index, statement in enumerate(compiled.statements):
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

        return ExecutionSummary(compiled_plan=compiled, results=results)
