"""
High-level orchestration of planning and execution.

`Engine.sync` splits the work by one rule: everything table-local and
read-only happens in one straight-line pass per table (`_prepare`), and
everything cross-table or world-mutating happens in its own walk over the
prepared results. On a real run, if any table fails, `SyncFailedError` is
raised with a formatted summary.

The steps are:
  1. Lower    — lower the declaration set into domain tables, deduplicated
  2. Resolve  — judge the declarations against each other: dependency-first
                order, dependency edges, structural verdicts
  3. Prepare  — per table, in dependency order: read the catalog state, diff
                it against the declaration, judge the complete diff at the
                planning boundary, and compile the accepted plan — freezing
                the outcomes into one complete `TableRun`
  4. Execute  — the one cross-table walk (real runs only): fold the blocking
                rule over the runs and attach attempted statement results
                to each executed table
  5. Account  — assemble the aggregate report, deriving dependency blocking
                from the same edges

Because prepare is read-only, every table is diffed and planned against the
catalog as it stood before any statement ran, and a dry run is a real run
that stops after prepare: the prepared runs are the preview. A run is born
frozen and complete for everything its table can know alone; `None` on it
means "not applicable" (no planning after a failed read), never "not yet".
Execution and blocking — the two facts that depend on other tables — are
attached afterwards as functional updates (`dataclasses.replace`), each
re-validated by the run's own invariants.

Resolution is a pure structural judgment of the declarations against each
other (CYCLE, UNRESOLVABLE_REFERENCE, ...). Whether a table is *blocked* by
another table's failure is nobody's outcome to record: it is the derived
consequence of the dependency edges and the other tables' fates. One rule
states it — a table did not converge if it has failures of its own or a
dependency did not — and execution and accounting each fold that same rule
over the dependency-ordered runs: execution to decide what not to attempt,
accounting to say why. Because it is derived rather than recorded, blocking
is equally visible in a dry run, which attempts nothing.

A table that fails an early phase returns early from prepare with that
failure on its run and is skipped by execution, so all tables are attempted
and the report is always complete.
"""

from dataclasses import replace
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
    PlanningSucceeded,
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
    TablePresent,
)
from delta_engine.application.relationships import TableResolution, resolve
from delta_engine.application.report import (
    SyncReport,
    TableRun,
)
from delta_engine.domain.model import (
    DesiredTable,
    QualifiedName,
)
from delta_engine.domain.plan import diff_table

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

        Runs lower → resolve → prepare → execute → account. Resolution
        orders the tables dependency-first with their static facts, prepare
        takes each table through its read-only phases (read, diff, plan,
        compile) and freezes one complete ``TableRun``, execution
        attaches attempted statement results to the tables it reaches, and
        assembly derives dependency blocking from the edges.

        A table that fails an early phase carries that failure on its run
        and is skipped by execution; it is still included in the report.

        Args:
            *tables: The table specifications to synchronize. Duplicate
                qualified names raise ``DuplicateTableDefinitionError`` before
                any phase runs.
            dry_run: When True, stop after prepare and attempt no statements
                (zero catalog mutations). No table retains attempted statement
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

        runs = tuple(self._prepare(resolution) for resolution in resolve(desired))
        if not dry_run:
            runs = self._execute(runs)

        report = SyncReport.assemble(
            started_at=run_started,
            ended_at=datetime.now(UTC),
            table_runs=runs,
            dry_run=dry_run,
        )

        if not dry_run and report.has_failures:
            raise SyncFailedError(report)

        logger.info(
            "%s completed successfully for %d table(s)",
            "Dry run" if dry_run else "Sync",
            len(report.table_runs),
        )

        return report

    def _prepare(self, resolution: TableResolution) -> TableRun:
        """
        Take one table through its read-only phases: read, diff, plan, compile.

        Straight-line, table-local, and free of catalog mutations. Each early
        return is a lifecycle rule — a failed read leaves nothing to diff, a
        rejected diff leaves nothing to compile — so every ``None`` on the
        returned run means "not applicable", never "not yet". Execution is
        deliberately absent: whether this table may execute depends on other
        tables' fates, which is the execute walk's concern.
        """
        if resolution.structural_failures:
            logger.error("Foreign key resolution failed for %s", resolution.qualified_name)

        try:
            read = self.reader.fetch_state(resolution.qualified_name)
        except ReadError as error:
            logger.error(
                "Read failed for %s: %s - %s",
                resolution.qualified_name,
                error.exception_type,
                error,
            )
            return TableRun(
                resolution=resolution,
                read=ReadFailure(exception_type=error.exception_type, message=str(error)),
            )

        observed = read.table if isinstance(read, TablePresent) else None
        planning = plan_diff(diff_table(resolution.desired, observed))

        match planning:
            case PlanningFailed():
                logger.error("Planning failed for %s", resolution.qualified_name)
                return TableRun(resolution=resolution, read=read, planning=planning)
            case PlanningSucceeded(plan=plan):
                compiled = self.executor.compile(plan)
                logger.info(
                    "Prepared %s: %d action(s), %d statement(s)",
                    resolution.qualified_name,
                    len(plan),
                    len(compiled.statements),
                )
                return TableRun(
                    resolution=resolution, read=read, planning=planning, compiled=compiled
                )
            case _ as unreachable:
                assert_never(unreachable)

    def _execute(self, runs: tuple[TableRun, ...]) -> tuple[TableRun, ...]:
        """
        Execute every convergent table's compiled plan, skipping dependents of failure.

        One walk in dependency order applies the single blocking rule: a table
        with failures of its own, or with a dependency that will not converge,
        joins the not-converged set and is skipped. Nothing is recorded on a
        skipped table — the account derives blocking from the same edges — so
        the two arms differ only in what they can say about the skip. Compiled
        plan emptiness gates only statement execution: a no-op table still
        joins the set through the same rule when its dependency failed, so
        blocking propagates through tables with no work of their own. Each
        attempted table is replaced by a copy carrying its execution summary,
        re-validated by the run's own invariants.
        """
        not_converged: set[QualifiedName] = set()
        executed: list[TableRun] = []

        for run in runs:
            if run.has_failures:
                not_converged.add(run.qualified_name)
                executed.append(run)
                continue

            blocking_failures = run.resolution.blocked_by(not_converged)
            if blocking_failures:
                not_converged.add(run.qualified_name)
                logger.error(
                    "Execution blocked for %s (%d foreign key failure(s))",
                    run.qualified_name,
                    len(blocking_failures),
                )
                executed.append(run)
                continue

            compiled = run.compiled
            # Entailed by the run's invariants: no failures means planning
            # succeeded, and successful planning requires compilation.
            assert compiled is not None
            if not compiled.compiled_actions:
                executed.append(run)
                continue

            summary = self._execute_compiled(compiled)
            if summary.failures:
                not_converged.add(run.qualified_name)

            logger.info(
                "Executed %d statement(s) for %s (%d failed)",
                len(summary.results),
                run.qualified_name,
                len(summary.failures),
            )
            executed.append(replace(run, execution=summary))

        return tuple(executed)

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
