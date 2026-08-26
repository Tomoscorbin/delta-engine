from datetime import datetime
import json

import pytest

from delta_engine.application.diff_entries import DiffCategory
from delta_engine.application.failures import (
    ExecutionFailure,
    Failure,
    FailurePhase,
    ForeignKeyFailure,
    ForeignKeyFailureReason,
    ReadFailure,
    ValidationFailure,
)
from delta_engine.application.planning import (
    PlanningAccepted,
    PlanningDeferred,
    PlanningRejected,
)
from delta_engine.application.ports import (
    ExecutionResult,
    ReadResult,
    TableAbsent,
    TablePresent,
)
from delta_engine.application.relationships import TableResolution
from delta_engine.application.report import (
    RunCounts,
    SyncReport,
    TableChangeState,
    TableRun,
    TableRunStatus,
)
from delta_engine.domain.model import (
    DesiredColumn,
    DesiredTable,
    ForeignKeyConstraint,
    Integer,
    ObservedColumn,
    ObservedTable,
    QualifiedName,
    String,
    TableFeature,
)
from delta_engine.domain.plan import TableCreation, TableDiff, diff_table
from delta_engine.domain.plan.actions import (
    ActionPlan,
    CreateTable,
    EnableTableFeature,
    SetTableComment,
    SetTableTag,
    UnsetTableTag,
)
from tests.builders import build_compiled_plan

# ---------- test builders


def _name(table="observed"):
    """Build the qualified name every builder in this file works in."""
    return QualifiedName("cat", "schema", table)


def _an_observed_table(partitioned_by=()):
    """Build a real ObservedTable, so reports are exercised against the domain type."""
    return ObservedTable(
        qualified_name=_name(),
        columns=(ObservedColumn("id", Integer()),),
        partitioned_by=partitioned_by,
    )


def _a_desired_table(name="observed"):
    """Build a minimal real DesiredTable for pipeline-record construction."""
    return DesiredTable(
        qualified_name=_name(name),
        columns=(DesiredColumn("id", Integer()),),
    )


def _t0():
    return datetime(2025, 10, 2, 12, 0, 0)


def _t1():
    return datetime(2025, 10, 2, 12, 5, 0)


def _failed_exec(idx=0, preview="ALTER TABLE ...", exc="ValueError", msg="boom"):
    return ExecutionFailure(
        statement_index=idx,
        exception_type=exc,
        message=msg,
        statement=preview,
    )


def _blocked_failure():
    """Build the failure ``b`` earns when dependency ``a`` did not converge."""
    return ForeignKeyFailure(
        table=_name("b"),
        local_columns=("parent_id",),
        references=_name("a"),
        reason=ForeignKeyFailureReason.BLOCKED_BY_FAILED_DEPENDENCY,
    )


_PLAN_UNSET = object()


def _plan(name: str, *actions) -> ActionPlan:
    return ActionPlan(
        target=QualifiedName("cat", "schema", name),
        actions=actions,
    )


def _comment_plan(name: str, statement_count: int = 1) -> ActionPlan:
    return _plan(
        name,
        *(
            SetTableComment(
                desired_comment=f"new {index}",
                observed_comment=f"old {index}",
            )
            for index in range(statement_count)
        ),
    )


def _execution(
    plan: ActionPlan,
    statements: tuple[str, ...] = (),
    *,
    failure: ExecutionFailure | None = None,
) -> ExecutionResult:
    """Build an execution result over ``plan``: complete unless ``failure`` is given."""
    if not statements:
        statements = tuple(
            failure.statement
            if failure is not None and failure.statement_index == index
            else "ALTER TABLE ..."
            for index in range(len(plan))
        )
    compiled = build_compiled_plan(plan, statements)
    applied = len(statements) if failure is None else failure.statement_index
    return ExecutionResult(compiled_plan=compiled, applied_count=applied, failure=failure)


def _report(
    *,
    desired: DesiredTable,
    read: ReadResult,
    plan: ActionPlan | None | object = _PLAN_UNSET,
    statements: tuple[str, ...] = (),
    failures: tuple[Failure, ...] = (),
    dependencies: tuple[ForeignKeyConstraint, ...] = (),
    execution: ExecutionResult | None = None,
    blocked_failures: tuple[ForeignKeyFailure, ...] = (),
    diff: TableDiff | None = None,
) -> TableRun:
    """Construct a frozen report snapshot from concise test inputs."""
    planning_failures = tuple(
        failure for failure in failures if isinstance(failure, ValidationFailure)
    )
    resolution_failures = tuple(
        failure for failure in failures if isinstance(failure, ForeignKeyFailure)
    )
    if plan is _PLAN_UNSET:
        report_plan = (
            None
            if isinstance(read, ReadFailure) or planning_failures
            else execution.compiled_plan.plan
            if execution is not None
            else ActionPlan(target=desired.qualified_name)
        )
    else:
        assert plan is None or isinstance(plan, ActionPlan)
        report_plan = plan

    planning_diff = diff if diff is not None else TableCreation(desired)
    if isinstance(read, ReadFailure):
        planning = None
    elif planning_failures:
        planning = PlanningRejected(diff=planning_diff, failures=planning_failures)
    else:
        assert isinstance(report_plan, ActionPlan)
        planning = PlanningAccepted(diff=planning_diff, plan=report_plan)

    if report_plan is None:
        compiled = None
    elif execution is not None:
        compiled = execution.compiled_plan
    else:
        if not statements:
            statements = tuple(f"SQL {index}" for index in range(len(report_plan)))
        compiled = build_compiled_plan(report_plan, statements)

    resolution = TableResolution(
        desired=desired,
        dependencies=dependencies,
        structural_failures=resolution_failures,
    )

    return TableRun(
        read=read,
        planning=planning,
        compiled=compiled,
        resolution=resolution,
        execution=execution,
        blocked_failures=blocked_failures,
    )


# ---------- Tests


def test_table_status_success_when_all_actions_succeed():
    # Given successful read, no pre-execution failures, and only successful actions
    read = TablePresent(table=_an_observed_table())
    plan = _comment_plan("tbl", statement_count=2)
    execution = _execution(plan)

    # When aggregating
    report = _report(
        desired=_a_desired_table("tbl"),
        read=read,
        execution=execution,
    )

    # Then everything is SUCCESS and has_failures is False
    assert report.status is TableRunStatus.SUCCESS
    assert report.has_failures is False
    assert report.execution.failures == ()


def test_sync_report_has_failures_true_if_any_table_has_failures():
    # Given two tables: one success, one with execution failure
    ok_plan = _comment_plan("a")
    failed_plan = _comment_plan("b")
    t_ok = _report(
        desired=_a_desired_table("a"),
        read=TablePresent(table=_an_observed_table()),
        execution=_execution(ok_plan),
    )
    t_bad = _report(
        desired=_a_desired_table("b"),
        read=TablePresent(table=_an_observed_table()),
        execution=_execution(failed_plan, failure=_failed_exec(0)),
    )

    # When aggregating the sync
    sr = SyncReport(started_at=_t0(), ended_at=_t1(), table_runs=(t_ok, t_bad))

    # Then has_failures is True
    assert sr.has_failures is True


def test_table_has_changes_when_plan_is_non_empty():
    report = _report(
        desired=_a_desired_table("tbl"),
        read=TablePresent(table=_an_observed_table()),
        plan=_plan("tbl", SetTableComment(desired_comment="hello", observed_comment="")),
    )
    assert report.has_changes is True


def test_table_has_no_changes_when_plan_is_empty():
    report = _report(
        desired=_a_desired_table("tbl"),
        read=TablePresent(table=_an_observed_table()),
    )
    assert report.has_changes is False


def test_validation_failed_table_has_failures_but_no_changes():
    # Validation refuses the drift before a plan exists: the table reports
    # failures, not changes.
    report = _report(
        desired=_a_desired_table("tbl"),
        read=TablePresent(table=_an_observed_table()),
        failures=(ValidationFailure(rule_name="SomeRule", message="unsafe"),),
    )
    assert report.has_failures is True
    assert report.has_changes is False
    assert report.plan is None


def test_sync_report_has_changes_when_any_table_plans_actions():
    # Given one changed table and one unchanged table
    changed = _report(
        desired=_a_desired_table("a"),
        read=TablePresent(table=_an_observed_table()),
        plan=_plan("a", SetTableComment(desired_comment="hello", observed_comment="")),
    )
    unchanged = _report(
        desired=_a_desired_table("b"),
        read=TablePresent(table=_an_observed_table()),
    )

    # When aggregating a dry-run report
    report = SyncReport(
        started_at=_t0(),
        ended_at=_t1(),
        table_runs=(changed, unchanged),
        dry_run=True,
    )

    # Then the aggregate reports planned changes
    assert report.has_changes is True


def test_sync_report_has_no_changes_when_no_table_plans_actions():
    unchanged = _report(
        desired=_a_desired_table("b"),
        read=TablePresent(table=_an_observed_table()),
    )
    report = SyncReport(started_at=_t0(), ended_at=_t1(), table_runs=(unchanged,))
    assert report.has_changes is False


def test_sync_report_counts_put_every_table_in_exactly_one_bucket():
    # Given a run with one changed, one unchanged, and one failed table
    changed = _report(
        desired=_a_desired_table("a"),
        read=TablePresent(table=_an_observed_table()),
        plan=_plan("a", SetTableComment(desired_comment="hello", observed_comment="")),
    )
    unchanged = _report(
        desired=_a_desired_table("b"),
        read=TablePresent(table=_an_observed_table()),
    )
    failed = _report(
        desired=_a_desired_table("c"),
        read=ReadFailure(exception_type="AnalysisException", message="nope"),
    )

    # When aggregating the dry run
    report = SyncReport(
        started_at=_t0(),
        ended_at=_t1(),
        table_runs=(changed, unchanged, failed),
        dry_run=True,
    )

    # Then each is counted once, and the total is their sum
    counts = report.counts
    assert (counts.changed, counts.unchanged, counts.failed) == (1, 1, 1)
    assert counts.total == len(report.table_runs)


def test_a_failed_table_counts_as_failed_even_though_it_planned_changes():
    # Given a table that planned a change but whose execution failed
    statement = "COMMENT ON TABLE `cat`.`schema`.`a` IS 'hello'"
    plan = _plan("a", SetTableComment(desired_comment="hello", observed_comment=""))
    failed = _report(
        desired=_a_desired_table("a"),
        read=TablePresent(table=_an_observed_table()),
        plan=plan,
        execution=_execution(
            plan,
            failure=ExecutionFailure(
                statement_index=0,
                exception_type="SparkException",
                message="boom",
                statement=statement,
            ),
        ),
    )
    report = SyncReport(started_at=_t0(), ended_at=_t1(), table_runs=(failed,))

    # Then it counts as failed, not changed: the change was planned, not applied
    assert report.counts == RunCounts(changed=0, unchanged=0, failed=1)


def test_sync_report_reports_its_own_duration():
    report = SyncReport(started_at=_t0(), ended_at=_t1(), table_runs=())
    assert report.duration_seconds == 300.0


def test_sync_report_planned_sql_maps_dotted_names_and_omits_empty():
    # Given one table with compiled SQL and one with an empty plan
    with_sql = _report(
        desired=_a_desired_table("a"),
        read=TablePresent(table=_an_observed_table()),
        plan=_plan("a", SetTableComment(desired_comment="hello", observed_comment="")),
        statements=("ALTER TABLE a SET ...",),
    )
    without_sql = _report(
        desired=_a_desired_table("b"),
        read=TablePresent(table=_an_observed_table()),
    )

    # When aggregating the dry run
    report = SyncReport(
        started_at=_t0(),
        ended_at=_t1(),
        table_runs=(with_sql, without_sql),
        dry_run=True,
    )

    # Then only the changed table is mapped by its dotted name
    assert report.planned_sql_statements == {"cat.schema.a": ("ALTER TABLE a SET ...",)}


def test_sync_report_failures_by_table_maps_only_failed_tables():
    # Given one failed and one successful table
    failed_name = QualifiedName("cat", "schema", "y")
    successful_plan = _comment_plan("x")
    t_ok = _report(
        desired=_a_desired_table("x"),
        read=TablePresent(table=_an_observed_table()),
        execution=_execution(successful_plan),
    )
    t_bad = _report(
        desired=_a_desired_table("y"),
        read=TableAbsent(),
        failures=(ValidationFailure("R", "v"),),
    )

    # When aggregating the sync report
    sr = SyncReport(started_at=_t0(), ended_at=_t1(), table_runs=(t_ok, t_bad))

    # Then only the failed table appears, keyed by its QualifiedName, with its failures
    mapping = sr.failures_by_table
    assert list(mapping.keys()) == [failed_name]
    assert all(
        isinstance(f, ValidationFailure | ReadFailure | ExecutionFailure | ForeignKeyFailure)
        for f in mapping[failed_name]
    )


def test_table_run_status_is_foreign_key_failed_when_fk_failure_present():
    # Given a table that read cleanly but has an FK failure in failures
    report = _report(
        desired=_a_desired_table("orders"),
        read=TablePresent(table=_an_observed_table()),
        failures=(
            ForeignKeyFailure(
                table=QualifiedName("cat", "sch", "orders"),
                local_columns=("customer_id",),
                references=QualifiedName("cat", "sch", "customers"),
                reason=ForeignKeyFailureReason.CYCLE,
            ),
        ),
    )

    # Then its status reflects the FK failure and it counts as a failure
    assert report.status is TableRunStatus.FOREIGN_KEY_FAILED
    assert report.has_failures is True
    assert report.failures[0].format_lines()[0].startswith("Foreign key")


def test_table_run_status_is_planning_failed_when_only_validation_failure_present():
    # Given a table that read cleanly but has a validation failure and no FK failure
    report = _report(
        desired=_a_desired_table("tbl"),
        read=TablePresent(table=_an_observed_table()),
        failures=(
            ValidationFailure(rule_name="NonNullableColumnAdd", message="cannot add NOT NULL"),
        ),
    )

    # Then its status is PLANNING_FAILED (no FK failure takes priority)
    assert report.status is TableRunStatus.PLANNING_FAILED
    assert report.has_failures is True


def test_table_run_status_is_foreign_key_failed_when_both_fk_and_validation_present():
    # Given a table with both a validation failure and an FK failure
    report = _report(
        desired=_a_desired_table("orders"),
        read=TablePresent(table=_an_observed_table()),
        failures=(
            ValidationFailure(rule_name="NonNullableColumnAdd", message="cannot add NOT NULL"),
            ForeignKeyFailure(
                table=QualifiedName("cat", "sch", "orders"),
                local_columns=("customer_id",),
                references=QualifiedName("cat", "sch", "customers"),
                reason=ForeignKeyFailureReason.UNRESOLVABLE_REFERENCE,
            ),
        ),
    )

    # Then FOREIGN_KEY_FAILED wins: resolution judges the declaration set
    # before any single table is diffed, so it is the earlier phase and the
    # root cause — the plan cannot be right while the relationships are broken
    assert report.status is TableRunStatus.FOREIGN_KEY_FAILED
    assert len(report.failures) == 2


def test_multi_phase_failure_reports_the_earliest_pipeline_phase():
    # Given a table that failed resolution (structural FK) and also failed its read
    desired = _a_desired_table("orders")
    report = _report(
        desired=desired,
        read=ReadFailure("IOError", "boom"),
        failures=(
            ForeignKeyFailure(
                table=desired.qualified_name,
                local_columns=("customer_id",),
                references=QualifiedName("cat", "schema", "customers"),
                reason=ForeignKeyFailureReason.UNRESOLVABLE_REFERENCE,
            ),
        ),
    )

    # Then the earliest pipeline phase wins: resolution precedes read
    assert report.status is TableRunStatus.FOREIGN_KEY_FAILED


def test_table_run_with_no_failures_is_success():
    # Given a clean table with no failures
    plan = _comment_plan("ok")
    report = _report(
        desired=_a_desired_table("ok"),
        read=TablePresent(table=_an_observed_table()),
        execution=_execution(plan),
    )

    # Then it is a success and carries no failures
    assert report.status is TableRunStatus.SUCCESS
    assert report.failures == ()


def test_status_reflects_the_earliest_failing_phase():
    # Given a table with an execution failure only
    read = TablePresent(table=_an_observed_table())
    plan = _comment_plan("e")
    exec_only = _report(
        desired=_a_desired_table("e"),
        read=read,
        execution=_execution(plan, failure=_failed_exec(0)),
    )
    # Then it is EXECUTION_FAILED
    assert exec_only.status is TableRunStatus.EXECUTION_FAILED

    # Given a read failure
    read_failed = _report(
        desired=_a_desired_table("r"),
        read=ReadFailure("IOError", "boom"),
    )
    # Then its canonical read outcome determines the status
    assert read_failed.status is TableRunStatus.READ_FAILED


def test_table_run_carries_its_desired_definition():
    # Given a completed, unchanged table run
    desired = _a_desired_table("customers")
    report = _report(
        desired=desired,
        read=TablePresent(table=_an_observed_table()),
    )

    # Then it carries the desired definition and is a clean success so far
    assert report.desired is desired
    assert report.status is TableRunStatus.SUCCESS
    assert report.failures == ()
    assert report.plan == ActionPlan(target=desired.qualified_name)


def test_table_run_rejects_planning_after_a_failed_read():
    desired = _a_desired_table("orders")

    with pytest.raises(ValueError, match="Planning cannot follow a failed read"):
        TableRun(
            read=ReadFailure("IOError", "boom"),
            planning=PlanningAccepted(
                diff=TableCreation(desired), plan=ActionPlan(target=desired.qualified_name)
            ),
            compiled=None,
            resolution=TableResolution(desired, (), ()),
            execution=None,
        )


def test_table_run_rejects_successful_planning_without_compilation():
    desired = _a_desired_table("orders")

    with pytest.raises(ValueError, match="requires compilation"):
        TableRun(
            read=TableAbsent(),
            planning=PlanningAccepted(
                diff=TableCreation(desired), plan=ActionPlan(target=desired.qualified_name)
            ),
            compiled=None,
            resolution=TableResolution(desired, (), ()),
            execution=None,
        )


def test_table_run_rejects_compilation_of_another_plan():
    desired = _a_desired_table("orders")
    plan = _comment_plan("orders")
    other_plan = ActionPlan(target=desired.qualified_name)

    with pytest.raises(ValueError, match="must match the successful planning outcome"):
        TableRun(
            read=TableAbsent(),
            planning=PlanningAccepted(diff=TableCreation(desired), plan=plan),
            compiled=build_compiled_plan(other_plan, ()),
            resolution=TableResolution(desired, (), ()),
            execution=None,
        )


def test_table_run_rejects_execution_after_failed_resolution():
    desired = _a_desired_table("orders")
    plan = _plan("orders", SetTableComment(desired_comment="hello", observed_comment=""))
    compiled = build_compiled_plan(plan, ("SQL",))
    failure = ForeignKeyFailure(
        table=desired.qualified_name,
        local_columns=("customer_id",),
        references=QualifiedName("cat", "schema", "customers"),
        reason=ForeignKeyFailureReason.UNRESOLVABLE_REFERENCE,
    )

    with pytest.raises(ValueError, match="Execution cannot follow a failed earlier phase"):
        TableRun(
            read=TablePresent(table=_an_observed_table()),
            planning=PlanningAccepted(diff=TableCreation(desired), plan=plan),
            compiled=compiled,
            resolution=TableResolution(desired, (), (failure,)),
            execution=ExecutionResult(compiled_plan=compiled, applied_count=1),
        )


def test_table_run_rejects_execution_of_another_compiled_plan():
    desired = _a_desired_table("orders")
    plan = _plan("orders", SetTableComment(desired_comment="hello", observed_comment=""))
    reported = build_compiled_plan(plan, ("REPORTED SQL",))
    executed = build_compiled_plan(plan, ("EXECUTED SQL",))

    with pytest.raises(ValueError, match="reported compiled plan"):
        TableRun(
            read=TablePresent(table=_an_observed_table()),
            planning=PlanningAccepted(diff=TableCreation(desired), plan=plan),
            compiled=reported,
            resolution=TableResolution(desired, (), ()),
            execution=ExecutionResult(compiled_plan=executed, applied_count=1),
        )


def _a_changed_table_report():
    return _report(
        desired=_a_desired_table("orders"),
        read=TablePresent(table=_an_observed_table()),
        plan=_plan("orders", SetTableComment(desired_comment="hello", observed_comment="")),
        statements=("COMMENT ON TABLE `cat`.`schema`.`orders` IS 'hello'",),
    )


def test_a_table_run_knows_whether_its_plan_creates_the_table():
    # Given one report whose plan creates the table and one that alters it
    creating = _report(
        desired=_a_desired_table("orders"),
        read=TableAbsent(),
        plan=_plan("orders", CreateTable(table=_a_desired_table("orders"))),
    )

    # Then only the creating plan says so, and a table that failed to read says no
    assert creating.creates_table is True
    assert _a_changed_table_report().creates_table is False
    assert (
        _report(
            desired=_a_desired_table("orders"),
            read=ReadFailure(exception_type="AnalysisException", message="nope"),
        ).creates_table
        is False
    )


def test_statement_progress_is_absent_until_execution_runs():
    # Given a planned but unexecuted table
    # Then there is no progress to report, rather than a zero-applied count
    assert _a_changed_table_report().statement_progress is None


def test_statement_progress_counts_applied_against_planned():
    # Given a table whose execution applied one of two planned statements
    statements = ("SQL 0", "SQL 1")
    plan = _comment_plan("orders", statement_count=2)
    report = _report(
        desired=_a_desired_table("orders"),
        read=TablePresent(table=_an_observed_table()),
        plan=plan,
        execution=_execution(
            plan,
            statements,
            failure=ExecutionFailure(
                statement_index=1,
                exception_type="SparkException",
                message="boom",
                statement=statements[1],
            ),
        ),
    )

    # Then progress counts what was applied against everything planned
    assert report.statement_progress == (1, 2)


# ---------- Catalog change state


def test_change_state_is_not_planned_when_no_plan_was_accepted():
    # Given a table whose catalog read failed before planning
    report = _report(
        desired=_a_desired_table("orders"),
        read=ReadFailure("AnalysisException", "boom"),
    )

    # When deriving change states through the completed real-run report
    sync = SyncReport(started_at=_t0(), ended_at=_t1(), table_runs=(report,))

    # Then the table has no accepted plan to apply
    assert sync.table_change_states == (TableChangeState.NOT_PLANNED,)


def test_change_state_is_not_planned_when_planning_rejected_the_diff():
    # Given a table whose planner rejected the observed difference
    report = _report(
        desired=_a_desired_table("orders"),
        read=TablePresent(table=_an_observed_table()),
        failures=(ValidationFailure(rule_name="UnsafeChange", message="nope"),),
    )

    # When deriving change states through the completed real-run report
    sync = SyncReport(started_at=_t0(), ended_at=_t1(), table_runs=(report,))

    # Then status explains the planning failure and no change was planned
    assert report.status is TableRunStatus.PLANNING_FAILED
    assert sync.table_change_states == (TableChangeState.NOT_PLANNED,)


def test_change_state_is_unchanged_for_an_empty_plan_even_when_the_run_failed():
    # Given a failed table whose accepted plan contains no catalog changes
    desired = _a_desired_table("orders")
    report = _report(
        desired=desired,
        read=TablePresent(table=_an_observed_table()),
        failures=(
            ForeignKeyFailure(
                table=desired.qualified_name,
                local_columns=("customer_id",),
                references=_name("customers"),
                reason=ForeignKeyFailureReason.UNRESOLVABLE_REFERENCE,
            ),
        ),
    )

    # When deriving change states through the completed real-run report
    sync = SyncReport(started_at=_t0(), ended_at=_t1(), table_runs=(report,))

    # Then failure status and catalog change state remain independent
    assert report.status is TableRunStatus.FOREIGN_KEY_FAILED
    assert sync.table_change_states == (TableChangeState.UNCHANGED,)


# ---------- deferred planning


def _a_deferred_run(name: str = "orders") -> TableRun:
    """Build the run of an absent table whose declaration cannot create it."""
    desired = _a_desired_table(name)
    return TableRun(
        resolution=TableResolution(desired=desired, dependencies=(), structural_failures=()),
        read=TableAbsent(),
        planning=PlanningDeferred(diff=TableCreation(desired)),
    )


def test_a_deferred_table_reports_deferred_without_failures_or_changes():
    run = _a_deferred_run()

    assert run.status is TableRunStatus.DEFERRED
    assert run.has_failures is False
    assert run.has_changes is False


def test_a_deferred_run_rejects_compilation():
    # Given a deferred planning outcome, there is no accepted plan to compile
    desired = _a_desired_table("orders")
    stray_plan = ActionPlan(target=desired.qualified_name)

    # Then the run refuses compiled statements
    with pytest.raises(ValueError):
        TableRun(
            resolution=TableResolution(desired=desired, dependencies=(), structural_failures=()),
            read=TableAbsent(),
            planning=PlanningDeferred(diff=TableCreation(desired)),
            compiled=build_compiled_plan(stray_plan, ()),
        )


def test_a_deferred_run_rejects_execution():
    # Given a deferred planning outcome, no plan was accepted to execute
    desired = _a_desired_table("orders")
    plan = _plan("orders", SetTableComment(desired_comment="hello", observed_comment=""))
    compiled = build_compiled_plan(plan, ("SQL",))

    # Then the run refuses an execution outcome
    with pytest.raises(ValueError, match="Execution requires a successful planning outcome"):
        TableRun(
            resolution=TableResolution(desired=desired, dependencies=(), structural_failures=()),
            read=TableAbsent(),
            planning=PlanningDeferred(diff=TableCreation(desired)),
            execution=ExecutionResult(compiled_plan=compiled, applied_count=1),
        )


def test_failures_dominate_deferral():
    # Given a deferred table whose declaration also failed resolution
    desired = _a_desired_table("orders")
    run = TableRun(
        resolution=TableResolution(
            desired=desired,
            dependencies=(),
            structural_failures=(
                ForeignKeyFailure(
                    table=desired.qualified_name,
                    local_columns=("customer_id",),
                    references=_name("customers"),
                    reason=ForeignKeyFailureReason.UNRESOLVABLE_REFERENCE,
                ),
            ),
        ),
        read=TableAbsent(),
        planning=PlanningDeferred(diff=TableCreation(desired)),
    )

    # When deriving the run's outcomes
    sync = SyncReport(started_at=_t0(), ended_at=_t1(), table_runs=(run,))

    # Then the failure leads and the change state does not claim deferral
    assert run.status is TableRunStatus.FOREIGN_KEY_FAILED
    assert sync.table_change_states == (TableChangeState.NOT_PLANNED,)
    assert sync.counts == RunCounts(changed=0, unchanged=0, failed=1, deferred=0)


def test_change_state_is_deferred_for_an_absent_table_the_scope_cannot_create():
    sync = SyncReport(started_at=_t0(), ended_at=_t1(), table_runs=(_a_deferred_run(),))

    assert sync.table_change_states == (TableChangeState.DEFERRED,)


def test_counts_place_a_deferred_table_in_its_own_bucket():
    sync = SyncReport(started_at=_t0(), ended_at=_t1(), table_runs=(_a_deferred_run(),))

    assert sync.counts == RunCounts(changed=0, unchanged=0, failed=0, deferred=1)
    assert sync.counts.total == 1


def test_a_deferred_table_to_dict_reports_deferred_status():
    payload = _a_deferred_run().to_dict()

    assert payload["status"] == "DEFERRED"
    assert payload["has_changes"] is False
    assert payload["has_failures"] is False
    assert payload["changes"] == []


def test_change_state_is_planned_for_a_dry_run_with_changes():
    # Given a dry-run table with a compiled, non-empty plan
    report = _a_changed_table_report()

    # When deriving change states through the validated aggregate
    sync = SyncReport(
        started_at=_t0(),
        ended_at=_t1(),
        table_runs=(report,),
        dry_run=True,
    )

    # Then the intended change is planned rather than applied
    assert sync.table_change_states == (TableChangeState.PLANNED,)


def test_change_state_is_not_applied_when_a_real_change_is_dependency_blocked():
    # Given a real-run table whose non-empty plan was blocked by a failed dependency
    report = _report(
        desired=_a_desired_table("b"),
        read=TablePresent(table=_an_observed_table()),
        plan=_comment_plan("b"),
        blocked_failures=(_blocked_failure(),),
    )

    # When deriving change states through the validated aggregate
    sync = SyncReport(started_at=_t0(), ended_at=_t1(), table_runs=(report,))

    # Then none of the intended catalog change was applied
    assert sync.table_change_states == (TableChangeState.NOT_APPLIED,)


def test_first_and_later_execution_failures_have_distinct_change_states():
    # Given one plan that failed immediately and one that applied a statement first
    statements = ("SQL 0", "SQL 1")
    first_plan = _comment_plan("first", statement_count=2)
    first_failed = _report(
        desired=_a_desired_table("first"),
        read=TablePresent(table=_an_observed_table()),
        execution=ExecutionResult(
            compiled_plan=build_compiled_plan(first_plan, statements),
            applied_count=0,
            failure=_failed_exec(0, statements[0]),
        ),
    )
    later_plan = _comment_plan("later", statement_count=2)
    later_failed = _report(
        desired=_a_desired_table("later"),
        read=TablePresent(table=_an_observed_table()),
        execution=_execution(later_plan, statements, failure=_failed_exec(1, statements[1])),
    )

    # When both table outcomes are aggregated in report order
    sync = SyncReport(
        started_at=_t0(),
        ended_at=_t1(),
        table_runs=(first_failed, later_failed),
    )

    # Then their failure status matches but their catalog effects differ
    assert first_failed.status is later_failed.status is TableRunStatus.EXECUTION_FAILED
    assert sync.table_change_states == (
        TableChangeState.NOT_APPLIED,
        TableChangeState.PARTIALLY_APPLIED,
    )


def test_change_state_is_applied_after_complete_successful_execution():
    # Given a real-run plan whose every statement succeeded
    plan = _comment_plan("orders", statement_count=2)
    report = _report(
        desired=_a_desired_table("orders"),
        read=TablePresent(table=_an_observed_table()),
        execution=_execution(plan),
    )

    # When deriving change states through the validated aggregate
    sync = SyncReport(started_at=_t0(), ended_at=_t1(), table_runs=(report,))

    # Then the intended catalog change is fully applied
    assert sync.table_change_states == (TableChangeState.APPLIED,)


# ---------- Run consistency


def test_sync_report_rejects_execution_results_on_a_dry_run():
    # Given a table carrying a completed execution result
    plan = _comment_plan("orders")
    executed = _report(
        desired=_a_desired_table("orders"),
        read=TablePresent(table=_an_observed_table()),
        execution=_execution(plan),
    )

    # When constructing a dry-run aggregate
    # Then the contradictory execution history is rejected
    with pytest.raises(ValueError):
        SyncReport(
            started_at=_t0(),
            ended_at=_t1(),
            table_runs=(executed,),
            dry_run=True,
        )


def test_sync_report_rejects_an_unexplained_unexecuted_real_change():
    # Given a planned change with neither execution nor a blocking failure
    changed = _a_changed_table_report()

    # When constructing a real-run aggregate
    # Then the unexplained missing execution is rejected
    with pytest.raises(ValueError):
        SyncReport(started_at=_t0(), ended_at=_t1(), table_runs=(changed,))


def test_sync_report_accepts_a_real_change_blocked_by_a_failed_dependency():
    # Given a real-run change carrying the failure that blocked its execution
    blocked = _report(
        desired=_a_desired_table("b"),
        read=TablePresent(table=_an_observed_table()),
        plan=_comment_plan("b"),
        blocked_failures=(_blocked_failure(),),
    )

    # When constructing the aggregate
    report = SyncReport(started_at=_t0(), ended_at=_t1(), table_runs=(blocked,))

    # Then the explained lack of execution is accepted
    assert report.table_runs == (blocked,)


def test_change_states_do_not_extend_the_structured_report_schema():
    # Given a dry-run report exposing its Python-level change states
    report = SyncReport(
        started_at=_t0(),
        ended_at=_t1(),
        table_runs=(_a_changed_table_report(),),
        dry_run=True,
    )

    # When projecting the report into its stable structured schema
    payload = report.to_dict()

    # Then neither the run nor table records gain a change-state field
    assert "table_change_states" not in payload
    assert "change_state" not in payload["tables"][0]


def test_table_to_dict_states_the_planned_change():
    payload = _a_changed_table_report().to_dict()

    assert payload["name"] == "cat.schema.orders"
    assert payload["status"] == "SUCCESS"
    assert payload["has_changes"] is True
    assert payload["has_failures"] is False
    assert payload["changes"] == [
        {"kind": "comments", "operation": "change", "subject": "table", "detail": "'hello'"}
    ]
    assert payload["planned_sql_statements"] == [
        "COMMENT ON TABLE `cat`.`schema`.`orders` IS 'hello'"
    ]
    assert payload["failures"] == []
    assert payload["execution"] is None


def test_table_to_dict_exposes_feature_enablement_as_a_public_change_kind():
    # Given a successful table report planning a permanent feature enablement
    report = _report(
        desired=_a_desired_table("orders"),
        read=TablePresent(table=_an_observed_table()),
        plan=_plan(
            "orders",
            EnableTableFeature(TableFeature.TIMESTAMP_NTZ),
        ),
        statements=("ALTER TABLE ... SET TBLPROPERTIES (...)",),
    )

    # When serializing the public report
    changes = report.to_dict()["changes"]

    # Then the feature upgrade has its own public change kind
    assert changes == [
        {
            "kind": "features",
            "operation": "add",
            "subject": "timestampNtz",
            "detail": "— permanent protocol upgrade",
        }
    ]


def test_table_to_dict_spells_every_operation_as_one_of_three_plain_words():
    # Given a plan holding one change of each operation
    report = _report(
        desired=_a_desired_table("orders"),
        read=TablePresent(table=_an_observed_table()),
        plan=_plan(
            "orders",
            SetTableTag(name="env", desired_value="prod", observed_value=None),
            UnsetTableTag(name="legacy"),
            SetTableComment(desired_comment="hello", observed_comment=""),
        ),
        statements=("SQL 0", "SQL 1", "ALTER TABLE ... SET TAGS (...)"),
    )

    # When serializing the public report
    operations = [change["operation"] for change in report.to_dict()["changes"]]

    # Then each is the documented word for its operation, and a plain string
    # rather than the enum — to_dict promises data, not application types.
    assert operations == ["add", "remove", "change"]
    assert all(type(operation) is str for operation in operations)


def test_table_to_dict_reports_failures_with_phase_and_type():
    report = _report(
        desired=_a_desired_table("orders"),
        read=TablePresent(table=_an_observed_table()),
        failures=(ValidationFailure(rule_name="SomeRule", message="unsafe"),),
    )
    payload = report.to_dict()

    assert payload["status"] == "PLANNING_FAILED"
    assert payload["failures"] == [
        {
            "phase": "PLANNING",
            "type": "ValidationFailure",
            "message": "Validation failed: SomeRule - unsafe",
            "rule": "SomeRule",
            "subject": "",
            "details": [],
        }
    ]


def test_a_read_failure_record_retains_the_full_backend_message():
    # The display message keeps only the first five lines; the machine
    # record must carry the whole backend message.
    long_message = "\n".join(f"line {n}" for n in range(1, 10))
    report = _report(
        desired=_a_desired_table("orders"),
        read=ReadFailure(exception_type="AnalysisException", message=long_message),
    )

    (record,) = report.to_dict()["failures"]

    assert record["exception_type"] == "AnalysisException"
    assert record["diagnostic"] == long_message


def test_an_execution_failure_record_carries_its_statement_facts():
    statement = "ALTER TABLE `cat`.`schema`.`orders` ALTER COLUMN id COMMENT 'x'"
    long_message = "\n".join(f"line {n}" for n in range(1, 10))
    plan = _plan("orders", SetTableComment(desired_comment="hello", observed_comment=""))
    report = _report(
        desired=_a_desired_table("orders"),
        read=TablePresent(table=_an_observed_table()),
        plan=plan,
        execution=_execution(
            plan,
            failure=_failed_exec(
                0, preview=statement, exc="DeltaAnalysisException", msg=long_message
            ),
        ),
    )

    payload = report.to_dict()
    (record,) = payload["failures"]

    assert record["exception_type"] == "DeltaAnalysisException"
    assert record["diagnostic"] == long_message
    assert record["sql"] == statement
    # 0-based on purpose: the index addresses the sibling statement list.
    assert payload["planned_sql_statements"][record["statement_index"]] == statement


def test_a_validation_failure_record_carries_rule_subject_and_detail_lines():
    failure = ValidationFailure(
        rule_name="UnmanagedAspectDrift",
        message="observed partitioning is not declared",
        subject="partitioning",
        details=("partitioning: observed (a) desired ()",),
    )
    report = _report(
        desired=_a_desired_table("orders"),
        read=TablePresent(table=_an_observed_table()),
        failures=(failure,),
    )

    (record,) = report.to_dict()["failures"]

    assert record["rule"] == "UnmanagedAspectDrift"
    assert record["subject"] == "partitioning"
    assert record["details"] == ["partitioning: observed (a) desired ()"]


def test_a_foreign_key_failure_record_names_its_edge_and_reason_code():
    report = _report(
        desired=_a_desired_table("b"),
        read=TablePresent(table=_an_observed_table()),
        plan=_comment_plan("b"),
        blocked_failures=(_blocked_failure(),),
    )

    (record,) = report.to_dict()["failures"]

    assert record["reason"] == "BLOCKED_BY_FAILED_DEPENDENCY"
    assert record["columns"] == ["parent_id"]
    assert record["references"] == "cat.schema.a"
    # No "table" key: it would duplicate the enclosing record's "name".
    assert "table" not in record


def test_the_wire_vocabulary_is_frozen():
    # These strings are the schema_version 2 contract (reference-run-report.md).
    # A rename that reaches this test is a wire-format change: extend the
    # contract and the reference doc deliberately, or revert the rename.
    # Exact equality on purpose — an addition must arrive here too.
    assert {phase.name for phase in FailurePhase} == {
        "READ",
        "PLANNING",
        "FOREIGN_KEY",
        "EXECUTION",
    }
    assert {cls.__name__ for cls in Failure.__subclasses__()} == {
        "ReadFailure",
        "ValidationFailure",
        "ExecutionFailure",
        "ForeignKeyFailure",
    }
    assert {reason.value for reason in ForeignKeyFailureReason} == {
        "CYCLE",
        "UNRESOLVABLE_REFERENCE",
        "BLOCKED_BY_FAILED_DEPENDENCY",
        "REFERENCED_COLUMNS_NOT_A_KEY",
        "REFERENCED_COLUMN_TYPE_MISMATCH",
        "REFERENCED_COLUMN_CASE_MISMATCH",
    }
    assert {category.name.lower() for category in DiffCategory} == {
        "columns",
        "keys",
        "clustering",
        "partitioning",
        "features",
        "properties",
        "tags",
        "comments",
    }


def _one_run_per_failure_variant() -> tuple[TableRun, ...]:
    """One run per failure variant, for contract checks that sweep the family."""
    read_failed = _report(
        desired=_a_desired_table("orders"),
        read=ReadFailure(exception_type="AnalysisException", message="line 1\nline 2"),
    )
    validation_failed = _report(
        desired=_a_desired_table("orders"),
        read=TablePresent(table=_an_observed_table()),
        failures=(ValidationFailure(rule_name="SomeRule", message="unsafe"),),
    )
    plan = _comment_plan("orders")
    execution_failed = _report(
        desired=_a_desired_table("orders"),
        read=TablePresent(table=_an_observed_table()),
        plan=plan,
        execution=_execution(
            plan, failure=_failed_exec(0, exc="DeltaAnalysisException", msg="boom")
        ),
    )
    blocked = _report(
        desired=_a_desired_table("b"),
        read=TablePresent(table=_an_observed_table()),
        plan=_comment_plan("b"),
        blocked_failures=(_blocked_failure(),),
    )
    return (read_failed, validation_failed, execution_failed, blocked)


def test_failure_records_expose_exactly_the_documented_keys():
    # An accidental extra key would silently grow the public contract; the
    # reference doc's per-type tables are the source of these sets.
    base = {"phase", "type", "message"}
    expected_by_type = {
        "ReadFailure": base | {"exception_type", "diagnostic"},
        "ValidationFailure": base | {"rule", "subject", "details"},
        "ExecutionFailure": base | {"exception_type", "diagnostic", "statement_index", "sql"},
        "ForeignKeyFailure": base | {"reason", "columns", "references"},
    }

    records = [run.to_dict()["failures"][0] for run in _one_run_per_failure_variant()]

    assert {record["type"] for record in records} == set(expected_by_type)
    for record in records:
        assert set(record) == expected_by_type[record["type"]]


def test_a_payload_with_failures_is_json_serialisable():
    # The per-type fields include a list and an int; every variant's record
    # must stay within JSON-plain types, like the failure-free payload does.
    for run in _one_run_per_failure_variant():
        json.dumps(run.to_dict())  # must not raise


def test_table_to_dict_reports_execution_counts_when_executed():
    # The counts are statement-denominated: statements applied of statements planned.
    statement = "COMMENT ON TABLE `cat`.`schema`.`orders` IS 'hello'"
    plan = _plan("orders", SetTableComment(desired_comment="hello", observed_comment=""))
    report = _report(
        desired=_a_desired_table("orders"),
        read=TablePresent(table=_an_observed_table()),
        plan=plan,
        execution=_execution(plan, (statement,)),
    )
    assert report.to_dict()["execution"] == {"applied": 1, "total": 1}


def test_sync_report_to_dict_is_json_serialisable_and_complete():
    report = SyncReport(
        started_at=_t0(),
        ended_at=_t1(),
        table_runs=(_a_changed_table_report(),),
        dry_run=True,
    )
    payload = report.to_dict()

    assert payload["schema_version"] == 2
    assert payload["started_at"] == _t0().isoformat()
    assert payload["ended_at"] == _t1().isoformat()
    assert payload["dry_run"] is True
    assert payload["has_changes"] is True
    assert payload["has_failures"] is False
    assert [t["name"] for t in payload["tables"]] == ["cat.schema.orders"]
    json.dumps(payload)  # plain types only — must not raise


def test_to_dict_is_deterministic():
    # Given a valid dry-run report with a planned change
    report = SyncReport(
        started_at=_t0(),
        ended_at=_t1(),
        table_runs=(_a_changed_table_report(),),
        dry_run=True,
    )

    # When projecting the same immutable report twice
    first = report.to_dict()
    second = report.to_dict()

    # Then both structured values are identical
    assert first == second


# ---------- Derived dependency blocking


def _fk_edge(parent: QualifiedName, name: str = "blocked_edge_fk"):
    """Build a dependency edge onto ``parent``; blocking reads its columns and target."""
    return ForeignKeyConstraint(
        local_columns=("parent_id",),
        referenced_table=parent,
        referenced_columns=("id",),
        name=name,
    )


def _sound_report(name: str, dependencies: tuple[ForeignKeyConstraint, ...] = ()):
    """Build a table that read, planned, and validated cleanly, with ``dependencies``."""
    return _report(
        desired=_a_desired_table(name),
        read=TablePresent(table=_an_observed_table()),
        dependencies=dependencies,
    )


def _assemble(*table_runs: TableRun) -> SyncReport:
    return SyncReport.assemble(
        started_at=_t0(),
        ended_at=_t1(),
        table_runs=table_runs,
        dry_run=True,
    )


def test_assemble_bakes_blocked_failures_onto_a_sound_dependent_of_a_failed_parent():
    # Given a parent that failed its read and a sound child depending on it
    parent = _report(desired=_a_desired_table("a"), read=ReadFailure("IOError", "boom"))
    child = _sound_report("b", dependencies=(_fk_edge(_name("a")),))

    # When the run report is assembled
    report = _assemble(parent, child)

    # Then the child's blocking is derived into its frozen projection
    _, derived_child = report.table_runs
    (failure,) = derived_child.blocked_failures
    assert failure.table == _name("b")
    assert failure.references == _name("a")
    assert failure.reason is ForeignKeyFailureReason.BLOCKED_BY_FAILED_DEPENDENCY
    assert derived_child.execution is None
    assert derived_child.status is TableRunStatus.FOREIGN_KEY_FAILED


def test_assemble_propagates_blocking_along_chains():
    # Given a -> b -> c where a failed its read: b is blocked, and c through b
    reports = (
        _report(desired=_a_desired_table("a"), read=ReadFailure("IOError", "boom")),
        _sound_report("b", dependencies=(_fk_edge(_name("a")),)),
        _sound_report("c", dependencies=(_fk_edge(_name("b"), name="c_b_fk"),)),
    )

    # When the run report is assembled
    report = _assemble(*reports)

    # Then each blocked table names the parent that blocked it
    _, derived_b, derived_c = report.table_runs
    assert [failure.references for failure in derived_b.blocked_failures] == [_name("a")]
    assert [failure.references for failure in derived_c.blocked_failures] == [_name("b")]


def test_assemble_does_not_bake_onto_a_table_with_its_own_failures():
    # Given a parent that failed its read and a child that failed planning on its own
    parent = _report(desired=_a_desired_table("a"), read=ReadFailure("IOError", "boom"))
    child = _report(
        desired=_a_desired_table("b"),
        read=TablePresent(table=_an_observed_table()),
        failures=(ValidationFailure(rule_name="SomeRule", message="unsafe"),),
        dependencies=(_fk_edge(_name("a")),),
    )

    # When the run report is assembled
    report = _assemble(parent, child)

    # Then the child keeps exactly its own failures — blocking is not stacked on
    # top of a table that already failed (it still counts as not converged for
    # its own dependents, which the chain test covers)
    _, derived_child = report.table_runs
    assert derived_child.blocked_failures == ()
    assert derived_child.status is TableRunStatus.PLANNING_FAILED


def test_assemble_with_no_failures_returns_the_reports_unchanged():
    # Given a sound dependency edge in a run where nothing failed
    reports = (_sound_report("a"), _sound_report("b", dependencies=(_fk_edge(_name("a")),)))

    report = _assemble(*reports)

    assert report.table_runs == reports
    assert report.has_failures is False


def test_baked_blocked_failures_flatten_into_the_report_failures():
    # Given a report whose blocking was baked in at assembly
    child = _report(
        desired=_a_desired_table("b"),
        read=TablePresent(table=_an_observed_table()),
        blocked_failures=(_blocked_failure(),),
    )

    # Then it reaches callers through the ordinary failure flattening
    assert child.has_failures is True
    assert child.failures == child.blocked_failures
    assert child.status is TableRunStatus.FOREIGN_KEY_FAILED


def test_blocked_failures_reject_a_recorded_execution_outcome():
    plan = _plan("b")

    with pytest.raises(ValueError, match="records no execution outcome"):
        _report(
            desired=_a_desired_table("b"),
            read=TablePresent(table=_an_observed_table()),
            execution=_execution(plan),
            blocked_failures=(_blocked_failure(),),
        )


def test_blocked_failures_reject_a_table_with_its_own_failures():
    # Given a table that failed planning on its own account: assembly never
    # attaches blocking to such a table, so a value claiming both is false
    with pytest.raises(ValueError, match="cannot also carry its own failures"):
        _report(
            desired=_a_desired_table("b"),
            read=TablePresent(table=_an_observed_table()),
            failures=(ValidationFailure(rule_name="SomeRule", message="unsafe"),),
            blocked_failures=(_blocked_failure(),),
        )


def test_blocked_failures_require_the_dependency_blocking_reason():
    with pytest.raises(ValueError, match="dependency-blocking reason"):
        _report(
            desired=_a_desired_table("b"),
            read=TablePresent(table=_an_observed_table()),
            blocked_failures=(
                ForeignKeyFailure(
                    table=_name("b"),
                    local_columns=("parent_id",),
                    references=_name("a"),
                    reason=ForeignKeyFailureReason.CYCLE,
                ),
            ),
        )


def test_table_run_rejects_a_planning_outcome_targeting_another_table():
    # Given a planning outcome built for a different table
    desired = _a_desired_table("orders")
    other = _a_desired_table("other")
    other_plan = ActionPlan(target=other.qualified_name)
    foreign_outcome = PlanningAccepted(diff=TableCreation(other), plan=other_plan)

    # Then the run refuses to carry it
    with pytest.raises(ValueError, match="must target the reported table"):
        TableRun(
            read=TableAbsent(),
            planning=foreign_outcome,
            compiled=build_compiled_plan(other_plan, ()),
            resolution=TableResolution(desired, (), ()),
            execution=None,
        )


def test_a_rejected_table_projects_the_changes_it_refused():
    # Given a table whose diff was rejected
    qualified_name = _name("orders")
    desired = DesiredTable(
        qualified_name=qualified_name,
        columns=(
            DesiredColumn("id", Integer(), nullable=False),
            DesiredColumn("email", String(), nullable=False),
        ),
    )
    observed = ObservedTable(
        qualified_name=qualified_name,
        columns=(ObservedColumn("id", Integer(), nullable=False),),
    )
    report = _report(
        desired=desired,
        read=TablePresent(table=observed),
        failures=(ValidationFailure(rule_name="NonNullableColumnAdd", message="nope"),),
        diff=diff_table(desired, observed),
    )

    # When it is projected
    record = report.to_dict()

    # Then `changes` stays empty — nothing was planned — and the refused
    # differences are stated separately
    assert record["changes"] == []
    assert record["rejected_changes"] == [
        {
            "kind": "columns",
            "operation": "add",
            "subject": "email",
            "detail": "String NOT NULL",
        }
    ]


def test_a_successful_table_projects_no_rejected_changes():
    # Given a table that planned cleanly
    report = _report(
        desired=_a_desired_table("tbl"),
        read=TablePresent(table=_an_observed_table()),
        plan=_plan("tbl", SetTableComment(desired_comment="hello", observed_comment="")),
    )

    # Then the rejected list is empty rather than absent
    assert report.to_dict()["rejected_changes"] == []
