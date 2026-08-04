from datetime import datetime
import json

import pytest

from delta_engine.application.failures import (
    ExecutionFailure,
    Failure,
    ForeignKeyFailure,
    ForeignKeyFailureReason,
    ReadFailure,
    ValidationFailure,
)
from delta_engine.application.planning import PlanningFailed, PlanningSucceeded
from delta_engine.application.ports import (
    ExecutionResult,
    ExecutionSucceeded,
    ExecutionSummary,
    ReadResult,
    TableAbsent,
    TablePresent,
)
from delta_engine.application.relationships import TableResolution
from delta_engine.application.report import (
    SyncReport,
    TableChangeState,
    TableRunReport,
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
from delta_engine.domain.plan import TableDiff, diff_table
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


def _ok_exec(idx=0, preview="ALTER TABLE ..."):
    return ExecutionSucceeded(statement_index=idx, statement=preview)


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


def _execution(plan: ActionPlan, *results: ExecutionResult) -> ExecutionSummary:
    return ExecutionSummary(
        compiled_plan=build_compiled_plan(
            plan,
            tuple(result.statement for result in results),
        ),
        results=results,
    )


def _report(
    *,
    desired: DesiredTable,
    read: ReadResult,
    plan: ActionPlan | None | object = _PLAN_UNSET,
    statements: tuple[str, ...] = (),
    failures: tuple[Failure, ...] = (),
    dependencies: tuple[ForeignKeyConstraint, ...] = (),
    execution: ExecutionSummary | None = None,
    blocked_failures: tuple[ForeignKeyFailure, ...] = (),
    diff: TableDiff | None = None,
) -> TableRunReport:
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

    if isinstance(read, ReadFailure):
        planning = None
    elif planning_failures:
        planning = PlanningFailed(planning_failures)
    else:
        assert isinstance(report_plan, ActionPlan)
        planning = PlanningSucceeded(report_plan)

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

    return TableRunReport(
        read=read,
        planning=planning,
        compiled=compiled,
        resolution=resolution,
        execution=execution,
        blocked_failures=blocked_failures,
        diff=diff,
    )


# ---------- Tests


def test_table_status_success_when_all_actions_succeed():
    # Given successful read, no pre-execution failures, and only successful actions
    read = TablePresent(table=_an_observed_table())
    plan = _comment_plan("tbl", statement_count=2)
    execution = _execution(plan, _ok_exec(0), _ok_exec(1))

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
        execution=_execution(ok_plan, _ok_exec(0)),
    )
    t_bad = _report(
        desired=_a_desired_table("b"),
        read=TablePresent(table=_an_observed_table()),
        execution=_execution(failed_plan, _failed_exec(0)),
    )

    # When aggregating the sync
    sr = SyncReport(started_at=_t0(), ended_at=_t1(), table_reports=(t_ok, t_bad))

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
        table_reports=(changed, unchanged),
        dry_run=True,
    )

    # Then the aggregate reports planned changes
    assert report.has_changes is True


def test_sync_report_has_no_changes_when_no_table_plans_actions():
    unchanged = _report(
        desired=_a_desired_table("b"),
        read=TablePresent(table=_an_observed_table()),
    )
    report = SyncReport(started_at=_t0(), ended_at=_t1(), table_reports=(unchanged,))
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
        table_reports=(changed, unchanged, failed),
        dry_run=True,
    )

    # Then each is counted once, and the total is their sum
    counts = report.counts
    assert (counts.changed, counts.unchanged, counts.failed) == (1, 1, 1)
    assert counts.total == len(report.table_reports)


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
            ExecutionFailure(
                statement_index=0,
                exception_type="SparkException",
                message="boom",
                statement=statement,
            ),
        ),
    )
    report = SyncReport(started_at=_t0(), ended_at=_t1(), table_reports=(failed,))

    # Then it counts as failed, not changed: the change was planned, not applied
    assert report.counts == (0, 0, 1)


def test_sync_report_reports_its_own_duration():
    report = SyncReport(started_at=_t0(), ended_at=_t1(), table_reports=())
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
        table_reports=(with_sql, without_sql),
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
        execution=_execution(successful_plan, _ok_exec(0)),
    )
    t_bad = _report(
        desired=_a_desired_table("y"),
        read=TableAbsent(),
        failures=(ValidationFailure("R", "v"),),
    )

    # When aggregating the sync report
    sr = SyncReport(started_at=_t0(), ended_at=_t1(), table_reports=(t_ok, t_bad))

    # Then only the failed table appears, keyed by its QualifiedName, with its failures
    mapping = sr.failures_by_table
    assert list(mapping.keys()) == [failed_name]
    assert all(
        isinstance(f, ValidationFailure | ReadFailure | ExecutionFailure | ForeignKeyFailure)
        for f in mapping[failed_name]
    )


def test_table_run_report_status_is_foreign_key_failed_when_fk_failure_present():
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


def test_table_run_report_status_is_planning_failed_when_only_validation_failure_present():
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


def test_table_run_report_status_is_foreign_key_failed_when_both_fk_and_validation_present():
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


def test_table_run_report_with_no_failures_is_success():
    # Given a clean table with no failures
    plan = _comment_plan("ok")
    report = _report(
        desired=_a_desired_table("ok"),
        read=TablePresent(table=_an_observed_table()),
        execution=_execution(plan, _ok_exec(0)),
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
        execution=_execution(plan, _failed_exec(0)),
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


def test_table_run_report_carries_its_desired_definition():
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


def test_table_run_report_rejects_planning_after_a_failed_read():
    desired = _a_desired_table("orders")

    with pytest.raises(ValueError, match="Planning cannot follow a failed read"):
        TableRunReport(
            read=ReadFailure("IOError", "boom"),
            planning=PlanningSucceeded(ActionPlan(target=desired.qualified_name)),
            compiled=None,
            resolution=TableResolution(desired, (), ()),
            execution=None,
        )


def test_table_run_report_rejects_successful_planning_without_compilation():
    desired = _a_desired_table("orders")

    with pytest.raises(ValueError, match="requires compilation"):
        TableRunReport(
            read=TableAbsent(),
            planning=PlanningSucceeded(ActionPlan(target=desired.qualified_name)),
            compiled=None,
            resolution=TableResolution(desired, (), ()),
            execution=None,
        )


def test_table_run_report_rejects_compilation_of_another_plan():
    desired = _a_desired_table("orders")
    plan = _comment_plan("orders")
    other_plan = ActionPlan(target=desired.qualified_name)

    with pytest.raises(ValueError, match="must match the successful planning outcome"):
        TableRunReport(
            read=TableAbsent(),
            planning=PlanningSucceeded(plan),
            compiled=build_compiled_plan(other_plan, ()),
            resolution=TableResolution(desired, (), ()),
            execution=None,
        )


def test_table_run_report_rejects_execution_after_failed_resolution():
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
        TableRunReport(
            read=TablePresent(table=_an_observed_table()),
            planning=PlanningSucceeded(plan),
            compiled=compiled,
            resolution=TableResolution(desired, (), (failure,)),
            execution=ExecutionSummary(
                compiled_plan=compiled,
                results=(_ok_exec(0, "SQL"),),
            ),
        )


def test_table_run_report_rejects_execution_of_another_compiled_plan():
    desired = _a_desired_table("orders")
    plan = _plan("orders", SetTableComment(desired_comment="hello", observed_comment=""))
    reported = build_compiled_plan(plan, ("REPORTED SQL",))
    executed = build_compiled_plan(plan, ("EXECUTED SQL",))

    with pytest.raises(ValueError, match="reported compiled plan"):
        TableRunReport(
            read=TablePresent(table=_an_observed_table()),
            planning=PlanningSucceeded(plan),
            compiled=reported,
            resolution=TableResolution(desired, (), ()),
            execution=ExecutionSummary(
                compiled_plan=executed,
                results=(_ok_exec(0, "EXECUTED SQL"),),
            ),
        )


def _a_changed_table_report():
    return _report(
        desired=_a_desired_table("orders"),
        read=TablePresent(table=_an_observed_table()),
        plan=_plan("orders", SetTableComment(desired_comment="hello", observed_comment="")),
        statements=("COMMENT ON TABLE `cat`.`schema`.`orders` IS 'hello'",),
    )


def test_a_table_report_knows_whether_its_plan_creates_the_table():
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
            ExecutionSucceeded(statement_index=0, statement=statements[0]),
            ExecutionFailure(
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


def test_change_state_values_are_human_readable():
    # Given the complete catalog change-state vocabulary
    states = tuple(TableChangeState)

    # When reading the public string values
    values = tuple(state.value for state in states)

    # Then each value is suitable for direct human-readable rendering
    assert values == (
        "not planned",
        "unchanged",
        "planned",
        "not applied",
        "partially applied",
        "applied",
    )


def test_change_state_is_not_planned_when_no_plan_was_accepted():
    # Given a table whose catalog read failed before planning
    report = _report(
        desired=_a_desired_table("orders"),
        read=ReadFailure("AnalysisException", "boom"),
    )

    # When deriving change states through the completed real-run report
    sync = SyncReport(started_at=_t0(), ended_at=_t1(), table_reports=(report,))

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
    sync = SyncReport(started_at=_t0(), ended_at=_t1(), table_reports=(report,))

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
    sync = SyncReport(started_at=_t0(), ended_at=_t1(), table_reports=(report,))

    # Then failure status and catalog change state remain independent
    assert report.status is TableRunStatus.FOREIGN_KEY_FAILED
    assert sync.table_change_states == (TableChangeState.UNCHANGED,)


def test_change_state_is_planned_for_a_dry_run_with_changes():
    # Given a dry-run table with a compiled, non-empty plan
    report = _a_changed_table_report()

    # When deriving change states through the validated aggregate
    sync = SyncReport(
        started_at=_t0(),
        ended_at=_t1(),
        table_reports=(report,),
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
    sync = SyncReport(started_at=_t0(), ended_at=_t1(), table_reports=(report,))

    # Then none of the intended catalog change was applied
    assert sync.table_change_states == (TableChangeState.NOT_APPLIED,)


def test_first_and_later_execution_failures_have_distinct_change_states():
    # Given one plan that failed immediately and one that applied a statement first
    statements = ("SQL 0", "SQL 1")
    first_plan = _comment_plan("first", statement_count=2)
    first_failed = _report(
        desired=_a_desired_table("first"),
        read=TablePresent(table=_an_observed_table()),
        execution=ExecutionSummary(
            compiled_plan=build_compiled_plan(first_plan, statements),
            results=(_failed_exec(0, statements[0]),),
        ),
    )
    later_plan = _comment_plan("later", statement_count=2)
    later_failed = _report(
        desired=_a_desired_table("later"),
        read=TablePresent(table=_an_observed_table()),
        execution=_execution(
            later_plan,
            _ok_exec(0, statements[0]),
            _failed_exec(1, statements[1]),
        ),
    )

    # When both table outcomes are aggregated in report order
    sync = SyncReport(
        started_at=_t0(),
        ended_at=_t1(),
        table_reports=(first_failed, later_failed),
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
        execution=_execution(plan, _ok_exec(0), _ok_exec(1)),
    )

    # When deriving change states through the validated aggregate
    sync = SyncReport(started_at=_t0(), ended_at=_t1(), table_reports=(report,))

    # Then the intended catalog change is fully applied
    assert sync.table_change_states == (TableChangeState.APPLIED,)


# ---------- Run consistency


def test_sync_report_rejects_execution_results_on_a_dry_run():
    # Given a table carrying a completed execution result
    plan = _comment_plan("orders")
    executed = _report(
        desired=_a_desired_table("orders"),
        read=TablePresent(table=_an_observed_table()),
        execution=_execution(plan, _ok_exec()),
    )

    # When constructing a dry-run aggregate
    # Then the contradictory execution history is rejected
    with pytest.raises(ValueError):
        SyncReport(
            started_at=_t0(),
            ended_at=_t1(),
            table_reports=(executed,),
            dry_run=True,
        )


def test_sync_report_rejects_an_unexplained_unexecuted_real_change():
    # Given a planned change with neither execution nor a blocking failure
    changed = _a_changed_table_report()

    # When constructing a real-run aggregate
    # Then the unexplained missing execution is rejected
    with pytest.raises(ValueError):
        SyncReport(started_at=_t0(), ended_at=_t1(), table_reports=(changed,))


def test_sync_report_accepts_a_real_change_blocked_by_a_failed_dependency():
    # Given a real-run change carrying the failure that blocked its execution
    blocked = _report(
        desired=_a_desired_table("b"),
        read=TablePresent(table=_an_observed_table()),
        plan=_comment_plan("b"),
        blocked_failures=(_blocked_failure(),),
    )

    # When constructing the aggregate
    report = SyncReport(started_at=_t0(), ended_at=_t1(), table_reports=(blocked,))

    # Then the explained lack of execution is accepted
    assert report.table_reports == (blocked,)


def test_change_states_do_not_extend_the_structured_report_schema():
    # Given a dry-run report exposing its Python-level change states
    report = SyncReport(
        started_at=_t0(),
        ended_at=_t1(),
        table_reports=(_a_changed_table_report(),),
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
        }
    ]


def test_table_to_dict_reports_execution_counts_when_executed():
    # The counts are statement-denominated: statements applied of statements planned.
    statement = "COMMENT ON TABLE `cat`.`schema`.`orders` IS 'hello'"
    plan = _plan("orders", SetTableComment(desired_comment="hello", observed_comment=""))
    report = _report(
        desired=_a_desired_table("orders"),
        read=TablePresent(table=_an_observed_table()),
        plan=plan,
        execution=_execution(plan, _ok_exec(0, preview=statement)),
    )
    assert report.to_dict()["execution"] == {"applied": 1, "total": 1}


def test_sync_report_to_dict_is_json_serialisable_and_complete():
    report = SyncReport(
        started_at=_t0(),
        ended_at=_t1(),
        table_reports=(_a_changed_table_report(),),
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
        table_reports=(_a_changed_table_report(),),
        dry_run=True,
    )

    # When projecting the same immutable report twice
    first = report.to_dict()
    second = report.to_dict()

    # Then both structured values are identical
    assert first == second


# ---------- Derived dependency blocking


def _fk_edge(parent: QualifiedName, constraint_name: str = "blocked_edge_fk"):
    """Build a dependency edge onto ``parent``; blocking reads its columns and target."""
    return ForeignKeyConstraint(
        local_columns=("parent_id",),
        referenced_table=parent,
        referenced_columns=("id",),
        constraint_name=constraint_name,
    )


def _sound_report(name: str, dependencies: tuple[ForeignKeyConstraint, ...] = ()):
    """Build a table that read, planned, and validated cleanly, with ``dependencies``."""
    return _report(
        desired=_a_desired_table(name),
        read=TablePresent(table=_an_observed_table()),
        dependencies=dependencies,
    )


def _assemble(*table_reports: TableRunReport) -> SyncReport:
    return SyncReport.assemble(
        started_at=_t0(),
        ended_at=_t1(),
        table_reports=table_reports,
        dry_run=True,
    )


def test_assemble_bakes_blocked_failures_onto_a_sound_dependent_of_a_failed_parent():
    # Given a parent that failed its read and a sound child depending on it
    parent = _report(desired=_a_desired_table("a"), read=ReadFailure("IOError", "boom"))
    child = _sound_report("b", dependencies=(_fk_edge(_name("a")),))

    # When the run report is assembled
    report = _assemble(parent, child)

    # Then the child's blocking is derived into its frozen projection
    _, derived_child = report.table_reports
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
        _sound_report("c", dependencies=(_fk_edge(_name("b"), constraint_name="c_b_fk"),)),
    )

    # When the run report is assembled
    report = _assemble(*reports)

    # Then each blocked table names the parent that blocked it
    _, derived_b, derived_c = report.table_reports
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
    _, derived_child = report.table_reports
    assert derived_child.blocked_failures == ()
    assert derived_child.status is TableRunStatus.PLANNING_FAILED


def test_assemble_with_no_failures_returns_the_reports_unchanged():
    # Given a sound dependency edge in a run where nothing failed
    reports = (_sound_report("a"), _sound_report("b", dependencies=(_fk_edge(_name("a")),)))

    report = _assemble(*reports)

    assert report.table_reports == reports
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


def test_a_report_cannot_claim_a_diff_after_a_failed_read():
    # Given a read that failed, there is nothing to have compared
    desired = _a_desired_table()
    observed = _an_observed_table()

    # Then constructing a report that claims both is rejected
    with pytest.raises(ValueError, match="failed read produces no diff"):
        _report(
            desired=desired,
            read=ReadFailure(exception_type="IOError", message="boom"),
            diff=diff_table(desired, observed),
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
