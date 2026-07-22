from datetime import datetime
import json

from delta_engine.application.failures import (
    ExecutionFailure,
    Failure,
    ForeignKeyFailure,
    ForeignKeyFailureReason,
    ReadFailure,
    ValidationFailure,
)
from delta_engine.application.ports import (
    ExecutionSucceeded,
    ExecutionSummary,
    ReadResult,
    TableAbsent,
    TablePresent,
)
from delta_engine.application.report import (
    SyncReport,
    TableRunReport,
    TableRunStatus,
)
from delta_engine.domain.model import (
    DesiredColumn,
    DesiredTable,
    Integer,
    ObservedColumn,
    ObservedTable,
    QualifiedName,
)
from delta_engine.domain.plan.actions import ActionPlan, SetTableComment

# ---------- test builders


def _an_observed_table(partitioned_by=()):
    """Build a real ObservedTable, so reports are exercised against the domain type."""
    return ObservedTable(
        qualified_name=QualifiedName("cat", "schema", "observed"),
        columns=(ObservedColumn("id", Integer()),),
        partitioned_by=partitioned_by,
    )


def _a_desired_table(name="observed"):
    """Build a minimal real DesiredTable for pipeline-record construction."""
    return DesiredTable(
        qualified_name=QualifiedName("cat", "schema", name),
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


_PLAN_UNSET = object()


def _plan(name: str, *actions) -> ActionPlan:
    return ActionPlan(
        target=QualifiedName("cat", "schema", name),
        actions=actions,
    )


def _report(
    *,
    desired: DesiredTable,
    read: ReadResult,
    plan: ActionPlan | None | object = _PLAN_UNSET,
    planned_sql_statements: tuple[str, ...] = (),
    failures: tuple[Failure, ...] = (),
    execution: ExecutionSummary | None = None,
) -> TableRunReport:
    """Construct a frozen report snapshot from concise test inputs."""
    if execution is not None and not planned_sql_statements:
        planned_sql_statements = tuple(result.statement for result in execution.results)

    read_failures = (read,) if isinstance(read, ReadFailure) else ()
    reported_failures = tuple(
        failure for failure in failures if not isinstance(failure, ReadFailure | ExecutionFailure)
    )
    execution_failures = () if execution is None else execution.failures
    if plan is _PLAN_UNSET:
        planning_failed = any(isinstance(failure, ValidationFailure) for failure in failures)
        report_plan = (
            None
            if isinstance(read, ReadFailure) or planning_failed
            else ActionPlan(target=desired.qualified_name)
        )
    else:
        assert plan is None or isinstance(plan, ActionPlan)
        report_plan = plan

    return TableRunReport(
        desired=desired,
        read=read,
        plan=report_plan,
        planned_sql_statements=planned_sql_statements,
        failures=(*read_failures, *reported_failures, *execution_failures),
        execution=execution,
    )


# ---------- Tests


def test_table_status_success_when_all_actions_succeed():
    # Given successful read, no pre-execution failures, and only successful actions
    read = TablePresent(table=_an_observed_table())
    execution = ExecutionSummary((_ok_exec(0), _ok_exec(1)))

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
    t_ok = _report(
        desired=_a_desired_table("a"),
        read=TablePresent(table=_an_observed_table()),
        execution=ExecutionSummary((_ok_exec(0),)),
    )
    t_bad = _report(
        desired=_a_desired_table("b"),
        read=TablePresent(table=_an_observed_table()),
        execution=ExecutionSummary((_failed_exec(0),)),
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
    changed = _report(
        desired=_a_desired_table("a"),
        read=TablePresent(table=_an_observed_table()),
        plan=_plan("a", SetTableComment(desired_comment="hello", observed_comment="")),
    )
    unchanged = _report(
        desired=_a_desired_table("b"),
        read=TablePresent(table=_an_observed_table()),
    )
    report = SyncReport(started_at=_t0(), ended_at=_t1(), table_reports=(changed, unchanged))
    assert report.has_changes is True


def test_sync_report_has_no_changes_when_no_table_plans_actions():
    unchanged = _report(
        desired=_a_desired_table("b"),
        read=TablePresent(table=_an_observed_table()),
    )
    report = SyncReport(started_at=_t0(), ended_at=_t1(), table_reports=(unchanged,))
    assert report.has_changes is False


def test_sync_report_planned_sql_maps_dotted_names_and_omits_empty():
    with_sql = _report(
        desired=_a_desired_table("a"),
        read=TablePresent(table=_an_observed_table()),
        plan=_plan("a", SetTableComment(desired_comment="hello", observed_comment="")),
        planned_sql_statements=("ALTER TABLE a SET ...",),
    )
    without_sql = _report(
        desired=_a_desired_table("b"),
        read=TablePresent(table=_an_observed_table()),
    )
    report = SyncReport(started_at=_t0(), ended_at=_t1(), table_reports=(with_sql, without_sql))
    assert report.planned_sql_statements == {"cat.schema.a": ("ALTER TABLE a SET ...",)}


def test_sync_report_failures_by_table_maps_only_failed_tables():
    # Given one failed and one successful table
    failed_name = QualifiedName("cat", "schema", "y")
    t_ok = _report(
        desired=_a_desired_table("x"),
        read=TablePresent(table=_an_observed_table()),
        execution=ExecutionSummary((_ok_exec(0),)),
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


def test_table_run_report_status_is_planning_failed_when_both_fk_and_validation_present():
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

    # Then PLANNING_FAILED wins: it is the earlier phase and the actionable root cause
    assert report.status is TableRunStatus.PLANNING_FAILED
    assert len(report.failures) == 2


def test_table_run_report_with_no_failures_is_success():
    # Given a clean table with no failures
    report = _report(
        desired=_a_desired_table("ok"),
        read=TablePresent(table=_an_observed_table()),
        execution=ExecutionSummary((_ok_exec(0),)),
    )

    # Then it is a success and carries no failures
    assert report.status is TableRunStatus.SUCCESS
    assert report.failures == ()


def test_status_reflects_the_earliest_failing_phase():
    # Given a table with an execution failure only
    read = TablePresent(table=_an_observed_table())
    exec_only = _report(
        desired=_a_desired_table("e"),
        read=read,
        execution=ExecutionSummary((_failed_exec(0),)),
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


def test_runtime_dependency_block_is_failure_but_not_statement_execution():
    desired = _a_desired_table("orders")
    failure = ForeignKeyFailure(
        table=desired.qualified_name,
        local_columns=("customer_id",),
        references=QualifiedName("cat", "schema", "customers"),
        reason=ForeignKeyFailureReason.BLOCKED_BY_FAILED_DEPENDENCY,
    )
    report = _report(
        desired=desired,
        read=TablePresent(table=_an_observed_table()),
        failures=(failure,),
    )

    assert report.status is TableRunStatus.FOREIGN_KEY_FAILED
    assert report.failures == (failure,)
    assert report.execution is None


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


def _a_changed_table_report():
    return _report(
        desired=_a_desired_table("orders"),
        read=TablePresent(table=_an_observed_table()),
        plan=_plan("orders", SetTableComment(desired_comment="hello", observed_comment="")),
        planned_sql_statements=("COMMENT ON TABLE `cat`.`schema`.`orders` IS 'hello'",),
    )


def test_table_to_dict_states_the_planned_change():
    payload = _a_changed_table_report().to_dict()

    assert payload["name"] == "cat.schema.orders"
    assert payload["status"] == "SUCCESS"
    assert payload["has_changes"] is True
    assert payload["has_failures"] is False
    assert payload["changes"] == [
        {"kind": "comments", "operation": "change", "subject": "table: 'hello'", "detail": ""}
    ]
    assert payload["planned_sql_statements"] == [
        "COMMENT ON TABLE `cat`.`schema`.`orders` IS 'hello'"
    ]
    assert payload["failures"] == []
    assert payload["execution"] is None


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
    report = _report(
        desired=_a_desired_table("orders"),
        read=TablePresent(table=_an_observed_table()),
        plan=_plan("orders", SetTableComment(desired_comment="hello", observed_comment="")),
        planned_sql_statements=(statement,),
        execution=ExecutionSummary((_ok_exec(0, preview=statement),)),
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
    report = SyncReport(
        started_at=_t0(), ended_at=_t1(), table_reports=(_a_changed_table_report(),)
    )
    assert report.to_dict() == report.to_dict()
