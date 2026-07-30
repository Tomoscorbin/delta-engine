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
    ExecutionSucceeded,
    ExecutionSummary,
    ReadResult,
    TableAbsent,
    TablePresent,
)
from delta_engine.application.relationships import TableResolution
from delta_engine.application.report import (
    SyncReport,
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
    TableFeature,
)
from delta_engine.domain.plan.actions import (
    ActionPlan,
    EnableTableFeature,
    SetTableComment,
)

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
    dependencies: tuple[ForeignKeyConstraint, ...] = (),
    execution: ExecutionSummary | None = None,
    blocked_failures: tuple[ForeignKeyFailure, ...] = (),
) -> TableRunReport:
    """Construct a frozen report snapshot from concise test inputs."""
    if execution is not None and not planned_sql_statements:
        planned_sql_statements = tuple(result.statement for result in execution.results)

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

    resolution = TableResolution(
        desired=desired,
        dependencies=dependencies,
        structural_failures=resolution_failures,
    )

    return TableRunReport(
        read=read,
        planning=planning,
        planned_sql_statements=planned_sql_statements,
        resolution=resolution,
        execution=execution,
        blocked_failures=blocked_failures,
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
            planned_sql_statements=(),
            resolution=TableResolution(desired, (), ()),
            execution=None,
        )


def test_table_run_report_rejects_execution_after_failed_resolution():
    desired = _a_desired_table("orders")
    failure = ForeignKeyFailure(
        table=desired.qualified_name,
        local_columns=("customer_id",),
        references=QualifiedName("cat", "schema", "customers"),
        reason=ForeignKeyFailureReason.UNRESOLVABLE_REFERENCE,
    )

    with pytest.raises(ValueError, match="Execution cannot follow a failed earlier phase"):
        TableRunReport(
            read=TablePresent(table=_an_observed_table()),
            planning=PlanningSucceeded(ActionPlan(target=desired.qualified_name)),
            planned_sql_statements=("SQL",),
            resolution=TableResolution(desired, (), (failure,)),
            execution=ExecutionSummary((_ok_exec(0, "SQL"),)),
        )


def test_table_run_report_rejects_execution_unrelated_to_planned_sql():
    desired = _a_desired_table("orders")

    with pytest.raises(ValueError, match="planned statement prefix"):
        TableRunReport(
            read=TablePresent(table=_an_observed_table()),
            planning=PlanningSucceeded(ActionPlan(target=desired.qualified_name)),
            planned_sql_statements=("PLANNED SQL",),
            resolution=TableResolution(desired, (), ()),
            execution=ExecutionSummary((_ok_exec(0, "OTHER SQL"),)),
        )


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


def test_table_to_dict_exposes_feature_enablement_as_a_public_change_kind():
    # Given a successful table report planning a permanent feature enablement
    report = _report(
        desired=_a_desired_table("orders"),
        read=TablePresent(table=_an_observed_table()),
        plan=_plan(
            "orders",
            EnableTableFeature(TableFeature.TIMESTAMP_NTZ),
        ),
        planned_sql_statements=("ALTER TABLE ... SET TBLPROPERTIES (...)",),
    )

    # When serializing the public report
    changes = report.to_dict()["changes"]

    # Then the feature upgrade has its own public change kind
    assert changes == [
        {
            "kind": "features",
            "operation": "add",
            "subject": "table feature timestampNtz — permanent protocol upgrade",
            "detail": "",
        }
    ]


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


# ---------- Derived dependency blocking


def _fk_edge(parent: QualifiedName, constraint_name: str = "blocked_edge_fk"):
    """Build a dependency edge onto ``parent``; blocking reads its columns and target."""
    return ForeignKeyConstraint(
        local_columns=("parent_id",),
        referenced_table=parent,
        referenced_columns=("id",),
        constraint_name=constraint_name,
    )


def _blocked_failure():
    """Build the failure ``b`` earns for its edge onto a parent ``a`` that did not converge."""
    return ForeignKeyFailure(
        table=_name("b"),
        local_columns=("parent_id",),
        references=_name("a"),
        reason=ForeignKeyFailureReason.BLOCKED_BY_FAILED_DEPENDENCY,
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
    with pytest.raises(ValueError, match="records no execution outcome"):
        _report(
            desired=_a_desired_table("b"),
            read=TablePresent(table=_an_observed_table()),
            execution=ExecutionSummary(),
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
