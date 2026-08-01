import pytest

from delta_engine.application.planning import PlanningSucceeded
from delta_engine.application.ports import (
    CompiledAction,
    CompiledPlan,
    ExecutionSucceeded,
    ExecutionSummary,
    TableAbsent,
)
from delta_engine.application.relationships import TableResolution
from delta_engine.application.report import TableRunReport
from delta_engine.domain.model import DesiredColumn, DesiredTable, Integer, QualifiedName
from delta_engine.domain.plan import ActionPlan, SetTableComment

_NAME = QualifiedName("cat", "schema", "orders")
_DESIRED = DesiredTable(
    qualified_name=_NAME,
    columns=(DesiredColumn("id", Integer()),),
)
_PLAN = ActionPlan(
    target=_NAME,
    actions=(SetTableComment(desired_comment="new", observed_comment="old"),),
)


def _compiled(statement: str) -> CompiledPlan:
    return CompiledPlan(
        plan=_PLAN,
        compiled_actions=(CompiledAction(action=_PLAN.actions[0], statement=statement),),
    )


def _report(
    compiled: CompiledPlan | None,
    execution: ExecutionSummary | None = None,
) -> TableRunReport:
    return TableRunReport(
        read=TableAbsent(),
        planning=PlanningSucceeded(_PLAN),
        compiled=compiled,
        resolution=TableResolution(_DESIRED, (), ()),
        execution=execution,
    )


def test_report_retains_the_compiled_plan_as_its_compilation_outcome():
    compiled = _compiled("STATEMENT")

    report = _report(compiled)

    assert report.compiled is compiled
    assert report.to_dict()["planned_sql_statements"] == ["STATEMENT"]


def test_report_rejects_successful_planning_without_compilation():
    with pytest.raises(ValueError, match="requires compilation"):
        _report(None)


def test_report_rejects_a_compiled_plan_from_another_planning_outcome():
    other_plan = ActionPlan(target=_NAME)
    compiled = CompiledPlan(plan=other_plan, compiled_actions=())

    with pytest.raises(ValueError, match="must match the successful planning outcome"):
        _report(compiled)


def test_report_rejects_execution_of_another_compiled_plan():
    reported = _compiled("REPORTED")
    executed = _compiled("EXECUTED")
    execution = ExecutionSummary(
        compiled_plan=executed,
        results=(ExecutionSucceeded(statement_index=0, statement="EXECUTED"),),
    )

    with pytest.raises(ValueError, match="must refer to the reported compiled plan"):
        _report(reported, execution)
