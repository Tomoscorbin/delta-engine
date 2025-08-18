from tabula.application.results import ExecutionOutcome, ExecutionResult, PlanPreview
from tabula.domain.plan.actions import ActionPlan, AddColumn
from tabula.domain.model.column import Column
from tabula.domain.model.qualified_name import QualifiedName
from tabula.domain.model.types import integer


def qn() -> QualifiedName:
    return QualifiedName("Cat", "Sch", "Tbl")


def test_execution_outcome_truthiness_and_defaults():
    ok = ExecutionOutcome(success=True, messages=("ok",), executed_count=2)
    bad = ExecutionOutcome(success=False, messages=("nope",), executed_count=1)
    assert ok
    assert not bad
    assert ok.executed_count == 2 and bad.executed_count == 1


def test_execution_result_carries_plan_and_counts():
    plan = ActionPlan(qn()).add(AddColumn(Column("a", integer())))
    res = ExecutionResult(plan=plan, messages=("done",), executed_count=1)
    assert res.plan is plan
    assert res.executed_count == 1
    assert "done" in res.messages


def test_plan_preview_fields_cohere():
    plan = ActionPlan(qn())  # empty
    preview = PlanPreview(plan=plan, is_noop=True, summary_counts={}, summary_text="noop")
    assert preview.is_noop
    assert preview.summary_counts == {}
    assert preview.summary_text == "noop"
