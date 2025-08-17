import pytest
from dataclasses import FrozenInstanceError

from tabula.application.results import PlanPreview, ExecutionResult, ExecutionOutcome
from tabula.domain.model.actions import ActionPlan, AddColumn
from tabula.domain.model.qualified_name import FullName
from tabula.domain.model.column import Column
from tabula.domain.model.data_type import DataType

def _full_name():
    return FullName("dev", "sales", "orders")

def _empty_plan():
    return ActionPlan(full_name=_full_name(), actions=())

def _plan_with_add():
    col = Column("b", DataType("string"))
    return ActionPlan(full_name=_full_name(), actions=(AddColumn(col),))

def test_plan_preview_noop_is_immutable_and_roundtrips():
    plan = _empty_plan()
    preview = PlanPreview(plan=plan, is_noop=True, summary="noop")

    assert preview.plan is plan
    assert preview.is_noop is True
    assert preview.summary == "noop"

    with pytest.raises(FrozenInstanceError):
        preview.summary = "changed"   # frozen dataclass

def test_execution_result_holds_plan_and_messages():
    plan = _plan_with_add()
    res = ExecutionResult(plan=plan, success=True, messages=("ok",))

    assert res.plan is plan
    assert res.success is True
    assert res.messages == ("ok",)
    assert isinstance(res.messages, tuple)

def test_execution_outcome_defaults_and_types():
    out = ExecutionOutcome(success=True)  # messages default, count default

    assert out.success is True
    assert out.messages == ()
    assert isinstance(out.messages, tuple)
    assert hasattr(out, "executed_count")
    assert out.executed_count == 0
