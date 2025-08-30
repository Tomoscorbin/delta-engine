from types import SimpleNamespace
from typing import cast

from delta_engine.application.results import ValidationFailure
from delta_engine.application.validation import (
    NonNullableColumnAdd,
    PlanContext,
    PlanValidator,
)
from delta_engine.domain.model.column import Column
from delta_engine.domain.model.data_type import Integer, String
from delta_engine.domain.model.table import ObservedTable
from delta_engine.domain.plan.actions import ActionPlan, AddColumn
from tests.factories import make_qualified_name

_QN = make_qualified_name("dev", "silver", "people")


def _ctx(observed, actions) -> PlanContext:
    plan = ActionPlan(target=_QN, actions=tuple(actions))
    return cast(PlanContext, SimpleNamespace(observed=observed, plan=plan))


def test_non_nullable_add_on_existing_table_returns_failure() -> None:
    observed_table = ObservedTable(_QN, (Column("id", Integer()),))
    add_non_nullable_age = AddColumn(Column("age", Integer(), is_nullable=False))
    ctx = _ctx(observed_table, [add_non_nullable_age])

    failure = NonNullableColumnAdd().evaluate(ctx)

    assert isinstance(failure, ValidationFailure)
    assert failure.rule_name == "NonNullableColumnAdd"
    assert isinstance(failure.message, str)  # catches an accidental tuple message bug I made
    assert "cannot add non-nullable" in failure.message.lower()
    assert "age" in failure.message.lower()


def test_nullable_add_on_existing_table_is_allowed() -> None:
    observed_table = ObservedTable(_QN, (Column("id", Integer()),))
    add_nullable_nickname = AddColumn(Column("nickname", String(), is_nullable=True))
    ctx = _ctx(observed_table, [add_nullable_nickname])

    failure = NonNullableColumnAdd().evaluate(ctx)

    assert failure is None


def test_non_nullable_add_on_missing_table_is_allowed() -> None:  # i.e. CreateTable
    add_non_nullable_age = AddColumn(Column("age", Integer(), is_nullable=False))
    ctx = _ctx(None, [add_non_nullable_age])

    failure = NonNullableColumnAdd().evaluate(ctx)

    assert failure is None


def test_plan_validator_returns_empty_tuple_when_no_failures() -> None:
    observed_table = ObservedTable(_QN, (Column("id", Integer()),))
    add_nullable_nickname = AddColumn(Column("nickname", String(), is_nullable=True))
    ctx = _ctx(observed_table, [add_nullable_nickname])

    failures = PlanValidator(rules=(NonNullableColumnAdd(),)).validate(ctx)

    assert failures == ()
