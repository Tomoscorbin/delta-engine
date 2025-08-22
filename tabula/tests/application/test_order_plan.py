from tabula.application.order_plan import order_plan
from tabula.domain.plan.actions import ActionPlan, Action, CreateTable, AddColumn, DropColumn
from tabula.domain.model import Column
from tabula.domain.model.data_type.types import integer



def col(name: str) -> Column:
    return Column(name, integer())

class UnknownAction(Action):
    """A new action type not covered by the ordering policy."""
    pass


def kinds(plan: ActionPlan) -> list[str]:
    return [type(a).__name__ for a in plan.actions]


def test_empty_plan_returns_same_instance(make_qn) -> None:
    plan = ActionPlan(make_qn())
    ordered = order_plan(plan)
    assert ordered is plan  # fast path returns original
    assert len(ordered) == 0


def test_orders_by_phase_create_then_add_then_drop(make_qn) -> None:
    plan = ActionPlan(
        make_qn(),
        actions=(
            DropColumn("b"),
            AddColumn(col("a")),
            CreateTable((col("id"),)),
        ),
    )
    ordered = order_plan(plan)
    assert kinds(ordered) == ["CreateTable", "AddColumn", "DropColumn"]
    # target unchanged
    assert ordered.target == plan.target


def test_stable_within_phase_preserves_relative_order_of_adds(make_qn) -> None:
    a1 = AddColumn(col("a"))
    a2 = AddColumn(col("b"))
    plan = ActionPlan(
        make_qn(),
        actions=(
            a2,                 # out of desired order
            DropColumn("x"),
            a1,
            CreateTable((col("id"),)),
        ),
    )
    ordered = order_plan(plan)
    # AddColumn phase comes after CreateTable, but within the phase original relative order (a2 then a1) is preserved
    assert kinds(ordered) == ["CreateTable", "AddColumn", "AddColumn", "DropColumn"]
    adds = [a for a in ordered.actions if isinstance(a, AddColumn)]
    assert adds[0] is a2 and adds[1] is a1  # identity & relative order preserved


def test_unknown_actions_sink_to_end_preserving_their_order(make_qn) -> None:
    u1, u2 = UnknownAction(), UnknownAction()
    plan = ActionPlan(
        make_qn(),
        actions=(
            u2,
            AddColumn(col("a")),
            u1,
            DropColumn("b"),
            CreateTable((col("id"),)),
        ),
    )
    ordered = order_plan(plan)
    # known phases first, unknowns last
    assert kinds(ordered)[:3] == ["CreateTable", "AddColumn", "DropColumn"]
    tail = [type(a).__name__ for a in ordered.actions[3:]]
    assert tail == ["UnknownAction", "UnknownAction"]
    # and the unknowns keep their original relative order
    u_tail = [a for a in ordered.actions if isinstance(a, UnknownAction)]
    assert u_tail[0] is u2 and u_tail[1] is u1


def test_does_not_mutate_original_plan_and_returns_new_instance(make_qn) -> None:
    a1, a2, a3 = DropColumn("b"), AddColumn(col("a")), CreateTable((col("id"),))
    plan = ActionPlan(make_qn(), actions=(a1, a2, a3))
    ordered = order_plan(plan)
    # new instance
    assert ordered is not plan
    # original actions tuple untouched
    assert plan.actions == (a1, a2, a3)
    # elements reused (no copying of actions)
    assert set(ordered.actions) == {a1, a2, a3}


def test_idempotent_sorting(make_qn) -> None:
    plan = ActionPlan(
        make_qn(),
        actions=(DropColumn("b"), AddColumn(col("a")), CreateTable((col("id"),))),
    )
    once = order_plan(plan)
    twice = order_plan(once)
    assert kinds(once) == kinds(twice)
    assert once.actions == twice.actions
