from delta_engine.application.plan import (
    _order_actions,
)
from delta_engine.domain.model import QualifiedName
from delta_engine.domain.plan import ActionPlan

_QUALIFIED_NAME = QualifiedName("dev", "silver", "test")


# ------- fakes


class _FakeAction:
    """Fake action with a label so our fake sort key can order it."""

    def __init__(self, label: str) -> None:
        self.label = label

    def __repr__(self) -> str:  # helpful when an assertion fails
        return f"_FakeAction({self.label})"


# ------- tests


def test_orders_actions_using_sort_key():
    # Given an ActionPlan whose actions are unsorted
    unsorted_plan = ActionPlan(
        target=_QUALIFIED_NAME,
        actions=(_FakeAction("b"), _FakeAction("a"), _FakeAction("c")),
    )

    # When ordering the plan using a label-based sort key
    def key(a: _FakeAction):
        return a.label

    ordered = _order_actions(unsorted_plan, sort_key=key)

    # Then actions are deterministically ordered
    assert [a.label for a in ordered.actions] == ["a", "b", "c"]


def test_preserves_input_order_when_sort_keys_tie():
    # Given two actions that will produce identical sort keys
    a1, a2 = _FakeAction("x1"), _FakeAction("x2")
    plan = ActionPlan(
        target=_QUALIFIED_NAME,
        actions=(a1, a2),
    )

    # When ordering with a constant key (all ties)
    ordered = _order_actions(plan, sort_key=lambda _: 0)

    # Then the actions are preserved in original order
    assert tuple(ordered.actions) == (a1, a2)


def test_empty_plan_results_in_empty_actions_tuple():
    # Given an ActionPlan with no actions
    empty_plan = ActionPlan(target=_QUALIFIED_NAME, actions=())

    # When ordering the plan (key doesn't matter)
    ordered = _order_actions(empty_plan, sort_key=lambda _: 0)

    # Then the result contains an empty
    assert ordered.actions == ()
