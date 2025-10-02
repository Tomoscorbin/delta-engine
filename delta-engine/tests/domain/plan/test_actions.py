from delta_engine.domain.model import QualifiedName
from delta_engine.domain.plan.actions import ActionPlan, DropColumn

_QUALIFIED_NAME = QualifiedName("dev", "silver", "orders")


def test_actionplan_truthiness_and_length():
    # Given: empty and non-empty plans
    empty = ActionPlan(_QUALIFIED_NAME, ())
    non_empty = ActionPlan(_QUALIFIED_NAME, (DropColumn("legacy"),))
    # When/Then
    assert bool(empty) is False
    assert len(empty) == 0
    assert bool(non_empty) is True
    assert len(non_empty) == 1
