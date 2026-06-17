from delta_engine.domain.plan.actions import ActionPlan, DropColumn


def test_actionplan_truthiness_and_length():
    # Given: empty and non-empty plans
    empty = ActionPlan(())
    non_empty = ActionPlan((DropColumn("legacy"),))
    # When/Then
    assert bool(empty) is False
    assert len(empty) == 0
    assert bool(non_empty) is True
    assert len(non_empty) == 1
