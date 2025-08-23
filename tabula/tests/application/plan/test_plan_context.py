
from tabula.application.plan.plan_context import PlanContext


class FakeChangeTarget:
    def __init__(self, qualified_name: str):
        self.qualified_name = qualified_name


class FakePlan:
    """Minimal ActionPlan stub for PlanContext."""

    def __init__(self, target: str):
        self.target = target


def test_plan_context_holds_subject_and_plan_refs():
    subject = FakeChangeTarget("cat.sch.tbl")
    plan = FakePlan(target="cat.sch.tbl")
    ctx = PlanContext(subject=subject, plan=plan)

    assert ctx.subject is subject
    assert ctx.plan is plan


def test_plan_context_target_proxies_to_plan_target():
    subject = FakeChangeTarget("cat.sch.tbl")
    plan = FakePlan(target="cat.sch.tbl")
    ctx = PlanContext(subject=subject, plan=plan)

    # property should return whatever the plan exposes as its target
    assert ctx.target == "cat.sch.tbl"
