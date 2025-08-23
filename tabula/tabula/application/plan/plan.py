from __future__ import annotations
from tabula.application.plan.order_plan import order_plan
from tabula.application.results import PlanPreview
from tabula.application.validation import PlanValidator, DEFAULT_VALIDATOR
from tabula.domain.plan.actions import ActionPlan
from tabula.domain.services.differ import diff
from tabula.domain.model import ChangeTarget
from tabula.application.plan.plan_context import PlanContext


def _compute_plan(subject: ChangeTarget) -> ActionPlan:
    """Pure: build the unordered ActionPlan from snapshots."""
    return diff(subject.observed, subject.desired)


def _validate_plan(plan: ActionPlan, subject: ChangeTarget, validator: PlanValidator) -> None:
    """Pure: enforce policy; raises on violation."""
    ctx = PlanContext(subject, plan)
    validator.validate(ctx)


def _make_preview(plan: ActionPlan) -> PlanPreview:
    """Pure: order + summarize the plan."""
    ordered = order_plan(plan)
    return PlanPreview(
        plan=ordered,
        is_noop=not plan,
        summary_counts=plan.count_by_action(),
        total_actions=len(plan),
    )


def preview_plan(
    subject: ChangeTarget, validator: PlanValidator = DEFAULT_VALIDATOR
) -> PlanPreview:
    plan = _compute_plan(subject)
    _validate_plan(plan, subject, validator)
    return _make_preview(plan)
