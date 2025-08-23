"""Planning utilities for schema changes."""

from __future__ import annotations

from tabula.application.plan.order_plan import order_plan
from tabula.application.plan.plan_context import PlanContext
from tabula.application.results import PlanPreview
from tabula.application.validation import DEFAULT_VALIDATOR, PlanValidator
from tabula.domain.model import ChangeTarget
from tabula.domain.plan.actions import ActionPlan
from tabula.domain.services.differ import diff


def _compute_plan(subject: ChangeTarget) -> ActionPlan:
    """Derive an unordered plan from desired and observed states."""

    return diff(subject.observed, subject.desired)


def _validate_plan(plan: ActionPlan, subject: ChangeTarget, validator: PlanValidator) -> None:
    """Enforce policy rules for the given plan.

    Raises ``ValidationError`` when a rule is violated.
    """

    ctx = PlanContext(subject, plan)
    validator.validate(ctx)


def _make_preview(plan: ActionPlan) -> PlanPreview:
    """Order and summarize the plan for presentation."""

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
    """Generate a preview of the plan for the given subject.

    Args:
        subject: Desired and observed table state.
        validator: Optional validator enforcing policy rules.

    Returns:
        Summary of the plan including counts and actions.
    """

    plan = _compute_plan(subject)
    _validate_plan(plan, subject, validator)
    return _make_preview(plan)
