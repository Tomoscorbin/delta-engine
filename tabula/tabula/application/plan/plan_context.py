"""Context information passed to validation rules."""

from dataclasses import dataclass

from tabula.domain.model import ChangeTarget, QualifiedName
from tabula.domain.plan.actions import ActionPlan


@dataclass(frozen=True, slots=True)
class PlanContext:
    """Subject and plan under validation.

    Attributes:
        subject: Desired and observed table state.
        plan: Action plan produced for the subject.
    """

    subject: ChangeTarget
    plan: ActionPlan

    @property
    def target(self) -> QualifiedName:
        """Qualified name of the plan target."""

        return self.plan.target
