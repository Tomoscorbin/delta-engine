from tabula.domain.model import ChangeTarget, QualifiedName
from dataclasses import dataclass
from tabula.domain.plan.actions import ActionPlan


@dataclass(frozen=True, slots=True)
class PlanContext:
    subject: ChangeTarget
    plan: ActionPlan

    @property
    def target(self) -> QualifiedName:
        return self.plan.target
