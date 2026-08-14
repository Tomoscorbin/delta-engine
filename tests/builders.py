"""Small shared builders for constructing real test values."""

from collections.abc import Iterable

from delta_engine.application.ports import CompiledPlan
from delta_engine.domain.model import DesiredColumn, ObservedColumn
from delta_engine.domain.plan import ActionPlan


def build_compiled_plan(
    plan: ActionPlan,
    statements: Iterable[str],
) -> CompiledPlan:
    """Pair a real action plan with one deterministic statement per action."""
    return CompiledPlan(plan=plan, statements=tuple(statements))


def as_observed_columns(
    columns: Iterable[DesiredColumn | ObservedColumn],
) -> tuple[ObservedColumn, ...]:
    """
    Project columns to their observed catalog image.

    Mirrors what a catalog read-back reports: the observable fields only.
    Declaration-only syntax on ``DesiredColumn`` does not survive the projection.
    """
    return tuple(
        ObservedColumn(
            name=column.name,
            data_type=column.data_type,
            nullable=column.nullable,
            comment=column.comment,
            tags=column.tags,
        )
        for column in columns
    )
