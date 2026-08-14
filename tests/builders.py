"""Small shared builders for constructing real test values."""

from collections.abc import Iterable

from delta_engine.application.ports import CompiledAction, CompiledPlan
from delta_engine.domain.model import (
    DesiredColumn,
    ForeignKeyConstraint,
    ObservedColumn,
    ObservedForeignKeyConstraint,
    ObservedPrimaryKeyConstraint,
    PrimaryKeyConstraint,
)
from delta_engine.domain.plan import ActionPlan


def build_compiled_plan(
    plan: ActionPlan,
    statements: Iterable[str],
) -> CompiledPlan:
    """Pair a real action plan with one deterministic statement per action."""
    return CompiledPlan(
        plan=plan,
        compiled_actions=tuple(
            CompiledAction(action=action, statement=statement)
            for action, statement in zip(plan.actions, statements, strict=True)
        ),
    )


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


def as_observed_primary_key(
    primary_key: PrimaryKeyConstraint | ObservedPrimaryKeyConstraint | None,
) -> ObservedPrimaryKeyConstraint | None:
    """Project a named primary-key declaration to its catalog representation."""
    if primary_key is None or isinstance(primary_key, ObservedPrimaryKeyConstraint):
        return primary_key
    if primary_key.name is None:
        raise ValueError("An observed primary-key fixture requires a name")
    return ObservedPrimaryKeyConstraint(primary_key.columns, primary_key.name)


def as_observed_foreign_keys(
    foreign_keys: Iterable[ForeignKeyConstraint | ObservedForeignKeyConstraint],
) -> tuple[ObservedForeignKeyConstraint, ...]:
    """Project named foreign-key declarations to their catalog representations."""
    observed = []
    for foreign_key in foreign_keys:
        if isinstance(foreign_key, ObservedForeignKeyConstraint):
            observed.append(foreign_key)
            continue
        if foreign_key.name is None:
            raise ValueError("An observed foreign-key fixture requires a name")
        observed.append(
            ObservedForeignKeyConstraint(
                local_columns=foreign_key.local_columns,
                referenced_table=foreign_key.referenced_table,
                referenced_columns=foreign_key.referenced_columns,
                name=foreign_key.name,
            )
        )
    return tuple(observed)
