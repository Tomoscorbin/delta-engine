"""Small shared builders for constructing real test values."""

from collections.abc import Iterable
from typing import Final

from delta_engine.application.failures import (
    Failure,
    ForeignKeyFailure,
    ReadFailure,
    ValidationFailure,
)
from delta_engine.application.planning import PlanningAccepted, PlanningRejected
from delta_engine.application.ports import (
    CompiledPlan,
    ExecutionResult,
    ReadResult,
)
from delta_engine.application.relationships import TableResolution
from delta_engine.application.report import TableRun
from delta_engine.domain.model import (
    DesiredColumn,
    DesiredTable,
    ForeignKeyConstraint,
    ObservedColumn,
    ObservedForeignKeyConstraint,
    ObservedPrimaryKeyConstraint,
    PrimaryKeyConstraint,
    QualifiedName,
)
from delta_engine.domain.plan import ActionPlan, SetTableComment, TableCreation, TableDiff


def build_compiled_plan(
    plan: ActionPlan,
    statements: Iterable[str],
) -> CompiledPlan:
    """Pair a real action plan with one deterministic statement per action."""
    return CompiledPlan(plan=plan, statements=tuple(statements))


def build_compiled_comment_plan(target: QualifiedName, *statements: str) -> CompiledPlan:
    """Build a compiled plan carrying one table-comment action per statement."""
    plan = ActionPlan(
        target=target,
        actions=tuple(
            SetTableComment(desired_comment=f"new {index}", observed_comment=f"old {index}")
            for index in range(len(statements))
        ),
    )
    return build_compiled_plan(plan, statements)


_PLAN_UNSET: Final = object()


def build_table_run(
    *,
    desired: DesiredTable,
    read: ReadResult,
    plan: ActionPlan | None | object = _PLAN_UNSET,
    statements: tuple[str, ...] = (),
    failures: tuple[Failure, ...] = (),
    dependencies: tuple[ForeignKeyConstraint, ...] = (),
    execution: ExecutionResult | None = None,
    blocked_failures: tuple[ForeignKeyFailure, ...] = (),
    diff: TableDiff | None = None,
) -> TableRun:
    """
    Build a frozen ``TableRun`` from concise inputs, deriving the rest.

    ``failures`` are routed by type: validation failures reject planning,
    foreign-key failures fail the resolution. ``plan`` left unset is derived —
    ``None`` after a failed read or rejected planning, the execution's plan
    when ``execution`` is given, an empty plan otherwise. ``statements``
    default to one placeholder per plan action, and ``diff`` to a creation of
    ``desired``.
    """
    planning_failures = tuple(
        failure for failure in failures if isinstance(failure, ValidationFailure)
    )
    resolution_failures = tuple(
        failure for failure in failures if isinstance(failure, ForeignKeyFailure)
    )
    if plan is _PLAN_UNSET:
        report_plan = (
            None
            if isinstance(read, ReadFailure) or planning_failures
            else execution.compiled_plan.plan
            if execution is not None
            else ActionPlan(target=desired.qualified_name)
        )
    else:
        assert plan is None or isinstance(plan, ActionPlan)
        report_plan = plan

    planning_diff = diff if diff is not None else TableCreation(desired)
    if isinstance(read, ReadFailure):
        planning = None
    elif planning_failures:
        planning = PlanningRejected(diff=planning_diff, failures=planning_failures)
    else:
        assert isinstance(report_plan, ActionPlan)
        planning = PlanningAccepted(diff=planning_diff, plan=report_plan)

    if report_plan is None:
        compiled = None
    elif execution is not None:
        compiled = execution.compiled_plan
    else:
        if not statements:
            statements = tuple(f"SQL {index}" for index in range(len(report_plan)))
        compiled = build_compiled_plan(report_plan, statements)

    resolution = TableResolution(
        desired=desired,
        dependencies=dependencies,
        structural_failures=resolution_failures,
    )

    return TableRun(
        read=read,
        planning=planning,
        compiled=compiled,
        resolution=resolution,
        execution=execution,
        blocked_failures=blocked_failures,
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
