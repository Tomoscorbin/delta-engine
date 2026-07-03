"""Validation rules for planned schema changes."""

from __future__ import annotations

from dataclasses import dataclass
from typing import ClassVar, Protocol

from delta_engine.application.failures import ValidationFailure
from delta_engine.domain.model import TableAspect
from delta_engine.domain.plan import (
    ActionPlan,
    AddColumn,
    ColumnTypeChange,
    PartitioningChange,
    SetColumnNullability,
    TargetColumnMissing,
    TargetTableMissing,
    UnenforceablePrimaryKey,
)


@dataclass(frozen=True, slots=True)
class ValidationResult:
    """Outcome of plan validation."""

    failures: tuple[ValidationFailure, ...] = ()

    @property
    def failed(self) -> bool:
        """True when any validation failures are present."""
        return bool(self.failures)


class Rule(Protocol):
    """Interface for plan validation rules."""

    name: ClassVar[str]

    def evaluate(self, plan: ActionPlan) -> tuple[ValidationFailure, ...]:
        """
        Evaluate the rule against a planned change.

        Args:
            plan: The action plan to reach the desired state. A creation plan
                contains ``CreateTable`` plus any follow-up metadata actions
                needed after creation; a migration plan contains the specific
                change actions. Rules inspect the actions they care about and
                ignore the rest.

        Returns:
            A tuple of failures — one per violation found. Empty when the rule
            passes. All violations are returned in a single call so the caller
            reports the full set rather than requiring a fix-and-rerun cycle
            per failure.

        """
        ...


class NonNullableColumnAdd:
    """
    Disallow adding non-nullable columns to existing tables.

    The rule flags any plan that adds a NOT NULL column when the table
    already exists (it does not attempt to infer data emptiness).
    """

    name: ClassVar[str] = "NonNullableColumnAdd"

    def evaluate(self, plan: ActionPlan) -> tuple[ValidationFailure, ...]:
        """Flag every NOT NULL column addition to an existing table."""
        return tuple(
            ValidationFailure(
                rule_name=self.name,
                message=(
                    f"Operation not allowed: cannot add non-nullable column '{action.column.name}'"
                ),
            )
            for action in plan
            if isinstance(action, AddColumn) and not action.column.nullable
        )


class NullabilityTighteningOnExistingColumn:
    """
    Disallow tightening an existing column to NOT NULL.

    Setting a previously-nullable column to NOT NULL fails at execution time if
    the column already holds NULLs, and the failure surfaces only after earlier
    actions have committed. The plan cannot know whether data is present, so --
    like :class:`NonNullableColumnAdd` -- the rule conservatively blocks the
    tightening and points to the safe path. Loosening to nullable is always safe
    and is not flagged.
    """

    name: ClassVar[str] = "NullabilityTighteningOnExistingColumn"

    def evaluate(self, plan: ActionPlan) -> tuple[ValidationFailure, ...]:
        """Flag every action that tightens an existing column to NOT NULL."""
        return tuple(
            ValidationFailure(
                rule_name=self.name,
                message=(
                    "Operation not allowed: cannot tighten existing column"
                    f" '{action.column_name}' to NOT NULL. Keep it nullable,"
                    " backfill any NULLs in a separate step, then set NOT NULL."
                ),
            )
            for action in plan
            if isinstance(action, SetColumnNullability) and not action.nullable
        )


class UnsupportedColumnTypeChange:
    """
    Disallow changing the data type of an existing column.

    The differ emits a :class:`~delta_engine.domain.plan.ColumnTypeChange`
    action when it detects a type mismatch between desired and observed. Delta
    Lake does not support type migrations, so this rule blocks any such action
    and surfaces the drift as a clear validation failure.
    """

    name: ClassVar[str] = "UnsupportedColumnTypeChange"

    def evaluate(self, plan: ActionPlan) -> tuple[ValidationFailure, ...]:
        """Flag every ColumnTypeChange action in the plan."""
        return tuple(
            ValidationFailure(
                rule_name=self.name,
                message=(
                    "Operation not allowed: cannot change the type of existing"
                    f" column '{action.column_name}' from {action.from_type} to"
                    f" {action.to_type}. Type migrations are not supported;"
                    " recreate the table to change a column's type."
                ),
            )
            for action in plan
            if isinstance(action, ColumnTypeChange)
        )


class DisallowPartitioningChange:
    """
    Disallow any plan that attempts to change partitioning.

    The differ emits a :class:`~delta_engine.domain.plan.PartitioningChange`
    action when desired and observed partition specs differ. Partitioning can
    only be set during table creation, so this rule blocks any such action.
    """

    name: ClassVar[str] = "DisallowPartitioningChange"

    def evaluate(self, plan: ActionPlan) -> tuple[ValidationFailure, ...]:
        """Flag the plan if it contains a PartitioningChange action."""
        return tuple(
            ValidationFailure(
                rule_name=self.name,
                message=(
                    "Operation not allowed: partitioning changes are not supported."
                    f" Current partition columns: {action.observed_partitioning}"
                    f" - Requested partition columns: {action.desired_partitioning}."
                    " Recreate the table with the desired partitioning."
                ),
            )
            for action in plan
            if isinstance(action, PartitioningChange)
        )


def _aspect_label(aspect: TableAspect) -> str:
    """Human-readable label for an aspect (e.g. ``column comments``)."""
    return aspect.name.lower().replace("_", " ")


class MissingTargetTable:
    """
    Disallow a sync against a table that is absent and cannot be created.

    The differ emits :class:`~delta_engine.domain.plan.TargetTableMissing` when
    the table does not exist but the desired table does not manage column
    structure — there is no schema to create it from, and metadata has nothing
    to attach to.
    """

    name: ClassVar[str] = "MissingTargetTable"

    def evaluate(self, plan: ActionPlan) -> tuple[ValidationFailure, ...]:
        """Flag the plan if it describes an uncreatable missing table."""
        return tuple(
            ValidationFailure(
                rule_name=self.name,
                message=(
                    "Operation not allowed: the table does not exist and this"
                    " definition does not manage column structure, so it cannot"
                    " be created. Create the table first, or manage it fully."
                ),
            )
            for action in plan
            if isinstance(action, TargetTableMissing)
        )


class MissingTargetColumn:
    """
    Disallow managed metadata that targets a column absent from the live table.

    The differ emits :class:`~delta_engine.domain.plan.TargetColumnMissing` per
    declared column that is absent live while carrying managed metadata. With
    column structure unmanaged the column will never be added, so the metadata
    can never land — failing at plan time beats failing mid-plan at execution.
    """

    name: ClassVar[str] = "MissingTargetColumn"

    def evaluate(self, plan: ActionPlan) -> tuple[ValidationFailure, ...]:
        """Flag every metadata target column that is missing from the live table."""
        return tuple(
            ValidationFailure(
                rule_name=self.name,
                message=(
                    f"Operation not allowed: column '{action.column_name}' is"
                    " missing from the live table but is targeted by managed"
                    f" metadata ({', '.join(_aspect_label(r) for r in action.reasons)})."
                    " Add the column out-of-band, or remove its metadata from"
                    " the declaration."
                ),
            )
            for action in plan
            if isinstance(action, TargetColumnMissing)
        )


class UnenforceablePrimaryKeyChange:
    """
    Disallow setting a primary key over columns that are nullable live.

    The differ emits :class:`~delta_engine.domain.plan.UnenforceablePrimaryKey`
    when a SetPrimaryKey is planned while nullability is unmanaged and the live
    key columns are nullable: no tightening precedes the constraint, so the ADD
    CONSTRAINT would fail at execution time.
    """

    name: ClassVar[str] = "UnenforceablePrimaryKeyChange"

    def evaluate(self, plan: ActionPlan) -> tuple[ValidationFailure, ...]:
        """Flag every planned primary key whose live columns are nullable."""
        return tuple(
            ValidationFailure(
                rule_name=self.name,
                message=(
                    "Operation not allowed: cannot set a primary key over"
                    " nullable live column(s):"
                    f" {', '.join(action.nullable_columns)}. Tighten"
                    " nullability out-of-band, then re-sync."
                ),
            )
            for action in plan
            if isinstance(action, UnenforceablePrimaryKey)
        )


DEFAULT_RULES: tuple[Rule, ...] = (
    NonNullableColumnAdd(),
    NullabilityTighteningOnExistingColumn(),
    UnsupportedColumnTypeChange(),
    DisallowPartitioningChange(),
    MissingTargetTable(),
    MissingTargetColumn(),
    UnenforceablePrimaryKeyChange(),
)


def validate_plan(
    plan: ActionPlan,
    rules: tuple[Rule, ...] = DEFAULT_RULES,
) -> ValidationResult:
    """
    Evaluate every rule against a planned change and return the verdict.

    A pure phase alongside :func:`~delta_engine.domain.plan.differ.compute_plan`:
    the same inputs always yield the same result. The caller reads
    ``ValidationResult.failed`` to gate execution; it does not assemble the verdict.

    Creation plans pass all rules automatically because none of the blocked
    action types appear in them.

    Args:
        plan: The action plan to reach the desired state.
        rules: The rules to apply, in evaluation order. Defaults to the full
            production set; override only to scope a check (e.g. in tests).

    Returns:
        A :class:`ValidationResult` carrying a failure from each broken rule.

    """
    failures = tuple(failure for rule in rules for failure in rule.evaluate(plan))
    return ValidationResult(failures=failures)
