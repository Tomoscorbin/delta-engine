"""Validation rules judging the diff between desired and observed table state."""

from __future__ import annotations

from dataclasses import dataclass
from typing import ClassVar, Protocol, assert_never

from delta_engine.application.failures import ValidationFailure
from delta_engine.domain.model.table_aspect import TableAspect
from delta_engine.domain.plan.diff import (
    Change,
    ColumnAdded,
    ColumnDataTypeChanged,
    ColumnNullabilityChanged,
    PartitioningChanged,
    TableDiff,
    TableDrift,
    TableMissing,
)


@dataclass(frozen=True, slots=True)
class ValidationResult:
    """Outcome of diff validation."""

    failures: tuple[ValidationFailure, ...] = ()

    @property
    def failed(self) -> bool:
        """True when any validation failures are present."""
        return bool(self.failures)


class Rule(Protocol):
    """Interface for drift validation rules."""

    name: ClassVar[str]

    def evaluate(self, changes: tuple[Change, ...]) -> tuple[ValidationFailure, ...]:
        """
        Evaluate the rule against a drift's managed changes.

        Receives only changes whose aspect the declaration manages — unmanaged
        drift is rejected by ``validate_diff`` itself before rules run, so a
        rule never judges a change the user did not ask for. Never called for
        a ``TableMissing`` diff.
        """
        ...


class NonNullableColumnAdd:
    """Disallow adding non-nullable columns to existing tables."""

    name: ClassVar[str] = "NonNullableColumnAdd"

    def evaluate(self, changes: tuple[Change, ...]) -> tuple[ValidationFailure, ...]:
        """Flag every NOT NULL column addition to an existing table."""
        return tuple(
            ValidationFailure(
                rule_name=self.name,
                message=(
                    f"Operation not allowed: cannot add non-nullable column '{change.column.name}'"
                ),
            )
            for change in changes
            if isinstance(change, ColumnAdded) and not change.column.nullable
        )


class NullabilityTighteningOnExistingColumn:
    """Disallow tightening an existing column to NOT NULL."""

    name: ClassVar[str] = "NullabilityTighteningOnExistingColumn"

    def evaluate(self, changes: tuple[Change, ...]) -> tuple[ValidationFailure, ...]:
        """Flag every existing column tightened to NOT NULL."""
        return tuple(
            ValidationFailure(
                rule_name=self.name,
                message=(
                    "Operation not allowed: cannot tighten existing column"
                    f" '{change.column_name}' to NOT NULL. Keep it nullable,"
                    " backfill any NULLs in a separate step, then set NOT NULL."
                ),
            )
            for change in changes
            if isinstance(change, ColumnNullabilityChanged) and not change.desired_nullable
        )


class ColumnDataTypeChangeNotSupported:
    """Disallow in-place column type changes."""

    name: ClassVar[str] = "ColumnDataTypeChangeNotSupported"

    def evaluate(self, changes: tuple[Change, ...]) -> tuple[ValidationFailure, ...]:
        """Flag every in-place column type change."""
        return tuple(
            ValidationFailure(
                rule_name=self.name,
                message=(
                    f"Operation not allowed: cannot change the type of existing column"
                    f" '{change.column_name}'"
                    f" from {change.observed_type} to {change.desired_type}."
                    " Type migrations are not supported;"
                    " recreate the table to change a column's type."
                ),
            )
            for change in changes
            if isinstance(change, ColumnDataTypeChanged)
        )


class PartitioningChangeNotSupported:
    """Disallow in-place partitioning changes."""

    name: ClassVar[str] = "PartitioningChangeNotSupported"

    def evaluate(self, changes: tuple[Change, ...]) -> tuple[ValidationFailure, ...]:
        """Flag every in-place partitioning change."""
        return tuple(
            ValidationFailure(
                rule_name=self.name,
                message=(
                    "Operation not allowed: partitioning cannot be changed in place."
                    f" Current: {change.observed_partitioning}."
                    f" Requested: {change.desired_partitioning}."
                    " Recreate the table with the desired partitioning."
                ),
            )
            for change in changes
            if isinstance(change, PartitioningChanged)
        )


DEFAULT_RULES: tuple[Rule, ...] = (
    NonNullableColumnAdd(),
    NullabilityTighteningOnExistingColumn(),
    ColumnDataTypeChangeNotSupported(),
    PartitioningChangeNotSupported(),
)


def _validate_managed_scope(drift: TableDrift) -> tuple[ValidationFailure, ...]:
    """
    One failure per unmanaged aspect that has drifted.

    dict.fromkeys deduplicates the aspects while preserving first-seen order,
    so failure order follows change order deterministically.
    """
    unmanaged_aspects = dict.fromkeys(
        change.aspect for change in drift.changes if change.aspect not in drift.managed_aspects
    )
    return tuple(
        ValidationFailure(
            rule_name="UnmanagedAspectDrift",
            message=(
                f"Operation not allowed: {aspect.label} has drifted"
                " but is not managed by this definition. Sync the table fully"
                " or update the declaration to match the live schema."
            ),
        )
        for aspect in unmanaged_aspects
    )


def validate_diff(diff: TableDiff, rules: tuple[Rule, ...] = DEFAULT_RULES) -> ValidationResult:
    """
    Evaluate a table diff and return the verdict.

    Two scope invariants are checked unconditionally — they define what a
    declaration is allowed to govern, and cannot be suppressed via ``rules``:

    - A missing table fails with ``MissingTableUnmanaged`` when the
      declaration does not manage column structure (it cannot be created).
    - Drift in an unmanaged aspect fails with ``UnmanagedAspectDrift``, once
      per drifted aspect.

    Rules are safety policy over the drift the declaration *does* manage:
    they receive only changes in managed aspects, so unmanaged drift produces
    exactly one scope failure rather than also tripping safety rules for
    changes the user never requested.
    """
    match diff:
        case TableMissing() as missing:
            if TableAspect.COLUMN_STRUCTURE not in missing.desired.managed_aspects:
                return ValidationResult(
                    failures=(
                        ValidationFailure(
                            rule_name="MissingTableUnmanaged",
                            message=(
                                "Operation not allowed: the table does not exist and this"
                                " definition does not manage column structure, so it cannot"
                                " be created. Manage the table fully or create it out-of-band"
                                " first."
                            ),
                        ),
                    )
                )
            return ValidationResult()
        case TableDrift() as drift:
            managed_changes = tuple(
                change for change in drift.changes if change.aspect in drift.managed_aspects
            )
            return ValidationResult(
                failures=(
                    *_validate_managed_scope(drift),
                    *(
                        failure
                        for rule in rules
                        for failure in rule.evaluate(managed_changes)
                    ),
                )
            )
        case _ as unreachable:
            assert_never(unreachable)
