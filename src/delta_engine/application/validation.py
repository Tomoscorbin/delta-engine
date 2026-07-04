"""Validation rules judging the diff between desired and observed table state."""

from __future__ import annotations

from dataclasses import dataclass
from typing import ClassVar, Protocol, assert_never

from delta_engine.application.failures import ValidationFailure
from delta_engine.domain.plan.diff import (
    ColumnAdded,
    ColumnDataTypeChanged,
    ColumnNullabilityChanged,
    ColumnsDimension,
    Dimension,
    PartitioningDimension,
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

    def evaluate(self, dimensions: tuple[Dimension, ...]) -> tuple[ValidationFailure, ...]:
        """
        Evaluate the rule against a drift's dimensions.

        Receives all dimensions from a ``TableDrift``; inspect the ones relevant
        to this rule and ignore the rest. Never called for a ``TableMissing``
        diff — creation is always safe.
        """
        ...


class NonNullableColumnAdd:
    """Disallow adding non-nullable columns to existing tables."""

    name: ClassVar[str] = "NonNullableColumnAdd"

    def evaluate(self, dimensions: tuple[Dimension, ...]) -> tuple[ValidationFailure, ...]:
        """Flag every NOT NULL column addition to an existing table."""
        cols_dim = next((d for d in dimensions if isinstance(d, ColumnsDimension)), None)
        if cols_dim is None:
            return ()
        return tuple(
            ValidationFailure(
                rule_name=self.name,
                message=(
                    f"Operation not allowed: cannot add non-nullable column '{entry.column.name}'"
                ),
            )
            for entry in cols_dim.entries
            if isinstance(entry, ColumnAdded) and not entry.column.nullable
        )


class NullabilityTighteningOnExistingColumn:
    """Disallow tightening an existing column to NOT NULL."""

    name: ClassVar[str] = "NullabilityTighteningOnExistingColumn"

    def evaluate(self, dimensions: tuple[Dimension, ...]) -> tuple[ValidationFailure, ...]:
        """Flag every existing column tightened to NOT NULL."""
        cols_dim = next((d for d in dimensions if isinstance(d, ColumnsDimension)), None)
        if cols_dim is None:
            return ()
        return tuple(
            ValidationFailure(
                rule_name=self.name,
                message=(
                    "Operation not allowed: cannot tighten existing column"
                    f" '{entry.column_name}' to NOT NULL. Keep it nullable,"
                    " backfill any NULLs in a separate step, then set NOT NULL."
                ),
            )
            for entry in cols_dim.entries
            if isinstance(entry, ColumnNullabilityChanged) and entry.change.desired is False
        )


class ColumnDataTypeChangeNotSupported:
    """Disallow in-place column type changes."""

    name: ClassVar[str] = "ColumnDataTypeChangeNotSupported"

    def evaluate(self, dimensions: tuple[Dimension, ...]) -> tuple[ValidationFailure, ...]:
        cols_dim = next((d for d in dimensions if isinstance(d, ColumnsDimension)), None)
        if cols_dim is None:
            return ()
        return tuple(
            ValidationFailure(
                rule_name=self.name,
                message=(
                    f"cannot change the type of existing column '{entry.column_name}'"
                    f" from {entry.change.observed} to {entry.change.desired}."
                    " Type migrations are not supported;"
                    " recreate the table to change a column's type."
                ),
            )
            for entry in cols_dim.entries
            if isinstance(entry, ColumnDataTypeChanged)
        )


class PartitioningChangeNotSupported:
    """Disallow in-place partitioning changes."""

    name: ClassVar[str] = "PartitioningChangeNotSupported"

    def evaluate(self, dimensions: tuple[Dimension, ...]) -> tuple[ValidationFailure, ...]:
        return tuple(
            ValidationFailure(
                rule_name=self.name,
                message=(
                    "partitioning changes are not supported."
                    f" Current partition columns: {d.change.observed}"
                    f" - Requested partition columns: {d.change.desired}."
                    " Recreate the table with the desired partitioning."
                ),
            )
            for d in dimensions
            if isinstance(d, PartitioningDimension)
        )


DEFAULT_RULES: tuple[Rule, ...] = (
    NonNullableColumnAdd(),
    NullabilityTighteningOnExistingColumn(),
    ColumnDataTypeChangeNotSupported(),
    PartitioningChangeNotSupported(),
)


def validate_diff(
    diff: TableDiff,
    rules: tuple[Rule, ...] = DEFAULT_RULES,
) -> ValidationResult:
    """
    Evaluate every rule against a table diff and return the verdict.

    A missing table passes automatically. For a drift, every rule is evaluated
    against the dimensions and failures are collected.
    """
    match diff:
        case TableMissing():
            return ValidationResult()
        case TableDrift() as drift:
            return ValidationResult(
                failures=tuple(
                    failure
                    for rule in rules
                    for failure in rule.evaluate(drift.dimensions)
                )
            )
        case _:
            assert_never(diff)
