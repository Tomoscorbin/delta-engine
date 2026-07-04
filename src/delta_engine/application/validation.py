"""Validation rules judging the diff between desired and observed table state."""

from __future__ import annotations

from dataclasses import dataclass
from typing import ClassVar, Protocol, assert_never

from delta_engine.application.failures import ValidationFailure
from delta_engine.domain.model.table import DesiredTable
from delta_engine.domain.model.table_aspect import TableAspect
from delta_engine.domain.plan.diff import (
    ColumnAdded,
    ColumnDataTypeChanged,
    ColumnNullabilityChanged,
    ColumnStructureDimension,
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

    def evaluate(
        self, dimensions: tuple[Dimension, ...], desired: DesiredTable
    ) -> tuple[ValidationFailure, ...]:
        """
        Evaluate the rule against a drift's dimensions.

        Receives all dimensions from a ``TableDrift`` and the desired table.
        Never called for a ``TableMissing`` diff — that case is handled
        directly in ``validate_diff``.
        """
        ...


class NonNullableColumnAdd:
    """Disallow adding non-nullable columns to existing tables."""

    name: ClassVar[str] = "NonNullableColumnAdd"

    def evaluate(
        self, dimensions: tuple[Dimension, ...], desired: DesiredTable
    ) -> tuple[ValidationFailure, ...]:
        """Flag every NOT NULL column addition to an existing table."""
        cols_dim = next((d for d in dimensions if isinstance(d, ColumnStructureDimension)), None)
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

    def evaluate(
        self, dimensions: tuple[Dimension, ...], desired: DesiredTable
    ) -> tuple[ValidationFailure, ...]:
        """Flag every existing column tightened to NOT NULL."""
        cols_dim = next((d for d in dimensions if isinstance(d, ColumnStructureDimension)), None)
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

    def evaluate(
        self, dimensions: tuple[Dimension, ...], desired: DesiredTable
    ) -> tuple[ValidationFailure, ...]:
        """Flag every in-place column type change."""
        cols_dim = next((d for d in dimensions if isinstance(d, ColumnStructureDimension)), None)
        if cols_dim is None:
            return ()
        return tuple(
            ValidationFailure(
                rule_name=self.name,
                message=(
                    f"Operation not allowed: cannot change the type of existing column"
                    f" '{entry.column_name}'"
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

    def evaluate(
        self, dimensions: tuple[Dimension, ...], desired: DesiredTable
    ) -> tuple[ValidationFailure, ...]:
        """Flag every in-place partitioning change."""
        return tuple(
            ValidationFailure(
                rule_name=self.name,
                message=(
                    "Operation not allowed: partitioning cannot be changed in place."
                    f" Current: {d.change.observed}."
                    f" Requested: {d.change.desired}."
                    " Recreate the table with the desired partitioning."
                ),
            )
            for d in dimensions
            if isinstance(d, PartitioningDimension)
        )


def _aspect_label(aspect: TableAspect) -> str:
    """Human-readable label for a TableAspect (e.g. COLUMN_STRUCTURE -> 'column structure')."""
    return aspect.name.lower().replace("_", " ")


class UnmanagedDimensionDrift:
    """Disallow any drift in a dimension the desired table does not manage."""

    name: ClassVar[str] = "UnmanagedDimensionDrift"

    def evaluate(
        self, dimensions: tuple[Dimension, ...], desired: DesiredTable
    ) -> tuple[ValidationFailure, ...]:
        """Fail for every dimension that drifted but is not in managed_aspects."""
        return tuple(
            ValidationFailure(
                rule_name=self.name,
                message=(
                    f"Operation not allowed: {_aspect_label(d.aspect)} has drifted"
                    " but is not managed by this definition. Sync the table fully"
                    " or update the declaration to match the live schema."
                ),
            )
            for d in dimensions
            if d.aspect not in desired.managed_aspects
        )


DEFAULT_RULES: tuple[Rule, ...] = (
    NonNullableColumnAdd(),
    NullabilityTighteningOnExistingColumn(),
    ColumnDataTypeChangeNotSupported(),
    PartitioningChangeNotSupported(),
    UnmanagedDimensionDrift(),
)


def validate_diff(
    diff: TableDiff,
    desired: DesiredTable,
    rules: tuple[Rule, ...] = DEFAULT_RULES,
) -> ValidationResult:
    """
    Evaluate every rule against a table diff and return the verdict.

    A missing table passes automatically when column structure is managed
    (creation is valid). When structure is unmanaged, the table cannot be
    created and the diff fails immediately. For a drift, every rule is
    evaluated against the dimensions and failures are collected.
    """
    match diff:
        case TableMissing():
            if TableAspect.COLUMN_STRUCTURE not in desired.managed_aspects:
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
            return ValidationResult(
                failures=tuple(
                    failure
                    for rule in rules
                    for failure in rule.evaluate(drift.dimensions, desired)
                )
            )
        case _ as unreachable:
            assert_never(unreachable)
