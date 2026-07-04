"""Validation rules judging the diff between desired and observed table state."""

from __future__ import annotations

from dataclasses import dataclass
from typing import ClassVar, Protocol, assert_never

from delta_engine.application.failures import ValidationFailure
from delta_engine.domain.plan.diff import (
    Added,
    ColumnChanged,
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

    def evaluate(self, drift: TableDrift) -> tuple[ValidationFailure, ...]:
        """
        Evaluate the rule against an existing table's drift.

        Rules judge facts: each inspects the drift dimensions it cares about
        and ignores the rest. Rules never see a missing table —
        ``validate_diff`` dispatches that variant once, so creation safety is
        not every rule's concern.

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

    The rule flags any drift that adds a NOT NULL column to a table that
    already exists (it does not attempt to infer data emptiness).
    """

    name: ClassVar[str] = "NonNullableColumnAdd"

    def evaluate(self, drift: TableDrift) -> tuple[ValidationFailure, ...]:
        """Flag every NOT NULL column addition to an existing table."""
        return tuple(
            ValidationFailure(
                rule_name=self.name,
                message=(
                    f"Operation not allowed: cannot add non-nullable column '{entry.item.name}'"
                ),
            )
            for entry in drift.columns
            if isinstance(entry, Added) and not entry.item.nullable
        )


class NullabilityTighteningOnExistingColumn:
    """
    Disallow tightening an existing column to NOT NULL.

    Setting a previously-nullable column to NOT NULL fails at execution time if
    the column already holds NULLs, and the failure surfaces only after earlier
    actions have committed. The diff cannot know whether data is present, so --
    like :class:`NonNullableColumnAdd` -- the rule conservatively blocks the
    tightening and points to the safe path. Loosening to nullable is always safe
    and is not flagged.
    """

    name: ClassVar[str] = "NullabilityTighteningOnExistingColumn"

    def evaluate(self, drift: TableDrift) -> tuple[ValidationFailure, ...]:
        """Flag every existing column tightened to NOT NULL."""
        return tuple(
            ValidationFailure(
                rule_name=self.name,
                message=(
                    "Operation not allowed: cannot tighten existing column"
                    f" '{entry.column_name}' to NOT NULL. Keep it nullable,"
                    " backfill any NULLs in a separate step, then set NOT NULL."
                ),
            )
            for entry in drift.columns
            if isinstance(entry, ColumnChanged)
            and entry.nullability is not None
            and entry.nullability.desired is False
        )


class UnsupportedColumnTypeChange:
    """
    Disallow changing the data type of an existing column.

    The diff records a type difference as a fact on the column's
    :class:`~delta_engine.domain.plan.diff.ColumnChanged` entry. Delta Lake
    does not support type migrations, so this rule blocks the drift and
    surfaces it as a clear validation failure.
    """

    name: ClassVar[str] = "UnsupportedColumnTypeChange"

    def evaluate(self, drift: TableDrift) -> tuple[ValidationFailure, ...]:
        """Flag every existing column whose data type differs."""
        failures: list[ValidationFailure] = []
        for entry in drift.columns:
            if not isinstance(entry, ColumnChanged):
                continue
            data_type = entry.data_type
            if data_type is None:
                continue
            failures.append(
                ValidationFailure(
                    rule_name=self.name,
                    message=(
                        "Operation not allowed: cannot change the type of existing"
                        f" column '{entry.column_name}' from {data_type.observed} to"
                        f" {data_type.desired}. Type migrations are not supported;"
                        " recreate the table to change a column's type."
                    ),
                )
            )
        return tuple(failures)


class DisallowPartitioningChange:
    """
    Disallow any drift in partitioning.

    The diff records a partition-spec difference as a fact. Partitioning can
    only be set during table creation, so this rule blocks any such drift.
    """

    name: ClassVar[str] = "DisallowPartitioningChange"

    def evaluate(self, drift: TableDrift) -> tuple[ValidationFailure, ...]:
        """Flag the drift if the partition specs differ."""
        if drift.partitioning is None:
            return ()
        return (
            ValidationFailure(
                rule_name=self.name,
                message=(
                    "Operation not allowed: partitioning changes are not supported."
                    f" Current partition columns: {drift.partitioning.observed}"
                    f" - Requested partition columns: {drift.partitioning.desired}."
                    " Recreate the table with the desired partitioning."
                ),
            ),
        )


DEFAULT_RULES: tuple[Rule, ...] = (
    NonNullableColumnAdd(),
    NullabilityTighteningOnExistingColumn(),
    UnsupportedColumnTypeChange(),
    DisallowPartitioningChange(),
)


def validate_diff(
    diff: TableDiff,
    rules: tuple[Rule, ...] = DEFAULT_RULES,
) -> ValidationResult:
    """
    Evaluate every rule against a table diff and return the verdict.

    A pure phase alongside :func:`~delta_engine.domain.plan.diff.diff_table`:
    the same inputs always yield the same result. The caller reads
    ``ValidationResult.failed`` to gate execution; it does not assemble the
    verdict.

    A missing table passes automatically — creating a table from its full
    declaration is always safe, so the ``TableMissing`` variant is dispatched
    here once and no rule ever sees it.

    Args:
        diff: The diff between desired and observed state.
        rules: The rules to apply, in evaluation order. Defaults to the full
            production set; override only to scope a check (e.g. in tests).

    Returns:
        A :class:`ValidationResult` carrying a failure from each broken rule.

    """
    match diff:
        case TableMissing():
            return ValidationResult()
        case TableDrift() as drift:
            failures = tuple(
                failure for rule in rules for failure in rule.evaluate(drift)
            )
            return ValidationResult(failures=failures)
        case _:
            assert_never(diff)
