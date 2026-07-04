"""Validation rules judging the diff between desired and observed table state."""

from __future__ import annotations

from dataclasses import dataclass
from typing import ClassVar, Protocol, assert_never

from delta_engine.application.failures import ValidationFailure
from delta_engine.domain.model.table_aspect import TableAspect
from delta_engine.domain.plan.diff import (
    ColumnAdded,
    ColumnDataTypeChanged,
    ColumnNullabilityChanged,
    DriftFact,
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

    def evaluate(
        self, facts: tuple[DriftFact, ...], managed_aspects: frozenset[TableAspect]
    ) -> tuple[ValidationFailure, ...]:
        """
        Evaluate the rule against a drift's facts.

        Receives the facts and managed aspects from a ``TableDrift``. Never
        called for a ``TableMissing`` diff — that case is handled directly in
        ``validate_diff``.
        """
        ...


class NonNullableColumnAdd:
    """Disallow adding non-nullable columns to existing tables."""

    name: ClassVar[str] = "NonNullableColumnAdd"

    def evaluate(
        self, facts: tuple[DriftFact, ...], managed_aspects: frozenset[TableAspect]
    ) -> tuple[ValidationFailure, ...]:
        """Flag every NOT NULL column addition to an existing table."""
        return tuple(
            ValidationFailure(
                rule_name=self.name,
                message=(
                    f"Operation not allowed: cannot add non-nullable column '{fact.column.name}'"
                ),
            )
            for fact in facts
            if isinstance(fact, ColumnAdded) and not fact.column.nullable
        )


class NullabilityTighteningOnExistingColumn:
    """Disallow tightening an existing column to NOT NULL."""

    name: ClassVar[str] = "NullabilityTighteningOnExistingColumn"

    def evaluate(
        self, facts: tuple[DriftFact, ...], managed_aspects: frozenset[TableAspect]
    ) -> tuple[ValidationFailure, ...]:
        """Flag every existing column tightened to NOT NULL."""
        return tuple(
            ValidationFailure(
                rule_name=self.name,
                message=(
                    "Operation not allowed: cannot tighten existing column"
                    f" '{fact.column_name}' to NOT NULL. Keep it nullable,"
                    " backfill any NULLs in a separate step, then set NOT NULL."
                ),
            )
            for fact in facts
            if isinstance(fact, ColumnNullabilityChanged) and not fact.desired_nullable
        )


class ColumnDataTypeChangeNotSupported:
    """Disallow in-place column type changes."""

    name: ClassVar[str] = "ColumnDataTypeChangeNotSupported"

    def evaluate(
        self, facts: tuple[DriftFact, ...], managed_aspects: frozenset[TableAspect]
    ) -> tuple[ValidationFailure, ...]:
        """Flag every in-place column type change."""
        return tuple(
            ValidationFailure(
                rule_name=self.name,
                message=(
                    f"Operation not allowed: cannot change the type of existing column"
                    f" '{fact.column_name}'"
                    f" from {fact.observed_type} to {fact.desired_type}."
                    " Type migrations are not supported;"
                    " recreate the table to change a column's type."
                ),
            )
            for fact in facts
            if isinstance(fact, ColumnDataTypeChanged)
        )


class PartitioningChangeNotSupported:
    """Disallow in-place partitioning changes."""

    name: ClassVar[str] = "PartitioningChangeNotSupported"

    def evaluate(
        self, facts: tuple[DriftFact, ...], managed_aspects: frozenset[TableAspect]
    ) -> tuple[ValidationFailure, ...]:
        """Flag every in-place partitioning change."""
        return tuple(
            ValidationFailure(
                rule_name=self.name,
                message=(
                    "Operation not allowed: partitioning cannot be changed in place."
                    f" Current: {fact.observed_partitioning}."
                    f" Requested: {fact.desired_partitioning}."
                    " Recreate the table with the desired partitioning."
                ),
            )
            for fact in facts
            if isinstance(fact, PartitioningChanged)
        )


def _aspect_label(aspect: TableAspect) -> str:
    """Human-readable label for a TableAspect (e.g. COLUMN_STRUCTURE -> 'column structure')."""
    return aspect.name.lower().replace("_", " ")


class UnmanagedAspectDrift:
    """Disallow any drift in an aspect the desired table does not manage."""

    name: ClassVar[str] = "UnmanagedAspectDrift"

    def evaluate(
        self, facts: tuple[DriftFact, ...], managed_aspects: frozenset[TableAspect]
    ) -> tuple[ValidationFailure, ...]:
        """
        Fail once per unmanaged aspect that has drifted.

        dict.fromkeys deduplicates the aspects while preserving first-seen
        order, so failure order follows fact order deterministically.
        """
        unmanaged_aspects = dict.fromkeys(
            fact.aspect for fact in facts if fact.aspect not in managed_aspects
        )
        return tuple(
            ValidationFailure(
                rule_name=self.name,
                message=(
                    f"Operation not allowed: {_aspect_label(aspect)} has drifted"
                    " but is not managed by this definition. Sync the table fully"
                    " or update the declaration to match the live schema."
                ),
            )
            for aspect in unmanaged_aspects
        )


DEFAULT_RULES: tuple[Rule, ...] = (
    NonNullableColumnAdd(),
    NullabilityTighteningOnExistingColumn(),
    ColumnDataTypeChangeNotSupported(),
    PartitioningChangeNotSupported(),
    UnmanagedAspectDrift(),
)


def validate_diff(diff: TableDiff, rules: tuple[Rule, ...] = DEFAULT_RULES) -> ValidationResult:
    """
    Evaluate every rule against a table diff and return the verdict.

    The diff is self-contained: ``TableDrift`` carries its facts and managed
    aspects, and ``TableMissing`` carries the desired table. A missing table
    passes automatically when column structure is managed (creation is valid).
    When structure is unmanaged, the table cannot be created and the diff
    fails immediately — this check is unconditional and cannot be suppressed
    via ``rules``, because bypassing it would silently permit metadata-only
    creates.
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
            return ValidationResult(
                failures=tuple(
                    failure
                    for rule in rules
                    for failure in rule.evaluate(drift.facts, drift.managed_aspects)
                )
            )
        case _ as unreachable:
            assert_never(unreachable)
