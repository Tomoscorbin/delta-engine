"""Validation rules judging the diff between desired and observed table state."""

from __future__ import annotations

from dataclasses import dataclass
from typing import ClassVar, Protocol, assert_never

from delta_engine.application.failures import ValidationFailure
from delta_engine.application.properties import (
    COLUMN_MAPPING_MODE_KEY,
    DELTA_PROPERTY_REGISTRY,
    PropertyRegistry,
)
from delta_engine.domain.model.table import DesiredTable
from delta_engine.domain.model.table_aspect import TableAspect
from delta_engine.domain.plan.diff import (
    Change,
    ColumnAdded,
    ColumnDataTypeChanged,
    ColumnNullabilityChanged,
    ColumnRemoved,
    PartitioningChanged,
    PropertySet,
    PropertyUnset,
    TableDiff,
    TableDrift,
    TableMissing,
    UndeclaredProperty,
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


@dataclass(frozen=True, slots=True)
class PropertyTransitionNotSupported:
    """
    Disallow property transitions the catalog will reject.

    A removal is a transition to absence: a ``PropertyUnset`` is judged as
    ``(observed_value, None)`` against the same permitted set as value
    changes, so a key whose registry entry permits no ``(value, None)``
    pair cannot be declared absent.
    """

    property_registry: PropertyRegistry

    name: ClassVar[str] = "PropertyTransitionNotSupported"

    def evaluate(self, changes: tuple[Change, ...]) -> tuple[ValidationFailure, ...]:
        """Flag every restricted-key transition that is not permitted."""
        failures: list[ValidationFailure] = []
        for change in changes:
            match change:
                case PropertySet(
                    name=name, desired_value=desired_value, observed_value=str() as observed_value
                ) if self._is_blocked(name, observed_value, desired_value):
                    failures.append(
                        ValidationFailure(
                            rule_name=self.name,
                            message=(
                                f"Operation not allowed: {name} cannot change"
                                f" from '{observed_value}' to '{desired_value}'."
                                " Update the declaration to match the catalog value."
                            ),
                        )
                    )
                case PropertyUnset(name=name, observed_value=observed_value) if self._is_blocked(
                    name, observed_value, None
                ):
                    failures.append(
                        ValidationFailure(
                            rule_name=self.name,
                            message=(
                                f"Operation not allowed: {name} cannot be removed —"
                                " the change is permanent on the table. Declare its"
                                " current value."
                            ),
                        )
                    )
                case _:
                    pass
        return tuple(failures)

    def _is_blocked(self, name: str, observed_value: str, desired_value: str | None) -> bool:
        definition = self.property_registry.get(name)
        if definition is None or not definition.permitted_transitions:
            return False
        return (observed_value, desired_value) not in definition.permitted_transitions


@dataclass(frozen=True, slots=True)
class PropertyMustBeDeclared:
    """Disallow leaving a registered catalog property undeclared."""

    property_registry: PropertyRegistry

    name: ClassVar[str] = "PropertyMustBeDeclared"

    def evaluate(self, changes: tuple[Change, ...]) -> tuple[ValidationFailure, ...]:
        """Flag every registered key set on the table but absent from the declaration."""
        return tuple(
            ValidationFailure(rule_name=self.name, message=self._message(change))
            for change in changes
            if isinstance(change, UndeclaredProperty)
        )

    def _message(self, change: UndeclaredProperty) -> str:
        if not self._removal_permitted(change.name, change.observed_value):
            return (
                f"Operation not allowed: {change.name} is set on the table"
                f" (value '{change.observed_value}') but not declared; it cannot"
                " be unset — declare it to continue managing this table's"
                " properties."
            )
        return (
            f"Operation not allowed: {change.name} is set on the table"
            f" (value '{change.observed_value}') but not declared. Declare it"
            " to manage it, or declare it as None to remove it."
        )

    def _removal_permitted(self, name: str, observed_value: str) -> bool:
        definition = self.property_registry.get(name)
        if definition is None or not definition.permitted_transitions:
            return True
        return (observed_value, None) in definition.permitted_transitions


DEFAULT_RULES: tuple[Rule, ...] = (
    NonNullableColumnAdd(),
    NullabilityTighteningOnExistingColumn(),
    ColumnDataTypeChangeNotSupported(),
    PartitioningChangeNotSupported(),
    PropertyTransitionNotSupported(DELTA_PROPERTY_REGISTRY),
    PropertyMustBeDeclared(DELTA_PROPERTY_REGISTRY),
)


def _validate_managed_scope(drift: TableDrift) -> tuple[ValidationFailure, ...]:
    """
    One failure per unmanaged aspect that has drifted.

    dict.fromkeys deduplicates the aspects while preserving first-seen order,
    so failure order follows change order deterministically.
    """
    managed_aspects = drift.desired.managed_aspects
    unmanaged_aspects = dict.fromkeys(
        change.aspect for change in drift.changes if change.aspect not in managed_aspects
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


def _validate_column_drop_preconditions(
    desired: DesiredTable, managed_changes: tuple[Change, ...]
) -> tuple[ValidationFailure, ...]:
    """
    Fail a plan that drops a column without column mapping declared.

    Delta only permits DROP COLUMN when ``delta.columnMapping.mode`` is
    ``name``. This is a precondition on state, not drift policy: when the
    declaration and catalog already agree on the mode, no property change
    exists in the diff, so no rule could judge it. Exact declaration
    guarantees that a validated declaration states the mode whenever the
    catalog has it, so checking the declaration alone is sufficient.
    Declaring the mode in the same sync as the drop is safe: SET_PROPERTY
    phases before DROP_COLUMN.

    Scans managed changes only: an unmanaged ColumnRemoved (metadata-only
    table with structural drift) is a scope violation, not a requested
    drop — UnmanagedAspectDrift reports it and this check stays silent.
    """
    drops_a_column = any(isinstance(change, ColumnRemoved) for change in managed_changes)
    if not drops_a_column:
        return ()
    if desired.properties.get(COLUMN_MAPPING_MODE_KEY) == "name":
        return ()
    return (
        ValidationFailure(
            rule_name="ColumnMappingRequiredForDrop",
            message=(
                "Operation not allowed: dropping a column requires"
                f" {COLUMN_MAPPING_MODE_KEY}='name'. Declare"
                f" properties={{'{COLUMN_MAPPING_MODE_KEY}': 'name'}} on this table."
            ),
        ),
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
            managed_aspects = drift.desired.managed_aspects
            managed_changes = tuple(
                change for change in drift.changes if change.aspect in managed_aspects
            )
            return ValidationResult(
                failures=(
                    *_validate_managed_scope(drift),
                    *(failure for rule in rules for failure in rule.evaluate(managed_changes)),
                    *_validate_column_drop_preconditions(drift.desired, managed_changes),
                )
            )
        case _ as unreachable:
            assert_never(unreachable)
