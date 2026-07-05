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
from delta_engine.domain.model.table_aspect import TableAspect
from delta_engine.domain.plan.diff import (
    ColumnAdded,
    ColumnDataTypeChanged,
    ColumnNullabilityChanged,
    ColumnRemoved,
    PartitioningChanged,
    PropertySet,
    PropertyUndeclared,
    PropertyUnset,
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
    """
    Interface for drift validation rules.

    A rule judges whether a managed change is safe, given the declaration it
    belongs to. It receives the whole ``TableDrift`` — the self-contained
    diff — and reads what it needs: ``drift.managed_changes`` for the changes
    to judge (unmanaged drift is a scope violation the validator reports
    separately, never rule input), and ``drift.desired`` for declaration
    context such as declared properties. Never called for a ``TableMissing``
    diff.
    """

    name: ClassVar[str]

    def evaluate(self, drift: TableDrift) -> tuple[ValidationFailure, ...]:
        """Return one failure per unsafe managed change, or an empty tuple."""
        ...


class NonNullableColumnAdd:
    """Disallow adding non-nullable columns to existing tables."""

    name: ClassVar[str] = "NonNullableColumnAdd"

    def evaluate(self, drift: TableDrift) -> tuple[ValidationFailure, ...]:
        """Flag every NOT NULL column addition to an existing table."""
        return tuple(
            ValidationFailure(
                rule_name=self.name,
                message=(
                    f"Operation not allowed: cannot add non-nullable column '{change.column.name}'"
                ),
            )
            for change in drift.managed_changes
            if isinstance(change, ColumnAdded) and not change.column.nullable
        )


class NullabilityTighteningOnExistingColumn:
    """Disallow tightening an existing column to NOT NULL."""

    name: ClassVar[str] = "NullabilityTighteningOnExistingColumn"

    def evaluate(self, drift: TableDrift) -> tuple[ValidationFailure, ...]:
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
            for change in drift.managed_changes
            if isinstance(change, ColumnNullabilityChanged) and not change.desired_nullable
        )


class ColumnDataTypeChangeNotSupported:
    """Disallow in-place column type changes."""

    name: ClassVar[str] = "ColumnDataTypeChangeNotSupported"

    def evaluate(self, drift: TableDrift) -> tuple[ValidationFailure, ...]:
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
            for change in drift.managed_changes
            if isinstance(change, ColumnDataTypeChanged)
        )


class PartitioningChangeNotSupported:
    """Disallow in-place partitioning changes."""

    name: ClassVar[str] = "PartitioningChangeNotSupported"

    def evaluate(self, drift: TableDrift) -> tuple[ValidationFailure, ...]:
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
            for change in drift.managed_changes
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

    def evaluate(self, drift: TableDrift) -> tuple[ValidationFailure, ...]:
        """Flag every restricted-key transition that is not permitted."""
        failures: list[ValidationFailure] = []
        for change in drift.managed_changes:
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

    def evaluate(self, drift: TableDrift) -> tuple[ValidationFailure, ...]:
        """Flag every registered key set on the table but absent from the declaration."""
        return tuple(
            ValidationFailure(rule_name=self.name, message=self._message(change))
            for change in drift.managed_changes
            if isinstance(change, PropertyUndeclared)
        )

    def _message(self, change: PropertyUndeclared) -> str:
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


class ColumnMappingRequiredForDrop:
    """
    Disallow dropping a column without column mapping declared.

    Delta only permits DROP COLUMN when ``delta.columnMapping.mode`` is
    ``name``. This judges a managed change (a ``ColumnRemoved``) against the
    declaration — the safe case is when declaration and catalog already agree
    on the mode, in which case no property change exists to inspect, so the
    rule reads the declaration directly. Exact declaration guarantees a
    validated declaration states the mode whenever the catalog has it, so the
    declaration alone is sufficient; declaring the mode in the same sync as
    the drop is safe (SET_PROPERTY phases before DROP_COLUMN).
    """

    name: ClassVar[str] = "ColumnMappingRequiredForDrop"

    def evaluate(self, drift: TableDrift) -> tuple[ValidationFailure, ...]:
        """Flag a column drop when the declaration lacks column mapping."""
        drops_a_column = any(
            isinstance(change, ColumnRemoved) for change in drift.managed_changes
        )
        if not drops_a_column:
            return ()
        if drift.desired.properties.get(COLUMN_MAPPING_MODE_KEY) == "name":
            return ()
        return (
            ValidationFailure(
                rule_name=self.name,
                message=(
                    "Operation not allowed: dropping a column requires"
                    f" {COLUMN_MAPPING_MODE_KEY}='name'. Declare"
                    f" properties={{'{COLUMN_MAPPING_MODE_KEY}': 'name'}} on this table."
                ),
            ),
        )


DEFAULT_RULES: tuple[Rule, ...] = (
    NonNullableColumnAdd(),
    NullabilityTighteningOnExistingColumn(),
    ColumnDataTypeChangeNotSupported(),
    PartitioningChangeNotSupported(),
    PropertyTransitionNotSupported(DELTA_PROPERTY_REGISTRY),
    PropertyMustBeDeclared(DELTA_PROPERTY_REGISTRY),
    ColumnMappingRequiredForDrop(),
)


class UnmanagedAspectDrift:
    """
    Fail once per unmanaged aspect that has drifted.

    A scope invariant with the ``Rule`` interface, but not a member of
    ``DEFAULT_RULES``: it defines what a declaration is allowed to govern
    and runs unconditionally in ``validate_diff``. It must not be
    suppressible — ``TableDrift.plan()`` iterates *all* changes, and the
    guarantee that a passing validation implies only managed actions holds
    only because this check always runs; ``rules=()`` letting unmanaged
    drift through would make ``plan()`` emit changes the declaration never
    asked for.

    Unlike the safety rules it reads ``drift.changes`` (unfiltered): its
    subject is exactly the changes ``managed_changes`` excludes.
    dict.fromkeys deduplicates the aspects while preserving first-seen
    order, so failure order follows change order deterministically.
    """

    name: ClassVar[str] = "UnmanagedAspectDrift"

    def evaluate(self, drift: TableDrift) -> tuple[ValidationFailure, ...]:
        """Flag every drifted aspect the declaration does not manage."""
        managed_aspects = drift.desired.managed_aspects
        unmanaged_aspects = dict.fromkeys(
            change.aspect for change in drift.changes if change.aspect not in managed_aspects
        )
        return tuple(
            ValidationFailure(
                rule_name=self.name,
                message=(
                    f"Operation not allowed: {aspect.label} has drifted"
                    " but is not managed by this definition. Sync the table fully"
                    " or update the declaration to match the live schema."
                ),
            )
            for aspect in unmanaged_aspects
        )


_SCOPE_INVARIANTS: tuple[Rule, ...] = (UnmanagedAspectDrift(),)


def validate_diff(diff: TableDiff, rules: tuple[Rule, ...] = DEFAULT_RULES) -> ValidationResult:
    """
    Evaluate a table diff and return the verdict.

    Two scope invariants are checked unconditionally — they define what a
    declaration is allowed to govern, and cannot be suppressed via ``rules``:

    - A missing table fails with ``MissingTableUnmanaged`` when the
      declaration does not manage column structure (it cannot be created).
    - Drift in an unmanaged aspect fails with ``UnmanagedAspectDrift``, once
      per drifted aspect. It shares the ``Rule`` interface but runs from its
      own always-on tier, prepended to whatever ``rules`` are supplied.

    Rules are safety policy over the drift the declaration *does* manage:
    each reads ``drift.managed_changes``, so unmanaged drift produces exactly
    one scope failure rather than also tripping safety rules for changes the
    user never requested.
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
                    for rule in (*_SCOPE_INVARIANTS, *rules)
                    for failure in rule.evaluate(drift)
                )
            )
        case _ as unreachable:
            assert_never(unreachable)
