"""Validation rules judging the diff between desired and observed table state."""

from collections.abc import Mapping
from dataclasses import dataclass
from types import MappingProxyType
from typing import ClassVar, Final, Protocol, assert_never

from delta_engine.application.failures import ValidationFailure
from delta_engine.application.properties import (
    DELTA_PROPERTY_REGISTRY,
    Property,
    PropertyRegistry,
)
from delta_engine.domain.model import (
    Byte,
    DataType,
    Date,
    Decimal,
    Double,
    Float,
    Integer,
    Long,
    Short,
    TableAspect,
    TimestampNtz,
)
from delta_engine.domain.plan import (
    ColumnAdded,
    ColumnDataTypeChanged,
    ColumnNullabilityChanged,
    ColumnRemoved,
    ForeignKeyRemoved,
    PartitioningChanged,
    PrimaryKeyChanged,
    PrimaryKeyRemoved,
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
                    " backfill any NULLs, set NOT NULL outside the engine"
                    " (ALTER TABLE ... SET NOT NULL), then declare"
                    " nullable=False — the next sync sees no drift."
                ),
            )
            for change in drift.managed_changes
            if isinstance(change, ColumnNullabilityChanged) and not change.desired_nullable
        )


# The widenings Delta can apply in place (observed -> desired), per the
# Databricks type-widening matrix. Decimal targets are handled separately —
# whether they fit depends on precision and scale, not the type alone.
# Composite types are deliberately absent: this engine models arrays, maps,
# and structs atomically, so they are never widened as a whole and any change
# to them stays blocked (Delta itself can widen nested fields).
_WIDENING_TARGETS: Final[Mapping[type[DataType], frozenset[type[DataType]]]] = MappingProxyType(
    {
        Byte: frozenset({Short, Integer, Long, Double}),
        Short: frozenset({Integer, Long, Double}),
        Integer: frozenset({Long, Double}),
        Float: frozenset({Double}),
        Date: frozenset({TimestampNtz}),
    }
)

# Widening an integer column to Decimal needs room for every value the source
# type can hold. Databricks specifies DECIMAL(10,0) as the minimum for
# Byte/Short/Integer and DECIMAL(20,0) for Long — i.e. this many integer
# digits (precision minus scale).
_DECIMAL_INTEGER_DIGITS_REQUIRED: Final[Mapping[type[DataType], int]] = MappingProxyType(
    {
        Byte: 10,
        Short: 10,
        Integer: 10,
        Long: 20,
    }
)


def _is_safe_widening(observed: DataType, desired: DataType) -> bool:
    """Whether Delta type widening can apply this type change in place."""
    if isinstance(desired, Decimal):
        return _is_safe_widening_to_decimal(observed, desired)
    return type(desired) in _WIDENING_TARGETS.get(type(observed), frozenset())


def _is_safe_widening_to_decimal(observed: DataType, desired: Decimal) -> bool:
    """Decimal widening keeps integer digits: scale may grow only if precision grows with it."""
    desired_integer_digits = desired.precision - desired.scale
    if isinstance(observed, Decimal):
        return (
            desired.scale >= observed.scale
            and desired_integer_digits >= observed.precision - observed.scale
        )
    required_integer_digits = _DECIMAL_INTEGER_DIGITS_REQUIRED.get(type(observed))
    if required_integer_digits is None:
        return False
    return desired_integer_digits >= required_integer_digits


class NonWideningColumnTypeChange:
    """Disallow in-place column type changes outside the type-widening matrix."""

    name: ClassVar[str] = "NonWideningColumnTypeChange"

    def evaluate(self, drift: TableDrift) -> tuple[ValidationFailure, ...]:
        """Flag every type change that widening cannot apply in place."""
        return tuple(
            ValidationFailure(
                rule_name=self.name,
                message=(
                    f"Operation not allowed: cannot change the type of existing column"
                    f" '{change.column_name}'"
                    f" from {change.observed_type} to {change.desired_type}."
                    " Only Delta type widenings can be applied in place"
                    " (integer widenings, integer types to Double or a sufficiently"
                    " wide Decimal, Float to Double, Decimal digit growth, Date to"
                    " TimestampNtz); recreate the table to make any other type change."
                ),
            )
            for change in drift.managed_changes
            if isinstance(change, ColumnDataTypeChanged)
            and not _is_safe_widening(change.observed_type, change.desired_type)
        )


class TypeWideningRequiredForTypeChange:
    """
    Disallow a widening type change without type widening declared.

    Mirrors ColumnMappingRequiredForDrop: exact declaration guarantees a
    validated declaration states the property whenever the catalog has it, so
    the declaration alone is sufficient input; declaring it 'true' in the same
    sync as the widen is safe (SET_PROPERTY phases before ALTER_COLUMN_TYPE).
    """

    name: ClassVar[str] = "TypeWideningRequiredForTypeChange"

    def evaluate(self, drift: TableDrift) -> tuple[ValidationFailure, ...]:
        """Flag every widening type change when the declaration lacks the property."""
        if drift.desired.properties.get(Property.TYPE_WIDENING) == "true":
            return ()
        return tuple(
            ValidationFailure(
                rule_name=self.name,
                message=(
                    f"Operation not allowed: widening column '{change.column_name}'"
                    f" from {change.observed_type} to {change.desired_type} requires"
                    f" {Property.TYPE_WIDENING}='true'. Declare"
                    f" properties={{'{Property.TYPE_WIDENING}': 'true'}} on this table."
                ),
            )
            for change in drift.managed_changes
            if isinstance(change, ColumnDataTypeChanged)
            and _is_safe_widening(change.observed_type, change.desired_type)
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
        if definition is None:
            return False
        return not definition.permits_transition(observed_value, desired_value)


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
        return definition is None or definition.permits_transition(observed_value, None)


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
        drops_a_column = any(isinstance(change, ColumnRemoved) for change in drift.managed_changes)
        if not drops_a_column:
            return ()
        if drift.desired.properties.get(Property.COLUMN_MAPPING_MODE) == "name":
            return ()
        return (
            ValidationFailure(
                rule_name=self.name,
                message=(
                    "Operation not allowed: dropping a column requires"
                    f" {Property.COLUMN_MAPPING_MODE}='name'. Declare"
                    f" properties={{'{Property.COLUMN_MAPPING_MODE}': 'name'}} on this table."
                ),
            ),
        )


class PrimaryKeyReferencedByForeignKeys:
    """
    Disallow dropping or changing a primary key while foreign keys reference it.

    DROP PRIMARY KEY is RESTRICT by default: it fails while any FK references
    the key. The referencing constraints ride on the primary-key change (an
    observed fact), so this rule needs no second input. A referencing FK on
    *this* table that this same sync also drops is exempt — DROP_FOREIGN_KEY
    phases before DROP_PRIMARY_KEY, so that plan executes cleanly. FKs on
    other tables cannot be exempted per-table: even when the other table's
    declaration drops the FK in the same sync run, tables execute
    parent-first, so the PK drop would still hit the live FK.

    information_schema is per-catalog, so references from other catalogs are
    not observed; those still fail at execution.
    """

    name: ClassVar[str] = "PrimaryKeyReferencedByForeignKeys"

    def evaluate(self, drift: TableDrift) -> tuple[ValidationFailure, ...]:
        """Flag every PK drop/change still referenced by a surviving foreign key."""
        dropped_here = {
            change.constraint.constraint_name
            for change in drift.managed_changes
            if isinstance(change, ForeignKeyRemoved)
        }
        failures: list[ValidationFailure] = []
        for change in drift.managed_changes:
            if not isinstance(change, PrimaryKeyRemoved | PrimaryKeyChanged):
                continue
            blockers = tuple(
                reference
                for reference in change.referencing_foreign_keys
                if not (
                    reference.referencing_table == drift.desired.qualified_name
                    and reference.constraint_name in dropped_here
                )
            )
            if blockers:
                referenced_by = ", ".join(
                    f"{ref.constraint_name} on {ref.referencing_table}" for ref in blockers
                )
                failures.append(
                    ValidationFailure(
                        rule_name=self.name,
                        message=(
                            "Operation not allowed: the primary key cannot be"
                            f" dropped or changed while foreign keys reference it:"
                            f" {referenced_by}. Sync the referencing tables without"
                            " those foreign keys first, then change the key."
                        ),
                    )
                )
        return tuple(failures)


DEFAULT_RULES: Final[tuple[Rule, ...]] = (
    NonNullableColumnAdd(),
    NullabilityTighteningOnExistingColumn(),
    NonWideningColumnTypeChange(),
    TypeWideningRequiredForTypeChange(),
    PartitioningChangeNotSupported(),
    PropertyTransitionNotSupported(DELTA_PROPERTY_REGISTRY),
    PropertyMustBeDeclared(DELTA_PROPERTY_REGISTRY),
    ColumnMappingRequiredForDrop(),
    PrimaryKeyReferencedByForeignKeys(),
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


class MissingTableUnmanaged:
    """
    Fail creation of a table whose declaration does not manage column structure.

    The scope invariant for the ``TableMissing`` arm, peer to
    ``UnmanagedAspectDrift`` on the drift arm. It shares the naming shape of
    a rule (its ``name`` supplies the failure's ``rule_name``) but not the
    ``Rule`` protocol — its subject is a missing table, which has no changes
    to evaluate. Like the drift-arm invariant it runs unconditionally:
    ``rules=()`` must not enable metadata-only creates.
    """

    name: ClassVar[str] = "MissingTableUnmanaged"

    def evaluate(self, missing: TableMissing) -> tuple[ValidationFailure, ...]:
        """Flag a missing table that this declaration cannot create."""
        if TableAspect.COLUMN_STRUCTURE in missing.desired.managed_aspects:
            return ()
        return (
            ValidationFailure(
                rule_name=self.name,
                message=(
                    "Operation not allowed: the table does not exist and this"
                    " definition does not manage column structure, so it cannot"
                    " be created. Manage the table fully or create it out-of-band"
                    " first."
                ),
            ),
        )


_SCOPE_INVARIANTS: Final[tuple[Rule, ...]] = (UnmanagedAspectDrift(),)
_MISSING_TABLE_UNMANAGED: Final = MissingTableUnmanaged()


def validate_diff(diff: TableDiff, rules: tuple[Rule, ...] = DEFAULT_RULES) -> ValidationResult:
    """
    Evaluate a table diff and return the verdict.

    Two scope invariants are checked unconditionally — they define what a
    declaration is allowed to govern, and cannot be suppressed via ``rules``:

    - ``MissingTableUnmanaged`` judges the missing arm: a table that does
      not exist cannot be created by a declaration that does not manage
      column structure.
    - ``UnmanagedAspectDrift`` judges the drift arm: one failure per
      drifted unmanaged aspect. It shares the ``Rule`` interface but runs
      from its own always-on tier, prepended to whatever ``rules`` are
      supplied.

    Rules are safety policy over the drift the declaration *does* manage:
    each reads ``drift.managed_changes``, so unmanaged drift produces exactly
    one scope failure rather than also tripping safety rules for changes the
    user never requested.
    """
    match diff:
        case TableMissing() as missing:
            return ValidationResult(failures=_MISSING_TABLE_UNMANAGED.evaluate(missing))
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
