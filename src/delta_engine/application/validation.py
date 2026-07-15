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
    AddColumn,
    AlterColumnType,
    ColumnRenameConflict,
    DropColumn,
    DropForeignKey,
    DropPrimaryKey,
    PartitioningChanged,
    PropertyUndeclared,
    SetColumnNullability,
    SetProperty,
    TableDiff,
    TableDrift,
    TableMissing,
    UnsetProperty,
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

    A rule judges whether a change is safe, given the declaration it belongs
    to. It receives the whole ``TableDrift`` and reads ``drift.actions`` or
    ``drift.findings`` for the differences to judge, and ``drift.desired``
    for declaration context such as declared properties. Because the scope
    gate runs first and short-circuits, a rule is only ever evaluated on a
    fully in-scope diff, so its actions and findings are exactly the ones the
    declaration manages — it does no scope filtering of its own. Never called
    for a ``TableMissing`` diff.
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
            for change in drift.actions
            if isinstance(change, AddColumn) and not change.column.nullable
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
            for change in drift.actions
            if isinstance(change, SetColumnNullability) and not change.desired_nullable
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
            for change in drift.actions
            if isinstance(change, AlterColumnType)
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
            for change in drift.actions
            if isinstance(change, AlterColumnType)
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
                    f" Current: {finding.observed_partitioning}."
                    f" Requested: {finding.desired_partitioning}."
                    " Recreate the table with the desired partitioning."
                ),
            )
            for finding in drift.findings
            if isinstance(finding, PartitioningChanged)
        )


@dataclass(frozen=True, slots=True)
class PropertyTransitionNotSupported:
    """
    Disallow property transitions the catalog will reject.

    A removal is a transition to absence: an ``UnsetProperty`` is judged as
    ``(observed_value, None)`` against the same permitted set as value
    changes, so a key whose registry entry permits no ``(value, None)``
    pair cannot be declared absent.
    """

    property_registry: PropertyRegistry

    name: ClassVar[str] = "PropertyTransitionNotSupported"

    def evaluate(self, drift: TableDrift) -> tuple[ValidationFailure, ...]:
        """Flag every restricted-key transition that is not permitted."""
        failures: list[ValidationFailure] = []
        for change in drift.actions:
            match change:
                case SetProperty(
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
                case UnsetProperty(name=name, observed_value=observed_value) if self._is_blocked(
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
            ValidationFailure(rule_name=self.name, message=self._message(finding))
            for finding in drift.findings
            if isinstance(finding, PropertyUndeclared)
        )

    def _message(self, finding: PropertyUndeclared) -> str:
        if not self._removal_permitted(finding.name, finding.observed_value):
            return (
                f"Operation not allowed: {finding.name} is set on the table"
                f" (value '{finding.observed_value}') but not declared; it cannot"
                " be unset — declare it to continue managing this table's"
                " properties."
            )
        return (
            f"Operation not allowed: {finding.name} is set on the table"
            f" (value '{finding.observed_value}') but not declared. Declare it"
            " to manage it, or declare it as None to remove it."
        )

    def _removal_permitted(self, name: str, observed_value: str) -> bool:
        definition = self.property_registry.get(name)
        return definition is None or definition.permits_transition(observed_value, None)


class ColumnMappingRequiredForDrop:
    """
    Disallow dropping a column without column mapping declared.

    Delta only permits DROP COLUMN when ``delta.columnMapping.mode`` is
    ``name``. This judges a managed ``DropColumn`` action against the
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
        drops_a_column = any(isinstance(change, DropColumn) for change in drift.actions)
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


class AmbiguousColumnRename:
    """Disallow a declared rename whose source and target both exist."""

    name: ClassVar[str] = "AmbiguousColumnRename"

    def evaluate(self, drift: TableDrift) -> tuple[ValidationFailure, ...]:
        """Flag every explicit rename conflict."""
        return tuple(
            ValidationFailure(
                rule_name=self.name,
                message=(
                    f"Operation not allowed: cannot rename '{finding.old_name}' to"
                    f" '{finding.new_name}' — both columns exist"
                    " on the table. If the old column should be dropped, remove the"
                    " renamed_from hint and drop it in its own sync."
                ),
            )
            for finding in drift.findings
            if isinstance(finding, ColumnRenameConflict)
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
            for change in drift.actions
            if isinstance(change, DropForeignKey)
        }
        failures: list[ValidationFailure] = []
        for change in drift.actions:
            if not isinstance(change, DropPrimaryKey):
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
    AmbiguousColumnRename(),
    PrimaryKeyReferencedByForeignKeys(),
)


class UnmanagedAspectDrift:
    """
    Fail once per unmanaged aspect that has drifted.

    The drift arm of the scope gate: it defines what a declaration is allowed
    to govern and runs before any safety rule, short-circuiting
    ``validate_diff``. It is not a member of ``DEFAULT_RULES`` and cannot be
    suppressed — the accepted planning boundary turns every validated action
    into executable work, so ``rules=()`` still cannot admit actions from an
    aspect the declaration does not manage.

    It reads the diff's raw actions and findings — its subject is exactly the
    out-of-scope differences the gate exists to reject. dict.fromkeys
    deduplicates the aspects while preserving first-seen order, so failure
    order follows diff order deterministically.
    """

    name: ClassVar[str] = "UnmanagedAspectDrift"

    def evaluate(self, drift: TableDrift) -> tuple[ValidationFailure, ...]:
        """Flag every drifted aspect the declaration does not manage."""
        managed_aspects = drift.desired.managed_aspects
        unmanaged_aspects = dict.fromkeys(
            difference.aspect
            for difference in (*drift.actions, *drift.findings)
            if difference.aspect not in managed_aspects
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
    Fail creation of a table whose declaration does not manage table existence.

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
        if TableAspect.TABLE_EXISTENCE in missing.desired.managed_aspects:
            return ()
        return (
            ValidationFailure(
                rule_name=self.name,
                message=(
                    "Operation not allowed: the table does not exist and this"
                    " definition does not manage table existence, so it cannot"
                    " be created. Manage the table fully or create it out-of-band"
                    " first."
                ),
            ),
        )


def validate_diff(diff: TableDiff, rules: tuple[Rule, ...] = DEFAULT_RULES) -> ValidationResult:
    """
    Evaluate a table diff and return the verdict.

    Scope is a gate, checked before any safety rule. An out-of-scope
    difference — a drifted aspect the declaration does not manage, or a
    missing table it may not create — fails here and short-circuits, so the
    safety rules never run on a diff the engine has already rejected on
    scope grounds. This is what makes an unmanaged difference produce
    exactly the scope failure rather than also tripping rules for work the
    user never requested. The gate cannot be suppressed via ``rules``.

    Past the gate every difference is in scope, so the safety rules judge
    the managed drift. A missing table that clears the gate is a
    fully-managed create and needs no safety judgement.
    """
    scope_failures = _scope_failures(diff)
    if scope_failures:
        return ValidationResult(failures=scope_failures)
    match diff:
        case TableMissing():
            return ValidationResult()
        case TableDrift() as drift:
            return ValidationResult(
                failures=tuple(failure for rule in rules for failure in rule.evaluate(drift))
            )
        case _ as unreachable:
            assert_never(unreachable)


def _scope_failures(diff: TableDiff) -> tuple[ValidationFailure, ...]:
    """Return the scope-gate failures for either diff arm; empty when in scope."""
    # TODO: these are stateless single-method classes, constructed per call and
    # invoked directly here (not pluggable rules in DEFAULT_RULES). Reconsider
    # whether they should be plain module-level functions rather than classes.
    match diff:
        case TableMissing() as missing:
            return MissingTableUnmanaged().evaluate(missing)
        case TableDrift() as drift:
            return UnmanagedAspectDrift().evaluate(drift)
        case _ as unreachable:
            assert_never(unreachable)
