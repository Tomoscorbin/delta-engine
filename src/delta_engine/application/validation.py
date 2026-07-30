"""Validation rules judging the diff between desired and observed table state."""

from collections.abc import Mapping
from dataclasses import dataclass
from types import MappingProxyType
from typing import ClassVar, Final, Protocol, assert_never

from delta_engine.application.failures import ValidationFailure
from delta_engine.application.properties import (
    DELTA_PROPERTY_POLICY,
    Property,
    PropertyPolicy,
)
from delta_engine.application.scopes import TAG_ASPECTS
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
    TableKind,
    TimestampNtz,
)
from delta_engine.domain.plan import (
    AddColumn,
    AlterColumnType,
    ColumnCaseDrift,
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


class EligibilityCheck(Protocol):
    """
    Decides whether a diff is fit for safety validation.

    Eligibility concerns the declaration's authority and its reference to the
    table — what it is allowed to govern, and whether it names the table's
    columns the way the catalog does — not whether a given change is safe.
    These are laws rather than policies: every check runs before any safety
    rule, and none can be suppressed via ``rules``.
    """

    name: ClassVar[str]

    def evaluate(self, diff: TableDiff) -> tuple[ValidationFailure, ...]:
        """Return this check's failures for the diff, or an empty tuple."""
        ...


class SafetyRule(Protocol):
    """
    Interface for drift validation rules.

    A rule judges whether a change is safe, given the declaration it belongs
    to. It receives the whole ``TableDrift`` and reads ``drift.actions`` or
    ``drift.unresolvable`` for the differences to judge; ``drift.desired``
    and ``drift.observed`` are the endpoints, available as context (declared
    properties, observed relation kind) — the differences themselves are what
    a rule judges. Because the eligibility checks run first and short-circuit,
    a rule is only ever evaluated on a diff that is fully in scope and spelled
    as the catalog spells it, so its actions and unresolvable differences are
    exactly the ones the declaration manages — it does no scope filtering of
    its own. Never called for a ``TableMissing`` diff.
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
                    f" Current: {unresolvable.observed_partitioning}."
                    f" Requested: {unresolvable.desired_partitioning}."
                    " Recreate the table with the desired partitioning."
                ),
            )
            for unresolvable in drift.unresolvable
            if isinstance(unresolvable, PartitioningChanged)
        )


@dataclass(frozen=True, slots=True)
class PropertyTransitionNotSupported:
    """
    Disallow property transitions the catalog will reject.

    A removal is a transition to absence: an ``UnsetProperty`` is judged as
    ``(observed_value, None)`` against the same permitted set as value
    changes, so a key whose policy definition permits no ``(value, None)``
    pair cannot be declared absent.
    """

    property_policy: PropertyPolicy

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
        return not self.property_policy.permits_transition(
            name=name,
            observed=observed_value,
            desired=desired_value,
        )


@dataclass(frozen=True, slots=True)
class PropertyMustBeDeclared:
    """Disallow leaving a managed catalog property undeclared."""

    property_policy: PropertyPolicy

    name: ClassVar[str] = "PropertyMustBeDeclared"

    def evaluate(self, drift: TableDrift) -> tuple[ValidationFailure, ...]:
        """Flag every managed key set on the table but absent from the declaration."""
        return tuple(
            ValidationFailure(rule_name=self.name, message=self._message(unresolvable))
            for unresolvable in drift.unresolvable
            if isinstance(unresolvable, PropertyUndeclared)
        )

    def _message(self, unresolvable: PropertyUndeclared) -> str:
        if not self.property_policy.permits_removal(unresolvable.name, unresolvable.observed_value):
            return (
                f"Operation not allowed: {unresolvable.name} is set on the table"
                f" (value '{unresolvable.observed_value}') but not declared; it cannot"
                " be unset — declare it to continue managing this table's"
                " properties."
            )
        return (
            f"Operation not allowed: {unresolvable.name} is set on the table"
            f" (value '{unresolvable.observed_value}') but not declared. Declare it"
            " to manage it, or declare it as None to remove it."
        )


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
                    f"Operation not allowed: cannot rename '{unresolvable.old_name}' to"
                    f" '{unresolvable.new_name}' — both columns exist"
                    " on the table. If the old column should be dropped, remove the"
                    " renamed_from hint and drop it in its own sync."
                ),
            )
            for unresolvable in drift.unresolvable
            if isinstance(unresolvable, ColumnRenameConflict)
        )


class PrimaryKeyReferencedByForeignKeys:
    """
    Disallow dropping or changing a primary key while foreign keys reference it.

    DROP PRIMARY KEY is RESTRICT by default: it fails while any FK references
    the key. The referencing constraints are read from the drift's observed
    table — carried as judging context — so this rule needs no second input.
    A referencing FK on
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
                for reference in drift.observed.referencing_foreign_keys
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


class ColumnSpellingMustMatchCatalog:
    """
    Fail any declaration whose column case disagrees with the catalog.

    One of the ``ELIGIBILITY_CHECKS``: it runs unconditionally and cannot be
    suppressed via ``rules``. Exact spelling is a
    law rather than a safety policy, matching its foreign-key sibling
    ``REFERENCED_COLUMN_CASE_MISMATCH``, which is already a structural
    resolution verdict no caller can turn off.

    A misspelled reference is a defect in the declaration, not drift in one of
    the table's aspects, so it is judged at every scope: a declaration managing
    only keys or tags still names its columns, and naming them wrongly is wrong
    whatever else it declines to manage. This is why ``UnmanagedAspectDrift``
    passes over ``ColumnCaseDrift`` — were it not exempt, the narrow scopes most
    likely to meet case drift would report unmanaged column-structure drift
    instead of the spelling that caused it. Reported first among the checks so
    the actionable root defect leads.

    A missing table has no catalog counterpart to disagree with: what a
    declaration creates, it spells freely.
    """

    name: ClassVar[str] = "ColumnSpellingMustMatchCatalog"

    def evaluate(self, diff: TableDiff) -> tuple[ValidationFailure, ...]:
        """Flag every column reference spelled differently from the catalog."""
        match diff:
            case TableMissing():
                return ()

            case TableDrift() as drift:
                return tuple(
                    ValidationFailure(
                        rule_name=self.name,
                        message=(
                            f"Column '{unresolvable.declared_name}' is spelled"
                            f" '{unresolvable.observed_name}' in the catalog. Update the"
                            " declaration to match the catalog's spelling exactly."
                        ),
                    )
                    for unresolvable in drift.unresolvable
                    if isinstance(unresolvable, ColumnCaseDrift)
                )

            case _ as unreachable:
                assert_never(unreachable)


class UnmanagedAspectDrift:
    """
    Fail once per unmanaged aspect that has drifted.

    One of the ``ELIGIBILITY_CHECKS``: it defines what a declaration is allowed
    to govern and runs before any safety rule, short-circuiting
    ``validate_diff``. It is not a member of ``DEFAULT_SAFETY_RULES`` and cannot be
    suppressed — the accepted planning boundary turns every validated action
    into executable work, so ``rules=()`` still cannot admit actions from an
    aspect the declaration does not manage.

    It reads the diff's raw actions and unresolvable differences — its subject
    is exactly the out-of-scope differences it exists to reject. dict.fromkeys
    deduplicates the aspects while preserving first-seen order, so failure
    order follows diff order deterministically.

    ``ColumnCaseDrift`` is the one difference it passes over: a column spelled
    differently from the catalog is a defect in the declaration rather than
    drift in the column structure, and ``ColumnSpellingMustMatchCatalog`` names
    it at every scope. Judging it here instead would tell a metadata-scoped
    declaration that its column structure had drifted, which is both untrue and
    a worse instruction than fixing the spelling.
    """

    name: ClassVar[str] = "UnmanagedAspectDrift"

    def evaluate(self, diff: TableDiff) -> tuple[ValidationFailure, ...]:
        """Flag every drifted aspect the declaration does not manage."""
        match diff:
            case TableMissing():
                return ()

            case TableDrift() as drift:
                unmanaged_aspects = dict.fromkeys(
                    difference.aspect
                    for difference in (*drift.actions, *drift.unresolvable)
                    if not isinstance(difference, ColumnCaseDrift)
                    and difference.aspect not in drift.desired.managed_aspects
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

    One of the ``ELIGIBILITY_CHECKS``, and the only one that judges the
    ``TableMissing`` arm. It shares the naming shape of a rule (its ``name``
    supplies the failure's ``rule_name``) but not the ``SafetyRule`` protocol —
    a missing table has no changes to evaluate. Like every eligibility check it
    runs unconditionally: ``rules=()`` must not enable metadata-only creates.
    """

    name: ClassVar[str] = "MissingTableUnmanaged"

    def evaluate(self, diff: TableDiff) -> tuple[ValidationFailure, ...]:
        """Flag a missing table that this declaration cannot create."""
        match diff:
            case TableDrift():
                return ()

            case TableMissing() as missing:
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

            case _ as unreachable:
                assert_never(unreachable)


class StreamingTableTagsOnly:
    """
    Fail any declaration that manages more than tags on a streaming table.

    One of the ``ELIGIBILITY_CHECKS``: it runs unconditionally and cannot be
    suppressed via ``rules``. A streaming table's definition is owned by its
    pipeline; Unity Catalog tags are the one aspect durably manageable from
    outside it. It judges the declaration's claimed aspects against the
    observed kind — not the drift — so it fires even when the table is
    currently in sync, and a dry run surfaces the misdeclaration immediately.
    ``TAG_ASPECTS`` is shared with the public ``"tags"`` scope so the two
    policies cannot diverge.
    """

    name: ClassVar[str] = "StreamingTableTagsOnly"

    def evaluate(self, diff: TableDiff) -> tuple[ValidationFailure, ...]:
        """Flag a streaming-table declaration whose managed aspects exceed the tag aspects."""
        match diff:
            case TableMissing():
                return ()

            case TableDrift() as drift:
                if drift.observed.kind is not TableKind.STREAMING_TABLE:
                    return ()
                if drift.desired.managed_aspects <= TAG_ASPECTS:
                    return ()
                return (
                    ValidationFailure(
                        rule_name=self.name,
                        message=(
                            "Operation not allowed: this relation is a streaming table,"
                            " whose definition is owned by its pipeline. Only Unity"
                            " Catalog tags can be managed on it: declare the table with"
                            ' scope="tags", or change its definition in the owning'
                            " pipeline."
                        ),
                    ),
                )


ELIGIBILITY_CHECKS: Final[tuple[EligibilityCheck, ...]] = (
    ColumnSpellingMustMatchCatalog(),
    MissingTableUnmanaged(),
    StreamingTableTagsOnly(),
    UnmanagedAspectDrift(),
)

DEFAULT_SAFETY_RULES: Final[tuple[SafetyRule, ...]] = (
    NonNullableColumnAdd(),
    NullabilityTighteningOnExistingColumn(),
    NonWideningColumnTypeChange(),
    TypeWideningRequiredForTypeChange(),
    PartitioningChangeNotSupported(),
    PropertyTransitionNotSupported(DELTA_PROPERTY_POLICY),
    PropertyMustBeDeclared(DELTA_PROPERTY_POLICY),
    ColumnMappingRequiredForDrop(),
    AmbiguousColumnRename(),
    PrimaryKeyReferencedByForeignKeys(),
)


def validate_diff(
    diff: TableDiff,
    rules: tuple[SafetyRule, ...] = DEFAULT_SAFETY_RULES,
) -> tuple[ValidationFailure, ...]:
    """
    Evaluate a table diff and return its failures, empty when it is valid.

    Eligibility is settled before any safety rule. A column spelled differently
    from the catalog, a drifted aspect the declaration does not manage, a
    missing table it may not create, or a streaming table it claims more than
    tags on all fail here and short-circuit, so the safety rules never run on a
    diff the engine has already rejected. This is what makes an unmanaged
    difference produce exactly the scope failure rather than also tripping rules
    for work the user never requested, and a misspelled column produce the
    spelling failure rather than safety verdicts on a diff whose column
    references cannot be trusted. Failures are collected across every check and
    reported together, spelling first, so the actionable root defect leads. No
    eligibility check can be suppressed via ``rules``.

    Past that point every difference is in scope and spelled as the catalog
    spells it, so the safety rules judge the managed drift. A missing table
    that is eligible is a fully-managed create and needs no safety judgement.
    """
    ineligible = tuple(failure for check in ELIGIBILITY_CHECKS for failure in check.evaluate(diff))

    if ineligible:
        return ineligible

    match diff:
        case TableMissing():
            return ()
        case TableDrift() as drift:
            return tuple(failure for rule in rules for failure in rule.evaluate(drift))
        case _ as unreachable:
            assert_never(unreachable)
