"""
Typed description of the difference between a desired and an observed table.

`diff_table` is the single fact-producing entry point: given the desired
definition and the observed one (or ``None`` when the table is missing), it
returns a `TableDiff` — a closed sum of `TableMissing` and `TableDrift`. The
diff states facts only; it carries no judgement about which differences are
permitted (the validator's job) and no knowledge of how a difference is acted
on (the lowerer's job). Every variant carries the data its consumers need, so
the diff is self-contained.
"""

from __future__ import annotations

from collections.abc import Mapping
from dataclasses import dataclass
from typing import Protocol, assert_never

from delta_engine.domain.model import Column, DesiredTable, ObservedTable
from delta_engine.domain.model.data_type import DataType
from delta_engine.domain.model.foreign_key import ForeignKeyConstraint
from delta_engine.domain.model.primary_key import PrimaryKeyConstraint
from delta_engine.domain.plan.actions import (
    Action,
    ActionPlan,
    AddColumn,
    CreateTable,
    DropColumn,
    DropForeignKey,
    DropPrimaryKey,
    SetColumnComment,
    SetColumnNullability,
    SetColumnTag,
    SetForeignKey,
    SetPrimaryKey,
    SetProperty,
    SetTableComment,
    SetTableTag,
    UnsetColumnTag,
    UnsetTableTag,
)


@dataclass(frozen=True, slots=True)
class Added[T]:
    """A desired-only item: present in the declaration, absent from the catalog."""

    item: T


@dataclass(frozen=True, slots=True)
class Removed[T]:
    """An observed-only item: present in the catalog, absent from the declaration."""

    item: T


@dataclass(frozen=True, slots=True)
class Changed[T]:
    """An item present on both sides with different content."""

    desired: T
    observed: T

    def __post_init__(self) -> None:
        if self.desired == self.observed:
            raise ValueError(f"Changed carries no difference: {self.desired!r}")


type Entry[T] = Added[T] | Removed[T] | Changed[T]


@dataclass(frozen=True, slots=True)
class UnhandledFact:
    """A diff fact the engine has no action for; surfaced to validation."""

    description: str


class Dimension(Protocol):
    """A single aspect of table drift: produces actions and declares unhandled facts."""

    def actions(self) -> tuple[Action, ...]:
        """Return the actions this dimension contributes to the plan."""
        ...

    def unhandled(self) -> tuple[UnhandledFact, ...]:
        """Return facts this dimension has no action for, surfaced to validation."""
        ...


@dataclass(frozen=True, slots=True)
class KeyValue:
    """A named string value — one property or one tag."""

    name: str
    value: str


@dataclass(frozen=True, slots=True)
class ColumnChanged:
    """
    A name-matched column whose attributes differ.

    Carries one optional sub-fact per attribute so no consumer ever re-diffs a
    column to discover *what* changed. At least one sub-fact must be present —
    a vacuous entry is a malformed diff and is rejected at construction.
    """

    column_name: str
    data_type: Changed[DataType] | None = None
    nullability: Changed[bool] | None = None
    comment: Changed[str] | None = None
    tags: tuple[Entry[KeyValue], ...] = ()

    def __post_init__(self) -> None:
        """Reject an entry that records no differences."""
        if (
            self.data_type is None
            and self.nullability is None
            and self.comment is None
            and not self.tags
        ):
            raise ValueError(f"ColumnChanged for {self.column_name!r} carries no differences")


type ColumnDrift = Added[Column] | Removed[Column] | ColumnChanged
type ForeignKeyDrift = Added[ForeignKeyConstraint] | Removed[ForeignKeyConstraint]


@dataclass(frozen=True, slots=True)
class ColumnsDimension:
    """Column drift: added, removed, and changed columns."""

    entries: tuple[ColumnDrift, ...]

    def actions(self) -> tuple[Action, ...]:
        """Return one action per column drift entry, plus tag actions for added columns."""
        result: list[Action] = []
        for entry in self.entries:
            match entry:
                case Added(item=column):
                    tag_actions = tuple(
                        SetColumnTag(column_name=column.name, name=name, value=value)
                        for name, value in column.tags.items()
                    )
                    result.append(AddColumn(column=column))
                    result.extend(tag_actions)
                case Removed(item=column):
                    result.append(DropColumn(column.name))
                case ColumnChanged() as changed:
                    result.extend(self._lower_column_changed(changed))
        return tuple(result)

    def unhandled(self) -> tuple[UnhandledFact, ...]:
        """Return an UnhandledFact for each column whose data type changed."""
        facts: list[UnhandledFact] = []
        for entry in self.entries:
            if isinstance(entry, ColumnChanged) and entry.data_type is not None:
                facts.append(
                    UnhandledFact(
                        description=(
                            f"cannot change the type of existing column"
                            f" '{entry.column_name}' from"
                            f" {entry.data_type.observed} to {entry.data_type.desired}."
                            " Type migrations are not supported;"
                            " recreate the table to change a column's type."
                        )
                    )
                )
        return tuple(facts)

    @staticmethod
    def _lower_column_changed(changed: ColumnChanged) -> tuple[Action, ...]:
        if changed.data_type is not None:
            # A data_type change is unhandled; suppress all other actions for
            # this column so the dry-run report does not show partial actions
            # that will never execute (the unhandled fact surfaces via
            # .unhandled()).
            return ()
        result: list[Action] = []
        if changed.nullability is not None:
            result.append(
                SetColumnNullability(
                    column_name=changed.column_name, nullable=changed.nullability.desired
                )
            )
        if changed.comment is not None:
            result.append(SetColumnComment(changed.column_name, changed.comment.desired))
        for tag_entry in changed.tags:
            match tag_entry:
                case Added(item=pair) | Changed(desired=pair):
                    result.append(
                        SetColumnTag(
                            column_name=changed.column_name, name=pair.name, value=pair.value
                        )
                    )
                case Removed(item=pair):
                    result.append(
                        UnsetColumnTag(column_name=changed.column_name, name=pair.name)
                    )
        return tuple(result)


@dataclass(frozen=True, slots=True)
class TableCommentDimension:
    """Table comment drift."""

    change: Changed[str]

    def actions(self) -> tuple[Action, ...]:
        """Return a SetTableComment action for the desired comment."""
        return (SetTableComment(comment=self.change.desired),)

    def unhandled(self) -> tuple[UnhandledFact, ...]:
        """Return no unhandled facts — comment changes are always actionable."""
        return ()


@dataclass(frozen=True, slots=True)
class PropertiesDimension:
    """Table property drift — declared-subset semantics: Removed entries are ignored."""

    entries: tuple[Entry[KeyValue], ...]

    def actions(self) -> tuple[Action, ...]:
        """Return SetProperty for each Added or Changed entry; Removed entries produce no action."""
        result: list[Action] = []
        for entry in self.entries:
            match entry:
                case Added(item=pair) | Changed(desired=pair):
                    result.append(SetProperty(name=pair.name, value=pair.value))
                case Removed():
                    pass
        return tuple(result)

    def unhandled(self) -> tuple[UnhandledFact, ...]:
        """Return no unhandled facts — property changes are always actionable."""
        return ()


@dataclass(frozen=True, slots=True)
class TableTagsDimension:
    """Table tag drift — full-state semantics: Removed entries are unset."""

    entries: tuple[Entry[KeyValue], ...]

    def actions(self) -> tuple[Action, ...]:
        """Return SetTableTag for Added/Changed entries and UnsetTableTag for Removed entries."""
        result: list[Action] = []
        for entry in self.entries:
            match entry:
                case Added(item=pair) | Changed(desired=pair):
                    result.append(SetTableTag(name=pair.name, value=pair.value))
                case Removed(item=pair):
                    result.append(UnsetTableTag(name=pair.name))
        return tuple(result)

    def unhandled(self) -> tuple[UnhandledFact, ...]:
        """Return no unhandled facts — tag changes are always actionable."""
        return ()


@dataclass(frozen=True, slots=True)
class PartitioningDimension:
    """Partitioning drift — no action is possible; always surfaces an unhandled fact."""

    change: Changed[tuple[str, ...]]

    def actions(self) -> tuple[Action, ...]:
        """Return no actions — partitioning changes cannot be applied in place."""
        return ()

    def unhandled(self) -> tuple[UnhandledFact, ...]:
        """Return one UnhandledFact describing the unsupported partitioning change."""
        return (
            UnhandledFact(
                description=(
                    "partitioning changes are not supported."
                    f" Current partition columns: {self.change.observed}"
                    f" - Requested partition columns: {self.change.desired}."
                    " Recreate the table with the desired partitioning."
                )
            ),
        )


@dataclass(frozen=True, slots=True)
class PrimaryKeyDimension:
    """Primary key drift."""

    entry: Entry[PrimaryKeyConstraint]

    def actions(self) -> tuple[Action, ...]:
        """Return SetPrimaryKey, DropPrimaryKey, or both depending on the entry type."""
        match self.entry:
            case Added(item=pk):
                return (SetPrimaryKey(columns=pk.columns, constraint_name=pk.constraint_name),)
            case Removed():
                return (DropPrimaryKey(),)
            case Changed(desired=pk):
                return (
                    DropPrimaryKey(),
                    SetPrimaryKey(columns=pk.columns, constraint_name=pk.constraint_name),
                )
            case _ as unreachable:
                assert_never(unreachable)

    def unhandled(self) -> tuple[UnhandledFact, ...]:
        """Return no unhandled facts — primary key changes are always actionable."""
        return ()


@dataclass(frozen=True, slots=True)
class ForeignKeysDimension:
    """Foreign key drift."""

    entries: tuple[ForeignKeyDrift, ...]

    def actions(self) -> tuple[Action, ...]:
        """Return SetForeignKey for Added entries and DropForeignKey for Removed entries."""
        result: list[Action] = []
        for entry in self.entries:
            match entry:
                case Added(item=fk):
                    result.append(
                        SetForeignKey(
                            local_columns=fk.local_columns,
                            referenced_table=fk.referenced_table,
                            referenced_columns=fk.referenced_columns,
                            constraint_name=fk.constraint_name,
                        )
                    )
                case Removed(item=fk):
                    result.append(DropForeignKey(constraint_name=fk.constraint_name))
        return tuple(result)

    def unhandled(self) -> tuple[UnhandledFact, ...]:
        """Return no unhandled facts — foreign key changes are always actionable."""
        return ()


@dataclass(frozen=True, slots=True)
class TableMissing:
    """The table does not exist in the catalog; carries what should exist."""

    desired: DesiredTable

    def plan(self) -> ActionPlan:
        """Build the creation plan: CREATE TABLE plus tag and FK follow-up actions."""
        tag_actions = tuple(
            SetTableTag(name=name, value=value) for name, value in self.desired.tags.items()
        )
        column_tag_actions = tuple(
            SetColumnTag(column_name=column.name, name=name, value=value)
            for column in self.desired.columns
            for name, value in column.tags.items()
        )
        foreign_key_actions = tuple(
            SetForeignKey(
                local_columns=fk.local_columns,
                referenced_table=fk.referenced_table,
                referenced_columns=fk.referenced_columns,
                constraint_name=fk.constraint_name,
            )
            for fk in self.desired.foreign_keys
        )
        return ActionPlan(
            (CreateTable(self.desired), *tag_actions, *column_tag_actions, *foreign_key_actions)
        )


@dataclass(frozen=True, slots=True)
class TableDrift:
    """
    Per-dimension differences between a desired and an observed table.

    Each dimension in the tuple owns its own facts, actions, and unhandled
    facts. An empty tuple is the natural zero — a table with no drift has no
    dimensions.
    """

    dimensions: tuple[Dimension, ...] = ()

    def plan(self) -> ActionPlan:
        """Build the action plan by collecting actions from every dimension."""
        return ActionPlan(tuple(action for d in self.dimensions for action in d.actions()))


type TableDiff = TableMissing | TableDrift


def diff_table(desired: DesiredTable, observed: ObservedTable | None) -> TableDiff:
    """
    Compute the facts separating ``observed`` from ``desired``.

    Returns ``TableMissing`` when the table does not exist, else a
    ``TableDrift`` whose dimensions each record the differences in one aspect.
    Empty aspects produce no dimension — an equal pair yields an empty drift.
    """
    if observed is None:
        return TableMissing(desired=desired)

    dimensions: list[Dimension] = []

    column_entries = _diff_columns(desired.columns, observed.columns)
    if column_entries:
        dimensions.append(ColumnsDimension(entries=column_entries))

    comment = _changed(desired.comment, observed.comment)
    if comment is not None:
        dimensions.append(TableCommentDimension(change=comment))

    property_entries = _diff_mapping(desired.properties, observed.properties)
    if property_entries:
        dimensions.append(PropertiesDimension(entries=property_entries))

    tag_entries = _diff_mapping(desired.tags, observed.tags)
    if tag_entries:
        dimensions.append(TableTagsDimension(entries=tag_entries))

    partitioning = _changed(desired.partitioned_by, observed.partitioned_by)
    if partitioning is not None:
        dimensions.append(PartitioningDimension(change=partitioning))

    pk_entry = _diff_primary_key(desired.primary_key, observed.primary_key)
    if pk_entry is not None:
        dimensions.append(PrimaryKeyDimension(entry=pk_entry))

    fk_entries = _diff_foreign_keys(desired.foreign_keys, observed.foreign_keys)
    if fk_entries:
        dimensions.append(ForeignKeysDimension(entries=fk_entries))

    return TableDrift(dimensions=tuple(dimensions))


def _changed[T](desired: T, observed: T) -> Changed[T] | None:
    """Return the Changed fact for a pair of values, or None when they are equal."""
    if desired == observed:
        return None
    return Changed(desired=desired, observed=observed)


def _diff_mapping(
    desired: Mapping[str, str], observed: Mapping[str, str]
) -> tuple[Entry[KeyValue], ...]:
    """
    Diff two string mappings into uniform entries.

    Reports facts full-state: desired-only keys are Added, observed-only keys
    are Removed, keys on both sides with different values are Changed. Whether
    a Removed key is acted on (tags) or ignored (properties) is lowering
    policy, not a diffing concern.
    """
    result: list[Entry[KeyValue]] = []
    for name, value in desired.items():
        if name not in observed:
            result.append(Added(KeyValue(name, value)))
        elif observed[name] != value:
            result.append(
                Changed(
                    desired=KeyValue(name, value),
                    observed=KeyValue(name, observed[name]),
                )
            )
    for name, value in observed.items():
        if name not in desired:
            result.append(Removed(KeyValue(name, value)))
    return tuple(result)


def _diff_columns(
    desired: tuple[Column, ...], observed: tuple[Column, ...]
) -> tuple[ColumnDrift, ...]:
    """Diff columns by name into Added/Removed entries and per-column ColumnChanged facts."""
    desired_by_name = {col.name: col for col in desired}
    observed_by_name = {col.name: col for col in observed}
    result: list[ColumnDrift] = []
    for name, col in desired_by_name.items():
        if name not in observed_by_name:
            result.append(Added(col))
    for name, col in observed_by_name.items():
        if name not in desired_by_name:
            result.append(Removed(col))
    for name, desired_col in desired_by_name.items():
        if name in observed_by_name:
            drift = _diff_column_pair(desired_col, observed_by_name[name])
            if drift is not None:
                result.append(drift)
    return tuple(result)


def _diff_column_pair(desired: Column, observed: Column) -> ColumnChanged | None:
    """Return the ColumnChanged fact for a name-matched pair, or None when identical."""
    data_type = _changed(desired.data_type, observed.data_type)
    nullability = _changed(desired.nullable, observed.nullable)
    comment = _changed(desired.comment, observed.comment)
    tags = _diff_mapping(desired.tags, observed.tags)
    if data_type is None and nullability is None and comment is None and not tags:
        return None
    return ColumnChanged(
        column_name=desired.name,
        data_type=data_type,
        nullability=nullability,
        comment=comment,
        tags=tags,
    )


def _diff_primary_key(
    desired: PrimaryKeyConstraint | None,
    observed: PrimaryKeyConstraint | None,
) -> Entry[PrimaryKeyConstraint] | None:
    """
    Diff primary keys by column-set identity.

    The key columns compared as a set are the constraint's identity: order and
    constraint name do not make two keys different (matching today's
    behaviour, where equal sets produce no action).
    """
    if desired is not None and observed is None:
        return Added(desired)
    if desired is None and observed is not None:
        return Removed(observed)
    if (
        desired is not None
        and observed is not None
        and set(desired.columns) != set(observed.columns)
    ):
        return Changed(desired=desired, observed=observed)
    return None


def _diff_foreign_keys(
    desired: tuple[ForeignKeyConstraint, ...],
    observed: tuple[ForeignKeyConstraint, ...],
) -> tuple[ForeignKeyDrift, ...]:
    """
    Diff foreign keys by content signature.

    The signature (local columns, referenced table, referenced columns) *is*
    the FK's identity, so a matched pair is content-identical and records no
    fact — an FK has no changed state, only added or removed, which is why
    ``ForeignKeyDrift`` admits no ``Changed`` variant. An FK present on both
    sides under different constraint names produces nothing, so a sync over an
    unchanged catalog stays idempotent.
    """
    desired_by_sig = {fk.signature: fk for fk in desired}
    observed_by_sig = {fk.signature: fk for fk in observed}
    added: tuple[ForeignKeyDrift, ...] = tuple(
        Added(fk) for sig, fk in desired_by_sig.items() if sig not in observed_by_sig
    )
    removed: tuple[ForeignKeyDrift, ...] = tuple(
        Removed(fk) for sig, fk in observed_by_sig.items() if sig not in desired_by_sig
    )
    return added + removed
