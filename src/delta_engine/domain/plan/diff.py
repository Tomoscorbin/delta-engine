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

from collections.abc import Callable, Hashable, Iterable, Mapping
from dataclasses import dataclass
from typing import Protocol, assert_never

from delta_engine.domain.model import Column, DesiredTable, ObservedTable
from delta_engine.domain.model.data_type import DataType
from delta_engine.domain.model.foreign_key import ForeignKeyConstraint
from delta_engine.domain.model.primary_key import PrimaryKeyConstraint
from delta_engine.domain.plan.actions import (
    Action,
    AddColumn,
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
                case Added(item=pair):
                    result.append(
                        SetColumnTag(
                            column_name=changed.column_name, name=pair.name, value=pair.value
                        )
                    )
                case Changed(desired=pair):
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
                    f"partitioning changes are not supported."
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


@dataclass(frozen=True, slots=True)
class TableDrift:
    """
    Per-dimension differences between a desired and an observed table.

    Every field defaults to its empty value, so a drift with no differences is
    the natural zero. There is deliberately no ``is_empty`` property: "no
    drift" and "no actions" are different questions (a type-drift-only table
    has drift but lowers to an empty plan), and every current consumer asks
    the plan question.
    """

    columns: tuple[ColumnDrift, ...] = ()
    table_comment: Changed[str] | None = None
    properties: tuple[Entry[KeyValue], ...] = ()
    table_tags: tuple[Entry[KeyValue], ...] = ()
    partitioning: Changed[tuple[str, ...]] | None = None
    primary_key: Entry[PrimaryKeyConstraint] | None = None
    foreign_keys: tuple[ForeignKeyDrift, ...] = ()


type TableDiff = TableMissing | TableDrift


@dataclass(frozen=True, slots=True)
class Matched[T]:
    """
    The outcome of matching desired items against observed items by identity.

    Partitions purely by key membership: ``added`` (desired-only), ``dropped``
    (observed-only), and ``common`` (present on both sides, paired as
    ``(desired, observed)``). It says nothing about whether a common pair is
    equal — deciding what a match *means* is the caller's concern, not the
    matcher's. ``added`` preserves desired declaration order; ``dropped``
    preserves observed order.
    """

    added: tuple[T, ...]
    dropped: tuple[T, ...]
    common: tuple[tuple[T, T], ...]


def match_by_key[T](
    desired: Iterable[T],
    observed: Iterable[T],
    *,
    key: Callable[[T], Hashable],
) -> Matched[T]:
    """Match ``desired`` against ``observed`` by ``key`` into added/dropped/common."""
    desired_by_key = {key(item): item for item in desired}
    observed_by_key = {key(item): item for item in observed}

    added = tuple(
        item for identity, item in desired_by_key.items() if identity not in observed_by_key
    )
    dropped = tuple(
        item for identity, item in observed_by_key.items() if identity not in desired_by_key
    )
    common = tuple(
        (desired_item, observed_by_key[identity])
        for identity, desired_item in desired_by_key.items()
        if identity in observed_by_key
    )
    return Matched(added=added, dropped=dropped, common=common)


def diff_table(desired: DesiredTable, observed: ObservedTable | None) -> TableDiff:
    """
    Compute the facts separating ``observed`` from ``desired``.

    Args:
        desired: Desired table definition.
        observed: Current table definition, or ``None`` if the table is missing.

    Returns:
        ``TableMissing`` when the table does not exist, else a ``TableDrift``
        whose every field records the differences in one dimension (empty
        fields mean no difference).

    """
    if observed is None:
        return TableMissing(desired=desired)
    return TableDrift(
        columns=_diff_columns(desired.columns, observed.columns),
        table_comment=_changed(desired.comment, observed.comment),
        properties=_diff_mapping(desired.properties, observed.properties),
        table_tags=_diff_mapping(desired.tags, observed.tags),
        partitioning=_changed(desired.partitioned_by, observed.partitioned_by),
        primary_key=_diff_primary_key(desired.primary_key, observed.primary_key),
        foreign_keys=_diff_foreign_keys(desired.foreign_keys, observed.foreign_keys),
    )


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
    matched = match_by_key(
        (KeyValue(name, value) for name, value in desired.items()),
        (KeyValue(name, value) for name, value in observed.items()),
        key=lambda pair: pair.name,
    )
    added: tuple[Entry[KeyValue], ...] = tuple(Added(item) for item in matched.added)
    changed: tuple[Entry[KeyValue], ...] = tuple(
        Changed(desired=desired_item, observed=observed_item)
        for desired_item, observed_item in matched.common
        if desired_item.value != observed_item.value
    )
    removed: tuple[Entry[KeyValue], ...] = tuple(Removed(item) for item in matched.dropped)
    return added + changed + removed


def _diff_columns(
    desired: tuple[Column, ...], observed: tuple[Column, ...]
) -> tuple[ColumnDrift, ...]:
    """Diff columns by name into Added/Removed entries and per-column ColumnChanged facts."""
    matched = match_by_key(desired, observed, key=lambda column: column.name)
    added: tuple[ColumnDrift, ...] = tuple(Added(column) for column in matched.added)
    removed: tuple[ColumnDrift, ...] = tuple(Removed(column) for column in matched.dropped)
    changed: tuple[ColumnDrift, ...] = tuple(
        drift
        for desired_column, observed_column in matched.common
        if (drift := _diff_column_pair(desired_column, observed_column)) is not None
    )
    return added + removed + changed


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
    matched = match_by_key(desired, observed, key=lambda foreign_key: foreign_key.signature)
    added: tuple[ForeignKeyDrift, ...] = tuple(Added(item) for item in matched.added)
    removed: tuple[ForeignKeyDrift, ...] = tuple(Removed(item) for item in matched.dropped)
    return added + removed
