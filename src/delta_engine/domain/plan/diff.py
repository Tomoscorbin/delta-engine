"""
Typed description of the difference between a desired and an observed table.

`diff_table` is the single fact-producing entry point: given the desired
definition and the observed one (or ``None`` when the table is missing), it
returns a `TableDiff` — a closed sum of `TableMissing` and `TableDrift`. The
diff states facts only; it carries no judgement about which differences are
permitted (the validator's job) and no knowledge of how a difference is acted
on (the dimension's job). Every variant carries the data its consumers need, so
the diff is self-contained.

Each dimension type owns two concerns: how to detect that it differs (via its
``diff`` staticmethod) and what actions to produce. Adding a new dimension means
adding one class here — ``diff_table`` requires no changes.

Column-level drift entries (`ColumnAdded`, `ColumnRemoved`, `ColumnDataTypeChanged`,
etc.) satisfy the same `Dimension` protocol as table-level dimensions: each carries
exactly the fact it describes with no optionals and produces its own actions.
`ColumnsDimension` delegates to them directly. Whether a dimension's drift is
permitted is policy — that belongs in validation, not here.
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


class Dimension(Protocol):
    """A single aspect of table drift: produces actions to reconcile the difference."""

    def actions(self) -> tuple[Action, ...]:
        """Return the actions this dimension contributes to the plan."""
        ...


@dataclass(frozen=True, slots=True)
class KeyValue:
    """A named string value — one property or one tag."""

    name: str
    value: str


# ---------------------------------------------------------------------------
# Column-level drift entries
#
# Each describes exactly one kind of column difference. All satisfy the
# Dimension protocol so ColumnsDimension can delegate to them uniformly.
# ---------------------------------------------------------------------------


@dataclass(frozen=True, slots=True)
class ColumnAdded:
    """A column present in the declaration but absent from the catalog."""

    column: Column

    def actions(self) -> tuple[Action, ...]:
        return (AddColumn(column=self.column), *[
            SetColumnTag(column_name=self.column.name, name=name, value=value)
            for name, value in self.column.tags.items()
        ])


@dataclass(frozen=True, slots=True)
class ColumnRemoved:
    """A column present in the catalog but absent from the declaration."""

    column: Column

    def actions(self) -> tuple[Action, ...]:
        return (DropColumn(self.column.name),)


@dataclass(frozen=True, slots=True)
class ColumnDataTypeChanged:
    """A column whose data type differs — no in-place action is possible."""

    column_name: str
    change: Changed[DataType]

    def actions(self) -> tuple[Action, ...]:
        return ()


@dataclass(frozen=True, slots=True)
class ColumnNullabilityChanged:
    """A column whose nullability differs."""

    column_name: str
    change: Changed[bool]

    def actions(self) -> tuple[Action, ...]:
        return (SetColumnNullability(column_name=self.column_name, nullable=self.change.desired),)


@dataclass(frozen=True, slots=True)
class ColumnCommentChanged:
    """A column whose comment differs."""

    column_name: str
    change: Changed[str]

    def actions(self) -> tuple[Action, ...]:
        return (SetColumnComment(self.column_name, self.change.desired),)


@dataclass(frozen=True, slots=True)
class ColumnTagsChanged:
    """A column whose tags differ."""

    column_name: str
    entries: tuple[Entry[KeyValue], ...]

    def actions(self) -> tuple[Action, ...]:
        """Return SetColumnTag for Added/Changed entries and UnsetColumnTag for Removed."""
        result: list[Action] = []
        for entry in self.entries:
            match entry:
                case Added(item=pair) | Changed(desired=pair):
                    result.append(
                        SetColumnTag(
                            column_name=self.column_name, name=pair.name, value=pair.value
                        )
                    )
                case Removed(item=pair):
                    result.append(UnsetColumnTag(column_name=self.column_name, name=pair.name))
        return tuple(result)


type ColumnDrift = (
    ColumnAdded
    | ColumnRemoved
    | ColumnDataTypeChanged
    | ColumnNullabilityChanged
    | ColumnCommentChanged
    | ColumnTagsChanged
)
type ForeignKeyDrift = Added[ForeignKeyConstraint] | Removed[ForeignKeyConstraint]


@dataclass(frozen=True, slots=True)
class ColumnsDimension:
    """Column drift: a flat sequence of per-column and per-attribute entries."""

    entries: tuple[ColumnDrift, ...]

    @staticmethod
    def diff(
        desired: tuple[Column, ...], observed: tuple[Column, ...]
    ) -> ColumnsDimension | None:
        """Return a ColumnsDimension for any column differences, or None when identical."""
        desired_by_name = {col.name: col for col in desired}
        observed_by_name = {col.name: col for col in observed}
        result: list[ColumnDrift] = []
        for name, col in desired_by_name.items():
            if name not in observed_by_name:
                result.append(ColumnAdded(column=col))
        for name, col in observed_by_name.items():
            if name not in desired_by_name:
                result.append(ColumnRemoved(column=col))
        for name, desired_col in desired_by_name.items():
            if name in observed_by_name:
                result.extend(ColumnsDimension._diff_pair(desired_col, observed_by_name[name]))
        return ColumnsDimension(entries=tuple(result)) if result else None

    @staticmethod
    def _diff_pair(desired: Column, observed: Column) -> tuple[ColumnDrift, ...]:
        """
        Return per-attribute drift entries for a name-matched column pair.

        When a data type change is present, only that entry is returned — other
        attribute drift is suppressed because a type-changed column must be
        recreated; showing nullability or comment entries would suggest
        actionable work that is moot until the column is dropped and re-added.
        """
        if desired.data_type != observed.data_type:
            return (
                ColumnDataTypeChanged(
                    column_name=desired.name,
                    change=Changed(desired=desired.data_type, observed=observed.data_type),
                ),
            )
        entries: list[ColumnDrift] = []
        if desired.nullable != observed.nullable:
            entries.append(
                ColumnNullabilityChanged(
                    column_name=desired.name,
                    change=Changed(desired=desired.nullable, observed=observed.nullable),
                )
            )
        if desired.comment != observed.comment:
            entries.append(
                ColumnCommentChanged(
                    column_name=desired.name,
                    change=Changed(desired=desired.comment, observed=observed.comment),
                )
            )
        tag_entries = _diff_mapping(desired.tags, observed.tags)
        if tag_entries:
            entries.append(ColumnTagsChanged(column_name=desired.name, entries=tag_entries))
        return tuple(entries)

    def actions(self) -> tuple[Action, ...]:
        return tuple(action for entry in self.entries for action in entry.actions())


@dataclass(frozen=True, slots=True)
class TableCommentDimension:
    """Table comment drift."""

    change: Changed[str]

    @staticmethod
    def diff(desired: str, observed: str) -> TableCommentDimension | None:
        """Return a TableCommentDimension when the comment differs, or None when identical."""
        change = _changed(desired, observed)
        return TableCommentDimension(change=change) if change is not None else None

    def actions(self) -> tuple[Action, ...]:
        return (SetTableComment(comment=self.change.desired),)


@dataclass(frozen=True, slots=True)
class PropertiesDimension:
    """Table property drift — declared-subset semantics: Removed entries are ignored."""

    entries: tuple[Entry[KeyValue], ...]

    @staticmethod
    def diff(
        desired: Mapping[str, str], observed: Mapping[str, str]
    ) -> PropertiesDimension | None:
        """Return a PropertiesDimension when any property differs, or None when identical."""
        entries = _diff_mapping(desired, observed)
        return PropertiesDimension(entries=entries) if entries else None

    def actions(self) -> tuple[Action, ...]:
        result: list[Action] = []
        for entry in self.entries:
            match entry:
                case Added(item=pair) | Changed(desired=pair):
                    result.append(SetProperty(name=pair.name, value=pair.value))
                case _:
                    pass
        return tuple(result)


@dataclass(frozen=True, slots=True)
class TableTagsDimension:
    """Table tag drift — full-state semantics: Removed entries are unset."""

    entries: tuple[Entry[KeyValue], ...]

    @staticmethod
    def diff(
        desired: Mapping[str, str], observed: Mapping[str, str]
    ) -> TableTagsDimension | None:
        """Return a TableTagsDimension when any tag differs, or None when identical."""
        entries = _diff_mapping(desired, observed)
        return TableTagsDimension(entries=entries) if entries else None

    def actions(self) -> tuple[Action, ...]:
        result: list[Action] = []
        for entry in self.entries:
            match entry:
                case Added(item=pair) | Changed(desired=pair):
                    result.append(SetTableTag(name=pair.name, value=pair.value))
                case Removed(item=pair):
                    result.append(UnsetTableTag(name=pair.name))
        return tuple(result)


@dataclass(frozen=True, slots=True)
class PartitioningDimension:
    """Partitioning drift — records the fact; policy on whether it is allowed lives in validation."""

    change: Changed[tuple[str, ...]]

    @staticmethod
    def diff(
        desired: tuple[str, ...], observed: tuple[str, ...]
    ) -> PartitioningDimension | None:
        """Return a PartitioningDimension when partitioning differs, or None when identical."""
        change = _changed(desired, observed)
        return PartitioningDimension(change=change) if change is not None else None

    def actions(self) -> tuple[Action, ...]:
        return ()


@dataclass(frozen=True, slots=True)
class PrimaryKeyDimension:
    """Primary key drift."""

    entry: Entry[PrimaryKeyConstraint]

    @staticmethod
    def diff(
        desired: PrimaryKeyConstraint | None,
        observed: PrimaryKeyConstraint | None,
    ) -> PrimaryKeyDimension | None:
        """
        Return a PrimaryKeyDimension when the primary key differs, or None when identical.

        Identity is column-set equality: order and constraint name do not make
        two keys different.
        """
        if desired is not None and observed is None:
            return PrimaryKeyDimension(entry=Added(desired))
        if desired is None and observed is not None:
            return PrimaryKeyDimension(entry=Removed(observed))
        if (
            desired is not None
            and observed is not None
            and set(desired.columns) != set(observed.columns)
        ):
            return PrimaryKeyDimension(entry=Changed(desired=desired, observed=observed))
        return None

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


@dataclass(frozen=True, slots=True)
class ForeignKeysDimension:
    """Foreign key drift."""

    entries: tuple[ForeignKeyDrift, ...]

    @staticmethod
    def diff(
        desired: tuple[ForeignKeyConstraint, ...],
        observed: tuple[ForeignKeyConstraint, ...],
    ) -> ForeignKeysDimension | None:
        """
        Return a ForeignKeysDimension when any FK differs, or None when identical.

        Identity is content signature (local columns, referenced table, referenced
        columns): a matched pair is content-identical and records no fact. An FK
        present on both sides under different constraint names produces nothing,
        so a sync over an unchanged catalog stays idempotent.
        """
        desired_by_sig = {fk.signature: fk for fk in desired}
        observed_by_sig = {fk.signature: fk for fk in observed}
        added: tuple[ForeignKeyDrift, ...] = tuple(
            Added(fk) for sig, fk in desired_by_sig.items() if sig not in observed_by_sig
        )
        removed: tuple[ForeignKeyDrift, ...] = tuple(
            Removed(fk) for sig, fk in observed_by_sig.items() if sig not in desired_by_sig
        )
        entries = added + removed
        return ForeignKeysDimension(entries=entries) if entries else None

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

    Each dimension records the facts for one aspect of the table and produces
    actions to reconcile it. An empty tuple is the natural zero — a table with
    no drift has no dimensions.
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
    dimensions = [
        d
        for d in [
            ColumnsDimension.diff(desired.columns, observed.columns),
            TableCommentDimension.diff(desired.comment, observed.comment),
            PropertiesDimension.diff(desired.properties, observed.properties),
            TableTagsDimension.diff(desired.tags, observed.tags),
            PartitioningDimension.diff(desired.partitioned_by, observed.partitioned_by),
            PrimaryKeyDimension.diff(desired.primary_key, observed.primary_key),
            ForeignKeysDimension.diff(desired.foreign_keys, observed.foreign_keys),
        ]
        if d is not None
    ]
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
