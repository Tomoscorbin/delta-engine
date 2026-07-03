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

from collections.abc import Callable, Hashable, Iterable
from dataclasses import dataclass

from delta_engine.domain.model import Column, DesiredTable
from delta_engine.domain.model.data_type import DataType
from delta_engine.domain.model.foreign_key import ForeignKeyConstraint
from delta_engine.domain.model.primary_key import PrimaryKeyConstraint


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


type Entry[T] = Added[T] | Removed[T] | Changed[T]


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
