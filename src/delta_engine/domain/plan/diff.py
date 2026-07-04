"""
Facts describing how an observed table differs from its desired declaration.

``diff_table`` is the single entry point: given the desired definition and the
observed one (or ``None`` when the table is missing), it returns a
``TableDiff`` — a closed sum of ``TableMissing`` and ``TableDrift``.
``TableDrift`` carries a flat tuple of drift facts plus the ``managed_aspects``
of the declaration that produced it, so the diff is self-contained and
``validate_diff`` needs no other input.

Each fact is a frozen dataclass recording one atomic difference. Every fact
carries two things:

- ``aspect`` — the :class:`TableAspect` the difference belongs to. Validation
  uses this to gate drift in aspects the declaration does not manage.
- ``actions()`` — the imperative actions that reconcile the difference. Facts
  for differences with no in-place remedy (a column type change, a
  partitioning change) return no actions; validation blocks them instead.

Naming conventions:

- Facts are named for what is true relative to the declaration
  (``ColumnAdded``, ``TableTagUnset``); actions in ``actions.py`` are
  imperative commands (``AddColumn``, ``UnsetTableTag``). The two vocabularies
  live in separate modules.
- ``*Changed`` facts carry both sides of the difference (``desired_*`` /
  ``observed_*``) as one atomic pair: rules read the change direction and
  report from/to values without re-correlating separate facts, and a
  ``__post_init__`` guard makes a no-difference fact unrepresentable.

Semantics that differ by aspect:

- Properties are declared-projection: only declared keys are compared, so an
  observed-only property (e.g. one written by a previous full sync or by the
  platform) is not drift and produces no fact.
- Tags are full-state: an observed-only tag is drift and is unset.
- Nullability drift is suppressed for a column whose type also drifted — the
  column must be recreated first, so a nullability fact would be noise.
"""

from __future__ import annotations

from collections.abc import Mapping
from dataclasses import dataclass
from typing import ClassVar

from delta_engine.domain.model import Column, DesiredTable, ObservedTable
from delta_engine.domain.model.data_type import DataType
from delta_engine.domain.model.foreign_key import ForeignKeyConstraint
from delta_engine.domain.model.primary_key import PrimaryKeyConstraint
from delta_engine.domain.model.table_aspect import TableAspect
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

# ---------------------------------------------------------------------------
# Drift facts
# ---------------------------------------------------------------------------


@dataclass(frozen=True, slots=True)
class ColumnAdded:
    """A column present in the declaration but absent from the catalog."""

    column: Column

    aspect: ClassVar[TableAspect] = TableAspect.COLUMN_STRUCTURE

    def actions(self) -> tuple[Action, ...]:
        """AddColumn for the new column; its tags arrive as ColumnTagSet facts."""
        return (AddColumn(column=self.column),)


@dataclass(frozen=True, slots=True)
class ColumnRemoved:
    """A column present in the catalog but absent from the declaration."""

    column: Column

    aspect: ClassVar[TableAspect] = TableAspect.COLUMN_STRUCTURE

    def actions(self) -> tuple[Action, ...]:
        """DropColumn for the removed column."""
        return (DropColumn(self.column.name),)


@dataclass(frozen=True, slots=True)
class ColumnDataTypeChanged:
    """A column whose data type differs — no in-place action is possible."""

    column_name: str
    desired_type: DataType
    observed_type: DataType

    aspect: ClassVar[TableAspect] = TableAspect.COLUMN_STRUCTURE

    def __post_init__(self) -> None:
        if self.desired_type == self.observed_type:
            raise ValueError(f"ColumnDataTypeChanged carries no difference: {self.desired_type!r}")

    def actions(self) -> tuple[Action, ...]:
        """No actions — type changes require recreation; see ColumnDataTypeChangeNotSupported."""
        return ()


@dataclass(frozen=True, slots=True)
class ColumnNullabilityChanged:
    """A column whose nullability differs."""

    column_name: str
    desired_nullable: bool
    observed_nullable: bool

    aspect: ClassVar[TableAspect] = TableAspect.COLUMN_STRUCTURE

    def __post_init__(self) -> None:
        if self.desired_nullable == self.observed_nullable:
            raise ValueError(
                f"ColumnNullabilityChanged carries no difference: {self.desired_nullable!r}"
            )

    def actions(self) -> tuple[Action, ...]:
        """SetColumnNullability to the desired value."""
        return (SetColumnNullability(column_name=self.column_name, nullable=self.desired_nullable),)


@dataclass(frozen=True, slots=True)
class ColumnCommentChanged:
    """A column whose comment differs."""

    column_name: str
    desired_comment: str
    observed_comment: str

    aspect: ClassVar[TableAspect] = TableAspect.COLUMN_COMMENTS

    def __post_init__(self) -> None:
        if self.desired_comment == self.observed_comment:
            raise ValueError(
                f"ColumnCommentChanged carries no difference: {self.desired_comment!r}"
            )

    def actions(self) -> tuple[Action, ...]:
        """SetColumnComment to the desired value."""
        return (SetColumnComment(self.column_name, self.desired_comment),)


@dataclass(frozen=True, slots=True)
class ColumnTagSet:
    """A declared column tag absent from the catalog or carrying a different value."""

    column_name: str
    tag_name: str
    tag_value: str

    aspect: ClassVar[TableAspect] = TableAspect.COLUMN_TAGS

    def actions(self) -> tuple[Action, ...]:
        """SetColumnTag with the desired value."""
        return (
            SetColumnTag(column_name=self.column_name, name=self.tag_name, value=self.tag_value),
        )


@dataclass(frozen=True, slots=True)
class ColumnTagUnset:
    """A column tag present in the catalog but absent from the declaration (full-state)."""

    column_name: str
    tag_name: str

    aspect: ClassVar[TableAspect] = TableAspect.COLUMN_TAGS

    def actions(self) -> tuple[Action, ...]:
        """UnsetColumnTag for the undeclared tag."""
        return (UnsetColumnTag(column_name=self.column_name, name=self.tag_name),)


@dataclass(frozen=True, slots=True)
class TableCommentChanged:
    """A table comment that differs from the declaration."""

    desired_comment: str
    observed_comment: str

    aspect: ClassVar[TableAspect] = TableAspect.TABLE_COMMENT

    def __post_init__(self) -> None:
        if self.desired_comment == self.observed_comment:
            raise ValueError(f"TableCommentChanged carries no difference: {self.desired_comment!r}")

    def actions(self) -> tuple[Action, ...]:
        """SetTableComment to the desired value."""
        return (SetTableComment(comment=self.desired_comment),)


@dataclass(frozen=True, slots=True)
class PropertySet:
    """
    A declared property absent from the catalog or carrying a different value.

    Properties are declared-projection: only declared keys are compared, so
    there is no PropertyUnset fact — an observed-only property is not drift.
    Like the tag set facts, this is an upsert: it carries only the desired
    value, because the remedy is the same whether the key was absent or stale.
    """

    name: str
    desired_value: str

    aspect: ClassVar[TableAspect] = TableAspect.PROPERTIES

    def actions(self) -> tuple[Action, ...]:
        """SetProperty with the desired value."""
        return (SetProperty(name=self.name, value=self.desired_value),)


@dataclass(frozen=True, slots=True)
class TableTagSet:
    """A declared table tag absent from the catalog or carrying a different value."""

    name: str
    value: str

    aspect: ClassVar[TableAspect] = TableAspect.TABLE_TAGS

    def actions(self) -> tuple[Action, ...]:
        """SetTableTag with the desired value."""
        return (SetTableTag(name=self.name, value=self.value),)


@dataclass(frozen=True, slots=True)
class TableTagUnset:
    """A table tag present in the catalog but absent from the declaration (full-state)."""

    name: str

    aspect: ClassVar[TableAspect] = TableAspect.TABLE_TAGS

    def actions(self) -> tuple[Action, ...]:
        """UnsetTableTag for the undeclared tag."""
        return (UnsetTableTag(name=self.name),)


@dataclass(frozen=True, slots=True)
class PartitioningChanged:
    """Partitioning that differs from the declaration — no in-place action is possible."""

    desired_partitioning: tuple[str, ...]
    observed_partitioning: tuple[str, ...]

    aspect: ClassVar[TableAspect] = TableAspect.PARTITIONING

    def __post_init__(self) -> None:
        if self.desired_partitioning == self.observed_partitioning:
            raise ValueError(
                f"PartitioningChanged carries no difference: {self.desired_partitioning!r}"
            )

    def actions(self) -> tuple[Action, ...]:
        """No actions — repartitioning requires recreation; see PartitioningChangeNotSupported."""
        return ()


@dataclass(frozen=True, slots=True)
class PrimaryKeyAdded:
    """A declared primary key absent from the catalog."""

    primary_key: PrimaryKeyConstraint

    aspect: ClassVar[TableAspect] = TableAspect.PRIMARY_KEY

    def actions(self) -> tuple[Action, ...]:
        """SetPrimaryKey for the declared key."""
        return (
            SetPrimaryKey(
                columns=self.primary_key.columns,
                constraint_name=self.primary_key.constraint_name,
            ),
        )


@dataclass(frozen=True, slots=True)
class PrimaryKeyRemoved:
    """A primary key present in the catalog but absent from the declaration."""

    observed_primary_key: PrimaryKeyConstraint

    aspect: ClassVar[TableAspect] = TableAspect.PRIMARY_KEY

    def actions(self) -> tuple[Action, ...]:
        """DropPrimaryKey for the undeclared key."""
        return (DropPrimaryKey(),)


@dataclass(frozen=True, slots=True)
class PrimaryKeyChanged:
    """
    A primary key whose column set differs from the declaration.

    Both sides travel as one atomic pair so validation can report from/to and
    ``actions()`` can emit Drop then Set. Splitting into separate
    added/removed facts would make an orphaned half representable.
    """

    desired_primary_key: PrimaryKeyConstraint
    observed_primary_key: PrimaryKeyConstraint

    aspect: ClassVar[TableAspect] = TableAspect.PRIMARY_KEY

    def __post_init__(self) -> None:
        if set(self.desired_primary_key.columns) == set(self.observed_primary_key.columns):
            raise ValueError(
                f"PrimaryKeyChanged carries no difference: {self.desired_primary_key!r}"
            )

    def actions(self) -> tuple[Action, ...]:
        """DropPrimaryKey then SetPrimaryKey (ActionPhase orders the pair)."""
        return (
            DropPrimaryKey(),
            SetPrimaryKey(
                columns=self.desired_primary_key.columns,
                constraint_name=self.desired_primary_key.constraint_name,
            ),
        )


@dataclass(frozen=True, slots=True)
class ForeignKeyAdded:
    """A declared foreign key absent from the catalog (by content signature)."""

    constraint: ForeignKeyConstraint

    aspect: ClassVar[TableAspect] = TableAspect.FOREIGN_KEYS

    def actions(self) -> tuple[Action, ...]:
        """SetForeignKey for the declared constraint."""
        return (
            SetForeignKey(
                local_columns=self.constraint.local_columns,
                referenced_table=self.constraint.referenced_table,
                referenced_columns=self.constraint.referenced_columns,
                constraint_name=self.constraint.constraint_name,
            ),
        )


@dataclass(frozen=True, slots=True)
class ForeignKeyRemoved:
    """A foreign key present in the catalog but absent from the declaration."""

    constraint: ForeignKeyConstraint

    aspect: ClassVar[TableAspect] = TableAspect.FOREIGN_KEYS

    def actions(self) -> tuple[Action, ...]:
        """DropForeignKey using the catalog-stored constraint name."""
        return (DropForeignKey(constraint_name=self.constraint.constraint_name),)


type DriftFact = (
    ColumnAdded
    | ColumnRemoved
    | ColumnDataTypeChanged
    | ColumnNullabilityChanged
    | ColumnCommentChanged
    | ColumnTagSet
    | ColumnTagUnset
    | TableCommentChanged
    | PropertySet
    | TableTagSet
    | TableTagUnset
    | PartitioningChanged
    | PrimaryKeyAdded
    | PrimaryKeyRemoved
    | PrimaryKeyChanged
    | ForeignKeyAdded
    | ForeignKeyRemoved
)


# ---------------------------------------------------------------------------
# Table-level diff sum
# ---------------------------------------------------------------------------


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
    Flat sequence of drift facts separating an observed table from its declaration.

    ``managed_aspects`` is copied from the declaration at diff time so the
    diff is self-contained; there is deliberately no default — a drift always
    belongs to a declaration with a known scope. The natural zero is an empty
    facts tuple (no drift).
    """

    facts: tuple[DriftFact, ...]
    managed_aspects: frozenset[TableAspect]

    def plan(self) -> ActionPlan:
        """Build the action plan by collecting actions from every fact."""
        return ActionPlan(tuple(action for fact in self.facts for action in fact.actions()))


type TableDiff = TableMissing | TableDrift


# ---------------------------------------------------------------------------
# diff_table — fact production
# ---------------------------------------------------------------------------


def diff_table(desired: DesiredTable, observed: ObservedTable | None) -> TableDiff:
    """
    Compute the facts separating ``observed`` from ``desired``.

    Returns ``TableMissing`` when the table does not exist, else a
    ``TableDrift`` whose facts each record one atomic difference. An equal
    pair yields an empty drift. The diff is scope-blind: every aspect is
    compared regardless of ``managed_aspects``; scope is judged in validation.
    """
    if observed is None:
        return TableMissing(desired=desired)
    facts: list[DriftFact] = []
    facts.extend(_diff_column_structure(desired.columns, observed.columns))
    facts.extend(_diff_column_comments(desired.columns, observed.columns))
    facts.extend(_diff_column_tags(desired.columns, observed.columns))
    if desired.comment != observed.comment:
        facts.append(
            TableCommentChanged(desired_comment=desired.comment, observed_comment=observed.comment)
        )
    facts.extend(_diff_properties(desired.properties, observed.properties))
    facts.extend(_diff_table_tags(desired.tags, observed.tags))
    if desired.partitioned_by != observed.partitioned_by:
        facts.append(
            PartitioningChanged(
                desired_partitioning=desired.partitioned_by,
                observed_partitioning=observed.partitioned_by,
            )
        )
    primary_key_fact = _diff_primary_key(desired.primary_key, observed.primary_key)
    if primary_key_fact is not None:
        facts.append(primary_key_fact)
    facts.extend(_diff_foreign_keys(desired.foreign_keys, observed.foreign_keys))
    return TableDrift(facts=tuple(facts), managed_aspects=desired.managed_aspects)


def _diff_column_structure(
    desired: tuple[Column, ...], observed: tuple[Column, ...]
) -> list[DriftFact]:
    """Facts for column additions, removals, type changes, and nullability changes."""
    desired_by_name = {column.name: column for column in desired}
    observed_by_name = {column.name: column for column in observed}
    facts: list[DriftFact] = []
    for name, column in desired_by_name.items():
        if name not in observed_by_name:
            facts.append(ColumnAdded(column=column))
    for name, column in observed_by_name.items():
        if name not in desired_by_name:
            facts.append(ColumnRemoved(column=column))
    for name, desired_column in desired_by_name.items():
        if name not in observed_by_name:
            continue
        observed_column = observed_by_name[name]
        if desired_column.data_type != observed_column.data_type:
            facts.append(
                ColumnDataTypeChanged(
                    column_name=name,
                    desired_type=desired_column.data_type,
                    observed_type=observed_column.data_type,
                )
            )
        elif desired_column.nullable != observed_column.nullable:
            facts.append(
                ColumnNullabilityChanged(
                    column_name=name,
                    desired_nullable=desired_column.nullable,
                    observed_nullable=observed_column.nullable,
                )
            )
    return facts


def _diff_column_comments(
    desired: tuple[Column, ...], observed: tuple[Column, ...]
) -> list[DriftFact]:
    """Comment facts for name-matched column pairs."""
    observed_by_name = {column.name: column for column in observed}
    facts: list[DriftFact] = []
    for column in desired:
        if column.name not in observed_by_name:
            continue
        observed_column = observed_by_name[column.name]
        if column.comment != observed_column.comment:
            facts.append(
                ColumnCommentChanged(
                    column_name=column.name,
                    desired_comment=column.comment,
                    observed_comment=observed_column.comment,
                )
            )
    return facts


def _diff_column_tags(
    desired: tuple[Column, ...], observed: tuple[Column, ...]
) -> list[DriftFact]:
    """
    Tag facts for every desired column, matched or added (full-state).

    A desired-only column's tags are included: the ADD_COLUMN phase precedes
    SET_COLUMN_TAG, so the column exists by the time its tags are applied.
    """
    observed_by_name = {column.name: column for column in observed}
    facts: list[DriftFact] = []
    for column in desired:
        observed_tags: Mapping[str, str] = (
            observed_by_name[column.name].tags if column.name in observed_by_name else {}
        )
        for tag_name, tag_value in column.tags.items():
            if tag_name not in observed_tags or observed_tags[tag_name] != tag_value:
                facts.append(
                    ColumnTagSet(column_name=column.name, tag_name=tag_name, tag_value=tag_value)
                )
        for tag_name in observed_tags:
            if tag_name not in column.tags:
                facts.append(ColumnTagUnset(column_name=column.name, tag_name=tag_name))
    return facts


def _diff_properties(desired: Mapping[str, str], observed: Mapping[str, str]) -> list[DriftFact]:
    """
    Property facts under declared-projection semantics: only desired keys are compared.

    An observed-only property is not drift — the declaration does not own it.
    A metadata-only table declares no properties, so this loop body never
    executes for it and catalog properties written by a previous full sync
    (e.g. delta.columnMapping.mode) produce no facts.
    """
    return [
        PropertySet(name=name, desired_value=value)
        for name, value in desired.items()
        if name not in observed or observed[name] != value
    ]


def _diff_table_tags(desired: Mapping[str, str], observed: Mapping[str, str]) -> list[DriftFact]:
    """Tag facts under full-state semantics: observed-only tags are drift and are unset."""
    facts: list[DriftFact] = []
    for name, value in desired.items():
        if name not in observed or observed[name] != value:
            facts.append(TableTagSet(name=name, value=value))
    for name in observed:
        if name not in desired:
            facts.append(TableTagUnset(name=name))
    return facts


def _diff_primary_key(
    desired: PrimaryKeyConstraint | None,
    observed: PrimaryKeyConstraint | None,
) -> DriftFact | None:
    """
    Return the primary key fact, or None when the keys agree.

    Identity is column-set equality: order and constraint name do not make
    two keys different.
    """
    if desired is not None and observed is None:
        return PrimaryKeyAdded(primary_key=desired)
    if desired is None and observed is not None:
        return PrimaryKeyRemoved(observed_primary_key=observed)
    if (
        desired is not None
        and observed is not None
        and set(desired.columns) != set(observed.columns)
    ):
        return PrimaryKeyChanged(desired_primary_key=desired, observed_primary_key=observed)
    return None


def _diff_foreign_keys(
    desired: tuple[ForeignKeyConstraint, ...],
    observed: tuple[ForeignKeyConstraint, ...],
) -> list[DriftFact]:
    """
    Foreign key facts, matched by content signature.

    Identity is content (local columns, referenced table, referenced columns):
    an FK present on both sides under different constraint names produces
    nothing, so a sync over an unchanged catalog stays idempotent.
    """
    desired_by_signature = {fk.signature: fk for fk in desired}
    observed_by_signature = {fk.signature: fk for fk in observed}
    facts: list[DriftFact] = []
    for signature, fk in desired_by_signature.items():
        if signature not in observed_by_signature:
            facts.append(ForeignKeyAdded(constraint=fk))
    for signature, fk in observed_by_signature.items():
        if signature not in desired_by_signature:
            facts.append(ForeignKeyRemoved(constraint=fk))
    return facts
