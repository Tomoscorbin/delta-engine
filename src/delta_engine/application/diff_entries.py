"""
Interpretation of plan actions as category-tagged diff entries.

The shared meaning layer between the two report views: text rendering
(`rendering.py`) and the machine projection (`SyncReport.to_dict`). States
what each action *means* (category, operation, subject, detail); presentation
(grouping, grids, dict shapes) belongs to the consumers.
"""

from collections.abc import Sequence
from dataclasses import dataclass
from enum import IntEnum, StrEnum
import functools
from typing import Final, assert_never

from delta_engine.domain.collection_types import ListOrTuple
from delta_engine.domain.model import DesiredColumn
from delta_engine.domain.plan import (
    Action,
    ActionPlan,
    AddColumn,
    AlterClustering,
    AlterColumnType,
    ColumnCaseDrift,
    ColumnRenameConflict,
    CreateTable,
    DropColumn,
    DropForeignKey,
    DropPrimaryKey,
    EnableTableFeature,
    PartitioningChanged,
    PropertyUndeclared,
    RenameColumn,
    SetColumnComment,
    SetColumnNullability,
    SetColumnTag,
    SetForeignKey,
    SetPrimaryKey,
    SetProperty,
    SetTableComment,
    SetTableTag,
    TableDrift,
    Unresolvable,
    UnsetColumnTag,
    UnsetProperty,
    UnsetTableTag,
)


class DiffCategory(IntEnum):
    """
    Diff line groups, in display order (enum value = order).

    Each member carries the nouns its consumers name it by, for the same
    reason ``DiffOperation`` carries its symbol: as a lookup table beside the
    enum, a category added without an entry type-checked fine and surfaced as
    a ``KeyError`` the first time a report rendered it.
    """

    COLUMNS = 1
    KEYS = 2
    CLUSTERING = 3
    PARTITIONING = 4
    FEATURES = 5
    PROPERTIES = 6
    TAGS = 7
    COMMENTS = 8

    @property
    def plural(self) -> str:
        """The heading this category's lines are grouped under in a text diff."""
        return self._nouns[1]

    def counted(self, count: int) -> str:
        """Humanised count for the report grid, e.g. ``1 key`` or ``2 columns``."""
        singular, plural = self._nouns
        return f"{count} {singular if count == 1 else plural}"

    @property
    def _nouns(self) -> tuple[str, str]:
        """The (singular, plural) nouns naming this category's lines."""
        match self:
            case DiffCategory.COLUMNS:
                return ("column", "columns")
            case DiffCategory.KEYS:
                return ("key", "keys")
            case DiffCategory.CLUSTERING:
                return ("clustering", "clustering")
            case DiffCategory.PARTITIONING:
                return ("partitioning", "partitioning")
            case DiffCategory.FEATURES:
                return ("table feature", "table features")
            case DiffCategory.PROPERTIES:
                return ("property", "properties")
            case DiffCategory.TAGS:
                return ("tag", "tags")
            case DiffCategory.COMMENTS:
                return ("comment", "comments")
            case _ as unreachable:
                assert_never(unreachable)


class DiffOperation(StrEnum):
    """
    What a diff line does to its subject.

    One fact in the two spellings its consumers need: the member value is the
    stable word in the ``to_dict`` contract, and ``symbol`` is the character
    the text renderer prefixes the line with. Pairing them on the member is
    what makes a bad operation unconstructable — as a free ``str`` symbol, a
    typo took any character, rendered it happily, and surfaced only as a
    ``KeyError`` once some caller projected the run to a dict.
    """

    ADD = "add"
    REMOVE = "remove"
    CHANGE = "change"

    @property
    def symbol(self) -> str:
        """The character the text diff prefixes this operation's line with."""
        match self:
            case DiffOperation.ADD:
                return "+"
            case DiffOperation.REMOVE:
                return "-"
            case DiffOperation.CHANGE:
                return "~"
            case _ as unreachable:
                assert_never(unreachable)


@dataclass(frozen=True, slots=True)
class DiffEntry:
    """
    One interpreted diff line: what it targets, and how that thing changed.

    ``subject`` always names the target — a column, a property, ``table`` —
    never the change itself, so the machine projection can key on it. Each
    ``detail`` element is one phrase describing the change; they are separate
    elements rather than one string so a text renderer can align them into
    columns down a group, as with the ``(was '...')`` clause of a value change.
    """

    category: DiffCategory
    operation: DiffOperation
    subject: str
    detail: ListOrTuple[str] = ()

    def __post_init__(self) -> None:
        object.__setattr__(self, "detail", tuple(self.detail))

    @property
    def symbol(self) -> str:
        """The +/-/~ character the text renderer prefixes this line with."""
        return self.operation.symbol

    @property
    def cells(self) -> tuple[str, ...]:
        """Subject then detail, as the aligned columns a text diff renders."""
        return (self.subject, *self.detail)


def _column_add_entry(column: DesiredColumn) -> DiffEntry:
    """Build a '+' columns entry for a created column: its type, then NOT NULL if declared."""
    detail = [str(column.data_type)]
    if not column.nullable:
        detail.append("NOT NULL")
    return DiffEntry(DiffCategory.COLUMNS, DiffOperation.ADD, subject=column.name, detail=detail)


def _named_value_entry(
    category: DiffCategory, subject: str, desired_value: str, observed_value: str | None
) -> DiffEntry:
    """
    Build the entry for setting a named value — a property or a tag.

    An absent observed value means the name is new, so the line adds it;
    otherwise it changes, and the old value trails as its own detail phrase.
    """
    if observed_value is None:
        return DiffEntry(
            category, DiffOperation.ADD, subject=subject, detail=(f"= '{desired_value}'",)
        )
    return DiffEntry(
        category,
        DiffOperation.CHANGE,
        subject=subject,
        detail=(f"= '{desired_value}'", f"(was '{observed_value}')"),
    )


def _columns_detail(columns: Sequence[str]) -> tuple[str, ...]:
    """Render a column list as the single parenthesised detail phrase it reads as."""
    return (f"({', '.join(columns)})",)


@functools.singledispatch
def action_entries(action: Action) -> tuple[DiffEntry, ...]:
    """Render one plan action as one or more category-tagged diff entries."""
    raise NotImplementedError(f"No diff entries for action {type(action).__name__}")


def plan_entries(plan: ActionPlan) -> tuple[DiffEntry, ...]:
    """Every diff entry a plan lowers to, in plan order — one action may yield several."""
    return tuple(entry for action in plan for entry in action_entries(action))


def drift_entries(drift: TableDrift) -> tuple[DiffEntry, ...]:
    """
    Every diff entry a drift lowers to, in diff order — actions, then the rest.

    The sibling of :func:`plan_entries` for a diff that never became a plan.
    Both of the drift's streams are included: an unresolvable difference is
    frequently the very reason the diff was rejected, so showing only the
    actions would omit the answer.
    """
    return (
        *(entry for action in drift.actions for entry in action_entries(action)),
        *(
            entry
            for unresolvable in drift.unresolvable
            for entry in unresolvable_entries(unresolvable)
        ),
    )


@action_entries.register
def _(action: CreateTable) -> tuple[DiffEntry, ...]:
    entries = [_column_add_entry(column) for column in action.table.columns]

    primary_key_columns = action.table.primary_key_columns
    if primary_key_columns:
        entries.append(
            DiffEntry(
                DiffCategory.KEYS,
                DiffOperation.ADD,
                subject="primary key",
                detail=_columns_detail(primary_key_columns),
            )
        )

    if action.table.clustered_by:
        entries.append(
            DiffEntry(
                DiffCategory.CLUSTERING,
                DiffOperation.ADD,
                subject="clustering",
                detail=_columns_detail(action.table.clustered_by),
            )
        )

    if action.table.partitioned_by:
        entries.append(
            DiffEntry(
                DiffCategory.PARTITIONING,
                DiffOperation.ADD,
                subject="partitioning",
                detail=_columns_detail(action.table.partitioned_by),
            )
        )

    entries.extend(
        DiffEntry(
            DiffCategory.PROPERTIES,
            DiffOperation.ADD,
            subject=name,
            detail=(f"= '{value}'",),
        )
        for name, value in sorted(action.table.properties.items())
        if value is not None
    )

    entries.extend(
        DiffEntry(
            DiffCategory.COMMENTS,
            DiffOperation.ADD,
            subject=f"column {column.name}",
            detail=(f"'{column.comment}'",),
        )
        for column in action.table.columns
        if column.comment
    )
    if action.table.comment:
        entries.append(
            DiffEntry(
                DiffCategory.COMMENTS,
                DiffOperation.ADD,
                subject="table",
                detail=(f"'{action.table.comment}'",),
            )
        )

    return tuple(entries)


@action_entries.register
def _(action: AddColumn) -> tuple[DiffEntry, ...]:
    return (_column_add_entry(action.column),)


@action_entries.register
def _(action: DropColumn) -> tuple[DiffEntry, ...]:
    return (DiffEntry(DiffCategory.COLUMNS, DiffOperation.REMOVE, subject=action.column.name),)


@action_entries.register
def _(action: RenameColumn) -> tuple[DiffEntry, ...]:
    return (
        DiffEntry(
            DiffCategory.COLUMNS,
            DiffOperation.CHANGE,
            subject=action.old_name,
            detail=(f"renamed → {action.new_name}",),
        ),
    )


@action_entries.register
def _(action: SetColumnNullability) -> tuple[DiffEntry, ...]:
    if action.desired_nullable:
        change = "drop NOT NULL (was NOT NULL)"
    else:
        change = "set NOT NULL (was nullable)"
    return (
        DiffEntry(
            DiffCategory.COLUMNS,
            DiffOperation.CHANGE,
            subject=action.column_name,
            detail=(change,),
        ),
    )


@action_entries.register
def _(action: AlterColumnType) -> tuple[DiffEntry, ...]:
    return (
        DiffEntry(
            DiffCategory.COLUMNS,
            DiffOperation.CHANGE,
            subject=action.column_name,
            detail=(f"{action.observed_type} → {action.desired_type}",),
        ),
    )


@action_entries.register
def _(action: SetColumnComment) -> tuple[DiffEntry, ...]:
    comment = action.desired_comment
    return (
        DiffEntry(
            DiffCategory.COMMENTS,
            DiffOperation.CHANGE,
            subject=f"column {action.column_name}",
            detail=(f"'{comment}'",) if comment else ("(unset)",),
        ),
    )


@action_entries.register
def _(action: SetTableComment) -> tuple[DiffEntry, ...]:
    comment = action.desired_comment
    return (
        DiffEntry(
            DiffCategory.COMMENTS,
            DiffOperation.CHANGE,
            subject="table",
            detail=(f"'{comment}'",) if comment else ("(unset)",),
        ),
    )


@action_entries.register
def _(action: EnableTableFeature) -> tuple[DiffEntry, ...]:
    return (
        DiffEntry(
            DiffCategory.FEATURES,
            DiffOperation.ADD,
            subject=str(action.feature),
            detail=("— permanent protocol upgrade",),
        ),
    )


@action_entries.register
def _(action: SetProperty) -> tuple[DiffEntry, ...]:
    return (
        _named_value_entry(
            DiffCategory.PROPERTIES, action.name, action.desired_value, action.observed_value
        ),
    )


@action_entries.register
def _(action: UnsetProperty) -> tuple[DiffEntry, ...]:
    return (DiffEntry(DiffCategory.PROPERTIES, DiffOperation.REMOVE, subject=action.name),)


@action_entries.register
def _(action: SetTableTag) -> tuple[DiffEntry, ...]:
    return (
        _named_value_entry(
            DiffCategory.TAGS, action.name, action.desired_value, action.observed_value
        ),
    )


@action_entries.register
def _(action: UnsetTableTag) -> tuple[DiffEntry, ...]:
    return (DiffEntry(DiffCategory.TAGS, DiffOperation.REMOVE, subject=action.name),)


@action_entries.register
def _(action: SetColumnTag) -> tuple[DiffEntry, ...]:
    return (
        _named_value_entry(
            DiffCategory.TAGS,
            f"column {action.column_name}.{action.name}",
            action.desired_value,
            action.observed_value,
        ),
    )


@action_entries.register
def _(action: UnsetColumnTag) -> tuple[DiffEntry, ...]:
    return (
        DiffEntry(
            DiffCategory.TAGS,
            DiffOperation.REMOVE,
            subject=f"column {action.column_name}.{action.name}",
        ),
    )


@action_entries.register
def _(action: SetPrimaryKey) -> tuple[DiffEntry, ...]:
    return (
        DiffEntry(
            DiffCategory.KEYS,
            DiffOperation.ADD,
            subject="primary key",
            detail=_columns_detail(action.primary_key.columns),
        ),
    )


@action_entries.register
def _(action: DropPrimaryKey) -> tuple[DiffEntry, ...]:
    return (DiffEntry(DiffCategory.KEYS, DiffOperation.REMOVE, subject="primary key"),)


@action_entries.register
def _(action: SetForeignKey) -> tuple[DiffEntry, ...]:
    # A table has one primary key but many foreign keys, so a foreign key's
    # local columns are part of its identity rather than a detail of it.
    local_columns = ", ".join(action.constraint.local_columns)
    return (
        DiffEntry(
            DiffCategory.KEYS,
            DiffOperation.ADD,
            subject=f"foreign key ({local_columns})",
            detail=(f"→ {action.constraint.referenced_table}",),
        ),
    )


@action_entries.register
def _(action: DropForeignKey) -> tuple[DiffEntry, ...]:
    return (
        DiffEntry(
            DiffCategory.KEYS,
            DiffOperation.REMOVE,
            subject=f"foreign key {action.constraint.constraint_name}",
        ),
    )


_OPTIMIZE_FULL_HINT: Final[str] = "run OPTIMIZE FULL to recluster existing data"


@action_entries.register
def _(action: AlterClustering) -> tuple[DiffEntry, ...]:
    # No hint on removal: OPTIMIZE FULL errors on a table without clustering
    # columns (DELTA_OPTIMIZE_FULL_NOT_SUPPORTED); existing files simply keep
    # their old layout after CLUSTER BY NONE.
    if not action.desired_clustering:
        return (DiffEntry(DiffCategory.CLUSTERING, DiffOperation.REMOVE, subject="clustering"),)
    return (
        DiffEntry(
            DiffCategory.CLUSTERING,
            DiffOperation.CHANGE,
            subject="clustering",
            detail=(*_columns_detail(action.desired_clustering), f"— {_OPTIMIZE_FULL_HINT}"),
        ),
    )


@functools.singledispatch
def unresolvable_entries(unresolvable: Unresolvable) -> tuple[DiffEntry, ...]:
    """
    Describe one unresolvable difference as category-tagged diff entries.

    The sibling of :func:`action_entries` for differences no action can close.
    Every entry is a ``CHANGE``: an unresolvable difference is neither an
    addition nor a removal the engine could make, it is a disagreement between
    the declaration and the catalog that something outside this sync must
    settle.
    """
    raise NotImplementedError(f"No diff entries for unresolvable {type(unresolvable).__name__}")


@unresolvable_entries.register
def _(unresolvable: ColumnCaseDrift) -> tuple[DiffEntry, ...]:
    return (
        DiffEntry(
            DiffCategory.COLUMNS,
            DiffOperation.CHANGE,
            subject=unresolvable.declared_name,
            detail=(f"spelled '{unresolvable.observed_name}' in the catalog",),
        ),
    )


@unresolvable_entries.register
def _(unresolvable: ColumnRenameConflict) -> tuple[DiffEntry, ...]:
    return (
        DiffEntry(
            DiffCategory.COLUMNS,
            DiffOperation.CHANGE,
            subject=unresolvable.old_name,
            detail=(f"renamed → {unresolvable.new_name}, but both columns exist",),
        ),
    )


@unresolvable_entries.register
def _(unresolvable: PropertyUndeclared) -> tuple[DiffEntry, ...]:
    # Same two-phrase shape as a property change, so the trailing clause aligns
    # into its own column down a group of property lines.
    return (
        DiffEntry(
            DiffCategory.PROPERTIES,
            DiffOperation.CHANGE,
            subject=unresolvable.name,
            detail=(f"= '{unresolvable.observed_value}'", "(set on the table, undeclared)"),
        ),
    )


@unresolvable_entries.register
def _(unresolvable: PartitioningChanged) -> tuple[DiffEntry, ...]:
    observed = ", ".join(unresolvable.observed_partitioning)
    desired = ", ".join(unresolvable.desired_partitioning)
    return (
        DiffEntry(
            DiffCategory.PARTITIONING,
            DiffOperation.CHANGE,
            subject="partitioning",
            detail=(f"({observed}) → ({desired})",),
        ),
    )
