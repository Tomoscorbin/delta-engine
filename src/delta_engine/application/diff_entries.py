"""
Interpretation of plan actions as category-tagged diff entries.

The shared meaning layer between the two report views: text rendering
(`rendering.py`) and the machine projection (`SyncReport.to_dict`). States
what each action *means* (category, operation, subject cells); presentation
(grouping, grids, dict shapes) belongs to the consumers.
"""

from collections.abc import Mapping
from dataclasses import dataclass
from enum import IntEnum, StrEnum
import functools
from types import MappingProxyType
from typing import Final, assert_never

from delta_engine.domain.model import DesiredColumn
from delta_engine.domain.plan import (
    Action,
    AddColumn,
    AlterClustering,
    AlterColumnType,
    CreateTable,
    DropColumn,
    DropForeignKey,
    DropPrimaryKey,
    EnableTableFeature,
    RenameColumn,
    SetColumnComment,
    SetColumnNullability,
    SetColumnTag,
    SetForeignKey,
    SetPrimaryKey,
    SetProperty,
    SetTableComment,
    SetTableTag,
    UnsetColumnTag,
    UnsetProperty,
    UnsetTableTag,
)


class DiffCategory(IntEnum):
    """Diff line groups, in display order (enum value = order)."""

    COLUMNS = 1
    KEYS = 2
    CLUSTERING = 3
    PARTITIONING = 4
    FEATURES = 5
    PROPERTIES = 6
    TAGS = 7
    COMMENTS = 8


# (singular, plural) nouns per category: the plural names the diff group heading;
# the singular is used in the grid's humanized detail count.
CATEGORY_NOUN: Final[Mapping[DiffCategory, tuple[str, str]]] = MappingProxyType(
    {
        DiffCategory.COLUMNS: ("column", "columns"),
        DiffCategory.KEYS: ("key", "keys"),
        DiffCategory.CLUSTERING: ("clustering", "clustering"),
        DiffCategory.PARTITIONING: ("partitioning", "partitioning"),
        DiffCategory.FEATURES: ("table feature", "table features"),
        DiffCategory.PROPERTIES: ("property", "properties"),
        DiffCategory.TAGS: ("tag", "tags"),
        DiffCategory.COMMENTS: ("comment", "comments"),
    }
)


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
    """One interpreted diff line: its category, operation, and aligned cells."""

    category: DiffCategory
    operation: DiffOperation
    cells: tuple[str, ...]

    @property
    def symbol(self) -> str:
        """The +/-/~ character the text renderer prefixes this line with."""
        return self.operation.symbol

    @property
    def subject(self) -> str:
        """What the entry targets, e.g. a column name — the first cell."""
        return self.cells[0]

    @property
    def detail(self) -> str:
        """The remaining cells joined as extra detail; empty when there is none."""
        return " ".join(self.cells[1:])


def _column_add_entry(column: DesiredColumn) -> DiffEntry:
    """Build a '+' columns entry for a created column (name, type, optional NOT NULL)."""
    cells = [column.name, str(column.data_type)]
    if not column.nullable:
        cells.append("NOT NULL")
    return DiffEntry(DiffCategory.COLUMNS, DiffOperation.ADD, tuple(cells))


@functools.singledispatch
def action_entries(action: Action) -> tuple[DiffEntry, ...]:
    """Render one plan action as one or more category-tagged diff entries."""
    raise NotImplementedError(f"No diff entries for action {type(action).__name__}")


@action_entries.register
def _(action: CreateTable) -> tuple[DiffEntry, ...]:
    entries = [_column_add_entry(column) for column in action.table.columns]

    primary_key_columns = action.table.primary_key_columns
    if primary_key_columns:
        entries.append(
            DiffEntry(
                DiffCategory.KEYS,
                DiffOperation.ADD,
                (f"primary key ({', '.join(primary_key_columns)})",),
            )
        )

    if action.table.clustered_by:
        columns = ", ".join(action.table.clustered_by)
        entries.append(
            DiffEntry(DiffCategory.CLUSTERING, DiffOperation.ADD, (f"clustering ({columns})",))
        )

    if action.table.partitioned_by:
        columns = ", ".join(action.table.partitioned_by)
        entries.append(
            DiffEntry(DiffCategory.PARTITIONING, DiffOperation.ADD, (f"partitioning ({columns})",))
        )

    entries.extend(
        DiffEntry(DiffCategory.PROPERTIES, DiffOperation.ADD, (f"{name} = '{value}'",))
        for name, value in sorted(action.table.properties.items())
        if value is not None
    )

    entries.extend(
        DiffEntry(
            DiffCategory.COMMENTS, DiffOperation.ADD, (f"column {column.name}: '{column.comment}'",)
        )
        for column in action.table.columns
        if column.comment
    )
    if action.table.comment:
        entries.append(
            DiffEntry(
                DiffCategory.COMMENTS, DiffOperation.ADD, (f"table: '{action.table.comment}'",)
            )
        )

    return tuple(entries)


@action_entries.register
def _(action: AddColumn) -> tuple[DiffEntry, ...]:
    return (_column_add_entry(action.column),)


@action_entries.register
def _(action: DropColumn) -> tuple[DiffEntry, ...]:
    return (DiffEntry(DiffCategory.COLUMNS, DiffOperation.REMOVE, (action.column.name,)),)


@action_entries.register
def _(action: RenameColumn) -> tuple[DiffEntry, ...]:
    return (
        DiffEntry(
            DiffCategory.COLUMNS,
            DiffOperation.CHANGE,
            (action.old_name, f"renamed → {action.new_name}"),
        ),
    )


@action_entries.register
def _(action: SetColumnNullability) -> tuple[DiffEntry, ...]:
    if action.desired_nullable:
        change = "drop NOT NULL (was NOT NULL)"
    else:
        change = "set NOT NULL (was nullable)"
    return (DiffEntry(DiffCategory.COLUMNS, DiffOperation.CHANGE, (action.column_name, change)),)


@action_entries.register
def _(action: AlterColumnType) -> tuple[DiffEntry, ...]:
    change = f"{action.observed_type} → {action.desired_type}"
    return (DiffEntry(DiffCategory.COLUMNS, DiffOperation.CHANGE, (action.column_name, change)),)


@action_entries.register
def _(action: SetColumnComment) -> tuple[DiffEntry, ...]:
    if action.desired_comment:
        text = f"column {action.column_name}: '{action.desired_comment}'"
    else:
        text = f"column {action.column_name} comment (unset)"
    return (DiffEntry(DiffCategory.COMMENTS, DiffOperation.CHANGE, (text,)),)


@action_entries.register
def _(action: SetTableComment) -> tuple[DiffEntry, ...]:
    text = (
        f"table: '{action.desired_comment}'" if action.desired_comment else "table comment (unset)"
    )
    return (DiffEntry(DiffCategory.COMMENTS, DiffOperation.CHANGE, (text,)),)


@action_entries.register
def _(action: EnableTableFeature) -> tuple[DiffEntry, ...]:
    text = f"table feature {action.feature} — permanent protocol upgrade"
    return (DiffEntry(DiffCategory.FEATURES, DiffOperation.ADD, (text,)),)


@action_entries.register
def _(action: SetProperty) -> tuple[DiffEntry, ...]:
    if action.observed_value is None:
        return (
            DiffEntry(
                DiffCategory.PROPERTIES,
                DiffOperation.ADD,
                (f"{action.name} = '{action.desired_value}'",),
            ),
        )
    text = f"{action.name} = '{action.desired_value}' (was '{action.observed_value}')"
    return (DiffEntry(DiffCategory.PROPERTIES, DiffOperation.CHANGE, (text,)),)


@action_entries.register
def _(action: UnsetProperty) -> tuple[DiffEntry, ...]:
    return (DiffEntry(DiffCategory.PROPERTIES, DiffOperation.REMOVE, (action.name,)),)


@action_entries.register
def _(action: SetTableTag) -> tuple[DiffEntry, ...]:
    text = f"{action.name} = '{action.desired_value}'"
    if action.observed_value is None:
        return (DiffEntry(DiffCategory.TAGS, DiffOperation.ADD, (text,)),)
    return (
        DiffEntry(
            DiffCategory.TAGS, DiffOperation.CHANGE, (f"{text} (was '{action.observed_value}')",)
        ),
    )


@action_entries.register
def _(action: UnsetTableTag) -> tuple[DiffEntry, ...]:
    return (DiffEntry(DiffCategory.TAGS, DiffOperation.REMOVE, (action.name,)),)


@action_entries.register
def _(action: SetColumnTag) -> tuple[DiffEntry, ...]:
    text = f"column {action.column_name}.{action.name} = '{action.desired_value}'"
    if action.observed_value is None:
        return (DiffEntry(DiffCategory.TAGS, DiffOperation.ADD, (text,)),)
    return (
        DiffEntry(
            DiffCategory.TAGS, DiffOperation.CHANGE, (f"{text} (was '{action.observed_value}')",)
        ),
    )


@action_entries.register
def _(action: UnsetColumnTag) -> tuple[DiffEntry, ...]:
    return (
        DiffEntry(
            DiffCategory.TAGS, DiffOperation.REMOVE, (f"column {action.column_name}.{action.name}",)
        ),
    )


@action_entries.register
def _(action: SetPrimaryKey) -> tuple[DiffEntry, ...]:
    return (
        DiffEntry(
            DiffCategory.KEYS,
            DiffOperation.ADD,
            (f"primary key ({', '.join(action.primary_key.columns)})",),
        ),
    )


@action_entries.register
def _(action: DropPrimaryKey) -> tuple[DiffEntry, ...]:
    return (DiffEntry(DiffCategory.KEYS, DiffOperation.REMOVE, ("primary key",)),)


@action_entries.register
def _(action: SetForeignKey) -> tuple[DiffEntry, ...]:
    local_columns = ", ".join(action.constraint.local_columns)
    text = f"foreign key ({local_columns}) → {action.constraint.referenced_table}"
    return (DiffEntry(DiffCategory.KEYS, DiffOperation.ADD, (text,)),)


@action_entries.register
def _(action: DropForeignKey) -> tuple[DiffEntry, ...]:
    return (
        DiffEntry(
            DiffCategory.KEYS,
            DiffOperation.REMOVE,
            (f"foreign key {action.constraint.constraint_name}",),
        ),
    )


_OPTIMIZE_FULL_HINT: Final[str] = "run OPTIMIZE FULL to recluster existing data"


@action_entries.register
def _(action: AlterClustering) -> tuple[DiffEntry, ...]:
    # No hint on removal: OPTIMIZE FULL errors on a table without clustering
    # columns (DELTA_OPTIMIZE_FULL_NOT_SUPPORTED); existing files simply keep
    # their old layout after CLUSTER BY NONE.
    if not action.desired_clustering:
        return (DiffEntry(DiffCategory.CLUSTERING, DiffOperation.REMOVE, ("clustering",)),)
    columns = ", ".join(action.desired_clustering)
    text = f"clustering ({columns}) — {_OPTIMIZE_FULL_HINT}"
    return (DiffEntry(DiffCategory.CLUSTERING, DiffOperation.CHANGE, (text,)),)
