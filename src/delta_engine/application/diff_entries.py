"""
Interpretation of plan actions as category-tagged diff entries.

The shared meaning layer between the two report views: text rendering
(`rendering.py`) and the machine projection (`SyncReport.to_dict`). States
what each action *means* (category, operation, subject cells); presentation
(grouping, grids, dict shapes) belongs to the consumers.
"""

from collections.abc import Mapping
from dataclasses import dataclass
from enum import IntEnum
import functools
from types import MappingProxyType
from typing import Final

from delta_engine.domain.model import Column, DataType, Decimal
from delta_engine.domain.plan import (
    Action,
    AddColumn,
    AlterClustering,
    AlterColumnType,
    CreateTable,
    DropColumn,
    DropForeignKey,
    DropPrimaryKey,
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


def _type_name(data_type: DataType) -> str:
    """Backend-agnostic display name for a domain data type (e.g. 'String')."""
    return type(data_type).__name__


def _type_display(data_type: DataType) -> str:
    """Display name including decimal parameters, so a precision widen is visible."""
    if isinstance(data_type, Decimal):
        return f"Decimal({data_type.precision},{data_type.scale})"
    return _type_name(data_type)


class DiffCategory(IntEnum):
    """Diff line groups, in display order (enum value = order)."""

    COLUMNS = 1
    KEYS = 2
    CLUSTERING = 3
    PROPERTIES = 4
    TAGS = 5
    COMMENTS = 6


# (singular, plural) nouns per category: the plural names the diff group heading;
# the singular is used in the grid's humanized detail count.
CATEGORY_NOUN: Final[Mapping[DiffCategory, tuple[str, str]]] = MappingProxyType(
    {
        DiffCategory.COLUMNS: ("column", "columns"),
        DiffCategory.KEYS: ("key", "keys"),
        DiffCategory.CLUSTERING: ("clustering", "clustering"),
        DiffCategory.PROPERTIES: ("property", "properties"),
        DiffCategory.TAGS: ("tag", "tags"),
        DiffCategory.COMMENTS: ("comment", "comments"),
    }
)


_OPERATION_FOR_SYMBOL: Final[Mapping[str, str]] = MappingProxyType(
    {"+": "add", "-": "remove", "~": "change"}
)


@dataclass(frozen=True, slots=True)
class DiffEntry:
    """One interpreted diff line: its category, +/-/~ symbol, and aligned cells."""

    category: DiffCategory
    symbol: str
    cells: tuple[str, ...]

    @property
    def operation(self) -> str:
        """The symbol as a stable word for machine consumers: add, remove, or change."""
        return _OPERATION_FOR_SYMBOL[self.symbol]

    @property
    def subject(self) -> str:
        """What the entry targets, e.g. a column name — the first cell."""
        return self.cells[0]

    @property
    def detail(self) -> str:
        """The remaining cells joined as extra detail; empty when there is none."""
        return " ".join(self.cells[1:])


def _column_add_entry(column: Column) -> DiffEntry:
    """Build a '+' columns entry for a created column (name, type, optional NOT NULL)."""
    cells = [column.name, _type_name(column.data_type)]
    if not column.nullable:
        cells.append("NOT NULL")
    return DiffEntry(DiffCategory.COLUMNS, "+", tuple(cells))


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
            DiffEntry(DiffCategory.KEYS, "+", (f"primary key ({', '.join(primary_key_columns)})",))
        )
    if action.table.clustered_by:
        columns = ", ".join(action.table.clustered_by)
        entries.append(DiffEntry(DiffCategory.CLUSTERING, "+", (f"clustering ({columns})",)))
    return tuple(entries)


@action_entries.register
def _(action: AddColumn) -> tuple[DiffEntry, ...]:
    return (_column_add_entry(action.column),)


@action_entries.register
def _(action: DropColumn) -> tuple[DiffEntry, ...]:
    return (DiffEntry(DiffCategory.COLUMNS, "-", (action.column_name,)),)


@action_entries.register
def _(action: RenameColumn) -> tuple[DiffEntry, ...]:
    return (
        DiffEntry(DiffCategory.COLUMNS, "~", (action.old_name, f"renamed → {action.new_name}")),
    )


@action_entries.register
def _(action: SetColumnNullability) -> tuple[DiffEntry, ...]:
    change = "drop NOT NULL (was NOT NULL)" if action.nullable else "set NOT NULL (was nullable)"
    return (DiffEntry(DiffCategory.COLUMNS, "~", (action.column_name, change)),)


@action_entries.register
def _(action: AlterColumnType) -> tuple[DiffEntry, ...]:
    change = f"{_type_display(action.observed_type)} → {_type_display(action.data_type)}"
    return (DiffEntry(DiffCategory.COLUMNS, "~", (action.column_name, change)),)


@action_entries.register
def _(action: SetColumnComment) -> tuple[DiffEntry, ...]:
    if action.comment:
        text = f"column {action.column_name}: '{action.comment}'"
    else:
        text = f"column {action.column_name} comment (unset)"
    return (DiffEntry(DiffCategory.COMMENTS, "~", (text,)),)


@action_entries.register
def _(action: SetTableComment) -> tuple[DiffEntry, ...]:
    text = f"table: '{action.comment}'" if action.comment else "table comment (unset)"
    return (DiffEntry(DiffCategory.COMMENTS, "~", (text,)),)


@action_entries.register
def _(action: SetProperty) -> tuple[DiffEntry, ...]:
    if action.observed_value is None:
        return (DiffEntry(DiffCategory.PROPERTIES, "+", (f"{action.name} = '{action.value}'",)),)
    text = f"{action.name} = '{action.value}' (was '{action.observed_value}')"
    return (DiffEntry(DiffCategory.PROPERTIES, "~", (text,)),)


@action_entries.register
def _(action: UnsetProperty) -> tuple[DiffEntry, ...]:
    return (DiffEntry(DiffCategory.PROPERTIES, "-", (action.name,)),)


@action_entries.register
def _(action: SetTableTag) -> tuple[DiffEntry, ...]:
    return (DiffEntry(DiffCategory.TAGS, "~", (f"{action.name} = '{action.value}'",)),)


@action_entries.register
def _(action: UnsetTableTag) -> tuple[DiffEntry, ...]:
    return (DiffEntry(DiffCategory.TAGS, "-", (action.name,)),)


@action_entries.register
def _(action: SetColumnTag) -> tuple[DiffEntry, ...]:
    text = f"column {action.column_name}.{action.name} = '{action.value}'"
    return (DiffEntry(DiffCategory.TAGS, "~", (text,)),)


@action_entries.register
def _(action: UnsetColumnTag) -> tuple[DiffEntry, ...]:
    return (DiffEntry(DiffCategory.TAGS, "-", (f"column {action.column_name}.{action.name}",)),)


@action_entries.register
def _(action: SetPrimaryKey) -> tuple[DiffEntry, ...]:
    return (DiffEntry(DiffCategory.KEYS, "+", (f"primary key ({', '.join(action.columns)})",)),)


@action_entries.register
def _(action: DropPrimaryKey) -> tuple[DiffEntry, ...]:
    return (DiffEntry(DiffCategory.KEYS, "-", ("primary key",)),)


@action_entries.register
def _(action: SetForeignKey) -> tuple[DiffEntry, ...]:
    text = f"foreign key ({', '.join(action.local_columns)}) → {action.referenced_table}"
    return (DiffEntry(DiffCategory.KEYS, "+", (text,)),)


@action_entries.register
def _(action: DropForeignKey) -> tuple[DiffEntry, ...]:
    return (DiffEntry(DiffCategory.KEYS, "-", (f"foreign key {action.constraint_name}",)),)


_OPTIMIZE_FULL_HINT: Final[str] = "run OPTIMIZE FULL to recluster existing data"


@action_entries.register
def _(action: AlterClustering) -> tuple[DiffEntry, ...]:
    # No hint on removal: OPTIMIZE FULL errors on a table without clustering
    # columns (DELTA_OPTIMIZE_FULL_NOT_SUPPORTED); existing files simply keep
    # their old layout after CLUSTER BY NONE.
    if not action.columns:
        return (DiffEntry(DiffCategory.CLUSTERING, "-", ("clustering",)),)
    columns = ", ".join(action.columns)
    text = f"clustering ({columns}) — {_OPTIMIZE_FULL_HINT}"
    return (DiffEntry(DiffCategory.CLUSTERING, "~", (text,)),)
