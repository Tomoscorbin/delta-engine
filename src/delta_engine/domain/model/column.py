"""Domain model for table columns."""

from collections.abc import Mapping
from dataclasses import dataclass, field
from types import MappingProxyType

from delta_engine.domain.model.data_type import DataType


def _validate_column_fields(name: str, tags: Mapping[str, str]) -> None:
    """Invariants shared by declared and observed columns."""
    if not name.strip():
        raise ValueError(f"Column name must not be blank: {name!r}")
    if name != name.casefold():
        raise ValueError(f"Column name must be lowercase: {name!r}")
    for tag_key in tags:
        if not tag_key.strip():
            raise ValueError(f"Tag key must not be blank: {tag_key!r}")


@dataclass(frozen=True, slots=True)
class Column:
    """
    Immutable, case-insensitive column declaration (desired state).

    Attributes:
        name: Column name (normalized to lowercase).
        data_type: Logical data type of the column.
        nullable: Whether the column accepts ``NULL`` values.
        comment: Optional column comment.
        tags: Read-only mapping of Unity Catalog column tag keys to values. Tag
            keys are case-sensitive and are stored verbatim (never casefolded,
            unlike the column name).

    """

    name: str
    data_type: DataType
    nullable: bool = True
    comment: str = ""
    tags: Mapping[str, str] = field(default_factory=dict)

    def __post_init__(self) -> None:
        object.__setattr__(self, "tags", MappingProxyType(dict(self.tags)))
        _validate_column_fields(self.name, self.tags)


@dataclass(frozen=True, slots=True)
class ObservedColumn:
    """
    Immutable column as observed in the catalog (current state).

    The observed counterpart of :class:`Column`: the same observable fields,
    and none of the declaration-only syntax, so catalog state cannot carry
    declaration history by construction. Only reader adapters build these.
    """

    name: str
    data_type: DataType
    nullable: bool = True
    comment: str = ""
    tags: Mapping[str, str] = field(default_factory=dict)

    def __post_init__(self) -> None:
        object.__setattr__(self, "tags", MappingProxyType(dict(self.tags)))
        _validate_column_fields(self.name, self.tags)
