"""Domain model for table columns."""

from __future__ import annotations

from collections.abc import Mapping
from dataclasses import dataclass, field

from delta_engine.domain.model.data_type import DataType


@dataclass(frozen=True, slots=True)
class Column:
    """
    Immutable, case-insensitive column definition.

    Attributes:
        name: Column name (normalized to lowercase).
        data_type: Logical data type of the column.
        nullable: Whether the column accepts ``NULL`` values.
        comment: Optional column comment.
        primary_key: Whether this column is part of the table's primary key.
            Used by the API layer to derive the table-level primary key tuple.
        tags: Read-only mapping of Unity Catalog column tag keys to values. Tag
            keys are case-sensitive and are stored verbatim (never casefolded,
            unlike the column name).

    """

    name: str
    data_type: DataType
    nullable: bool = True
    comment: str = ""
    primary_key: bool = False
    tags: Mapping[str, str] = field(default_factory=dict)

    def __post_init__(self) -> None:
        """Raise if the name is blank/uppercase, or a tag key is blank."""
        if not self.name.strip():
            raise ValueError(f"Column name must not be blank: {self.name!r}")
        if self.name != self.name.casefold():
            raise ValueError(f"Column name must be lowercase: {self.name!r}")
        for tag_key in self.tags:
            if not tag_key.strip():
                raise ValueError(f"Tag key must not be blank: {tag_key!r}")
