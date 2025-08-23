"""Domain model for table columns."""

from __future__ import annotations

from dataclasses import dataclass

from tabula.domain.model._identifiers import normalize_identifier
from tabula.domain.model.data_type.data_type import DataType


@dataclass(frozen=True, slots=True)
class Column:
    """Immutable, case-insensitive column definition.

    Attributes:
        name: Column name (normalized to lowercase).
        data_type: Logical data type of the column.
        is_nullable: Whether the column accepts ``NULL`` values.
    """

    name: str
    data_type: DataType
    is_nullable: bool = True

    def __post_init__(self) -> None:
        object.__setattr__(self, "name", normalize_identifier(self.name))
