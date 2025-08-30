"""Domain model for table columns."""

from __future__ import annotations

from dataclasses import dataclass

from delta_engine.domain.model.data_type import DataType
from delta_engine.domain.model.normalise_identifier import normalize_identifier


@dataclass(frozen=True, slots=True)
class Column:
    """
    Immutable, case-insensitive column definition.

    Attributes:
        name: Column name (normalized to lowercase).
        data_type: Logical data type of the column.
        is_nullable: Whether the column accepts ``NULL`` values.

    """

    name: str
    data_type: DataType
    is_nullable: bool = True

    def __post_init__(self) -> None:
        """Normalize the column name to a valid, case-insensitive form."""
        object.__setattr__(self, "name", normalize_identifier(self.name))
