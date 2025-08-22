from __future__ import annotations

from dataclasses import dataclass
from tabula.domain.model.data_type import DataType
from tabula.domain._identifiers import normalize_identifier

@dataclass(frozen=True, slots=True)
class Column:
    """
    Immutable, case-insensitive column definition.

    - `name` is normalized by `normalize_identifier` (ASCII-only, no whitespace/dot, lowercased)
    - `data_type` is a DataType value object
    - `is_nullable` defaults to True
    """
    name: str
    data_type: DataType
    is_nullable: bool = True

    def __post_init__(self) -> None:
        if not isinstance(self.data_type, DataType):
            raise TypeError(f"data_type must be DataType, got {type(self.data_type).__name__}")
        object.__setattr__(self, "name", normalize_identifier(self.name))
