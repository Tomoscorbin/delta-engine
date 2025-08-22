from __future__ import annotations

from dataclasses import dataclass
from tabula.domain.model.data_type.data_type import DataType
from tabula.domain.model._identifiers import normalize_identifier

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
        object.__setattr__(self, "name", normalize_identifier(self.name))
