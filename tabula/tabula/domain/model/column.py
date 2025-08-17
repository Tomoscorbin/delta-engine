from __future__ import annotations
from dataclasses import dataclass
from tabula.domain.model.data_type import DataType

@dataclass(frozen=True, slots=True)
class Column:
    """
    Immutable, case-insensitive column definition.
    - name is normalized with .casefold()
    """
    name: str
    data_type: DataType
    is_nullable: bool = True

    def __post_init__(self) -> None:
        if not self.name or not self.name.strip():
            raise ValueError("Column.name cannot be empty")
        object.__setattr__(self, "name", self.name.casefold())

    @property
    def specification(self) -> str:
        suffix = "" if self.is_nullable else " not null"
        return f"{self.name} {self.data_type.specification}{suffix}"

    def __str__(self) -> str:
        return self.specification
