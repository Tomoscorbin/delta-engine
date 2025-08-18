from __future__ import annotations

from dataclasses import dataclass
from tabula.domain.model.data_type import DataType


@dataclass(frozen=True, slots=True)
class Column:
    """
    Immutable, case-insensitive column definition.
    - name is normalized with .casefold()
    - Disallows leading/trailing or internal whitespace, and dots in the name
    """

    name: str
    data_type: DataType
    is_nullable: bool = True

    def __post_init__(self) -> None:
        if not isinstance(self.name, str):
            raise TypeError(f"Column.name must be str, got {type(self.name).__name__}")
        if not isinstance(self.data_type, DataType):
            raise TypeError(f"data_type must be DataType, got {type(self.data_type).__name__}")
        if not self.name:
            raise ValueError("Column.name cannot be empty")
        if self.name.strip() != self.name:
            raise ValueError(f"Column.name must not have leading/trailing whitespace: {self.name!r}")
        if any(ch.isspace() for ch in self.name):
            raise ValueError("Column.name must not contain whitespace characters")
        if "." in self.name:
            raise ValueError("Column.name must not contain '.'")

        # Normalize case
        object.__setattr__(self, "name", self.name.casefold())

    def __repr__(self) -> str:
        return f"Column(name={self.name!r}, data_type={self.data_type!r}, is_nullable={self.is_nullable})"

    def __str__(self) -> str:
        suffix = "" if self.is_nullable else " not null"
        return f"{self.name} {self.data_type.specification}{suffix}"
