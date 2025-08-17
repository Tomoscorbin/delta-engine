from __future__ import annotations
from dataclasses import dataclass
from typing import Optional
from .data_type import DataType

@dataclass(frozen=True, slots=True)
class Column:
    """Immutable column definition used in TableSpec/TableState."""
    name: str
    data_type: DataType
    is_nullable: bool = True

    @property
    def specification(self) -> str:
        """
        Human-readable spec string like:
          'amount decimal(18,2) not null' or 'notes string'
        """
        nullness = "" if self.is_nullable else " not null"
        return f"{self.name} {self.data_type.specification}{nullness}"

    def __str__(self) -> str:
        return self.specification