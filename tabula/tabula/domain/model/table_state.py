from __future__ import annotations
from dataclasses import dataclass
from typing import Tuple
from tabula.domain.model.full_name import FullName
from tabula.domain.model.column import Column

@dataclass(frozen=True)
class TableState:
    """
    The current state of an existing table.
    """
    full_name: FullName
    columns: Tuple[Column, ...]  # ordered, immutable

    def __contains__(self, item: str | Column) -> bool:
        if isinstance(item, str):
            return any(c.name == item for c in self.columns)
        return item in self.columns