from __future__ import annotations
from dataclasses import dataclass
from typing import Tuple, Union

Param = Union[int, "DataType"]

@dataclass(frozen=True)
class DataType:
    """Logical, engine-agnostic data type definition."""
    name: str
    parameters: Tuple[Param, ...] = ()

    @property
    def specification(self) -> str:
        """Canonical representation like 'decimal(18,2)' or 'string'."""
        t = self.name.lower()
        if not self.parameters:
            return t
        parts = [p.specification if isinstance(p, DataType) else str(p) for p in self.parameters]
        return f"{t}({','.join(parts)})"

    def __str__(self) -> str:
        return self.specification