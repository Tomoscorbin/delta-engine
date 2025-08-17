from __future__ import annotations
from dataclasses import dataclass
from typing import Tuple, Union

Param = Union[int, "DataType"]

@dataclass(frozen=True, slots=True)
class DataType:
    """Engine-agnostic logical type. E.g., 'decimal(18,2)', 'bigint', 'array<int>'."""
    name: str
    parameters: Tuple[Param, ...] = ()

    def __post_init__(self) -> None:
        if not self.name or not self.name.strip():
            raise ValueError("DataType.name cannot be empty")
        n = self.name.casefold()
        # lightweight checks for common parameterized types
        if n == "decimal":
            if len(self.parameters) != 2 or not all(isinstance(p, int) for p in self.parameters):
                raise ValueError("decimal requires (precision:int, scale:int)")
            precision, scale = self.parameters  # type: ignore[assignment]
            if precision <= 0 or not (0 <= scale <= precision):
                raise ValueError("invalid decimal precision/scale")
        
        #TODO: (future: validate array/map/struct shapes)
        object.__setattr__(self, "name", n)

    @property
    def specification(self) -> str:
        if not self.parameters:
            return self.name
        parts = [p.specification if isinstance(p, DataType) else str(p) for p in self.parameters]
        return f"{self.name}({','.join(parts)})"

    def __str__(self) -> str:
        return self.specification
