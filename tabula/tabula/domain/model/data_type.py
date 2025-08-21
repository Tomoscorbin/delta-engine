from __future__ import annotations
from dataclasses import dataclass
from typing import Iterable, Union

Param = Union[int, "DataType"]


@dataclass(frozen=True, slots=True)
class DataType:
    """Engine-agnostic logical type. E.g., 'decimal(18,2)', 'bigint', 'array(int)'."""

    name: str
    parameters: tuple[Param, ...] = ()

    def __post_init__(self) -> None:
        # --- Type checks / coercions
        if not isinstance(self.name, str):
            raise TypeError(f"DataType.name must be str, got {type(self.name).__name__}")

        params = self.parameters
        # Validate each parameter type
        for p in params:
            if not isinstance(p, (int, DataType)):
                raise TypeError(f"Invalid parameter type: {type(p).__name__}; expected int or DataType")

        # --- Name validation
        if not self.name:
            raise ValueError("DataType.name cannot be empty")
        if self.name.strip() != self.name:
            raise ValueError(f"DataType.name must not have leading/trailing whitespace: {self.name!r}")
        if any(ch.isspace() for ch in self.name):
            raise ValueError("DataType.name must not contain whitespace characters")
        if "." in self.name:
            raise ValueError("DataType.name must not contain '.'")

        n = self.name.casefold()

        # --- Known type validations
        if n == "decimal":
            if len(params) != 2 or not all(isinstance(p, int) for p in params):
                raise ValueError("decimal requires (precision:int, scale:int)")
            precision, scale = params  # type: ignore[assignment]
            if precision <= 0 or not (0 <= scale <= precision):
                raise ValueError("invalid decimal precision/scale")

        # TODO: future: validate array/map/struct shapes

        # --- Finalize normalized state
        object.__setattr__(self, "name", n)
        object.__setattr__(self, "parameters", params)

