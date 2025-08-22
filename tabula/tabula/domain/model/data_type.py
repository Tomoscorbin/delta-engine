from __future__ import annotations
from dataclasses import dataclass
from typing import Union

from tabula.domain._identifiers import normalize_identifier
from tabula.domain.validation import coerce_params, validate_param_types, validate_by_type

Param = Union[int, "DataType"]


@dataclass(frozen=True, slots=True)
class DataType:
    """Engine-agnostic logical type. E.g., 'decimal(18,2)', 'bigint', 'array(int)'."""
    name: str
    parameters: tuple[Param, ...] = ()

    def __post_init__(self) -> None:
        normalized_name = normalize_identifier(self.name)
        params = coerce_params(self.parameters)
        validate_param_types(params)
        validate_by_type(normalized_name, params)

        object.__setattr__(self, "name", normalized_name)
        object.__setattr__(self, "parameters", params)

