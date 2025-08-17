from __future__ import annotations
from dataclasses import dataclass
from typing import Self

@dataclass(frozen=True, slots=True)
class QualifiedName:
    """
    Case-insensitive, fully-qualified identifier (catalog.schema.name).
    Applies to tables, views, etc.
    - Normalizes all parts with .casefold()
    - Rejects empty/whitespace and dots inside parts
    """
    catalog: str
    schema: str
    name: str

    def __post_init__(self) -> None:
        for label, value in (("catalog", self.catalog), ("schema", self.schema), ("name", self.name)):
            if value is None:
                raise ValueError(f"{label} cannot be None")
            if value.strip() != value:
                raise ValueError(f"{label} must not have leading/trailing whitespace: {value!r}")
            if not value:
                raise ValueError(f"{label} cannot be empty")
            if "." in value:
                raise ValueError(f"{label} must not contain '.'")
        
        # normalize case
        object.__setattr__(self, "catalog", self.catalog.casefold())
        object.__setattr__(self, "schema", self.schema.casefold())
        object.__setattr__(self, "name", self.name.casefold())

    def __str__(self) -> str:
        return f"{self.catalog}.{self.schema}.{self.name}"
