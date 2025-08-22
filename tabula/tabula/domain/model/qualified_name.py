from __future__ import annotations
from dataclasses import dataclass

from tabula.domain._identifiers import normalize_identifier


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
        self_catalog = normalize_identifier(self.catalog)
        self_schema  = normalize_identifier(self.schema)
        self_name    = normalize_identifier( self.name)

        # normalize case
        object.__setattr__(self, "catalog", self_catalog)
        object.__setattr__(self, "schema",  self_schema)
        object.__setattr__(self, "name",    self_name)

    @property
    def parts(self) -> tuple[str, str, str]:
        return (self.catalog, self.schema, self.name)

    @property
    def dotted(self) -> str:
        return ".".join(self.parts)
