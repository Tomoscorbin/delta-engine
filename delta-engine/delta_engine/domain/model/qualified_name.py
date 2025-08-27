"""Immutable fully qualified table name."""

from __future__ import annotations

from dataclasses import dataclass



@dataclass(frozen=True, slots=True)
class QualifiedName:
    """Case-insensitive, fully qualified identifier (catalog.schema.name).

    Attributes:
        catalog: Catalog name.
        schema: Schema name.
        name: Table or view name.

    """

    catalog: str
    schema: str
    name: str

    def __str__(self) -> str:
        parts = [p for p in (self.catalog, self.schema, self.name) if p]
        return ".".join(parts)

    @property
    def fully_qualified_name(self) -> str:
        return str(self)
