"""Immutable fully qualified table name."""

from __future__ import annotations

from dataclasses import dataclass

from delta_engine.domain.model.identifier import Identifier


@dataclass(frozen=True, slots=True)
class QualifiedName:
    """Case-insensitive, fully qualified identifier (catalog.schema.name).

    Attributes:
        catalog: Catalog name.
        schema: Schema name.
        name: Table or view name.

    """

    catalog: Identifier
    schema: Identifier
    name: Identifier

    def __post_init__(self) -> None:
        object.__setattr__(self, "catalog", Identifier(self.catalog))
        object.__setattr__(self, "schema", Identifier(self.schema))
        object.__setattr__(self, "name", Identifier(self.name))

    def __str__(self) -> str:
        parts = [p for p in (self.catalog, self.schema, self.name) if p]
        return ".".join(parts)

    @property
    def fully_qualified_name(self) -> str:
        return str(self)
