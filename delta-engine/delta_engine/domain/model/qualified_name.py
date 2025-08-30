"""Immutable fully qualified table name."""

from __future__ import annotations

from dataclasses import dataclass

from delta_engine.domain.model.normalise_identifier import normalize_identifier


@dataclass(frozen=True, slots=True)
class QualifiedName:
    """
    Case-insensitive, fully qualified identifier (catalog.schema.name).

    Attributes:
        catalog: Catalog name.
        schema: Schema name.
        name: Table or view name.

    """

    catalog: str
    schema: str
    name: str

    def __post_init__(self) -> None:
        """Normalize all identifier parts to validated, case-insensitive forms."""
        object.__setattr__(self, "catalog", normalize_identifier(self.catalog))
        object.__setattr__(self, "schema", normalize_identifier(self.schema))
        object.__setattr__(self, "name", normalize_identifier(self.name))

    def __str__(self) -> str:
        """Return the canonical fully qualified string ``catalog.schema.name``."""
        return f"{self.catalog}.{self.schema}.{self.name}"

    @property
    def fully_qualified_name(self) -> str:
        """Return the string form of the qualified name."""
        return str(self)
