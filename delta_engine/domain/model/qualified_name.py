"""Immutable fully qualified table name."""

from __future__ import annotations

from dataclasses import dataclass


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

    def __str__(self) -> str:
        """Return the canonical fully qualified string ``catalog.schema.name``."""
        return f"{self.catalog}.{self.schema}.{self.name}"

    @property  # TODO: rename to fully_qualified
    def fully_qualified_name(self) -> str:
        """Return the string form of the qualified name."""
        return str(self)

    @property
    def parts(self) -> tuple[str, str, str]:
        """Return the parts of the qualified name as a tuple."""
        return self.catalog, self.schema, self.name
