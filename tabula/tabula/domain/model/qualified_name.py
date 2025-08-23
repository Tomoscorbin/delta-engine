from __future__ import annotations

"""Immutable fully qualified table name."""

from dataclasses import dataclass

from tabula.domain.model._identifiers import normalize_identifier


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

    def __post_init__(self) -> None:
        self_catalog = normalize_identifier(self.catalog)
        self_schema = normalize_identifier(self.schema)
        self_name = normalize_identifier(self.name)

        object.__setattr__(self, "catalog", self_catalog)
        object.__setattr__(self, "schema", self_schema)
        object.__setattr__(self, "name", self_name)

    @property
    def parts(self) -> tuple[str, str, str]:
        """Return the identifier components as a tuple."""

        return (self.catalog, self.schema, self.name)

    @property
    def dotted(self) -> str:
        """Return the dotted ``catalog.schema.name`` string."""

        return ".".join(self.parts)
