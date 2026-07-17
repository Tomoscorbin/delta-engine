"""Immutable fully qualified table name."""

from dataclasses import dataclass
from typing import Self


@dataclass(frozen=True, slots=True)
class QualifiedName:
    """
    Case-insensitive, fully qualified identifier (catalog.schema.name).

    Parts are stored in canonical lowercase: identifiers are case-insensitive
    on the platform and Unity Catalog stores object names lowercase, so two
    names differing only in case are the same identifier and construct equal.

    Attributes:
        catalog: Catalog name, lowercased.
        schema: Schema name, lowercased.
        name: Table or view name, lowercased.

    """

    catalog: str
    schema: str
    name: str

    def __post_init__(self) -> None:
        for field_name, value in (
            ("catalog", self.catalog),
            ("schema", self.schema),
            ("name", self.name),
        ):
            if not value.strip():
                raise ValueError(f"QualifiedName {field_name} must not be blank: {value!r}")
            object.__setattr__(self, field_name, value.lower())

    def __str__(self) -> str:
        """Return the canonical fully qualified string ``catalog.schema.name``."""
        return f"{self.catalog}.{self.schema}.{self.name}"

    @classmethod
    def parse(cls, raw: str) -> Self:
        """Parse a canonical ``catalog.schema.name`` string into a qualified name."""
        parts = raw.split(".")
        if len(parts) != 3:
            raise ValueError(
                f"QualifiedName must be a fully qualified 'catalog.schema.table' name; got: {raw!r}"
            )
        catalog, schema, name = parts
        return cls(catalog=catalog, schema=schema, name=name)

    @property
    def parts(self) -> tuple[str, str, str]:
        """Return the parts of the qualified name as a tuple."""
        return self.catalog, self.schema, self.name
