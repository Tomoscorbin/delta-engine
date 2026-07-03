"""Immutable fully qualified table name."""

from __future__ import annotations

from dataclasses import dataclass
from typing import Self


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

    def __post_init__(self) -> None:
        """Raise if any part is blank or contains uppercase characters."""
        for field_name, value in (
            ("catalog", self.catalog),
            ("schema", self.schema),
            ("name", self.name),
        ):
            if not value.strip():
                raise ValueError(f"QualifiedName {field_name} must not be blank: {value!r}")
            if value != value.casefold():
                raise ValueError(f"QualifiedName {field_name} must be lowercase: {value!r}")

    def __str__(self) -> str:
        """Return the canonical fully qualified string ``catalog.schema.name``."""
        return f"{self.catalog}.{self.schema}.{self.name}"

    @property
    def parts(self) -> tuple[str, str, str]:
        """Return the parts of the qualified name as a tuple."""
        return self.catalog, self.schema, self.name
