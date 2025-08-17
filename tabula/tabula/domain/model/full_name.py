from __future__ import annotations
from dataclasses import dataclass

@dataclass(frozen=True)
class FullName:
    """Fully-qualified table identifier: catalog.schema.name."""
    catalog: str
    schema: str
    name: str

    def qualified_name(self) -> str:
        """Canonical dotted form: 'catalog.schema.name'."""
        return f"{self.catalog}.{self.schema}.{self.name}"

    def __str__(self) -> str:
        return self.qualified_name()