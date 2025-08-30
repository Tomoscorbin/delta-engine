"""
Public schema container for describing a Delta table.

`DeltaTable` is a thin, user-facing dataclass validated on init: identifiers
are normalized and duplicate column names are rejected to catch mistakes early.
"""

from collections.abc import Sequence
from dataclasses import dataclass

from delta_engine.domain.model.normalise_identifier import normalize_identifier
from delta_engine.schema.column import Column


@dataclass(frozen=True, slots=True)
class DeltaTable:
    """Convenience container for describing a Delta table schema."""

    catalog: str
    schema: str
    name: str
    columns: Sequence[Column]

    def __post_init__(self) -> None:
        normalize_identifier(self.catalog)
        normalize_identifier(self.schema)
        normalize_identifier(self.name)

        seen: set[str] = set()
        for c in self.columns:
            normalize_identifier(c.name)
            n = c.name.casefold()
            if n in seen:
                raise ValueError(f"Duplicate column name: {c.name}")
            seen.add(n)
