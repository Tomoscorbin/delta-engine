"""Public schema container for describing a Delta table"""

from collections.abc import Sequence, Mapping
from dataclasses import dataclass, field
from typing import ClassVar
from types import MappingProxyType
from typing import Mapping

from delta_engine.adapters.schema import Column
from delta_engine.adapters.schema.delta.properties import Property
from delta_engine.domain.normalise_identifier import normalise_identifier


# does user facing need to be frozen? 
# maybe this shouldnt ba dataclass at all
@dataclass(frozen=True, slots=True)
class DeltaTable:
    """Defines a Delta table schema."""

    default_properties: ClassVar[Mapping[str, str]] = MappingProxyType({
        Property.ENABLE_DELETION_VECTORS.value: "true",
        Property.COLUMN_MAPPING_MODE.value: "name",
    })
    
    catalog: str
    schema: str
    name: str
    columns: Sequence[Column]
    properties: dict[str, str] = field(default_factory=dict)

    def __post_init__(self) -> None:
        """Validate identifiers and ensure column names are unique (casefolded)."""
        normalise_identifier(self.catalog)
        normalise_identifier(self.schema)       # TODO: raise exception instead of coerce?
        normalise_identifier(self.name)

        # check for duplicate columns
        seen: set[str] = set()
        for c in self.columns:
            normalise_identifier(c.name)    # same here - exception instead of coerce
            n = c.name.casefold()
            if n in seen:
                raise ValueError(f"Duplicate column name: {c.name}")
            seen.add(n)

        #TODO: enforce allowed properties

    @property
    def effective_properties(self) -> dict[str, str]:
        """
        Defaults overlaid by user properties (user wins).
        """
        return {**self.default_properties, **self.properties}
