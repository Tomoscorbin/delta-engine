"""Public schema container for describing a Delta table."""

from collections.abc import Iterable, Mapping
from types import MappingProxyType
from typing import ClassVar

from delta_engine.adapters.schema import Column
from delta_engine.adapters.schema.delta.properties import Property
from delta_engine.domain.normalise_identifier import normalise_identifier


# TODO: add validation - allowed props, duplicate cols, etc
class DeltaTable:
    """Defines a Delta table schema."""

    default_properties: ClassVar[Mapping[str, str]] = MappingProxyType(
        {
            Property.ENABLE_DELETION_VECTORS.value: "true",
            Property.COLUMN_MAPPING_MODE.value: "name",
        }
    )

    def __init__(
        self,
        catalog: str,
        schema: str,
        name: str,
        columns: Iterable[Column],
        comment: str = "",
        properties: dict[str, str] | None = None,
    ) -> None:
        self.catalog = normalise_identifier(catalog)
        self.schema = normalise_identifier(schema)
        self.name = normalise_identifier(name)
        self.columns = columns
        self.comment = comment
        self.properties = dict(properties or {})

    @property
    def effective_properties(self) -> Mapping[str, str]:
        """Defaults overlaid by user properties (user wins)."""
        return {**self.default_properties, **self.properties}
