"""Public schema container for describing a Delta table."""

from collections.abc import Iterable, Mapping
from types import MappingProxyType
from typing import ClassVar

from delta_engine.adapters.schema import Column
from delta_engine.adapters.schema.delta.properties import Property
from delta_engine.domain.model import TableFormat


class DeltaTable:
    """Defines a Delta table schema."""

    format: ClassVar[TableFormat] = TableFormat.DELTA
    default_properties: ClassVar[Mapping[str, str]] = MappingProxyType(
        {
            Property.ENABLE_DELETION_VECTORS.value: "true",
            Property.COLUMN_MAPPING_MODE.value: "name",
        }
    )

    # lookup set of supported property keys
    _allowed_property_keys: ClassVar[frozenset[str]] = frozenset(p.value for p in Property)

    def __init__(
        self,
        catalog: str,
        schema: str,
        name: str,
        columns: Iterable[Column],
        comment: str = "",
        properties: dict[str, str] | None = None,
        partitioned_by: Iterable[str] | None = None,
    ) -> None:
        self.catalog = catalog
        self.schema = schema
        self.name = name
        # Materialize columns to allow safe repeated iteration
        self.columns = tuple(columns)
        self.comment = comment
        self.properties = dict(properties or {})
        self.partitioned_by = partitioned_by

        # Validate provided properties exist in the supported Property enum
        if self.properties:
            unknown = [k for k in self.properties.keys() if k not in self._allowed_property_keys]
            if unknown:
                raise ValueError(f"Unknown Delta table properties: {', '.join(sorted(unknown))}")

        # Validate that partition columns exist among defined columns
        if self.partitioned_by:
            seen = {c.name.casefold() for c in self.columns}
            for p in self.partitioned_by:
                if p.casefold() not in seen:
                    raise ValueError(f"Partition column not found: {p}")

    @property
    def effective_properties(self) -> Mapping[str, str]:
        """Defaults overlaid by user properties (user wins)."""
        return {**self.default_properties, **self.properties}
