"""Public schema container for describing a Delta table."""

from collections.abc import Iterable, Mapping
from types import MappingProxyType
from typing import ClassVar

from delta_engine.adapters.schema.delta.properties import MANAGED_PROPERTY_KEYS, Property
from delta_engine.domain.model import Column, DesiredTable, QualifiedName, TableFormat


class DeltaTable:
    """Defines a Delta table schema."""

    format: ClassVar[TableFormat] = TableFormat.DELTA
    default_properties: ClassVar[Mapping[str, str]] = MappingProxyType(
        {
            Property.ENABLE_DELETION_VECTORS.value: "true",
            Property.COLUMN_MAPPING_MODE.value: "name",
        }
    )

    _managed_property_keys: ClassVar[frozenset[str]] = MANAGED_PROPERTY_KEYS

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

        # Fast-fail on property keys this engine does not manage (e.g. typos)
        if self.properties:
            unmanaged = [k for k in self.properties if k not in self._managed_property_keys]
            if unmanaged:
                raise ValueError(
                    f"Properties not managed by this engine: {', '.join(sorted(unmanaged))}"
                )

    @property
    def effective_properties(self) -> Mapping[str, str]:
        """Defaults overlaid by user properties (user wins)."""
        return {**self.default_properties, **self.properties}

    def to_desired_table(self) -> DesiredTable:
        """
        Convert this user-facing definition into a domain :class:`DesiredTable`.

        Column, comment, property, and partition data are mapped onto the
        domain model. The domain model owns structural invariants (non-empty
        columns, unique names, partition columns must exist), so those are
        enforced here rather than duplicated on this user-facing type.

        Raises:
            ValueError: If the resulting table violates a domain invariant
                (e.g. a partition column not among the defined columns).

        """
        return DesiredTable(
            qualified_name=QualifiedName(self.catalog, self.schema, self.name),
            columns=tuple(self.columns),
            comment=self.comment,
            properties=self.effective_properties,
            partitioned_by=tuple(self.partitioned_by) if self.partitioned_by else (),
            format=self.format,
        )
