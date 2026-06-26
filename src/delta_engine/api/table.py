"""Public API container for describing a Delta table."""

from collections.abc import Iterable, Mapping
from types import MappingProxyType
from typing import ClassVar

from delta_engine.domain.model import Column, DesiredTable, QualifiedName
from delta_engine.api.properties import MANAGED_PROPERTY_KEYS, Property


class DeltaTable:
    """
    Defines a Delta table schema.

    Note on dropping columns: Delta only permits ``ALTER TABLE ... DROP COLUMN``
    when ``delta.columnMapping.mode`` is ``name``, which is the default applied
    here. If you override that property to ``none``, a sync that removes a column
    will fail at execution time. Keep column mapping enabled on tables whose
    columns may be dropped.
    """

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
        partitioned_by: Iterable[str] | None = None,
    ) -> None:
        user_properties = dict(properties or {})

        # Fast-fail on property keys this engine does not manage (e.g. typos)
        if user_properties:
            unmanaged = [k for k in user_properties if k not in MANAGED_PROPERTY_KEYS]
            if unmanaged:
                raise ValueError(
                    f"Properties not managed by this engine: {', '.join(sorted(unmanaged))}"
                )

        effective = {**self.default_properties, **user_properties}

        columns_tuple = tuple(columns)
        primary_key = tuple(col.name for col in columns_tuple if col.primary_key)

        # Building DesiredTable here enforces all domain invariants (non-empty
        # columns, unique names, partition columns must exist) at construction
        # time rather than deferring them to to_desired_table().
        self._desired_table = DesiredTable(
            qualified_name=QualifiedName(catalog, schema, name),
            columns=columns_tuple,
            comment=comment,
            properties=effective,
            partitioned_by=tuple(partitioned_by) if partitioned_by is not None else (),
            primary_key=primary_key,
        )

    @property
    def effective_properties(self) -> Mapping[str, str]:
        """Defaults overlaid by user properties (user wins)."""
        return self._desired_table.properties

    @property
    def primary_key(self) -> tuple[str, ...]:
        """Column names declared as the primary key, in declaration order."""
        return self._desired_table.primary_key

    @property
    def primary_key_constraint_name(self) -> str | None:
        """The constraint name for this table's primary key, or None if no PK is defined."""
        return self._desired_table.primary_key_constraint_name

    def to_desired_table(self) -> DesiredTable:
        """Return the domain :class:`DesiredTable` for this table definition."""
        return self._desired_table
