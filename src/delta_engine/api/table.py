"""Public API container for describing a Delta table."""

from collections.abc import Iterable, Mapping
from types import MappingProxyType
from typing import ClassVar

from delta_engine.api.foreign_key import ForeignKey, _SelfReference
from delta_engine.api.properties import MANAGED_PROPERTY_KEYS, Property
from delta_engine.domain.model import Column, DesiredTable, QualifiedName
from delta_engine.domain.model.foreign_key import ForeignKeyConstraint
from delta_engine.domain.model.primary_key import PrimaryKeyConstraint


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
        tags: dict[str, str] | None = None,
        partitioned_by: Iterable[str] | None = None,
        foreign_keys: Iterable[ForeignKey] | None = None,
    ) -> None:
        user_properties = dict(properties or {})

        # Fast-fail on property keys this engine does not manage (e.g. typos)
        if user_properties:
            unmanaged = [k for k in user_properties if k not in MANAGED_PROPERTY_KEYS]
            if unmanaged:
                raise ValueError(
                    f"Properties not managed by this engine: {', '.join(sorted(unmanaged))}"
                )

        effective_properties = {**self.default_properties, **user_properties}

        columns = tuple(columns)
        primary_key_columns = tuple(column.name for column in columns if column.primary_key)
        primary_key = (
            PrimaryKeyConstraint(columns=primary_key_columns) if primary_key_columns else None
        )

        qualified_name = QualifiedName(catalog, schema, name)
        lowered_foreign_keys = tuple(
            _lower_foreign_key(declaration, qualified_name, primary_key_columns)
            for declaration in (foreign_keys or ())
        )

        # Building DesiredTable here enforces all domain invariants (non-empty
        # columns, unique names, partition columns must exist, FK local columns
        # must exist) at construction time rather than deferring them to
        # to_desired_table().
        self._desired_table = DesiredTable(
            qualified_name=qualified_name,
            columns=columns,
            comment=comment,
            properties=effective_properties,
            tags=dict(tags or {}),
            partitioned_by=tuple(partitioned_by) if partitioned_by is not None else (),
            primary_key=primary_key,
            foreign_keys=lowered_foreign_keys,
        )

    @property
    def effective_properties(self) -> Mapping[str, str]:
        """Defaults overlaid by user properties (user wins)."""
        return self._desired_table.properties

    @property
    def primary_key(self) -> tuple[str, ...]:
        """Column names declared as the primary key, in declaration order."""
        constraint = self._desired_table.primary_key
        return constraint.columns if constraint is not None else ()

    @property
    def foreign_keys(self) -> tuple[ForeignKeyConstraint, ...]:
        """Foreign key constraints declared on this table."""
        return self._desired_table.foreign_keys

    def to_desired_table(self) -> DesiredTable:
        """Return the domain :class:`DesiredTable` for this table definition."""
        return self._desired_table


def _lower_foreign_key(
    declaration: ForeignKey,
    owner_name: QualifiedName,
    owner_primary_key: tuple[str, ...],
) -> ForeignKeyConstraint:
    """Lower a public FK declaration into a domain constraint, inferring referenced columns."""
    reference = declaration.references
    if isinstance(reference, _SelfReference):
        referenced_table = owner_name
        referenced_columns = owner_primary_key
    elif isinstance(reference, DeltaTable):
        referenced = reference.to_desired_table()
        referenced_table = referenced.qualified_name
        referenced_columns = (
            referenced.primary_key.columns if referenced.primary_key is not None else ()
        )
    else:
        raise TypeError(f"foreign key references must be a DeltaTable or Self; got {reference!r}")

    if not referenced_columns:
        raise ValueError(
            f"cannot infer referenced columns: {referenced_table} declares no primary key"
        )
    if len(declaration.local_columns) != len(referenced_columns):
        raise ValueError(
            "foreign key local column count must match the referenced primary key;"
            f" {owner_name} declares {len(declaration.local_columns)} local column(s)"
            f" but {referenced_table}'s primary key has {len(referenced_columns)}"
        )
    return ForeignKeyConstraint(
        local_columns=tuple(declaration.local_columns),
        referenced_table=referenced_table,
        referenced_columns=tuple(referenced_columns),
    )
