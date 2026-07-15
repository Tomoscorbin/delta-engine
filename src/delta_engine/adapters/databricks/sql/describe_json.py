"""
Parse a ``DESCRIBE TABLE EXTENDED <table> AS JSON`` document into a table snapshot.

Column types arrive as structured objects keyed by ``name`` (never DDL
strings), so this is the structured twin of the write path's type rendering.
The one embedded formatted string — ``table_constraints`` — is parsed by
``constraints.py`` and is documented there as less structurally stable.
"""

from collections.abc import Mapping
from dataclasses import dataclass
import json
import logging
from types import MappingProxyType
from typing import Final

from delta_engine.adapters.databricks.sql.constraints import (
    ParsedConstraints,
    parse_table_constraints,
)
from delta_engine.application.properties import DELTA_PROPERTY_REGISTRY
from delta_engine.domain.model import (
    Array,
    Binary,
    Boolean,
    Byte,
    DataType,
    Date,
    Decimal,
    Double,
    Float,
    ForeignKeyConstraint,
    Integer,
    Long,
    Map,
    ObservedColumn,
    PrimaryKeyConstraint,
    QualifiedName,
    Short,
    String,
    Struct,
    StructField,
    Timestamp,
    TimestampNtz,
    Variant,
)

_SIMPLE_TYPES: Final[dict[str, DataType]] = {
    "int": Integer(),
    "integer": Integer(),
    "bigint": Long(),
    "long": Long(),
    "smallint": Short(),
    "short": Short(),
    "tinyint": Byte(),
    "byte": Byte(),
    "float": Float(),
    "real": Float(),
    "double": Double(),
    "boolean": Boolean(),
    "string": String(),
    "date": Date(),
    "timestamp": Timestamp(),
    "timestamp_ltz": Timestamp(),
    "timestamp_ntz": TimestampNtz(),
    "binary": Binary(),
    "variant": Variant(),
}

_DEFAULT_DECIMAL_PRECISION: Final = 10
_DEFAULT_DECIMAL_SCALE: Final = 0


def data_type_from_json(type_obj: object) -> DataType | None:
    """
    Map an AS JSON type object to a domain ``DataType``, or ``None``.

    ``None`` covers a type the domain does not model (interval, void, geo,
    future types) and malformed input; both get the caller's skip-and-warn
    policy. Domain constructor rejections (decimal over the Delta limit,
    struct fields colliding after casefold) also yield ``None``.
    """
    try:
        return _data_type_from_json(type_obj)
    except (ValueError, RecursionError):
        return None


def _data_type_from_json(type_obj: object) -> DataType | None:
    if not isinstance(type_obj, dict):
        return None
    name = type_obj.get("name")
    if not isinstance(name, str):
        return None
    name = name.casefold()

    if name in _SIMPLE_TYPES:
        return _SIMPLE_TYPES[name]
    if name in ("char", "varchar", "character"):
        return String()  # length bound not modeled (matches the write path)
    if name in ("decimal", "dec", "numeric"):
        return _decimal_from_json(type_obj)
    if name == "array":
        element = data_type_from_json(type_obj.get("element_type"))
        return Array(element) if element is not None else None
    if name == "map":
        key = data_type_from_json(type_obj.get("key_type"))
        value = data_type_from_json(type_obj.get("value_type"))
        if key is None or value is None:
            return None
        return Map(key, value)
    if name == "struct":
        return _struct_from_json(type_obj)
    return None


def _decimal_from_json(type_obj: dict) -> DataType | None:
    precision = type_obj.get("precision", _DEFAULT_DECIMAL_PRECISION)
    scale = type_obj.get("scale", _DEFAULT_DECIMAL_SCALE)
    try:
        return Decimal(int(precision), int(scale))
    except (TypeError, ValueError):
        return None


def _struct_from_json(type_obj: dict) -> DataType | None:
    fields_json = type_obj.get("fields")
    if not isinstance(fields_json, list):
        return None
    fields: list[StructField] = []
    for field in fields_json:
        if not isinstance(field, dict):
            return None
        field_name = field.get("name")
        field_type = data_type_from_json(field.get("type"))
        if not isinstance(field_name, str) or field_type is None:
            return None
        fields.append(StructField(name=field_name.casefold(), data_type=field_type))
    try:
        return Struct(tuple(fields))
    except ValueError:
        return None


logger = logging.getLogger(__name__)


class MetadataParseError(Exception):
    """A DESCRIBE … AS JSON document is missing required structure."""


@dataclass(frozen=True, slots=True)
class TableSnapshot:
    """Backend-neutral table-local state parsed from one AS JSON document."""

    qualified_name: QualifiedName
    columns: tuple[ObservedColumn, ...]
    comment: str
    partitioned_by: tuple[str, ...]
    clustered_by: tuple[str, ...]
    properties: Mapping[str, str]
    primary_key: PrimaryKeyConstraint | None
    foreign_keys: tuple[ForeignKeyConstraint, ...]


def parse_table_snapshot(json_text: str, qualified_name: QualifiedName) -> TableSnapshot:
    """Parse one AS JSON document into a ``TableSnapshot``."""
    try:
        document = json.loads(json_text)
    except (ValueError, TypeError) as error:
        raise MetadataParseError(
            f"{qualified_name}: DESCRIBE AS JSON was not valid JSON"
        ) from error
    if not isinstance(document, dict):
        raise MetadataParseError(f"{qualified_name}: expected a JSON object")

    partitioned_by = _casefolded_list(document.get("partition_columns"))
    constraints = _lower_constraints(parse_table_constraints(document.get("table_constraints")))
    return TableSnapshot(
        qualified_name=qualified_name,
        columns=_columns_from_json(document, qualified_name, set(partitioned_by)),
        comment=document.get("comment") or "",
        partitioned_by=partitioned_by,
        clustered_by=_casefolded_list(document.get("clustering_columns")),
        properties=_managed_properties(document.get("table_properties"), qualified_name),
        primary_key=constraints[0],
        foreign_keys=constraints[1],
    )


def _columns_from_json(
    document: dict, qualified_name: QualifiedName, partition_names: set[str]
) -> tuple[ObservedColumn, ...]:
    columns_json = document.get("columns")
    if not isinstance(columns_json, list) or not columns_json:
        raise MetadataParseError(f"{qualified_name}: AS JSON has no columns array")

    columns: list[ObservedColumn] = []
    for entry in columns_json:
        if not isinstance(entry, dict) or not isinstance(entry.get("name"), str):
            raise MetadataParseError(f"{qualified_name}: malformed column entry {entry!r}")
        name = entry["name"].casefold()
        data_type = data_type_from_json(entry.get("type"))
        if data_type is None:
            if name in partition_names:
                raise MetadataParseError(
                    f"Partition column {name!r} in {qualified_name} has an unmappable"
                    f" type {entry.get('type')!r}; observed partitioning cannot be"
                    " determined, so the table cannot be read safely."
                )
            logger.warning(
                "Skipping column %r in %s: unrecognised type %r",
                name,
                qualified_name,
                entry.get("type"),
            )
            continue
        columns.append(
            ObservedColumn(
                name=name,
                data_type=data_type,
                nullable=bool(entry.get("nullable", True)),
                comment=entry.get("comment") or "",
            )
        )
    if not columns:
        raise MetadataParseError(f"{qualified_name}: no mappable columns")
    return tuple(columns)


def _managed_properties(
    table_properties: object, qualified_name: QualifiedName
) -> Mapping[str, str]:
    if table_properties is None:
        return MappingProxyType({})
    if not isinstance(table_properties, dict):
        raise MetadataParseError(f"{qualified_name}: table_properties is not an object")
    return MappingProxyType(
        {name: value for name, value in table_properties.items() if name in DELTA_PROPERTY_REGISTRY}
    )


def _lower_constraints(
    parsed: ParsedConstraints,
) -> tuple[PrimaryKeyConstraint | None, tuple[ForeignKeyConstraint, ...]]:
    primary_key = None
    if parsed.primary_key is not None:
        primary_key = PrimaryKeyConstraint(
            columns=parsed.primary_key.columns,
            constraint_name=parsed.primary_key.constraint_name,
        )
    foreign_keys = tuple(
        ForeignKeyConstraint(
            local_columns=fk.local_columns,
            referenced_table=QualifiedName(*fk.referenced_table),
            referenced_columns=fk.referenced_columns,
            constraint_name=fk.constraint_name,
        )
        for fk in parsed.foreign_keys
    )
    return primary_key, foreign_keys


def _casefolded_list(value: object) -> tuple[str, ...]:
    if not value:
        return ()
    if not isinstance(value, list):
        return ()
    return tuple(str(item).casefold() for item in value)
