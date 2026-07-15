"""
Parse a ``DESCRIBE TABLE EXTENDED <table> AS JSON`` document into a table snapshot.

Column types arrive as structured objects keyed by ``name`` (never DDL
strings), so this is the structured twin of the write path's type rendering.
The one embedded formatted string — ``table_constraints`` — is parsed by
``constraints.py`` and is documented there as less structurally stable.
"""

from typing import Final

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
    Integer,
    Long,
    Map,
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
