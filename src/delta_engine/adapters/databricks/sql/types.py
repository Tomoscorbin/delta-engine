"""
Map domain ``DataType`` values to and from Databricks type representations.

Both directions of the adapter's type mapping live here, PySpark-free:

- ``render_data_type`` (write): a domain type → a Spark SQL DDL type string,
  used by the plan compiler.
- ``data_type_from_json`` (read): a ``DESCRIBE … AS JSON`` structured type
  object → a domain type, or ``None`` for a type the domain does not model. It
  reads the structured type objects Unity Catalog returns, so there is no DDL
  type-string parsing on the read path.

``render_data_type`` uses ``match``/``case`` rather than
``functools.singledispatch`` (which the plan compiler uses): ``DataType`` is a
closed set and the mapping is a leaf lookup, where structural patterns like
``case Decimal(precision, scale)`` and ``case Array(element)`` destructure
fields inline. ``singledispatch`` fits the compiler because the ``Action``
hierarchy is open to extension; it would only add ceremony here.
"""

from typing import Final

from delta_engine.adapters.databricks.sql.dialect import backtick
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


def render_data_type(data_type: DataType) -> str:
    """Return a Spark SQL type string for a domain :class:`DataType`."""
    match data_type:
        case Integer():
            return "INT"
        case Long():
            return "BIGINT"
        case Float():
            return "FLOAT"
        case Double():
            return "DOUBLE"
        case Boolean():
            return "BOOLEAN"
        case String():
            return "STRING"
        case Date():
            return "DATE"
        case Timestamp():
            return "TIMESTAMP"
        case Decimal(precision, scale):
            return f"DECIMAL({precision},{scale})"
        case Array(element):
            return f"ARRAY<{render_data_type(element)}>"
        case Map(key, value):
            return f"MAP<{render_data_type(key)},{render_data_type(value)}>"
        case Byte():
            return "TINYINT"
        case Short():
            return "SMALLINT"
        case Binary():
            return "BINARY"
        case TimestampNtz():
            return "TIMESTAMP_NTZ"
        case Variant():
            return "VARIANT"
        case Struct(fields):
            rendered = ", ".join(
                f"{backtick(field.name)}: {render_data_type(field.data_type)}" for field in fields
            )
            return f"STRUCT<{rendered}>"
        case _:
            cls = data_type.__class__.__name__
            raise TypeError(f"Unsupported DataType variant: {cls}")


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


def data_type_from_json(type_obj: object) -> DataType | None:
    """
    Map an AS JSON type object to a domain ``DataType``, or ``None``.

    ``None`` covers a type the domain does not model (interval, void, geo,
    future types), malformed input, and domain constructor rejections (decimal
    over the Delta limit, struct fields colliding after lowercasing). The column
    reader treats ``None`` as an unreadable column and fails the read; the
    recursive element/key/value/field lookups here propagate it as an
    unmodelable nested type.
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
    name = name.lower()

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
    # No default precision or scale: Unity Catalog records concrete values for
    # every decimal column, so an absent field is a malformed document.
    # Inventing DECIMAL(10,0) for a column that is really DECIMAL(12,4) would
    # read as false type drift and be planned against; fail the read instead.
    precision = type_obj.get("precision")
    scale = type_obj.get("scale")
    # json.loads returns JSON integer tokens as int. Do not coerce strings,
    # floats, or booleans: accepting 10.9 as 10 or true as 1 would turn a
    # malformed catalog document into false observed type state.
    if type(precision) is not int or type(scale) is not int:
        return None
    try:
        return Decimal(precision, scale)
    except ValueError:
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
        fields.append(StructField(name=field_name, data_type=field_type))
    try:
        return Struct(tuple(fields))
    except ValueError:
        return None
