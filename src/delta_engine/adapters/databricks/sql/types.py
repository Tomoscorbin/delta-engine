"""
Render domain ``DataType`` values as Spark SQL DDL type strings.

The write half of the adapter's type mapping, and PySpark-free: the compiler
renders domain types into DDL text. The shared read half — parsing catalog
DDL type text into domain types — lives in
:mod:`delta_engine.adapters.databricks.sql.parse`, used directly by the
warehouse backend. The Spark backend has its own read-side mapping instead,
from parsed ``pyspark`` type instances to domain types, in
:mod:`delta_engine.adapters.databricks.spark.types`.

Uses ``match``/``case`` rather than ``functools.singledispatch`` (which the plan
compiler uses): ``DataType`` is a closed set and the mapping is a leaf lookup,
where structural patterns like ``case Decimal(precision, scale)`` and
``case Array(element)`` destructure fields inline. ``singledispatch`` fits the
compiler because the ``Action`` hierarchy is open to extension; it would only add
ceremony here.
"""

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
                f"{backtick(field.name)}: {render_data_type(field.data_type)}"
                for field in fields
            )
            return f"STRUCT<{rendered}>"
        case _:
            cls = data_type.__class__.__name__
            raise TypeError(f"Unsupported DataType variant: {cls}")
