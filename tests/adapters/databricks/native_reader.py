"""
Test-only Databricks reader for OSS Spark + Delta (no ``AS JSON`` support).

OSS Spark's ``DESCRIBE TABLE ... AS JSON`` rejects Delta tables, so the local
``local_e2e`` suite cannot exercise ``SparkReader`` directly. This reader
reaches the same observed state a different way: columns come from the
native ``StructType``, layout and properties come from ``DESCRIBE DETAIL``,
and the result feeds the same ``observed_table_from_description`` assembly the
shipped readers use. Unity Catalog tags, inbound foreign keys, and
primary/foreign key constraints have no OSS Spark equivalent, so those come
back empty; no local e2e test declares them.

This reader parses the ``DESCRIBE DETAIL`` row inline. The shipped readers get
layout and properties from AS JSON, so the shared ``sql`` core carries no
DESCRIBE DETAIL mappers for it to reuse.

Used only by the local engine e2e tests, to keep a real
read -> diff -> plan -> execute round trip credential-free. Production reads
go through ``SparkReader`` (AS JSON) instead.
"""

from __future__ import annotations

from types import MappingProxyType

from pyspark.sql import SparkSession
import pyspark.sql.types as T

from delta_engine.adapters.databricks.errors import exception_message, exception_type_name
from delta_engine.adapters.databricks.read import observed_table_from_description
from delta_engine.adapters.databricks.sql.describe import TableDescription
from delta_engine.application.failures import ReadFailure
from delta_engine.application.ports import CatalogState, ReadFailed, TableAbsent, TablePresent
from delta_engine.application.properties import DELTA_PROPERTY_REGISTRY
from delta_engine.domain.model import (
    Array,
    Binary,
    Boolean,
    DataType,
    Date,
    Decimal,
    Double,
    Float,
    Integer,
    Long,
    Map,
    ObservedColumn,
    QualifiedName,
    Short,
    String,
    Struct,
    StructField,
    Timestamp,
    TimestampNtz,
)

_SIMPLE_TYPES: dict[type[T.DataType], DataType] = {
    T.IntegerType: Integer(),
    T.LongType: Long(),
    T.ShortType: Short(),
    T.FloatType: Float(),
    T.DoubleType: Double(),
    T.BooleanType: Boolean(),
    T.StringType: String(),
    T.DateType: Date(),
    T.TimestampType: Timestamp(),
    T.TimestampNTZType: TimestampNtz(),
    T.BinaryType: Binary(),
}


def _data_type(spark_type: T.DataType) -> DataType | None:
    """Map a native PySpark ``DataType`` to a domain ``DataType``, or ``None`` when unmapped."""
    simple = _SIMPLE_TYPES.get(type(spark_type))
    if simple is not None:
        return simple
    if isinstance(spark_type, T.DecimalType):
        return Decimal(spark_type.precision, spark_type.scale)
    if isinstance(spark_type, T.ArrayType):
        element = _data_type(spark_type.elementType)
        return Array(element) if element is not None else None
    if isinstance(spark_type, T.MapType):
        key = _data_type(spark_type.keyType)
        value = _data_type(spark_type.valueType)
        return Map(key, value) if key is not None and value is not None else None
    if isinstance(spark_type, T.StructType):
        return _struct_type(spark_type)
    return None


def _struct_type(struct_type: T.StructType) -> DataType:
    """Map a native ``StructType`` to a domain ``Struct``, skipping unmappable fields."""
    fields = []
    for field in struct_type.fields:
        data_type = _data_type(field.dataType)
        if data_type is not None:
            fields.append(StructField(field.name.casefold(), data_type))
    return Struct(tuple(fields))


def _observed_columns(struct: T.StructType) -> tuple[ObservedColumn, ...]:
    """Build observed columns from a table's native StructType, skipping unmappable types."""
    columns = []
    for field in struct.fields:
        data_type = _data_type(field.dataType)
        if data_type is None:
            continue
        columns.append(
            ObservedColumn(
                name=field.name.casefold(),
                data_type=data_type,
                nullable=field.nullable,
                comment=field.metadata.get("comment") or "",
            )
        )
    return tuple(columns)


def _managed_properties(raw: dict[str, str] | None) -> MappingProxyType[str, str]:
    """Filter a DESCRIBE DETAIL properties map down to the registry the engine manages."""
    return MappingProxyType(
        {name: value for name, value in (raw or {}).items() if name in DELTA_PROPERTY_REGISTRY}
    )


class NativeSparkReader:
    """
    Reads observed table state from a local OSS SparkSession, for tests only.

    Existence is checked with the unqualified ``schema.table`` form:
    ``spark.catalog.tableExists`` does not resolve an explicit three-part
    ``spark_catalog.schema.table`` name against the local in-memory catalog,
    even though ``spark.table(...)``, ``DESCRIBE DETAIL``, and
    ``catalog.getTable(...)`` all accept it there. Real Unity Catalog reads
    (``SparkReader``) never hit this quirk, so the workaround stays confined
    to this test-only reader.
    """

    def __init__(self, spark: SparkSession) -> None:
        self.spark = spark

    def fetch_state(self, qualified_name: QualifiedName) -> CatalogState:
        """Return ``TablePresent``, ``TableAbsent``, or ``ReadFailed`` — the boundary is total."""
        try:
            return self._read(qualified_name)
        except Exception as exception:
            return ReadFailed(
                failure=ReadFailure(exception_type_name(exception), exception_message(exception))
            )

    def _read(self, qualified_name: QualifiedName) -> CatalogState:
        if not self.spark.catalog.tableExists(f"{qualified_name.schema}.{qualified_name.name}"):
            return TableAbsent()
        fq = str(qualified_name)
        struct = self.spark.table(fq).schema
        detail = self.spark.sql(f"DESCRIBE DETAIL {fq}").first()
        description = TableDescription(
            qualified_name=qualified_name,
            columns=_observed_columns(struct),
            comment=self.spark.catalog.getTable(fq).description or "",
            partitioned_by=tuple(c.casefold() for c in (detail["partitionColumns"] or [])),
            clustered_by=tuple(c.casefold() for c in (detail["clusteringColumns"] or [])),
            properties=_managed_properties(detail["properties"]),
        )
        observed = observed_table_from_description(
            description, run_info_schema_query=lambda query: []
        )
        return TablePresent(table=observed)
