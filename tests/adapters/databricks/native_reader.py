"""
Test-only Databricks reader for OSS Spark + Delta (no ``AS JSON`` support).

OSS Spark's ``DESCRIBE TABLE ... AS JSON`` rejects Delta tables, so the local
``local_e2e`` suite cannot exercise ``SparkReader`` directly. This reader
reaches the same observed state a different way: columns come from the
native ``StructType``, layout and properties come from ``DESCRIBE DETAIL``,
and the domain ``ObservedTable`` is constructed directly. Unity Catalog tags,
inbound foreign keys, and primary/foreign key constraints have no OSS Spark
equivalent, so they stay empty; no local e2e test declares them.

This reader parses layout and properties from the ``DESCRIBE DETAIL`` row
inline and reuses the production catalog-name normalization for table
features.

Used only by the local engine e2e tests, to keep a real
read -> diff -> plan -> execute round trip credential-free. Production reads
go through ``SparkReader`` (AS JSON) instead.
"""

from pyspark.sql import SparkSession
import pyspark.sql.types as T

from delta_engine.adapters.databricks.exception_inspection import (
    exception_message,
    exception_type_name,
)
from delta_engine.adapters.databricks.table_features import recognized_table_features
from delta_engine.application.errors import ReadError
from delta_engine.application.ports import CatalogState, TableAbsent, TablePresent
from delta_engine.application.properties import DELTA_PROPERTY_POLICY
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
    ObservedTable,
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
        """Return the known catalog state, raising ``ReadError`` when it cannot be read."""
        try:
            return self._read(qualified_name)
        except Exception as exception:
            raise ReadError(
                exception_type=exception_type_name(exception),
                message=exception_message(exception),
            ) from exception

    def _read(self, qualified_name: QualifiedName) -> CatalogState:
        if not self.spark.catalog.tableExists(f"{qualified_name.schema}.{qualified_name.name}"):
            return TableAbsent()
        fq = str(qualified_name)
        struct = self.spark.table(fq).schema
        detail = self.spark.sql(f"DESCRIBE DETAIL {fq}").first()
        observed = ObservedTable(
            qualified_name=qualified_name,
            columns=_observed_columns(struct),
            comment=self.spark.catalog.getTable(fq).description or "",
            partitioned_by=tuple(c.casefold() for c in (detail["partitionColumns"] or [])),
            clustered_by=tuple(c.casefold() for c in (detail["clusteringColumns"] or [])),
            properties=DELTA_PROPERTY_POLICY.project_observed(detail["properties"] or {}),
            supported_features=recognized_table_features(detail["tableFeatures"] or ()),
        )
        return TablePresent(table=observed)
