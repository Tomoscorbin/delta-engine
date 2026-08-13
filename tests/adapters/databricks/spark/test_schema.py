import subprocess
import sys

import pyspark.sql.types as T
import pytest

from delta_engine.adapters.databricks.spark.schema import to_spark_schema
from delta_engine.databricks import to_spark_schema as public_to_spark_schema
from delta_engine.domain.model import (
    Array,
    Binary,
    Boolean,
    Byte,
    DataType,
    Date,
    Decimal,
    DesiredColumn,
    DesiredTable,
    Double,
    Float,
    Integer,
    Long,
    Map,
    QualifiedName,
    Short,
    String,
    Struct,
    StructField,
    Timestamp,
    TimestampNtz,
    Variant,
)
from delta_engine.schema import Column, DeltaTable


def _table(*columns: DesiredColumn) -> DesiredTable:
    return DesiredTable(
        qualified_name=QualifiedName("catalog", "schema", "table"),
        columns=columns,
    )


@pytest.mark.parametrize(
    ("data_type", "spark_type"),
    [
        (Integer(), T.IntegerType()),
        (Long(), T.LongType()),
        (Float(), T.FloatType()),
        (Double(), T.DoubleType()),
        (Boolean(), T.BooleanType()),
        (String(), T.StringType()),
        (Date(), T.DateType()),
        (Timestamp(), T.TimestampType()),
        (Decimal(20, 4), T.DecimalType(20, 4)),
        (Byte(), T.ByteType()),
        (Short(), T.ShortType()),
        (Binary(), T.BinaryType()),
        (TimestampNtz(), T.TimestampNTZType()),
        (Variant(), T.VariantType()),
    ],
)
def test_converts_each_scalar_data_type(data_type: DataType, spark_type: T.DataType) -> None:
    schema = to_spark_schema(_table(DesiredColumn("value", data_type)))

    assert schema == T.StructType([T.StructField("value", spark_type, nullable=True)])


def test_preserves_nested_structure_order_names_and_nullability() -> None:
    table = _table(
        DesiredColumn("ID", Integer(), nullable=False, comment="catalog annotation"),
        DesiredColumn(
            "payload",
            Struct(
                (
                    StructField("label", String(), nullable=False),
                    StructField("scores", Array(Decimal(8, 2))),
                    StructField("lookup", Map(String(), Long())),
                )
            ),
            tags={"classification": "internal"},
        ),
    )

    schema = to_spark_schema(table)

    assert schema == T.StructType(
        [
            T.StructField("ID", T.IntegerType(), nullable=False),
            T.StructField(
                "payload",
                T.StructType(
                    [
                        T.StructField("label", T.StringType(), nullable=False),
                        T.StructField(
                            "scores",
                            T.ArrayType(T.DecimalType(8, 2), containsNull=True),
                            nullable=True,
                        ),
                        T.StructField(
                            "lookup",
                            T.MapType(T.StringType(), T.LongType(), valueContainsNull=True),
                            nullable=True,
                        ),
                    ]
                ),
                nullable=True,
            ),
        ]
    )
    assert all(field.metadata == {} for field in schema.fields)


def test_public_converter_accepts_a_delta_table_declaration() -> None:
    table = DeltaTable(
        catalog="catalog",
        schema="schema",
        name="table",
        columns=[Column("id", Long(), nullable=False), Column("name", String())],
    )

    assert public_to_spark_schema(table) == T.StructType(
        [
            T.StructField("id", T.LongType(), nullable=False),
            T.StructField("name", T.StringType(), nullable=True),
        ]
    )


def test_rejects_an_unmapped_data_type() -> None:
    class CustomType(DataType):
        pass

    with pytest.raises(TypeError, match="Unsupported DataType variant: CustomType"):
        to_spark_schema(_table(DesiredColumn("value", CustomType())))


def test_public_converter_explains_when_pyspark_is_unavailable() -> None:
    program = """
import sys

sys.modules["pyspark"] = None

from delta_engine.databricks import to_spark_schema
from delta_engine.schema import Column, DeltaTable, Integer

table = DeltaTable("catalog", "schema", "table", [Column("id", Integer())])
try:
    to_spark_schema(table)
except RuntimeError as error:
    print(error)
else:
    raise AssertionError("conversion unexpectedly succeeded without PySpark")
"""

    result = subprocess.run([sys.executable, "-c", program], capture_output=True, text=True)

    assert result.returncode == 0, result.stderr
    assert "requires the PySpark supplied by a supported Databricks Runtime" in result.stdout
