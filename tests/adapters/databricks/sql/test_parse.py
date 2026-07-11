"""Golden and round-trip tests for the DDL type-string parser."""

import pytest

from delta_engine.adapters.databricks.sql import sql_type_for_data_type
from delta_engine.adapters.databricks.sql.parse import parse_data_type
from delta_engine.domain.model import (
    Array,
    Binary,
    Boolean,
    Byte,
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


@pytest.mark.parametrize(
    ("ddl", "expected"),
    [
        ("int", Integer()),
        ("integer", Integer()),
        ("bigint", Long()),
        ("long", Long()),
        ("smallint", Short()),
        ("short", Short()),
        ("tinyint", Byte()),
        ("byte", Byte()),
        ("float", Float()),
        ("real", Float()),
        ("double", Double()),
        ("boolean", Boolean()),
        ("string", String()),
        ("date", Date()),
        ("timestamp", Timestamp()),
        ("timestamp_ntz", TimestampNtz()),
        ("binary", Binary()),
        ("variant", Variant()),
    ],
)
def test_parses_primitive_types(ddl, expected):
    assert parse_data_type(ddl) == expected


def test_parsing_is_case_insensitive_and_whitespace_tolerant():
    assert parse_data_type("  BIGINT ") == Long()
    assert parse_data_type("Decimal( 10 , 2 )") == Decimal(10, 2)


@pytest.mark.parametrize(
    ("ddl", "expected"),
    [
        ("decimal(10,2)", Decimal(10, 2)),
        ("decimal(5)", Decimal(5, 0)),
        ("decimal", Decimal(10, 0)),  # Spark's default precision/scale
        ("dec(5,2)", Decimal(5, 2)),
        ("numeric(5,2)", Decimal(5, 2)),
    ],
)
def test_parses_decimal_variants(ddl, expected):
    assert parse_data_type(ddl) == expected


@pytest.mark.parametrize("ddl", ["char(10)", "varchar(255)", "character(3)"])
def test_char_and_varchar_normalize_to_string(ddl):
    # Same lossy normalization as the Spark read path: length bounds are not
    # modeled, so the engine sees plain strings.
    assert parse_data_type(ddl) == String()


def test_parses_nested_containers():
    assert parse_data_type("array<int>") == Array(Integer())
    assert parse_data_type("map<string,int>") == Map(String(), Integer())
    assert parse_data_type("array<map<string,array<int>>>") == Array(
        Map(String(), Array(Integer()))
    )


def test_parses_struct_with_colon_and_space_field_separators():
    expected = Struct((StructField("a", Integer()), StructField("b", String())))
    assert parse_data_type("struct<a:int,b:string>") == expected
    assert parse_data_type("struct<a: int, b: string>") == expected
    assert parse_data_type("struct<a int, b string>") == expected


def test_parses_nested_struct():
    assert parse_data_type("struct<a: int, b: struct<c: string>>") == Struct(
        (
            StructField("a", Integer()),
            StructField("b", Struct((StructField("c", String()),))),
        )
    )


def test_struct_field_names_may_be_backticked():
    assert parse_data_type("struct<`weird name`: int>") == Struct(
        (StructField("weird name", Integer()),)
    )


def test_pathologically_nested_type_is_unmappable_rather_than_raising():
    assert parse_data_type("array<" * 10_000 + "int" + ">" * 10_000) is None


def test_backticked_field_names_unescape_doubled_backticks():
    assert parse_data_type("struct<`weird``name`: int>") == Struct(
        (StructField("weird`name", Integer()),)
    )


def test_struct_tolerates_and_discards_not_null_and_comment_clauses():
    # Field nullability and comments are deliberately not modeled (StructField).
    assert parse_data_type("struct<a int NOT NULL COMMENT 'primary id', b string>") == Struct(
        (StructField("a", Integer()), StructField("b", String()))
    )


def test_struct_field_names_are_casefolded():
    assert parse_data_type("struct<ID: int>") == Struct((StructField("id", Integer()),))


def test_struct_field_names_colliding_after_casefold_are_unmappable():
    assert parse_data_type("struct<a: int, A: string>") is None


@pytest.mark.parametrize(
    "ddl",
    [
        "interval day to second",
        "void",
        "geography",
        "decimal(40,2)",  # over the Delta/Spark precision limit
        "array<int",  # malformed: unclosed bracket
        "array<>",
        "struct<>",
        "int extra",  # trailing junk
        "",
        "   ",
    ],
)
def test_unmappable_or_malformed_types_return_none(ddl):
    assert parse_data_type(ddl) is None


@pytest.mark.parametrize(
    "data_type",
    [
        Integer(),
        Long(),
        Short(),
        Byte(),
        Float(),
        Double(),
        Boolean(),
        String(),
        Date(),
        Timestamp(),
        TimestampNtz(),
        Binary(),
        Variant(),
        Decimal(10, 2),
        Array(Integer()),
        Map(String(), Decimal(38, 18)),
        Struct((StructField("id", Long()), StructField("tags", Array(String())))),
        Array(Struct((StructField("k", String()), StructField("v", Map(String(), Integer()))))),
    ],
)
def test_round_trips_every_rendered_domain_type(data_type):
    # The parser must accept everything the compiler emits.
    assert parse_data_type(sql_type_for_data_type(data_type)) == data_type
