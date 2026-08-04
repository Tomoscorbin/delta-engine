"""Hypothesis strategies shared by the Databricks SQL and Spark adapter tests."""

from dataclasses import dataclass

from hypothesis import strategies as st

from delta_engine.application.properties import Property
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


@dataclass(frozen=True, slots=True)
class TypeCase:
    """One domain type and its canonical Databricks read/write representations."""

    data_type: DataType
    sql: str
    document: dict[str, object]


CANONICAL_IDENTIFIERS = st.from_regex(r"[a-z_][a-z0-9_]{0,11}", fullmatch=True)

_TEXT_CHARACTERS = st.one_of(
    st.characters(whitelist_categories=("Ll", "Lu", "Nd", "Zs")),
    st.sampled_from(("'", '"', "\\", "\n", "\r", "\t", "-", "_", ".", ",", "—", "🚀")),
)

SQL_LITERAL_VALUES = st.one_of(
    st.sampled_from(
        (
            "",
            "plain text",
            "O'Reilly",
            r"C:\landing\new",
            r"the two characters \n stay literal",
            "an actual\nnewline",
            "café — 東京",
            "data platform's team",
        )
    ),
    st.text(alphabet=_TEXT_CHARACTERS, max_size=40),
)

COLUMN_NAMES = st.one_of(
    st.sampled_from(
        (
            "id",
            "display name",
            "line-item",
            "select",
            "embedded`tick",
            "café",
            "東京",
        )
    ),
    st.text(
        alphabet=st.one_of(
            st.characters(whitelist_categories=("Ll", "Lu", "Nd")),
            st.sampled_from((" ", "-", "_", "`")),
        ),
        min_size=1,
        max_size=24,
    ).filter(lambda value: bool(value.strip())),
)

TAG_KEYS = st.one_of(
    st.sampled_from(("Owner", "classification", "data domain", "équipe")),
    st.lists(
        st.from_regex(r"[A-Za-z0-9_]{1,12}", fullmatch=True),
        min_size=1,
        max_size=3,
    ).map(" ".join),
)

TAG_VALUES = st.one_of(
    st.just(""),
    st.sampled_from(("Data-Platform", "Gold", "équipe's data", "東京")),
    st.lists(
        st.from_regex(r"[A-Za-z0-9_]{1,12}", fullmatch=True),
        min_size=1,
        max_size=4,
    ).map(" ".join),
)

OBSERVED_TABLE_PROPERTIES = st.lists(
    st.sampled_from(
        (
            ("delta.columnMapping.mode", "name"),
            ("delta.enableChangeDataFeed", "true"),
            ("delta.logRetentionDuration", "interval 30 days"),
            ("delta.minReaderVersion", "3"),
            ("delta.feature.clustering", "supported"),
        )
    ),
    unique_by=lambda entry: entry[0],
    max_size=5,
).map(dict)

_MANAGED_PROPERTY_VALUES = {
    Property.COLUMN_MAPPING_MODE: ("none", "name"),
    Property.CHANGE_DATA_FEED: ("true", "false"),
    Property.DELETED_FILE_RETENTION_DURATION: ("interval 1 day", "interval 7 days"),
    Property.LOG_RETENTION_DURATION: ("interval 7 days", "interval 30 days"),
    Property.DATA_SKIPPING_NUM_INDEXED_COLS: ("-1", "0", "32"),
    Property.TYPE_WIDENING: ("true", "false"),
}


@st.composite
def _managed_property_maps(draw: st.DrawFn) -> dict[str, str | None]:
    keys = draw(
        st.lists(
            st.sampled_from(tuple(Property)),
            unique=True,
            max_size=len(Property),
        )
    )
    properties: dict[str, str | None] = {}
    for key in keys:
        properties[key.value] = draw(
            st.one_of(st.none(), st.sampled_from(_MANAGED_PROPERTY_VALUES[key]))
        )
    return properties


MANAGED_PROPERTY_MAPS = _managed_property_maps()


_SIMPLE_TYPE_CASES: tuple[TypeCase, ...] = (
    TypeCase(Integer(), "INT", {"name": "int"}),
    TypeCase(Long(), "BIGINT", {"name": "bigint"}),
    TypeCase(Byte(), "TINYINT", {"name": "tinyint"}),
    TypeCase(Short(), "SMALLINT", {"name": "smallint"}),
    TypeCase(Float(), "FLOAT", {"name": "float"}),
    TypeCase(Double(), "DOUBLE", {"name": "double"}),
    TypeCase(Boolean(), "BOOLEAN", {"name": "boolean"}),
    TypeCase(String(), "STRING", {"name": "string"}),
    TypeCase(String(), "STRING", {"name": "string", "collation": "UTF8_BINARY"}),
    TypeCase(String(), "STRING", {"name": "varchar", "length": 20}),
    TypeCase(Binary(), "BINARY", {"name": "binary"}),
    TypeCase(Date(), "DATE", {"name": "date"}),
    TypeCase(Timestamp(), "TIMESTAMP", {"name": "timestamp"}),
    TypeCase(TimestampNtz(), "TIMESTAMP_NTZ", {"name": "timestamp_ntz"}),
    TypeCase(Variant(), "VARIANT", {"name": "variant"}),
)


@st.composite
def _decimal_cases(draw: st.DrawFn) -> TypeCase:
    precision = draw(st.integers(min_value=1, max_value=38))
    scale = draw(st.integers(min_value=0, max_value=precision))
    return TypeCase(
        Decimal(precision, scale),
        f"DECIMAL({precision},{scale})",
        {
            "name": "decimal",
            "precision": precision,
            "scale": scale,
        },
    )


def _array_case(item: TypeCase) -> TypeCase:
    return TypeCase(
        Array(item.data_type),
        f"ARRAY<{item.sql}>",
        {"name": "array", "element_type": item.document, "element_nullable": True},
    )


def _map_case(items: tuple[TypeCase, TypeCase]) -> TypeCase:
    key, value = items
    return TypeCase(
        Map(key.data_type, value.data_type),
        f"MAP<{key.sql},{value.sql}>",
        {
            "name": "map",
            "key_type": key.document,
            "value_type": value.document,
            "value_nullable": True,
        },
    )


@st.composite
def _struct_cases(
    draw: st.DrawFn,
    children: st.SearchStrategy[TypeCase],
) -> TypeCase:
    fields = draw(st.dictionaries(CANONICAL_IDENTIFIERS, children, min_size=1, max_size=3))
    expected_fields: list[StructField] = []
    sql_fields: list[str] = []
    document_fields: list[dict[str, object]] = []
    for name, field_case in fields.items():
        raw_name = draw(st.sampled_from((name, name.upper())))
        nullable = draw(st.booleans())
        expected_fields.append(StructField(raw_name, field_case.data_type, nullable=nullable))
        nullability = "" if nullable else " NOT NULL"
        sql_fields.append(f"`{raw_name}`: {field_case.sql}{nullability}")
        document_fields.append(
            {
                "name": raw_name,
                "type": field_case.document,
                "nullable": nullable,
            }
        )
    data_type = Struct(tuple(expected_fields))
    return TypeCase(
        data_type,
        f"STRUCT<{', '.join(sql_fields)}>",
        {"name": "struct", "fields": document_fields},
    )


def _nested_type_cases(
    children: st.SearchStrategy[TypeCase],
) -> st.SearchStrategy[TypeCase]:
    # A MAP key may be any type except MAP itself, so keys exclude map documents
    # (the value stays unrestricted).
    map_keys = children.filter(lambda item: item.document["name"] != "map")
    return st.one_of(
        children.map(_array_case),
        st.tuples(map_keys, children).map(_map_case),
        _struct_cases(children),
    )


TYPE_CASES = st.recursive(
    st.one_of(st.sampled_from(_SIMPLE_TYPE_CASES), _decimal_cases()),
    _nested_type_cases,
    max_leaves=10,
)
