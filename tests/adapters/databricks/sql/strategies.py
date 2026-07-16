"""Hypothesis strategies shared by the pure Databricks SQL adapter tests."""

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

type TypeDocument = tuple[DataType, dict[str, object]]


CANONICAL_IDENTIFIERS = st.from_regex(r"[a-z_][a-z0-9_]{0,11}", fullmatch=True)

_TEXT_CHARACTERS = st.one_of(
    st.characters(whitelist_categories=("Ll", "Lu", "Nd", "Zs")),
    st.sampled_from(("'", "\\", "\n", "\t", "-", "_", ".", ",", "—")),
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


_SIMPLE_TYPE_DOCUMENTS: tuple[TypeDocument, ...] = (
    (Integer(), {"name": "int"}),
    (Long(), {"name": "bigint"}),
    (Byte(), {"name": "tinyint"}),
    (Short(), {"name": "smallint"}),
    (Float(), {"name": "float"}),
    (Double(), {"name": "double"}),
    (Boolean(), {"name": "boolean"}),
    (String(), {"name": "string"}),
    (Binary(), {"name": "binary"}),
    (Date(), {"name": "date"}),
    (Timestamp(), {"name": "timestamp"}),
    (TimestampNtz(), {"name": "timestamp_ntz"}),
    (Variant(), {"name": "variant"}),
)


@st.composite
def _decimal_documents(draw: st.DrawFn) -> TypeDocument:
    precision = draw(st.integers(min_value=1, max_value=38))
    scale = draw(st.integers(min_value=0, max_value=precision))
    return Decimal(precision, scale), {
        "name": "decimal",
        "precision": precision,
        "scale": scale,
    }


def _array_document(item: TypeDocument) -> TypeDocument:
    data_type, document = item
    return Array(data_type), {"name": "array", "element_type": document}


def _map_document(items: tuple[TypeDocument, TypeDocument]) -> TypeDocument:
    (key_type, key_document), (value_type, value_document) = items
    return Map(key_type, value_type), {
        "name": "map",
        "key_type": key_document,
        "value_type": value_document,
    }


def _struct_document(fields: dict[str, TypeDocument]) -> TypeDocument:
    data_type = Struct(
        tuple(StructField(name, field_type) for name, (field_type, _) in fields.items())
    )
    document_fields: list[dict[str, object]] = [
        {"name": name, "type": field_document, "nullable": True}
        for name, (_, field_document) in fields.items()
    ]
    return data_type, {"name": "struct", "fields": document_fields}


def _nested_type_documents(
    children: st.SearchStrategy[TypeDocument],
) -> st.SearchStrategy[TypeDocument]:
    return st.one_of(
        children.map(_array_document),
        st.tuples(children, children).map(_map_document),
        st.dictionaries(CANONICAL_IDENTIFIERS, children, min_size=1, max_size=3).map(
            _struct_document
        ),
    )


TYPE_DOCUMENTS = st.recursive(
    st.one_of(st.sampled_from(_SIMPLE_TYPE_DOCUMENTS), _decimal_documents()),
    _nested_type_documents,
    max_leaves=10,
)
