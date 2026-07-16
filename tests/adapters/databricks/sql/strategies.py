"""Hypothesis strategies shared by the pure Databricks SQL adapter tests."""

from hypothesis import strategies as st

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


SQL_IDENTIFIERS = st.text(alphabet="abcdefghijklmnopqrstuvwxyz_", min_size=1, max_size=12)


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
        st.dictionaries(SQL_IDENTIFIERS, children, min_size=1, max_size=3).map(_struct_document),
    )


TYPE_DOCUMENTS = st.recursive(
    st.one_of(st.sampled_from(_SIMPLE_TYPE_DOCUMENTS), _decimal_documents()),
    _nested_type_documents,
    max_leaves=10,
)


JSON_VALUES = st.recursive(
    st.one_of(
        st.none(),
        st.booleans(),
        st.integers(),
        st.floats(allow_nan=False, allow_infinity=False),
        st.text(max_size=20),
    ),
    lambda children: st.one_of(
        st.lists(children, max_size=5),
        st.dictionaries(st.text(max_size=12), children, max_size=5),
    ),
    max_leaves=20,
)
