from typing import Any

from hypothesis import example, given, strategies as st
import pytest

from delta_engine.domain.model.data_type import (
    Array,
    Decimal,
    Integer,
    Map,
    String,
    Struct,
    StructField,
)
from tests.domain.model.strategies import NON_DATA_TYPES


@example(position="array element", invalid=None)
@example(position="map key", invalid=None)
@example(position="map value", invalid=None)
@example(position="struct field data type", invalid=None)
@example(position="struct field", invalid=None)
@given(
    position=st.sampled_from(
        ("array element", "map key", "map value", "struct field data type", "struct field")
    ),
    invalid=NON_DATA_TYPES,
)
def test_composite_types_reject_non_data_type_members(position: str, invalid: Any) -> None:
    # When any composite member is not a DataType instance, then construction fails
    with pytest.raises(TypeError):
        match position:
            case "array element":
                Array(invalid)
            case "map key":
                Map(invalid, String())
            case "map value":
                Map(String(), invalid)
            case "struct field data type":
                StructField("value", invalid)
            case "struct field":
                Struct((invalid,))


@example(38, 38)
@given(st.integers(min_value=-10, max_value=50), st.integers(min_value=-10, max_value=60))
def test_decimal_accepts_valid_pairs_and_rejects_invalid_ones(precision: int, scale: int) -> None:
    # Given: an arbitrary (precision, scale) pair
    valid = 1 <= precision <= 38 and 0 <= scale <= precision
    if valid:
        # When: the pair is within bounds, construction succeeds and preserves the values
        d = Decimal(precision, scale)
        assert d.precision == precision
        assert d.scale == scale
    else:
        # When: the pair violates any constraint, construction raises
        with pytest.raises(ValueError):
            Decimal(precision, scale)


@pytest.mark.parametrize(
    ("precision", "scale"),
    [
        pytest.param("1", 1, id="string-precision"),
        pytest.param(10, 1.0, id="float-scale"),
        pytest.param(True, 1, id="bool-precision"),
        pytest.param(10, False, id="bool-scale"),
    ],
)
def test_decimal_rejects_non_integer_precision_and_scale(precision: object, scale: object) -> None:
    # When precision or scale is not a plain int (bools included), then construction fails
    with pytest.raises(TypeError):
        Decimal(precision, scale)  # type: ignore[arg-type]


def test_struct_field_rejects_blank_name() -> None:
    # When the field name is blank, then construction fails
    with pytest.raises(ValueError):
        StructField("", Integer())


def test_struct_field_rejects_non_bool_nullable() -> None:
    # When nullability is not a bool, then construction fails
    with pytest.raises(TypeError):
        StructField("amount", Integer(), nullable=1)  # type: ignore[arg-type]


def test_struct_field_defaults_to_nullable() -> None:
    # Then a field with no explicit nullability is nullable, matching Databricks SQL
    assert StructField("Amount", Integer()).nullable is True


def test_struct_field_nullability_is_part_of_identity() -> None:
    # Given two fields differing only in nullability
    # Then they are different fields
    assert StructField("Amount", Integer(), nullable=False) != StructField("Amount", Integer())


def test_struct_field_renders_name_type_and_nullability() -> None:
    # Then the rendered form shows the field as it appears in messages
    assert str(StructField("Amount", Integer(), nullable=False)) == "Amount: Integer NOT NULL"


def test_struct_rejects_empty_fields() -> None:
    # When a struct has no fields, then construction fails
    with pytest.raises(ValueError):
        Struct(())


def test_struct_rejects_duplicate_field_names() -> None:
    # When two fields share a name, then construction fails
    with pytest.raises(ValueError):
        Struct((StructField("a", Integer()), StructField("a", String())))


def test_struct_rejects_fields_differing_only_by_case() -> None:
    # Given two fields whose names differ only in case (case is not identity)
    # Then construction fails as a duplicate
    with pytest.raises(ValueError):
        Struct((StructField("id", Integer()), StructField("ID", Integer())))


def test_struct_accepts_fields_as_a_list_and_compares_equal_to_tuple_form() -> None:
    # Given the same fields supplied as a list and as a tuple
    from_list = Struct([StructField("a", Integer())])
    from_tuple = Struct((StructField("a", Integer()),))

    # Then both construct the same struct
    assert from_list == from_tuple
    assert from_list.fields == (StructField("a", Integer()),)


def test_map_rejects_a_map_key_type() -> None:
    # Given a MAP key that is itself a MAP (Databricks allows any other key type)
    # Then construction fails
    with pytest.raises(ValueError):
        Map(Map(String(), Integer()), String())


def test_map_allows_a_map_value_type() -> None:
    # Given a MAP value that is itself a MAP (only the key is restricted)
    nested = Map(String(), Map(String(), Integer()))

    # Then the nested value type is preserved
    assert nested.value == Map(String(), Integer())


def test_types_differing_only_in_nested_field_case_are_equal() -> None:
    # Given deeply nested types whose only difference is a struct field's case
    nested_camel = Map(String(), Array(Struct((StructField("Amount", Integer()),))))
    nested_lower = Map(String(), Array(Struct((StructField("amount", Integer()),))))

    # Then they are the same type — field case is not identity
    assert nested_camel == nested_lower


def test_genuinely_different_field_names_stay_semantically_different() -> None:
    # Given structs whose field names differ beyond case
    underscore = Struct((StructField("request_id", String()),))
    camel = Struct((StructField("requestId", String()),))

    # Then they are different types
    assert underscore != camel
