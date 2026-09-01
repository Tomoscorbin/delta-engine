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
    with pytest.raises(TypeError, match=f"(?i){position}"):
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


def test_decimal_rejects_precision_above_delta_maximum() -> None:
    # Delta/Spark cap DECIMAL precision at 38
    with pytest.raises(ValueError, match="38"):
        Decimal(39, 0)

    # The limit message wins even when the scale is also out of range
    with pytest.raises(ValueError, match="38"):
        Decimal(40, 45)


@given(
    field=st.sampled_from(("precision", "scale")),
    malformed=st.sampled_from(["1", 1.0, True, False]),
)
def test_decimal_rejects_non_integer_precision_and_scale(field: str, malformed: object) -> None:
    values = {"precision": 10, "scale": 1}
    values[field] = malformed
    with pytest.raises(TypeError):
        Decimal(**values)


def test_struct_field_validates_name_and_nullability_and_preserves_them() -> None:
    with pytest.raises(ValueError):
        StructField("", Integer())
    with pytest.raises(TypeError, match="nullable"):
        StructField("amount", Integer(), nullable=1)

    assert StructField("Amount", Integer()).nullable is True
    assert StructField("Amount", Integer(), nullable=False) != StructField("Amount", Integer())
    assert str(StructField("Amount", Integer(), nullable=False)) == "Amount: Integer NOT NULL"
    assert StructField("straße", Integer()).name == "straße"


def test_struct_requires_at_least_one_field_and_unique_names() -> None:
    with pytest.raises(ValueError):
        Struct(())
    with pytest.raises(ValueError):
        Struct((StructField("a", Integer()), StructField("a", String())))


def test_struct_equality_is_structural() -> None:
    left = Struct((StructField("a", Integer()), StructField("b", Array(String()))))
    right = Struct((StructField("a", Integer()), StructField("b", Array(String()))))
    assert left == right


def test_struct_accepts_fields_as_a_list_and_compares_equal_to_tuple_form() -> None:
    from_list = Struct([StructField("a", Integer())])
    from_tuple = Struct((StructField("a", Integer()),))

    assert from_list == from_tuple
    assert from_list.fields == (StructField("a", Integer()),)


def test_map_rejects_a_map_key_type() -> None:
    # Databricks allows any MAP key type except MAP itself.
    with pytest.raises(ValueError, match="key"):
        Map(Map(String(), Integer()), String())


def test_map_allows_a_map_value_type() -> None:
    # Only the key is restricted; a MAP value may itself be a MAP.
    nested = Map(String(), Map(String(), Integer()))
    assert nested.value == Map(String(), Integer())


def test_types_differing_only_in_nested_field_case_are_equal() -> None:
    nested_camel = Map(String(), Array(Struct((StructField("Amount", Integer()),))))
    nested_lower = Map(String(), Array(Struct((StructField("amount", Integer()),))))

    assert nested_camel == nested_lower


def test_genuinely_different_field_names_stay_semantically_different() -> None:
    underscore = Struct((StructField("request_id", String()),))
    camel = Struct((StructField("requestId", String()),))

    assert underscore != camel


def test_struct_rejects_fields_differing_only_by_case() -> None:
    with pytest.raises(ValueError, match=r"[Dd]uplicate struct field"):
        Struct((StructField("id", Integer()), StructField("ID", Integer())))
