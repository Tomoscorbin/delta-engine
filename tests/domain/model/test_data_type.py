from hypothesis import given, strategies as st
import pytest

from delta_engine.domain.model.data_type import (
    Array,
    Decimal,
    Integer,
    String,
    Struct,
    StructField,
)


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


def test_decimal_accepts_maximum_precision_and_scale() -> None:
    assert Decimal(38, 38).precision == 38


def test_struct_field_requires_lowercase_non_blank_name() -> None:
    with pytest.raises(ValueError):
        StructField("", Integer())
    with pytest.raises(ValueError):
        StructField("Amount", Integer())


def test_struct_requires_at_least_one_field_and_unique_names() -> None:
    with pytest.raises(ValueError):
        Struct(())
    with pytest.raises(ValueError):
        Struct((StructField("a", Integer()), StructField("a", String())))


def test_struct_equality_is_structural() -> None:
    left = Struct((StructField("a", Integer()), StructField("b", Array(String()))))
    right = Struct((StructField("a", Integer()), StructField("b", Array(String()))))
    assert left == right
