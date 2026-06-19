import pytest
from hypothesis import given
from hypothesis import strategies as st

from delta_engine.domain.model.data_type import (
    Decimal,
)


@given(st.integers(min_value=-10, max_value=50), st.integers(min_value=-10, max_value=60))
def test_decimal_accepts_valid_pairs_and_rejects_invalid_ones(precision: int, scale: int) -> None:
    # Given: an arbitrary (precision, scale) pair
    valid = precision >= 1 and 0 <= scale <= precision
    if valid:
        # When: the pair is within bounds, construction succeeds and preserves the values
        d = Decimal(precision, scale)
        assert d.precision == precision
        assert d.scale == scale
    else:
        # When: the pair violates any constraint, construction raises
        with pytest.raises(ValueError):
            Decimal(precision, scale)
