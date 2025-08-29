import pytest

from delta_engine.domain.model.data_type import (
    Decimal,
)


@pytest.mark.parametrize(
    "precision,scale",
    [
        (1, 0),
        (5, 0),
        (5, 5),
    ],
)
def test_decimal_valid_boundaries(precision: int, scale: int) -> None:
    d = Decimal(precision, scale)
    assert d.precision == precision
    assert d.scale == scale


@pytest.mark.parametrize(
    "precision,scale",
    [
        (0, 0),
        (5, -1),
        (5, 6),
    ],
)
def test_decimal_invalid_values_raise(precision: int, scale: int) -> None:
    with pytest.raises(ValueError):
        Decimal(precision, scale)
