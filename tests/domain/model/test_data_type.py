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
    ids=["min-precision", "integer-decimal", "precision-equals-scale"],
)
def test_accepts_precision_and_scale_within_valid_boundaries(precision: int, scale: int) -> None:
    # Given: a precision/scale pair within the allowed bounds
    # When: constructing a Decimal type
    d = Decimal(precision, scale)
    # Then: the values are accepted and preserved
    assert d.precision == precision
    assert d.scale == scale


@pytest.mark.parametrize(
    "precision,scale",
    [
        (0, 0),  # precision must be >= 1
        (5, -1),  # scale must be >= 0
        (5, 6),  # scale cannot exceed precision
    ],
    ids=["zero-precision", "negative-scale", "scale-exceeds-precision"],
)
def test_rejects_invalid_precision_or_scale(precision: int, scale: int) -> None:
    # Given: an invalid precision/scale pair
    # When/Then: constructing a Decimal raises a validation error
    with pytest.raises(ValueError):
        Decimal(precision, scale)
