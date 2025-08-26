import pytest

from tabula.domain.model.column import Column
from tabula.domain.model.data_type import String


def test_column_normalizes_name_and_defaults_nullable_true():
    c = Column(name="CustomerID", data_type=String())
    assert c.name == "customerid"
    assert c.is_nullable is True


def test_column_rejects_invalid_name():
    with pytest.raises(ValueError):
        Column(name="bad name", data_type=String())
