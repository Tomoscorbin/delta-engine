from dataclasses import FrozenInstanceError

import pytest

from delta_engine.domain.model.column import Column
from delta_engine.domain.model.data_type import Integer, Long


def test_column_normalizes_name_and_defaults_nullable() -> None:
    col = Column("  ID  ", Integer())
    assert col.name == "id"
    assert col.is_nullable is True


def test_column_is_frozen() -> None:
    col = Column("id", Integer())
    with pytest.raises(FrozenInstanceError):
        col.name = "x"  # type: ignore[misc]


def test_column_equality_respects_normalization_and_type() -> None:
    a = Column("ID", Integer())
    b = Column("id", Integer())
    c = Column("id", Long())
    assert a == b
    assert a != c
