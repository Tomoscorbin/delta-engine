import pytest

from delta_engine.domain.model.column import Column
from delta_engine.domain.model.data_type import Integer


def test_defaults_to_nullable_true() -> None:
    # Given: a column with no explicit nullability
    # When: constructing a Column
    col = Column("id", Integer())
    # Then: it defaults to nullable=True
    assert col.nullable is True


def test_raises_when_name_is_not_lowercase() -> None:
    # Given: a column name containing uppercase characters
    # When: constructing a Column
    # Then: a ValueError is raised
    with pytest.raises(ValueError, match="lowercase"):
        Column("UserId", Integer())
