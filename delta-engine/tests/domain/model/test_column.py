from delta_engine.domain.model.column import Column
from delta_engine.domain.model.data_type import Integer


def test_defaults_to_nullable_true() -> None:
    # Given: a column with no explicit nullability
    # When: constructing a Column
    col = Column("id", Integer())
    # Then: it defaults to nullable=True
    assert col.is_nullable is True
