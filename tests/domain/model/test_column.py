from delta_engine.domain.model.column import Column
from delta_engine.domain.model.data_type import Integer


def test_defaults_to_nullable_true() -> None:
    # Given: a column with no explicit nullability
    # When: constructing a Column
    col = Column("id", Integer())
    # Then: it defaults to nullable=True
    assert col.nullable is True


def test_name_is_normalised_to_lowercase() -> None:
    # Given: a column name with mixed case
    # When: constructing a Column
    col = Column("UserId", Integer())
    # Then: the name is stored in lowercase
    assert col.name == "userid"


def test_columns_with_same_name_different_case_are_equal() -> None:
    # Given: two columns with the same name in different cases
    # When: comparing them
    # Then: they are equal because names are normalised
    assert Column("ID", Integer()) == Column("id", Integer())
