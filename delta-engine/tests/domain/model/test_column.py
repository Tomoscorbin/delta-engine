import pytest

from delta_engine.domain.model.column import Column
from delta_engine.domain.model.data_type import Integer


def test_canonicalizes_column_name_case_insensitively() -> None:
    # Given: a column name with mixed case
    # When: constructing a Column
    col = Column("EventDate", Integer())
    # Then: the name is canonicalized (e.g., lowercased per identifier rules)
    assert col.name == "eventdate"


@pytest.mark.parametrize("invalid", ["", " ", "user-id", "order.item", "a/b", '"quoted"'])
def test_rejects_invalid_column_identifier(invalid: str) -> None:
    # Given: an invalid column identifier
    # When / Then: construction fails with a validation error
    with pytest.raises(ValueError):
        Column(invalid, Integer())


def test_defaults_to_nullable_true() -> None:
    # Given: a column with no explicit nullability
    # When: constructing a Column
    col = Column("id", Integer())
    # Then: it defaults to nullable=True
    assert col.is_nullable is True
