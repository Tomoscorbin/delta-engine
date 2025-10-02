import pytest

from delta_engine.domain.model import Column, Date, Integer, String, TableSnapshot
from tests.factories import make_qualified_name

_QUALIFIED_NAME = make_qualified_name("dev", "silver", "orders")


def test_fails_when_no_columns_defined():
    # Given: a qualified table name and no columns
    # When: constructing a table snapshot with an empty column list
    # Then: validation fails because a table requires at least one column
    with pytest.raises(ValueError):
        TableSnapshot(_QUALIFIED_NAME, [])


def test_fails_when_column_names_duplicate_ignoring_case():
    # Given: two columns whose identifiers collide case-insensitively
    cols = (Column("ID", Integer()), Column("id", String()))
    # When: constructing a table snapshot
    # Then: validation fails due to non-unique column identifiers
    with pytest.raises(ValueError):
        TableSnapshot(_QUALIFIED_NAME, cols)


def test_fails_when_partition_references_missing_column_case_insensitive():
    # Given: columns 'visit_date' and 'id'
    cols = (Column("visit_date", Date()), Column("id", Integer()))
    # When: declaring a partition on a non-existent column name (case-insensitive check)
    # Then: validation fails because the partition column is not defined
    with pytest.raises(ValueError):
        TableSnapshot(_QUALIFIED_NAME, cols, partitioned_by=("date",))
