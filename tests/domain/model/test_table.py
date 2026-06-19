import pytest

from delta_engine.domain.model import Column, Date, Integer, QualifiedName, String, TableSnapshot

_QUALIFIED_NAME = QualifiedName("dev", "silver", "orders")


def test_fails_when_no_columns_defined():
    # Given: a qualified table name and no columns
    # When: constructing a table snapshot with an empty column list
    # Then: validation fails because a table requires at least one column
    with pytest.raises(ValueError):
        TableSnapshot(_QUALIFIED_NAME, ())


def test_fails_when_column_names_duplicate():
    # Given: two columns with the same lowercase name
    cols = (Column("id", Integer()), Column("id", String()))
    # When: constructing a table snapshot
    # Then: validation fails due to non-unique column identifiers
    with pytest.raises(ValueError):
        TableSnapshot(_QUALIFIED_NAME, cols)


def test_fails_when_partition_references_undefined_column():
    # Given: columns 'visit_date' and 'id'
    cols = (Column("visit_date", Date()), Column("id", Integer()))
    # When: declaring a partition on a column that is not defined
    # Then: validation fails because the partition column does not exist
    with pytest.raises(ValueError):
        TableSnapshot(_QUALIFIED_NAME, cols, partitioned_by=("date",))


def test_fails_when_partition_column_name_is_not_lowercase():
    # Given: a column 'visit_date' and a partition spec naming it in upper case
    cols = (Column("visit_date", Date()), Column("id", Integer()))

    # When: constructing the snapshot with a mixed-case partition reference
    # Then: validation fails — partition column names must be lowercase, consistent
    # with Column and QualifiedName which reject non-lowercase identifiers at construction
    with pytest.raises(ValueError, match="lowercase"):
        TableSnapshot(_QUALIFIED_NAME, cols, partitioned_by=("VISIT_DATE",))


def test_fails_when_partition_columns_are_duplicated():
    # Given: columns 'visit_date' and 'id'
    cols = (Column("visit_date", Date()), Column("id", Integer()))
    # When: the same partition column is listed twice (would emit malformed
    # PARTITIONED BY (visit_date, visit_date) DDL)
    # Then: validation fails rather than producing invalid SQL
    with pytest.raises(ValueError):
        TableSnapshot(_QUALIFIED_NAME, cols, partitioned_by=("visit_date", "visit_date"))
