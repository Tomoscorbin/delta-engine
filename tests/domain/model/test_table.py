import pytest

from delta_engine.domain.model import (
    Column,
    Date,
    DesiredTable,
    Integer,
    ObservedTable,
    QualifiedName,
    String,
    TableSnapshot,
)
from delta_engine.domain.model.foreign_key import ForeignKeyConstraint

_QUALIFIED_NAME = QualifiedName("dev", "silver", "orders")
_QN = QualifiedName("c", "s", "orders")
_COL = Column("id", Integer(), nullable=False)


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


def test_desired_table_primary_key_constraint_name_returns_table_name_pk():
    # Given a DesiredTable with a primary key
    table = DesiredTable(qualified_name=_QN, columns=(_COL,), primary_key=("id",))

    # Then the constraint name is {table_name}_pk
    assert table.primary_key_constraint_name == "orders_pk"


def test_desired_table_primary_key_constraint_name_returns_none_when_no_pk():
    # Given a DesiredTable with no primary key
    table = DesiredTable(qualified_name=_QN, columns=(_COL,))

    # Then the constraint name is None
    assert table.primary_key_constraint_name is None


def test_table_snapshot_primary_key_defaults_to_empty():
    # Given a DesiredTable constructed without primary_key
    table = DesiredTable(qualified_name=_QN, columns=(_COL,))

    # Then primary_key is an empty tuple
    assert table.primary_key == ()


def test_table_snapshot_rejects_pk_column_not_in_columns():
    # Given a primary_key naming a column that does not exist
    # Then construction raises ValueError
    with pytest.raises(ValueError, match="missing_col"):
        DesiredTable(qualified_name=_QN, columns=(_COL,), primary_key=("missing_col",))


def test_observed_table_has_primary_key_field():
    # Given an ObservedTable constructed with a primary key
    table = ObservedTable(qualified_name=_QN, columns=(_COL,), primary_key=("id",))

    # Then the field is readable
    assert table.primary_key == ("id",)


def test_table_snapshot_rejects_duplicate_pk_column_names():
    # Given a primary_key with the same column name twice
    # Then construction raises ValueError
    with pytest.raises(ValueError, match="id"):
        DesiredTable(
            qualified_name=_QN,
            columns=(Column("id", Integer(), nullable=False),),
            primary_key=("id", "id"),
        )


def test_table_snapshot_defaults_to_no_foreign_keys():
    # Given a minimal table definition
    table = DesiredTable(
        qualified_name=QualifiedName("cat", "sch", "orders"),
        columns=(Column("id", Integer()),),
    )

    # Then foreign_keys defaults to empty
    assert table.foreign_keys == ()


def test_table_snapshot_stores_foreign_keys():
    # Given a foreign key referencing another table
    fk = ForeignKeyConstraint(
        local_columns=("customer_id",),
        references="cat.sch.customers",
        referenced_columns=("id",),
    )
    table = DesiredTable(
        qualified_name=QualifiedName("cat", "sch", "orders"),
        columns=(Column("id", Integer()), Column("customer_id", Integer())),
        foreign_keys=(fk,),
    )

    # Then the FK is stored
    assert table.foreign_keys == (fk,)


def test_table_snapshot_rejects_fk_referencing_unknown_local_column():
    # Given a FK whose local column is not declared
    fk = ForeignKeyConstraint(
        local_columns=("nonexistent",),
        references="cat.sch.customers",
        referenced_columns=("id",),
    )

    # When / Then
    with pytest.raises(ValueError, match="nonexistent"):
        DesiredTable(
            qualified_name=QualifiedName("cat", "sch", "orders"),
            columns=(Column("id", Integer()),),
            foreign_keys=(fk,),
        )


def test_desired_table_resolve_foreign_key_constraint_name_delegates_to_fk():
    # Given a DesiredTable with a FK with no explicit name
    fk = ForeignKeyConstraint(
        local_columns=("customer_id",),
        references="cat.sch.customers",
        referenced_columns=("id",),
    )
    table = DesiredTable(
        qualified_name=QualifiedName("cat", "sch", "orders"),
        columns=(Column("id", Integer()), Column("customer_id", Integer())),
        foreign_keys=(fk,),
    )

    # When resolving the constraint name
    name = table.resolve_foreign_key_constraint_name(fk)

    # Then it is derived from table name + local columns
    assert name == "orders_customer_id_fk"
