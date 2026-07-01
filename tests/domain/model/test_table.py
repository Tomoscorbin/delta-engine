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
from delta_engine.domain.model.primary_key import PrimaryKeyConstraint

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


def test_table_snapshot_primary_key_defaults_to_none():
    # Given a DesiredTable constructed without primary_key
    table = DesiredTable(qualified_name=_QN, columns=(_COL,))

    # Then primary_key is None (no constraint defined)
    assert table.primary_key is None


def test_table_snapshot_rejects_pk_column_not_in_columns():
    # Given a primary_key naming a column that does not exist
    # Then construction raises ValueError
    with pytest.raises(ValueError, match="missing_col"):
        DesiredTable(
            qualified_name=_QN,
            columns=(_COL,),
            primary_key=PrimaryKeyConstraint(columns=("missing_col",)),
        )


def test_observed_table_has_primary_key_field():
    # Given an ObservedTable constructed with a primary key
    table = ObservedTable(
        qualified_name=_QN,
        columns=(_COL,),
        primary_key=PrimaryKeyConstraint(columns=("id",)),
    )

    # Then the field is readable and returns the value object
    assert table.primary_key == PrimaryKeyConstraint(columns=("id",))


def test_desired_table_rejects_nullable_primary_key_column():
    # Given a desired table whose primary key column is nullable
    # Then construction raises — a nullable PK is not a well-formed desired schema
    with pytest.raises(ValueError, match="Primary key column must be NOT NULL"):
        DesiredTable(
            qualified_name=_QN,
            columns=(Column("id", Integer(), nullable=True),),
            primary_key=PrimaryKeyConstraint(columns=("id",)),
        )


def test_desired_table_reports_the_offending_nullable_primary_key_column():
    # Given a composite primary key where one member is nullable
    # Then the failure names that column
    with pytest.raises(ValueError, match="tenant_id"):
        DesiredTable(
            qualified_name=_QN,
            columns=(
                Column("id", Integer(), nullable=False),
                Column("tenant_id", Integer(), nullable=True),
            ),
            primary_key=PrimaryKeyConstraint(columns=("id", "tenant_id")),
        )


def test_observed_table_allows_a_nullable_primary_key_column():
    # Given an ObservedTable read from a legacy catalog where a PK column is nullable
    table = ObservedTable(
        qualified_name=_QN,
        columns=(Column("id", Integer(), nullable=True),),
        primary_key=PrimaryKeyConstraint(columns=("id",)),
    )

    # Then it is accepted — an observed schema must stay representable, whatever
    # its shape, so the differ can plan against it
    assert table.primary_key == PrimaryKeyConstraint(columns=("id",))


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

    # Then the FK is stored, carrying its engine-generated constraint name
    assert table.foreign_keys == (fk.with_generated_name("orders"),)
    assert table.foreign_keys[0].constraint_name == "orders_customer_id_fk"


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


def test_table_snapshot_rejects_foreign_keys_with_duplicate_derived_names():
    # Given two FKs on the same local columns, neither with an explicit name
    first = ForeignKeyConstraint(
        local_columns=("customer_id",),
        references="cat.sch.customers",
        referenced_columns=("id",),
    )
    second = ForeignKeyConstraint(
        local_columns=("customer_id",),
        references="cat.sch.vips",
        referenced_columns=("id",),
    )

    # When / Then — both FKs govern the same local-column set, which is incoherent
    # and would derive the same constraint name under the adapter's naming policy
    with pytest.raises(ValueError, match="same local columns"):
        DesiredTable(
            qualified_name=QualifiedName("cat", "sch", "orders"),
            columns=(Column("id", Integer()), Column("customer_id", Integer())),
            foreign_keys=(first, second),
        )


def test_desired_table_rejects_two_foreign_keys_over_the_same_local_columns():
    # Given two FKs whose local-column sets are identical (would collide on the
    # derived name and are semantically incoherent)
    fk_one = ForeignKeyConstraint(
        local_columns=("customer_id",),
        references="cat.sch.customers",
        referenced_columns=("id",),
    )
    fk_two = ForeignKeyConstraint(
        local_columns=("customer_id",),
        references="cat.sch.accounts",
        referenced_columns=("id",),
    )

    # When / Then building a DesiredTable with both is rejected
    with pytest.raises(ValueError, match="same local columns"):
        DesiredTable(
            qualified_name=QualifiedName("cat", "sch", "orders"),
            columns=(Column("customer_id", Integer()),),
            foreign_keys=(fk_one, fk_two),
        )


def test_desired_table_rejects_foreign_keys_that_differ_only_in_local_column_order():
    # Given two FKs over the same columns in a different order (the reorder case
    # the old name-based guard missed)
    fk_one = ForeignKeyConstraint(
        local_columns=("tenant_id", "customer_id"),
        references="cat.sch.customers",
        referenced_columns=("tenant_id", "id"),
    )
    fk_two = ForeignKeyConstraint(
        local_columns=("customer_id", "tenant_id"),
        references="cat.sch.customers",
        referenced_columns=("id", "tenant_id"),
    )

    # When / Then building a DesiredTable with both is rejected
    with pytest.raises(ValueError, match="same local columns"):
        DesiredTable(
            qualified_name=QualifiedName("cat", "sch", "orders"),
            columns=(Column("tenant_id", Integer()), Column("customer_id", Integer())),
            foreign_keys=(fk_one, fk_two),
        )


def test_observed_table_allows_two_foreign_keys_over_the_same_local_columns():
    # Given the same clashing FK pair, but as an OBSERVED table (a catalog fact
    # must stay representable and reconcilable, not rejected at read time)
    fk_one = ForeignKeyConstraint(
        local_columns=("customer_id",),
        references="cat.sch.customers",
        referenced_columns=("id",),
        constraint_name="a_fk",
    )
    fk_two = ForeignKeyConstraint(
        local_columns=("customer_id",),
        references="cat.sch.accounts",
        referenced_columns=("id",),
        constraint_name="b_fk",
    )

    # When building an ObservedTable with both
    observed = ObservedTable(
        qualified_name=QualifiedName("cat", "sch", "orders"),
        columns=(Column("customer_id", Integer()),),
        foreign_keys=(fk_one, fk_two),
    )

    # Then it is accepted
    assert len(observed.foreign_keys) == 2


# ---------- tags ----------


def test_table_snapshot_defaults_to_no_tags():
    # Given a minimal table definition with no tags declared
    table = DesiredTable(qualified_name=_QN, columns=(_COL,))

    # Then tags defaults to an empty mapping
    assert dict(table.tags) == {}


def test_table_snapshot_stores_tags():
    # Given a table declared with two tags
    table = DesiredTable(
        qualified_name=_QN,
        columns=(_COL,),
        tags={"env": "prod", "domain": "sales"},
    )

    # Then the tags are stored verbatim
    assert dict(table.tags) == {"env": "prod", "domain": "sales"}


def test_table_snapshot_preserves_tag_key_case():
    # Given a tag key with mixed case (UC tag keys are case-sensitive)
    table = DesiredTable(
        qualified_name=_QN,
        columns=(_COL,),
        tags={"CostCentre": "data-eng"},
    )

    # Then the key case is preserved, not casefolded
    assert "CostCentre" in dict(table.tags)


def test_table_snapshot_rejects_blank_tag_key():
    # Given a tag whose key is blank (would emit a malformed SET TAGS ('') clause)
    # When / Then construction fails, naming the offending key as blank
    with pytest.raises(ValueError, match="blank"):
        DesiredTable(
            qualified_name=_QN,
            columns=(_COL,),
            tags={"  ": "x"},
        )


def test_observed_table_stores_tags():
    # Given an ObservedTable read from a catalog carrying a tag
    table = ObservedTable(
        qualified_name=_QN,
        columns=(_COL,),
        tags={"env": "prod"},
    )

    # Then the tag is readable on the observed snapshot
    assert dict(table.tags) == {"env": "prod"}
