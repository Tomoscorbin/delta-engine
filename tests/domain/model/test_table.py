import pytest

from delta_engine.domain.model import (
    Date,
    DesiredColumn,
    DesiredTable,
    ForeignKeyReference,
    Integer,
    ObservedColumn,
    ObservedTable,
    QualifiedName,
    String,
    TableKind,
    TableScope,
)
from delta_engine.domain.model.constraints import ForeignKeyConstraint, PrimaryKeyConstraint
from tests.builders import as_observed_columns

_QUALIFIED_NAME = QualifiedName("dev", "silver", "orders")
_QN = QualifiedName("c", "s", "orders")
_COL = DesiredColumn("id", Integer(), nullable=False)
_OBSERVED_COL = ObservedColumn("id", Integer(), nullable=False)

_EACH_TABLE_TYPE = pytest.mark.parametrize(
    "table_type", [DesiredTable, ObservedTable], ids=["desired", "observed"]
)
_EACH_TABLE_AND_COLUMN_TYPE = pytest.mark.parametrize(
    ("table_type", "column_type"),
    [(DesiredTable, DesiredColumn), (ObservedTable, ObservedColumn)],
    ids=["desired", "observed"],
)


@_EACH_TABLE_TYPE
def test_fails_when_no_columns_defined(table_type):
    # Given: a qualified table name and no columns
    # When: constructing a table with an empty column list
    # Then: validation fails because a table requires at least one column
    with pytest.raises(ValueError):
        table_type(_QUALIFIED_NAME, ())


@_EACH_TABLE_AND_COLUMN_TYPE
def test_fails_when_column_names_duplicate(table_type, column_type):
    # Given: two columns with the same lowercase name
    cols = (column_type("id", Integer()), column_type("id", String()))
    # When: constructing a table
    # Then: validation fails due to non-unique column identifiers
    with pytest.raises(ValueError):
        table_type(_QUALIFIED_NAME, cols)


@_EACH_TABLE_AND_COLUMN_TYPE
def test_rejects_columns_differing_only_by_case_as_duplicates(table_type, column_type):
    cols = (column_type("Id", Integer()), column_type("ID", Integer()))

    with pytest.raises(ValueError, match="Duplicate column name"):
        table_type(_QUALIFIED_NAME, cols)


@_EACH_TABLE_AND_COLUMN_TYPE
def test_fails_when_partition_references_undefined_column(table_type, column_type):
    # Given: columns 'visit_date' and 'id'
    cols = (column_type("visit_date", Date()), column_type("id", Integer()))
    # When: declaring a partition on a column that is not defined
    # Then: validation fails because the partition column does not exist
    with pytest.raises(ValueError):
        table_type(_QUALIFIED_NAME, cols, partitioned_by=("date",))


@_EACH_TABLE_AND_COLUMN_TYPE
def test_partition_reference_must_use_the_column_spelling(table_type, column_type):
    # Given a partition reference that matches a column only case-insensitively
    cols = (column_type("visit_date", Date()), column_type("id", Integer()))

    # Then construction rejects it — references must spell their column exactly
    with pytest.raises(ValueError, match="Partition column not found"):
        table_type(_QUALIFIED_NAME, cols, partitioned_by=("VISIT_DATE",))


@_EACH_TABLE_AND_COLUMN_TYPE
def test_fails_when_partition_columns_are_duplicated(table_type, column_type):
    # Given: columns 'visit_date' and 'id'
    cols = (column_type("visit_date", Date()), column_type("id", Integer()))
    # When: the same partition column is listed twice (would emit malformed
    # PARTITIONED BY (visit_date, visit_date) DDL)
    # Then: validation fails rather than producing invalid SQL
    with pytest.raises(ValueError):
        table_type(_QUALIFIED_NAME, cols, partitioned_by=("visit_date", "visit_date"))


def test_desired_table_primary_key_defaults_to_none():
    # Given a DesiredTable constructed without primary_key
    table = DesiredTable(qualified_name=_QN, columns=(_COL,))

    # Then primary_key is None (no constraint defined)
    assert table.primary_key is None


def test_primary_key_columns_is_empty_when_no_primary_key():
    # Given a table with no primary key
    table = DesiredTable(qualified_name=_QN, columns=(_COL,))

    # Then primary_key_columns is the empty tuple, not None
    assert table.primary_key_columns == ()


def test_primary_key_columns_returns_the_constraint_columns():
    # Given a table with a two-column primary key
    table = ObservedTable(
        qualified_name=_QN,
        columns=(ObservedColumn("id", Integer()), ObservedColumn("tenant_id", Integer())),
        primary_key=PrimaryKeyConstraint(columns=("id", "tenant_id"), name="t_pk"),
    )

    # Then primary_key_columns returns those column names in declaration order
    assert table.primary_key_columns == ("id", "tenant_id")


def test_desired_table_rejects_pk_column_not_in_columns():
    # Given a primary_key naming a column that does not exist
    # Then construction raises ValueError
    with pytest.raises(ValueError, match="missing_col"):
        DesiredTable(
            qualified_name=_QN,
            columns=(_COL,),
            primary_key=PrimaryKeyConstraint(columns=("missing_col",), name="orders_pk"),
        )


def test_observed_table_has_primary_key_field():
    # Given an ObservedTable constructed with a primary key
    table = ObservedTable(
        qualified_name=_QN,
        columns=(_OBSERVED_COL,),
        primary_key=PrimaryKeyConstraint(columns=("id",), name="t_pk"),
    )

    # Then the field is readable and returns the value object
    assert table.primary_key == PrimaryKeyConstraint(columns=("id",), name="t_pk")


def test_desired_table_rejects_nullable_primary_key_column():
    # Given a desired table whose primary key column is nullable
    # Then construction raises — a nullable PK is not a well-formed desired schema
    with pytest.raises(ValueError, match="Primary key column must be NOT NULL"):
        DesiredTable(
            qualified_name=_QN,
            columns=(DesiredColumn("id", Integer(), nullable=True),),
            primary_key=PrimaryKeyConstraint(columns=("id",), name="orders_pk"),
        )


@_EACH_TABLE_AND_COLUMN_TYPE
def test_primary_key_reference_must_use_the_column_spelling(table_type, column_type):
    # Given a key reference that matches a column only case-insensitively
    # Then construction rejects it — references must spell their column exactly
    with pytest.raises(ValueError, match="Primary key column not found"):
        table_type(
            qualified_name=_QUALIFIED_NAME,
            columns=(column_type("requestId", Integer(), nullable=False),),
            primary_key=PrimaryKeyConstraint(columns=("REQUESTID",), name="t_pk"),
        )


def test_desired_table_reports_the_offending_nullable_primary_key_column():
    # Given a composite primary key where one member is nullable
    # Then the failure names that column
    with pytest.raises(ValueError, match="tenant_id"):
        DesiredTable(
            qualified_name=_QN,
            columns=(
                DesiredColumn("id", Integer(), nullable=False),
                DesiredColumn("tenant_id", Integer(), nullable=True),
            ),
            primary_key=PrimaryKeyConstraint(columns=("id", "tenant_id"), name="orders_pk"),
        )


def test_observed_table_allows_a_nullable_primary_key_column():
    # Given an ObservedTable read from a legacy catalog where a PK column is nullable
    table = ObservedTable(
        qualified_name=_QN,
        columns=(ObservedColumn("id", Integer(), nullable=True),),
        primary_key=PrimaryKeyConstraint(columns=("id",), name="t_pk"),
    )

    # Then it is accepted — an observed schema must stay representable, whatever
    # its shape, so the differ can plan against it
    assert table.primary_key == PrimaryKeyConstraint(columns=("id",), name="t_pk")


def test_desired_table_defaults_to_no_foreign_keys():
    # Given a minimal table definition
    table = DesiredTable(
        qualified_name=QualifiedName("cat", "sch", "orders"),
        columns=(DesiredColumn("id", Integer()),),
    )

    # Then foreign_keys defaults to empty
    assert table.foreign_keys == ()


def test_desired_table_stores_foreign_keys():
    # Given a foreign key referencing another table under an explicit name
    fk = ForeignKeyConstraint(
        local_columns=("customer_id",),
        referenced_table=QualifiedName("cat", "sch", "customers"),
        referenced_columns=("id",),
        name="orders_customer_id_fk",
    )
    table = DesiredTable(
        qualified_name=QualifiedName("cat", "sch", "orders"),
        columns=(DesiredColumn("id", Integer()), DesiredColumn("customer_id", Integer())),
        foreign_keys=(fk,),
    )

    # Then the FK is stored with its complete declaration
    assert table.foreign_keys == (
        ForeignKeyConstraint(
            local_columns=("customer_id",),
            referenced_table=QualifiedName("cat", "sch", "customers"),
            referenced_columns=("id",),
            name="orders_customer_id_fk",
        ),
    )
    assert table.foreign_keys[0].name == "orders_customer_id_fk"


def test_desired_table_accepts_multiple_unnamed_foreign_keys():
    customer = ForeignKeyConstraint(
        local_columns=("customer_id",),
        referenced_table=QualifiedName("cat", "sch", "customers"),
        referenced_columns=("id",),
    )
    account = ForeignKeyConstraint(
        local_columns=("account_id",),
        referenced_table=QualifiedName("cat", "sch", "accounts"),
        referenced_columns=("id",),
    )

    table = DesiredTable(
        qualified_name=QualifiedName("cat", "sch", "orders"),
        columns=(DesiredColumn("customer_id", Integer()), DesiredColumn("account_id", Integer())),
        foreign_keys=(customer, account),
    )

    assert table.foreign_keys == (customer, account)


def test_observed_table_rejects_an_unnamed_primary_key():
    with pytest.raises(ValueError, match="Observed primary key must have a constraint name"):
        ObservedTable(
            qualified_name=_QN,
            columns=(_OBSERVED_COL,),
            primary_key=PrimaryKeyConstraint(columns=("id",)),
        )


def test_observed_table_rejects_an_unnamed_foreign_key():
    foreign_key = ForeignKeyConstraint(
        local_columns=("customer_id",),
        referenced_table=QualifiedName("cat", "sch", "customers"),
        referenced_columns=("id",),
    )

    with pytest.raises(ValueError, match="Observed foreign key must have a constraint name"):
        ObservedTable(
            qualified_name=QualifiedName("cat", "sch", "orders"),
            columns=(ObservedColumn("customer_id", Integer()),),
            foreign_keys=(foreign_key,),
        )


@_EACH_TABLE_AND_COLUMN_TYPE
def test_foreign_key_local_reference_must_use_the_column_spelling(table_type, column_type):
    # Given a local reference that matches a column only case-insensitively
    # Then construction rejects it — references must spell their column exactly
    with pytest.raises(ValueError, match="Foreign key local column not found"):
        table_type(
            qualified_name=_QUALIFIED_NAME,
            columns=(column_type("customerId", Integer()),),
            foreign_keys=(
                ForeignKeyConstraint(
                    local_columns=("CUSTOMERID",),
                    referenced_table=QualifiedName("dev", "silver", "customers"),
                    referenced_columns=("ID",),
                    name="orders_customer_id_fk",
                ),
            ),
        )


@_EACH_TABLE_AND_COLUMN_TYPE
def test_table_does_not_rewrite_foreign_key_references(table_type, column_type):
    # Given a self-referencing key whose referenced spelling differs from its column
    table = table_type(
        qualified_name=_QUALIFIED_NAME,
        columns=(
            column_type("employeeId", Integer()),
            column_type("managerId", Integer()),
        ),
        foreign_keys=(
            ForeignKeyConstraint(
                local_columns=("managerId",),
                referenced_table=_QUALIFIED_NAME,
                referenced_columns=("EMPLOYEEID",),
                name="orders_manager_id_fk",
            ),
        ),
    )

    # Then the reference is stored verbatim — the referenced side belongs to
    # another table's spelling, which is the adoption pass's job, not the model's
    constraint = table.foreign_keys[0]
    assert tuple(str(column) for column in constraint.local_columns) == ("managerId",)
    assert tuple(str(column) for column in constraint.referenced_columns) == ("EMPLOYEEID",)


def test_desired_table_rejects_fk_referencing_unknown_local_column():
    # Given a FK whose local column is not declared (name provided so construction succeeds)
    fk = ForeignKeyConstraint(
        local_columns=("nonexistent",),
        referenced_table=QualifiedName("cat", "sch", "customers"),
        referenced_columns=("id",),
        name="orders_nonexistent_fk",
    )

    # When / Then the DesiredTable rejects the FK referencing a column that does not exist
    with pytest.raises(ValueError, match="nonexistent"):
        DesiredTable(
            qualified_name=QualifiedName("cat", "sch", "orders"),
            columns=(DesiredColumn("id", Integer()),),
            foreign_keys=(fk,),
        )


def test_desired_table_rejects_two_foreign_keys_over_the_same_local_columns():
    # Given two FKs whose local-column sets are identical (semantically incoherent;
    # each carries a distinct name so construction succeeds)
    fk_one = ForeignKeyConstraint(
        local_columns=("customer_id",),
        referenced_table=QualifiedName("cat", "sch", "customers"),
        referenced_columns=("id",),
        name="orders_customer_id_fk",
    )
    fk_two = ForeignKeyConstraint(
        local_columns=("customer_id",),
        referenced_table=QualifiedName("cat", "sch", "accounts"),
        referenced_columns=("id",),
        name="orders_customer_id_accounts_fk",
    )

    # When / Then building a DesiredTable with both is rejected
    with pytest.raises(ValueError, match="same local columns"):
        DesiredTable(
            qualified_name=QualifiedName("cat", "sch", "orders"),
            columns=(DesiredColumn("customer_id", Integer()),),
            foreign_keys=(fk_one, fk_two),
        )


def test_desired_table_rejects_duplicate_explicit_foreign_key_names():
    # Given two FKs over different local columns with the same explicit name
    first = ForeignKeyConstraint(
        local_columns=("a", "b_c"),
        referenced_table=QualifiedName("cat", "sch", "p1"),
        referenced_columns=("x", "y"),
        name="t_a_b_c_fk",
    )
    second = ForeignKeyConstraint(
        local_columns=("a_b", "c"),
        referenced_table=QualifiedName("cat", "sch", "p2"),
        referenced_columns=("x", "y"),
        name="t_a_b_c_fk",
    )

    # When / Then the collision is rejected, naming both column tuples
    with pytest.raises(ValueError, match="same constraint name"):
        DesiredTable(
            qualified_name=QualifiedName("cat", "sch", "t"),
            columns=(
                DesiredColumn("a", Integer()),
                DesiredColumn("b_c", Integer()),
                DesiredColumn("a_b", Integer()),
                DesiredColumn("c", Integer()),
            ),
            foreign_keys=(first, second),
        )


def test_desired_table_rejects_foreign_keys_that_differ_only_in_local_column_order():
    # Given two FKs over the same columns in a different order (the reorder case
    # the old name-based guard missed); each carries a distinct name so construction succeeds
    fk_one = ForeignKeyConstraint(
        local_columns=("tenant_id", "customer_id"),
        referenced_table=QualifiedName("cat", "sch", "customers"),
        referenced_columns=("tenant_id", "id"),
        name="orders_tenant_id_customer_id_fk",
    )
    fk_two = ForeignKeyConstraint(
        local_columns=("customer_id", "tenant_id"),
        referenced_table=QualifiedName("cat", "sch", "customers"),
        referenced_columns=("id", "tenant_id"),
        name="orders_customer_id_tenant_id_fk",
    )

    # When / Then building a DesiredTable with both is rejected
    with pytest.raises(ValueError, match="same local columns"):
        DesiredTable(
            qualified_name=QualifiedName("cat", "sch", "orders"),
            columns=(
                DesiredColumn("tenant_id", Integer()),
                DesiredColumn("customer_id", Integer()),
            ),
            foreign_keys=(fk_one, fk_two),
        )


def test_desired_table_accepts_an_already_named_foreign_key():
    # Given a FK that already carries a name (as the API layer will produce it)
    fk = ForeignKeyConstraint(
        local_columns=("customer_id",),
        referenced_table=QualifiedName("cat", "sch", "customers"),
        referenced_columns=("id",),
        name="orders_customer_id_fk",
    )

    # When building a DesiredTable with it
    table = DesiredTable(
        qualified_name=QualifiedName("cat", "sch", "orders"),
        columns=(DesiredColumn("id", Integer()), DesiredColumn("customer_id", Integer())),
        foreign_keys=(fk,),
    )

    # Then the name is preserved (not regenerated, not rejected)
    assert table.foreign_keys[0].name == "orders_customer_id_fk"


def test_observed_table_allows_two_foreign_keys_over_the_same_local_columns():
    # Given the same clashing FK pair, but as an OBSERVED table (a catalog fact
    # must stay representable and reconcilable, not rejected at read time)
    fk_one = ForeignKeyConstraint(
        local_columns=("customer_id",),
        referenced_table=QualifiedName("cat", "sch", "customers"),
        referenced_columns=("id",),
        name="a_fk",
    )
    fk_two = ForeignKeyConstraint(
        local_columns=("customer_id",),
        referenced_table=QualifiedName("cat", "sch", "accounts"),
        referenced_columns=("id",),
        name="b_fk",
    )

    # When building an ObservedTable with both
    observed = ObservedTable(
        qualified_name=QualifiedName("cat", "sch", "orders"),
        columns=(ObservedColumn("customer_id", Integer()),),
        foreign_keys=(fk_one, fk_two),
    )

    # Then it is accepted
    assert len(observed.foreign_keys) == 2


# ---------- tags ----------


def test_desired_table_defaults_to_no_tags():
    # Given a minimal table definition with no tags declared
    table = DesiredTable(qualified_name=_QN, columns=(_COL,))

    # Then tags defaults to an empty mapping
    assert dict(table.tags) == {}


def test_desired_table_stores_tags():
    # Given a table declared with two tags
    table = DesiredTable(
        qualified_name=_QN,
        columns=(_COL,),
        tags={"env": "prod", "domain": "sales"},
    )

    # Then the tags are stored verbatim
    assert dict(table.tags) == {"env": "prod", "domain": "sales"}


def test_desired_table_preserves_tag_key_case():
    # Given a tag key with mixed case (UC tag keys are case-sensitive)
    table = DesiredTable(
        qualified_name=_QN,
        columns=(_COL,),
        tags={"CostCentre": "data-eng"},
    )

    # Then the key case is preserved, not casefolded
    assert "CostCentre" in dict(table.tags)


def test_desired_table_rejects_blank_tag_key():
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
        columns=(_OBSERVED_COL,),
        tags={"env": "prod"},
    )

    # Then the tag is readable on the observed snapshot
    assert dict(table.tags) == {"env": "prod"}


# ---------- scope ----------


def test_desired_table_manages_all_aspects_by_default():
    # Given a desired table built without a scope argument
    table = DesiredTable(
        qualified_name=_QUALIFIED_NAME,
        columns=(_COL,),
    )

    # Then every aspect is managed (full-management behaviour)
    assert table.scope is TableScope.FULL


def test_desired_table_accepts_a_restricted_scope():
    # Given a desired table managing only tags
    table = DesiredTable(
        qualified_name=_QUALIFIED_NAME,
        columns=(_COL,),
        scope=TableScope.TAGS,
    )

    # Then the declared scope is preserved
    assert table.scope is TableScope.TAGS


def test_desired_table_rejects_a_raw_scope_name():
    # Public strings are resolved before entering the domain.
    with pytest.raises(TypeError, match="TableScope"):
        DesiredTable(
            qualified_name=_QUALIFIED_NAME,
            columns=(_COL,),
            scope="tags",  # type: ignore[arg-type]
        )


def test_desired_table_accepts_none_property_assertions():
    # Given a declaration asserting one value and one absence
    desired = DesiredTable(
        qualified_name=_QUALIFIED_NAME,
        columns=(_COL,),
        properties={"delta.enableChangeDataFeed": "true", "delta.logRetentionDuration": None},
    )

    # Then both entries are carried verbatim
    assert desired.properties["delta.enableChangeDataFeed"] == "true"
    assert desired.properties["delta.logRetentionDuration"] is None


def test_observed_table_properties_carry_values_only():
    # Given an observed table — the catalog has values, never assertions
    observed = ObservedTable(
        qualified_name=_QUALIFIED_NAME,
        columns=(_OBSERVED_COL,),
        properties={"delta.enableChangeDataFeed": "true"},
    )

    assert observed.properties == {"delta.enableChangeDataFeed": "true"}


def test_domain_tables_accept_backend_specific_partition_layouts() -> None:
    # Desired and observed state stay representable even when a backend-specific
    # API would reject the layout as undeployable.
    desired = DesiredTable(
        qualified_name=QualifiedName("dev", "silver", "orders"),
        columns=(DesiredColumn("id", Integer()), DesiredColumn("day", Date())),
        partitioned_by=("id", "day"),
    )
    observed = ObservedTable(
        qualified_name=QualifiedName("dev", "silver", "orders"),
        columns=(ObservedColumn("id", Integer()), ObservedColumn("day", Date())),
        partitioned_by=("id", "day"),
    )
    assert desired.partitioned_by == ("id", "day")
    assert observed.partitioned_by == ("id", "day")


def test_domain_tables_accept_backend_specific_clustering_layouts() -> None:
    # The public API refuses to author these layouts (partitioning and
    # clustering together, more than four clustering keys), but desired and
    # observed state must stay representable: layout deployability is a
    # declaration-time rule, not a snapshot invariant.
    columns = tuple(DesiredColumn(name, Integer()) for name in ("a", "b", "c", "d", "e"))
    name = QualifiedName("dev", "silver", "orders")

    five_keys = DesiredTable(
        qualified_name=name, columns=columns, clustered_by=("a", "b", "c", "d", "e")
    )
    partitioned_and_clustered = DesiredTable(
        qualified_name=name, columns=columns, partitioned_by=("a",), clustered_by=("b",)
    )
    observed = ObservedTable(
        qualified_name=name,
        columns=as_observed_columns(columns),
        partitioned_by=("a",),
        clustered_by=("b",),
    )

    assert five_keys.clustered_by == ("a", "b", "c", "d", "e")
    assert partitioned_and_clustered.partitioned_by == ("a",)
    assert partitioned_and_clustered.clustered_by == ("b",)
    assert observed.clustered_by == ("b",)


# ---------- clustered_by ----------


@_EACH_TABLE_AND_COLUMN_TYPE
def test_table_defaults_to_no_clustering(table_type, column_type):
    # Given a table built without clustering
    table = table_type(_QUALIFIED_NAME, (column_type("id", Integer()),))
    # Then clustered_by is a stable empty tuple
    assert table.clustered_by == ()


@_EACH_TABLE_AND_COLUMN_TYPE
def test_table_stores_clustering_columns(table_type, column_type):
    # Given a table clustered by an existing column
    columns = (column_type("id", Integer()), column_type("region", String()))
    table = table_type(_QUALIFIED_NAME, columns, clustered_by=("region",))
    # Then it reads the clustering columns back
    assert table.clustered_by == ("region",)


@_EACH_TABLE_AND_COLUMN_TYPE
def test_table_rejects_clustering_column_not_in_columns(table_type, column_type):
    # Given a clustering column that is not defined
    with pytest.raises(ValueError, match=r"[Cc]luster"):
        table_type(_QUALIFIED_NAME, (column_type("id", Integer()),), clustered_by=("region",))


@_EACH_TABLE_AND_COLUMN_TYPE
def test_clustering_reference_must_use_the_column_spelling(table_type, column_type):
    # Given a clustering reference that matches a column only case-insensitively
    columns = (column_type("id", Integer()), column_type("region", String()))

    # Then construction rejects it — references must spell their column exactly
    with pytest.raises(ValueError, match="Clustering column not found"):
        table_type(_QUALIFIED_NAME, columns, clustered_by=("REGION",))


@_EACH_TABLE_AND_COLUMN_TYPE
def test_table_rejects_duplicate_clustering_column(table_type, column_type):
    columns = (column_type("id", Integer()), column_type("region", String()))
    with pytest.raises(ValueError, match=r"[Cc]luster"):
        table_type(_QUALIFIED_NAME, columns, clustered_by=("region", "region"))


# ---------- referencing_foreign_keys ----------


def test_observed_table_carries_referencing_foreign_keys() -> None:
    # Given a ForeignKeyReference describing an inbound FK
    reference = ForeignKeyReference(
        name="orders_customer_id_fk",
        referencing_table=QualifiedName("dev", "silver", "orders"),
    )
    # When building an ObservedTable with referencing_foreign_keys
    observed = ObservedTable(
        qualified_name=QualifiedName("dev", "silver", "customers"),
        columns=(ObservedColumn("id", Integer()),),
        referencing_foreign_keys=(reference,),
    )
    # Then the field is readable and returns the value object
    assert observed.referencing_foreign_keys == (reference,)


def test_foreign_key_reference_rejects_blank_constraint_name() -> None:
    # Given a blank constraint_name
    # When / Then construction raises ValueError
    with pytest.raises(ValueError):
        ForeignKeyReference(
            name="  ",
            referencing_table=QualifiedName("dev", "silver", "orders"),
        )


def test_desired_table_rejects_rename_source_that_is_still_declared() -> None:
    with pytest.raises(ValueError, match="still declared"):
        DesiredTable(
            qualified_name=_QN,
            columns=(
                DesiredColumn("customer_nm", String()),
                DesiredColumn("customer_name", String(), renamed_from="customer_nm"),
            ),
        )


def test_desired_table_rejects_duplicate_rename_sources() -> None:
    with pytest.raises(ValueError, match="renamed_from 'customer_nm'"):
        DesiredTable(
            qualified_name=_QN,
            columns=(
                DesiredColumn("a", String(), renamed_from="customer_nm"),
                DesiredColumn("b", String(), renamed_from="customer_nm"),
            ),
        )


def test_desired_table_rejects_renames_outside_column_structure_scope() -> None:
    # Given a declaration managing only metadata aspects — renames imply
    # column-structure changes, which this scope does not manage
    with pytest.raises(ValueError, match="column structure"):
        DesiredTable(
            qualified_name=_QN,
            columns=(DesiredColumn("customer_name", String(), renamed_from="customer_nm"),),
            scope=TableScope.METADATA,
        )


def test_desired_table_accepts_a_valid_rename() -> None:
    table = DesiredTable(
        qualified_name=_QN,
        columns=(
            DesiredColumn("id", Integer(), nullable=False),
            DesiredColumn("customer_name", String(), renamed_from="customer_nm"),
        ),
    )
    assert table.columns[1].renamed_from == "customer_nm"


def test_desired_table_rejects_partitioning_by_a_rename_source() -> None:
    # Layout keys name desired columns and are never resolved through rename
    # hints: renaming a partition column must move partitioned_by to the new
    # name in the same declaration.
    with pytest.raises(ValueError, match="Partition column not found: day"):
        DesiredTable(
            qualified_name=_QN,
            columns=(
                DesiredColumn("id", Integer()),
                DesiredColumn("event_day", String(), renamed_from="day"),
            ),
            partitioned_by=("day",),
        )


def test_desired_table_rejects_clustering_by_a_rename_source() -> None:
    with pytest.raises(ValueError, match="Clustering column not found: day"):
        DesiredTable(
            qualified_name=_QN,
            columns=(
                DesiredColumn("id", Integer()),
                DesiredColumn("event_day", String(), renamed_from="day"),
            ),
            clustered_by=("day",),
        )


def test_desired_table_rejects_a_primary_key_on_a_rename_source() -> None:
    # Constraint columns name desired columns, exactly like layout keys: a
    # renamed key column moves the primary_key declaration to the new name.
    with pytest.raises(ValueError, match="Primary key column not found"):
        DesiredTable(
            qualified_name=_QN,
            columns=(
                DesiredColumn(
                    "customer_name", String(), nullable=False, renamed_from="customer_nm"
                ),
            ),
            primary_key=PrimaryKeyConstraint(columns=("customer_nm",), name="orders_pk"),
        )


def test_desired_table_rejects_a_foreign_key_local_column_on_a_rename_source() -> None:
    with pytest.raises(ValueError, match="Foreign key local column not found"):
        DesiredTable(
            qualified_name=_QN,
            columns=(DesiredColumn("parent_id", Integer(), renamed_from="parent"),),
            foreign_keys=(
                ForeignKeyConstraint(
                    local_columns=("parent",),
                    referenced_table=QualifiedName("c", "s", "parent"),
                    referenced_columns=("id",),
                    name="orders_parent_fk",
                ),
            ),
        )


def test_observed_table_kind_defaults_to_an_ordinary_table():
    # Kind is an observed fact with a safe default: construction sites that
    # predate relation kinds still describe an ordinary table.
    table = ObservedTable(
        qualified_name=QualifiedName("cat", "sch", "tbl"),
        columns=(ObservedColumn("id", Integer()),),
    )

    assert table.kind is TableKind.TABLE


def test_observed_table_carries_a_streaming_table_kind():
    table = ObservedTable(
        qualified_name=QualifiedName("cat", "sch", "tbl"),
        columns=(ObservedColumn("id", Integer()),),
        kind=TableKind.STREAMING_TABLE,
    )

    assert table.kind is TableKind.STREAMING_TABLE


def test_observed_table_supported_features_default_to_empty():
    # Given catalog state with no managed table features
    # When constructing the observed table without an explicit feature set
    table = ObservedTable(
        qualified_name=QualifiedName("cat", "sch", "tbl"),
        columns=(ObservedColumn("id", Integer()),),
    )

    # Then its canonical supported-feature set is empty
    assert table.supported_features == frozenset()
