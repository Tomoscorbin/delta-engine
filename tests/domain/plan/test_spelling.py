from delta_engine.domain.model import (
    DesiredColumn,
    DesiredTable,
    Integer,
    ObservedColumn,
    ObservedTable,
    QualifiedName,
    String,
)
from delta_engine.domain.model.constraints import ForeignKeyConstraint, PrimaryKeyConstraint
from delta_engine.domain.plan.spelling import adopt_catalog_spellings

_CHILD = QualifiedName("dev", "silver", "orders")
_PARENT = QualifiedName("dev", "silver", "customers")


def _adopt_one(desired: DesiredTable, observed: ObservedTable | None) -> DesiredTable:
    return adopt_catalog_spellings(((desired, observed),))[desired.qualified_name]


def _column_spellings(table: DesiredTable) -> tuple[str, ...]:
    return tuple(str(column.name) for column in table.columns)


def test_table_without_observed_state_keeps_declared_spellings():
    # Given a table that does not exist in the catalog yet
    desired = DesiredTable(
        qualified_name=_CHILD,
        columns=(DesiredColumn("orderId", String(), nullable=False),),
        primary_key=PrimaryKeyConstraint(columns=("orderId",), constraint_name="orders_pk"),
        clustered_by=("orderId",),
    )

    adopted = _adopt_one(desired, None)

    # Then there is no catalog spelling to adopt — everything stays as declared
    assert _column_spellings(adopted) == ("orderId",)
    assert adopted.primary_key is not None
    assert tuple(str(c) for c in adopted.primary_key.columns) == ("orderId",)
    assert tuple(str(c) for c in adopted.clustered_by) == ("orderId",)


def test_existing_columns_adopt_the_catalog_spelling():
    # Given a column declared lowercase that the catalog spells camelCase
    desired = DesiredTable(
        qualified_name=_CHILD,
        columns=(DesiredColumn("requestid", String()),),
    )
    observed = ObservedTable(
        qualified_name=_CHILD,
        columns=(ObservedColumn("requestId", String()),),
    )

    adopted = _adopt_one(desired, observed)

    # Then the column takes the catalog's spelling
    assert _column_spellings(adopted) == ("requestId",)


def test_new_columns_keep_their_declared_spelling():
    # Given one column the catalog already holds and one it does not
    desired = DesiredTable(
        qualified_name=_CHILD,
        columns=(DesiredColumn("requestid", String()), DesiredColumn("newCol", Integer())),
    )
    observed = ObservedTable(
        qualified_name=_CHILD,
        columns=(ObservedColumn("REQUESTID", String()),),
    )

    adopted = _adopt_one(desired, observed)

    # Then only the existing column adopts the catalog spelling
    assert _column_spellings(adopted) == ("REQUESTID", "newCol")


def test_primary_key_columns_adopt_the_catalog_spelling():
    # Given a key column whose catalog spelling differs from the declaration
    desired = DesiredTable(
        qualified_name=_CHILD,
        columns=(DesiredColumn("requestid", String(), nullable=False),),
        primary_key=PrimaryKeyConstraint(columns=("requestid",), constraint_name="orders_pk"),
    )
    observed = ObservedTable(
        qualified_name=_CHILD,
        columns=(ObservedColumn("requestId", String(), nullable=False),),
    )

    adopted = _adopt_one(desired, observed)

    # Then the key reference follows its column onto the catalog spelling
    assert adopted.primary_key is not None
    assert tuple(str(c) for c in adopted.primary_key.columns) == ("requestId",)


def test_clustering_references_adopt_the_catalog_spelling():
    # Given a clustering column whose catalog spelling differs from the declaration
    desired = DesiredTable(
        qualified_name=_CHILD,
        columns=(DesiredColumn("region", String()),),
        clustered_by=("region",),
    )
    observed = ObservedTable(
        qualified_name=_CHILD,
        columns=(ObservedColumn("Region", String()),),
    )

    adopted = _adopt_one(desired, observed)

    # Then the clustering reference follows its column onto the catalog spelling
    assert tuple(str(c) for c in adopted.clustered_by) == ("Region",)


def test_partition_references_adopt_the_catalog_spelling():
    # Given a partition column whose catalog spelling differs from the declaration
    desired = DesiredTable(
        qualified_name=_CHILD,
        columns=(DesiredColumn("visitdate", String()),),
        partitioned_by=("visitdate",),
    )
    observed = ObservedTable(
        qualified_name=_CHILD,
        columns=(ObservedColumn("VisitDate", String()),),
    )

    adopted = _adopt_one(desired, observed)

    # Then the partition reference follows its column onto the catalog spelling
    assert tuple(str(c) for c in adopted.partitioned_by) == ("VisitDate",)


def test_foreign_key_local_columns_adopt_the_child_catalog_spelling():
    # Given a foreign key whose local column the catalog spells differently
    constraint = ForeignKeyConstraint(
        local_columns=("orderref",),
        referenced_table=_PARENT,
        referenced_columns=("orderid",),
        constraint_name="orders_orderref_fk",
    )
    desired = DesiredTable(
        qualified_name=_CHILD,
        columns=(DesiredColumn("orderref", Integer()),),
        foreign_keys=(constraint,),
    )
    observed = ObservedTable(
        qualified_name=_CHILD,
        columns=(ObservedColumn("orderRef", Integer()),),
    )

    adopted = _adopt_one(desired, observed)

    # Then the local side follows the child's own catalog spelling
    [adopted_fk] = adopted.foreign_keys
    assert tuple(str(c) for c in adopted_fk.local_columns) == ("orderRef",)


def test_foreign_key_referenced_columns_adopt_the_parent_catalog_spelling():
    # Given the live-pinned scenario: the child declares `orderId` while the
    # catalog spells the parent's column `orderid`
    constraint = ForeignKeyConstraint(
        local_columns=("orderRef",),
        referenced_table=_PARENT,
        referenced_columns=("orderId",),
        constraint_name="orders_orderref_fk",
    )
    child = DesiredTable(
        qualified_name=_CHILD,
        columns=(DesiredColumn("orderRef", Integer()),),
        foreign_keys=(constraint,),
    )
    parent = DesiredTable(
        qualified_name=_PARENT,
        columns=(DesiredColumn("orderId", Integer(), nullable=False),),
        primary_key=PrimaryKeyConstraint(columns=("orderId",), constraint_name="customers_pk"),
    )
    parent_observed = ObservedTable(
        qualified_name=_PARENT,
        columns=(ObservedColumn("orderid", Integer(), nullable=False),),
        primary_key=PrimaryKeyConstraint(columns=("orderid",), constraint_name="customers_pk"),
    )

    # When adopting spellings for both tables together
    adopted = adopt_catalog_spellings(((child, None), (parent, parent_observed)))

    # Then the referenced side adopts the parent's catalog spelling —
    # constraint SQL must say `orderid`
    [adopted_fk] = adopted[_CHILD].foreign_keys
    assert tuple(str(c) for c in adopted_fk.referenced_columns) == ("orderid",)


def test_foreign_key_to_a_parent_created_this_sync_uses_the_parent_declared_spelling():
    # Given a foreign key to a parent that does not exist in the catalog yet
    constraint = ForeignKeyConstraint(
        local_columns=("orderRef",),
        referenced_table=_PARENT,
        referenced_columns=("ORDERID",),
        constraint_name="orders_orderref_fk",
    )
    child = DesiredTable(
        qualified_name=_CHILD,
        columns=(DesiredColumn("orderRef", Integer()),),
        foreign_keys=(constraint,),
    )
    parent = DesiredTable(
        qualified_name=_PARENT,
        columns=(DesiredColumn("orderId", Integer(), nullable=False),),
        primary_key=PrimaryKeyConstraint(columns=("orderId",), constraint_name="customers_pk"),
    )

    # When adopting spellings for both tables together
    adopted = adopt_catalog_spellings(((child, None), (parent, None)))

    # Then the referenced side uses the spelling the parent will be created with
    [adopted_fk] = adopted[_CHILD].foreign_keys
    assert tuple(str(c) for c in adopted_fk.referenced_columns) == ("orderId",)


def test_foreign_key_to_an_unregistered_parent_keeps_the_declared_spelling():
    # Given a foreign key to a table outside this sync
    constraint = ForeignKeyConstraint(
        local_columns=("orderRef",),
        referenced_table=_PARENT,
        referenced_columns=("parent_id",),
        constraint_name="orders_orderref_fk",
    )
    desired = DesiredTable(
        qualified_name=_CHILD,
        columns=(DesiredColumn("orderRef", Integer()),),
        foreign_keys=(constraint,),
    )

    adopted = _adopt_one(desired, None)

    # Then there is no parent snapshot to consult — the declared spelling stands
    [adopted_fk] = adopted.foreign_keys
    assert tuple(str(c) for c in adopted_fk.referenced_columns) == ("parent_id",)


def test_self_referencing_foreign_key_adopts_its_own_catalog_spelling():
    # Given a self-referencing key declared lowercase against camelCase catalog columns
    constraint = ForeignKeyConstraint(
        local_columns=("parentref",),
        referenced_table=_CHILD,
        referenced_columns=("id",),
        constraint_name="orders_parentref_fk",
    )
    desired = DesiredTable(
        qualified_name=_CHILD,
        columns=(
            DesiredColumn("id", Integer(), nullable=False),
            DesiredColumn("parentref", Integer()),
        ),
        primary_key=PrimaryKeyConstraint(columns=("id",), constraint_name="orders_pk"),
        foreign_keys=(constraint,),
    )
    observed = ObservedTable(
        qualified_name=_CHILD,
        columns=(
            ObservedColumn("Id", Integer(), nullable=False),
            ObservedColumn("ParentRef", Integer()),
        ),
        primary_key=PrimaryKeyConstraint(columns=("Id",), constraint_name="orders_pk"),
    )

    adopted = _adopt_one(desired, observed)

    # Then both sides adopt the table's own catalog spelling
    [adopted_fk] = adopted.foreign_keys
    assert tuple(str(c) for c in adopted_fk.local_columns) == ("ParentRef",)
    assert tuple(str(c) for c in adopted_fk.referenced_columns) == ("Id",)


def test_renamed_column_keeps_its_declared_new_name():
    # Given a column being renamed away from its observed spelling
    desired = DesiredTable(
        qualified_name=_CHILD,
        columns=(DesiredColumn("orderNumber", Integer(), renamed_from="orderid"),),
    )
    observed = ObservedTable(
        qualified_name=_CHILD,
        columns=(ObservedColumn("OrderId", Integer()),),
    )

    adopted = _adopt_one(desired, observed)

    # Then the new declared name wins and the rename origin is untouched
    [column] = adopted.columns
    assert str(column.name) == "orderNumber"
    assert column.renamed_from is not None
    assert str(column.renamed_from) == "orderid"


def test_foreign_key_to_a_renamed_parent_key_keeps_the_new_declared_name():
    # Given a parent renaming its key column and a child referencing the new name
    constraint = ForeignKeyConstraint(
        local_columns=("ref",),
        referenced_table=_PARENT,
        referenced_columns=("orderNumber",),
        constraint_name="orders_ref_fk",
    )
    child = DesiredTable(
        qualified_name=_CHILD,
        columns=(DesiredColumn("ref", Integer()),),
        foreign_keys=(constraint,),
    )
    parent = DesiredTable(
        qualified_name=_PARENT,
        columns=(DesiredColumn("orderNumber", Integer(), nullable=False, renamed_from="orderid"),),
        primary_key=PrimaryKeyConstraint(columns=("orderNumber",), constraint_name="customers_pk"),
    )
    parent_observed = ObservedTable(
        qualified_name=_PARENT,
        columns=(ObservedColumn("OrderId", Integer(), nullable=False),),
        primary_key=PrimaryKeyConstraint(columns=("OrderId",), constraint_name="customers_pk"),
    )

    # When adopting spellings for both tables together
    adopted = adopt_catalog_spellings(((child, None), (parent, parent_observed)))

    # Then the reference keeps the declared post-rename name, not the observed old one
    [adopted_fk] = adopted[_CHILD].foreign_keys
    assert tuple(str(c) for c in adopted_fk.referenced_columns) == ("orderNumber",)


def test_adoption_is_idempotent():
    # Given a table already respelled against the catalog
    desired = DesiredTable(
        qualified_name=_CHILD,
        columns=(DesiredColumn("requestid", String(), nullable=False),),
        primary_key=PrimaryKeyConstraint(columns=("requestid",), constraint_name="orders_pk"),
    )
    observed = ObservedTable(
        qualified_name=_CHILD,
        columns=(ObservedColumn("requestId", String(), nullable=False),),
    )

    # When adopting a second time
    once = _adopt_one(desired, observed)
    twice = _adopt_one(once, observed)

    # Then nothing changes
    assert _column_spellings(twice) == _column_spellings(once)
    assert once.primary_key is not None
    assert twice.primary_key is not None
    assert tuple(str(c) for c in twice.primary_key.columns) == tuple(
        str(c) for c in once.primary_key.columns
    )


def test_adoption_preserves_column_identity():
    # Given a table whose only difference from the catalog is casing
    desired = DesiredTable(
        qualified_name=_CHILD,
        columns=(DesiredColumn("requestid", String()),),
    )
    observed = ObservedTable(
        qualified_name=_CHILD,
        columns=(ObservedColumn("requestId", String()),),
    )

    adopted = _adopt_one(desired, observed)

    # Then adoption changes how the table is written, never what it means —
    # under case-insensitive Identifier equality the adopted table still
    # equals the declared one
    assert adopted == desired
