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
    desired = DesiredTable(
        qualified_name=_CHILD,
        columns=(DesiredColumn("orderId", String(), nullable=False),),
        primary_key=PrimaryKeyConstraint(columns=("orderId",), constraint_name="orders_pk"),
        clustered_by=("orderId",),
    )

    adopted = _adopt_one(desired, None)

    assert _column_spellings(adopted) == ("orderId",)
    assert adopted.primary_key is not None
    assert tuple(str(c) for c in adopted.primary_key.columns) == ("orderId",)
    assert tuple(str(c) for c in adopted.clustered_by) == ("orderId",)


def test_existing_columns_adopt_the_catalog_spelling():
    desired = DesiredTable(
        qualified_name=_CHILD,
        columns=(DesiredColumn("requestid", String()),),
    )
    observed = ObservedTable(
        qualified_name=_CHILD,
        columns=(ObservedColumn("requestId", String()),),
    )

    adopted = _adopt_one(desired, observed)

    assert _column_spellings(adopted) == ("requestId",)


def test_new_columns_keep_their_declared_spelling():
    desired = DesiredTable(
        qualified_name=_CHILD,
        columns=(DesiredColumn("requestid", String()), DesiredColumn("newCol", Integer())),
    )
    observed = ObservedTable(
        qualified_name=_CHILD,
        columns=(ObservedColumn("REQUESTID", String()),),
    )

    adopted = _adopt_one(desired, observed)

    assert _column_spellings(adopted) == ("REQUESTID", "newCol")


def test_primary_key_columns_adopt_the_catalog_spelling():
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

    assert adopted.primary_key is not None
    assert tuple(str(c) for c in adopted.primary_key.columns) == ("requestId",)


def test_clustering_references_adopt_the_catalog_spelling():
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

    assert tuple(str(c) for c in adopted.clustered_by) == ("Region",)


def test_partition_references_adopt_the_catalog_spelling():
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

    assert tuple(str(c) for c in adopted.partitioned_by) == ("VisitDate",)


def test_foreign_key_local_columns_adopt_the_child_catalog_spelling():
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

    [adopted_fk] = adopted.foreign_keys
    assert tuple(str(c) for c in adopted_fk.local_columns) == ("orderRef",)


def test_foreign_key_referenced_columns_adopt_the_parent_catalog_spelling():
    # The live-pinned scenario: the child declares `orderId`, the catalog
    # spells the parent's column `orderid` — constraint SQL must say `orderid`.
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

    adopted = adopt_catalog_spellings(((child, None), (parent, parent_observed)))

    [adopted_fk] = adopted[_CHILD].foreign_keys
    assert tuple(str(c) for c in adopted_fk.referenced_columns) == ("orderid",)


def test_foreign_key_to_a_parent_created_this_sync_uses_the_parent_declared_spelling():
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

    adopted = adopt_catalog_spellings(((child, None), (parent, None)))

    [adopted_fk] = adopted[_CHILD].foreign_keys
    assert tuple(str(c) for c in adopted_fk.referenced_columns) == ("orderId",)


def test_foreign_key_to_an_unregistered_parent_keeps_the_declared_spelling():
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

    [adopted_fk] = adopted.foreign_keys
    assert tuple(str(c) for c in adopted_fk.referenced_columns) == ("parent_id",)


def test_self_referencing_foreign_key_adopts_its_own_catalog_spelling():
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

    [adopted_fk] = adopted.foreign_keys
    assert tuple(str(c) for c in adopted_fk.local_columns) == ("ParentRef",)
    assert tuple(str(c) for c in adopted_fk.referenced_columns) == ("Id",)


def test_renamed_column_keeps_its_declared_new_name():
    desired = DesiredTable(
        qualified_name=_CHILD,
        columns=(DesiredColumn("orderNumber", Integer(), renamed_from="orderid"),),
    )
    observed = ObservedTable(
        qualified_name=_CHILD,
        columns=(ObservedColumn("OrderId", Integer()),),
    )

    adopted = _adopt_one(desired, observed)

    [column] = adopted.columns
    assert str(column.name) == "orderNumber"
    assert column.renamed_from is not None
    assert str(column.renamed_from) == "orderid"


def test_foreign_key_to_a_renamed_parent_key_keeps_the_new_declared_name():
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

    adopted = adopt_catalog_spellings(((child, None), (parent, parent_observed)))

    [adopted_fk] = adopted[_CHILD].foreign_keys
    assert tuple(str(c) for c in adopted_fk.referenced_columns) == ("orderNumber",)


def test_adoption_is_idempotent():
    desired = DesiredTable(
        qualified_name=_CHILD,
        columns=(DesiredColumn("requestid", String(), nullable=False),),
        primary_key=PrimaryKeyConstraint(columns=("requestid",), constraint_name="orders_pk"),
    )
    observed = ObservedTable(
        qualified_name=_CHILD,
        columns=(ObservedColumn("requestId", String(), nullable=False),),
    )

    once = _adopt_one(desired, observed)
    twice = _adopt_one(once, observed)

    assert _column_spellings(twice) == _column_spellings(once)
    assert once.primary_key is not None
    assert twice.primary_key is not None
    assert tuple(str(c) for c in twice.primary_key.columns) == tuple(
        str(c) for c in once.primary_key.columns
    )


def test_adoption_preserves_column_identity():
    # Identifier equality is case-insensitive, so respelling must never
    # change what the table means — only how it is written.
    desired = DesiredTable(
        qualified_name=_CHILD,
        columns=(DesiredColumn("requestid", String()),),
    )
    observed = ObservedTable(
        qualified_name=_CHILD,
        columns=(ObservedColumn("requestId", String()),),
    )

    adopted = _adopt_one(desired, observed)

    assert adopted == desired
