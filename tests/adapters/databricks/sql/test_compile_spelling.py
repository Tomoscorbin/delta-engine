"""
The uniform spelling law, observed through the public compiler.

Every column reference the compiler renders is respelled: the catalog's
spelling where the column is known, the given spelling unchanged where it
is not. Scenarios that previously pinned respelling at the resolver and the
differ are pinned here instead — translation owns spelling.
"""

import inspect

from delta_engine.adapters.databricks.sql.compile import compile_plan
from delta_engine.adapters.databricks.sql.respell import _respell
from delta_engine.application.ports import CatalogSpellings
from delta_engine.domain.model import (
    DesiredColumn,
    DesiredTable,
    ObservedColumn,
    ObservedTable,
    QualifiedName,
    String,
)
from delta_engine.domain.model.constraints import ForeignKeyConstraint, PrimaryKeyConstraint
import delta_engine.domain.plan.actions as actions_module
from delta_engine.domain.plan.actions import (
    Action,
    ActionPlan,
    CreateTable,
    DropForeignKey,
    DropPrimaryKey,
    EnableTableFeature,
    RenameColumn,
    SetColumnComment,
    SetForeignKey,
    SetPrimaryKey,
    SetProperty,
    SetTableComment,
    SetTableTag,
    UnsetProperty,
    UnsetTableTag,
)

_ORDERS = QualifiedName("dev", "silver", "orders")
_CUSTOMERS = QualifiedName("dev", "silver", "customers")
_EMPLOYEES = QualifiedName("dev", "silver", "employees")


def _desired(name: QualifiedName, *columns: str) -> DesiredTable:
    return DesiredTable(
        qualified_name=name,
        columns=tuple(DesiredColumn(column, String()) for column in columns),
    )


def _observed(name: QualifiedName, *columns: str) -> ObservedTable:
    return ObservedTable(
        qualified_name=name,
        columns=tuple(ObservedColumn(column, String()) for column in columns),
    )


def _sql(action: Action, spellings: CatalogSpellings, target: QualifiedName = _ORDERS) -> str:
    (statement,) = compile_plan(ActionPlan(target=target, actions=(action,)), spellings)
    return statement


def test_set_primary_key_wears_the_catalog_spelling():
    # Given a PK declared camelCase over a column the catalog spells lowercase
    spellings = CatalogSpellings(((_desired(_ORDERS, "orderId"), _observed(_ORDERS, "orderid")),))
    action = SetPrimaryKey(primary_key=PrimaryKeyConstraint(("orderId",), "orders_pk"))

    # Then ADD CONSTRAINT carries the catalog's spelling
    assert _sql(action, spellings) == (
        "ALTER TABLE `dev`.`silver`.`orders` ADD CONSTRAINT `orders_pk` PRIMARY KEY (`orderid`)"
    )


def test_set_primary_key_keeps_the_declared_spelling_for_new_columns():
    # Given a PK over a declared column the catalog has not seen yet
    spellings = CatalogSpellings(((_desired(_ORDERS, "orderId"), _observed(_ORDERS, "other")),))
    action = SetPrimaryKey(primary_key=PrimaryKeyConstraint(("orderId",), "orders_pk"))

    # Then the new column keeps its declared spelling
    assert _sql(action, spellings) == (
        "ALTER TABLE `dev`.`silver`.`orders` ADD CONSTRAINT `orders_pk` PRIMARY KEY (`orderId`)"
    )


def test_set_foreign_key_wears_the_catalog_spelling_on_both_sides():
    # Given child column "customerId" the catalog spells "customerid", referencing
    # parent column "Id" the catalog spells "id"
    spellings = CatalogSpellings(
        (
            (_desired(_ORDERS, "customerId"), _observed(_ORDERS, "customerid")),
            (_desired(_CUSTOMERS, "Id"), _observed(_CUSTOMERS, "id")),
        )
    )
    action = SetForeignKey(
        constraint=ForeignKeyConstraint(
            local_columns=("customerId",),
            referenced_table=_CUSTOMERS,
            referenced_columns=("Id",),
            constraint_name="orders_customer_fk",
        )
    )

    assert _sql(action, spellings) == (
        "ALTER TABLE `dev`.`silver`.`orders` ADD CONSTRAINT `orders_customer_fk`"
        " FOREIGN KEY (`customerid`) REFERENCES `dev`.`silver`.`customers` (`id`)"
    )


def test_referenced_spelling_of_a_parent_created_this_sync_is_the_declared_one():
    # Given the referenced parent does not exist yet: declared spelling is all there is
    spellings = CatalogSpellings(
        (
            (_desired(_ORDERS, "customerId"), _observed(_ORDERS, "customerid")),
            (_desired(_CUSTOMERS, "Id"), None),
        )
    )
    action = SetForeignKey(
        constraint=ForeignKeyConstraint(
            local_columns=("customerId",),
            referenced_table=_CUSTOMERS,
            referenced_columns=("Id",),
            constraint_name="orders_customer_fk",
        )
    )

    assert _sql(action, spellings) == (
        "ALTER TABLE `dev`.`silver`.`orders` ADD CONSTRAINT `orders_customer_fk`"
        " FOREIGN KEY (`customerid`) REFERENCES `dev`.`silver`.`customers` (`Id`)"
    )


def test_self_referencing_foreign_key_wears_its_own_catalog_spelling():
    # Given a self-referencing key declared lowercase against camelCase catalog columns
    spellings = CatalogSpellings(
        ((_desired(_EMPLOYEES, "id", "managerid"), _observed(_EMPLOYEES, "Id", "ManagerId")),)
    )
    action = SetForeignKey(
        constraint=ForeignKeyConstraint(
            local_columns=("managerid",),
            referenced_table=_EMPLOYEES,
            referenced_columns=("id",),
            constraint_name="employees_manager_fk",
        )
    )

    assert _sql(action, spellings, target=_EMPLOYEES) == (
        "ALTER TABLE `dev`.`silver`.`employees` ADD CONSTRAINT `employees_manager_fk`"
        " FOREIGN KEY (`ManagerId`) REFERENCES `dev`.`silver`.`employees` (`Id`)"
    )


def test_foreign_key_to_a_renamed_parent_key_keeps_the_new_declared_name():
    # Given a parent renaming its key column — the catalog still spells the old
    # name — and a child referencing the declared new name
    spellings = CatalogSpellings(
        (
            (_desired(_ORDERS, "ref_id"), _observed(_ORDERS, "id", "ref_id")),
            (_desired(_CUSTOMERS, "orderNumber"), _observed(_CUSTOMERS, "OrderId")),
        )
    )
    action = SetForeignKey(
        constraint=ForeignKeyConstraint(
            local_columns=("ref_id",),
            referenced_table=_CUSTOMERS,
            referenced_columns=("orderNumber",),
            constraint_name="orders_ref_fk",
        )
    )

    # Then the reference keeps the declared post-rename name, not the observed old one
    assert _sql(action, spellings) == (
        "ALTER TABLE `dev`.`silver`.`orders` ADD CONSTRAINT `orders_ref_fk`"
        " FOREIGN KEY (`ref_id`) REFERENCES `dev`.`silver`.`customers` (`orderNumber`)"
    )


def test_ordinary_ddl_wears_the_catalog_spelling():
    # Given a column comment addressed with declared casing over a drifted catalog —
    # the law is uniform, not a constraint-DDL carve-out
    spellings = CatalogSpellings(
        ((_desired(_ORDERS, "CustomerID"), _observed(_ORDERS, "customerid")),)
    )
    action = SetColumnComment(column_name="CustomerID", desired_comment="who", observed_comment="")

    assert _sql(action, spellings) == (
        "ALTER TABLE `dev`.`silver`.`orders` ALTER COLUMN `customerid` COMMENT 'who'"
    )


def test_rename_column_wears_catalog_source_and_declared_target():
    # Given a rename whose source exists in the catalog and whose target does not
    spellings = CatalogSpellings(((_desired(_ORDERS, "OrderId"), _observed(_ORDERS, "legacyid")),))
    action = RenameColumn(old_name="LegacyId", new_name="OrderId")

    assert _sql(action, spellings) == (
        "ALTER TABLE `dev`.`silver`.`orders` RENAME COLUMN `legacyid` TO `OrderId`"
    )


def test_unregistered_referenced_table_keeps_the_given_spelling():
    # Given an FK whose referenced table is not in the spellings at all
    spellings = CatalogSpellings(((_desired(_ORDERS, "ref_id"), _observed(_ORDERS, "ref_id")),))
    action = SetForeignKey(
        constraint=ForeignKeyConstraint(
            local_columns=("ref_id",),
            referenced_table=_CUSTOMERS,
            referenced_columns=("Id",),
            constraint_name="orders_ref_fk",
        )
    )

    assert _sql(action, spellings) == (
        "ALTER TABLE `dev`.`silver`.`orders` ADD CONSTRAINT `orders_ref_fk`"
        " FOREIGN KEY (`ref_id`) REFERENCES `dev`.`silver`.`customers` (`Id`)"
    )


def test_create_table_renders_the_declared_spelling_verbatim():
    # Given a table created this sync: its declared spelling is the only one
    table = DesiredTable(
        qualified_name=_ORDERS,
        columns=(DesiredColumn("OrderId", String(), nullable=False),),
        primary_key=PrimaryKeyConstraint(("OrderId",), "orders_pk"),
    )
    spellings = CatalogSpellings(((table, None),))

    assert _sql(CreateTable(table), spellings) == (
        "CREATE TABLE `dev`.`silver`.`orders`"
        " (`OrderId` STRING NOT NULL, CONSTRAINT `orders_pk` PRIMARY KEY (`OrderId`))"
        " USING delta"
    )


def test_respelling_is_idempotent_over_already_catalog_spelled_actions():
    # Given the same constraint spelled as declared and as the catalog does —
    # the migration-safety property: upstream may pre-respell without changing SQL
    spellings = CatalogSpellings(
        (
            (_desired(_ORDERS, "customerId"), _observed(_ORDERS, "customerid")),
            (_desired(_CUSTOMERS, "Id"), _observed(_CUSTOMERS, "id")),
        )
    )
    declared = SetForeignKey(
        constraint=ForeignKeyConstraint(("customerId",), _CUSTOMERS, ("Id",), "fk")
    )
    catalog = SetForeignKey(
        constraint=ForeignKeyConstraint(("customerid",), _CUSTOMERS, ("id",), "fk")
    )

    assert _sql(declared, spellings) == _sql(catalog, spellings)


def test_empty_spellings_render_the_declared_spelling_verbatim():
    # Given no spellings at all, SQL is exactly what the plan declares
    action = SetPrimaryKey(primary_key=PrimaryKeyConstraint(("orderId",), "orders_pk"))

    assert _sql(action, CatalogSpellings(())) == (
        "ALTER TABLE `dev`.`silver`.`orders` ADD CONSTRAINT `orders_pk` PRIMARY KEY (`orderId`)"
    )


def _concrete_action_types() -> list[type[Action]]:
    """
    Return every concrete Action subclass exposed by the actions module.

    This uses the module namespace rather than Action.__subclasses__() because
    dataclass(slots=True) can leave stale pre-slots class objects there.
    """
    return [
        obj
        for _, obj in inspect.getmembers(actions_module, inspect.isclass)
        if issubclass(obj, Action)
        and obj is not Action
        and not getattr(obj, "__abstractmethods__", False)
    ]


# Actions that render no column references; everything else must be registered
# on the respell dispatcher. A new action type lands here or there — never in
# neither.
_SPELLING_FREE_ACTIONS = {
    DropForeignKey,  # renders only the constraint name
    DropPrimaryKey,  # DROP PRIMARY KEY renders no columns
    EnableTableFeature,
    SetProperty,
    SetTableComment,
    SetTableTag,
    UnsetProperty,
    UnsetTableTag,
}


def test_every_action_type_is_respelled_or_explicitly_spelling_free():
    respelled = set(_respell.registry) - {object}
    unaccounted = [
        action_type.__name__
        for action_type in _concrete_action_types()
        if action_type not in respelled and action_type not in _SPELLING_FREE_ACTIONS
    ]
    assert unaccounted == [], f"neither respelled nor declared spelling-free: {unaccounted}"
