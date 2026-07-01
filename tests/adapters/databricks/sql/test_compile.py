import inspect

import pytest

from delta_engine.adapters.databricks.sql.compile import (
    _compile_action,
    compile_plan,
    derive_constraint_name,
)
from delta_engine.domain.model import Column, DesiredTable, Integer, QualifiedName, String
from delta_engine.domain.model.foreign_key import ForeignKeyConstraint
from delta_engine.domain.model.primary_key import PrimaryKeyConstraint
import delta_engine.domain.plan.actions as actions_module
from delta_engine.domain.plan.actions import (
    Action,
    ActionPlan,
    AddColumn,
    CreateTable,
    DropColumn,
    DropForeignKey,
    DropPrimaryKey,
    SetColumnComment,
    SetColumnNullability,
    SetForeignKey,
    SetPrimaryKey,
    SetProperty,
    SetTableComment,
    UnsupportedChange,
    UnsupportedChangeKind,
)

_TARGET = QualifiedName("cat", "sch", "tbl")


def _create_table(
    *columns: Column,
    comment: str = "",
    properties=None,
    partitioned_by=(),
    primary_key: PrimaryKeyConstraint | None = None,
):
    """Wrap columns in a CreateTable action for the target table."""
    return CreateTable(
        table=DesiredTable(
            qualified_name=_TARGET,
            columns=columns,
            comment=comment,
            properties=properties or {},
            partitioned_by=partitioned_by,
            primary_key=primary_key,
        )
    )


def _concrete_action_types():
    """
    Return every concrete Action subclass the actions module exposes.

    Discovered from the module namespace rather than ``Action.__subclasses__()``
    because ``@dataclass(slots=True)`` leaves a stale pre-slots class object in
    ``__subclasses__()``; the module binds each name to the real class.
    """
    return [
        obj
        for _, obj in inspect.getmembers(actions_module, inspect.isclass)
        if issubclass(obj, Action)
        and obj is not Action
        and not getattr(obj, "__abstractmethods__", False)
    ]


def _compile_single(action) -> str:
    """Compile a one-action plan and return the single SQL statement."""
    (statement,) = compile_plan(_TARGET, ActionPlan(actions=(action,)))
    return statement


def test_add_column_with_comment_includes_comment_clause():
    # Given a new column that carries a comment
    action = AddColumn(Column("age", Integer(), comment="user age"))

    # When compiling the add
    statement = _compile_single(action)

    # Then the ADD COLUMN carries the comment
    assert statement == ("ALTER TABLE `cat`.`sch`.`tbl` ADD COLUMN `age` INT COMMENT 'user age'")


def test_add_column_without_comment_omits_comment_clause():
    # Given a new column with no comment (the common case)
    action = AddColumn(Column("age", Integer()))

    # When compiling the add
    statement = _compile_single(action)

    # Then no empty COMMENT clause is emitted
    assert statement == "ALTER TABLE `cat`.`sch`.`tbl` ADD COLUMN `age` INT"


def test_create_table_renders_column_comment_in_the_definition():
    # Given a CREATE TABLE whose column carries a comment
    action = _create_table(Column("id", Integer(), comment="primary key"))

    # When compiling the create
    statement = _compile_single(action)

    # Then the column definition carries the comment (lost before this fix, so a
    # freshly-created table kept its column comments only after a second sync)
    assert "`id` INT COMMENT 'primary key'" in statement


def test_create_table_renders_columns_nullability_comment_and_properties():
    # Given a CREATE TABLE with a NOT NULL column, a table comment, and a property
    action = _create_table(
        Column("id", Integer(), nullable=False),
        Column("name", String(), comment="customer"),
        comment="core table",
        properties={"delta.appendOnly": "true"},
    )

    # When compiling the create
    statement = _compile_single(action)

    # Then columns, nullability, comments, USING, table comment, and properties all render
    assert statement == (
        "CREATE TABLE IF NOT EXISTS `cat`.`sch`.`tbl`"
        " (`id` INT NOT NULL, `name` STRING COMMENT 'customer')"
        " USING delta"
        " COMMENT 'core table'"
        " TBLPROPERTIES ('delta.appendOnly'='true')"
    )


def test_create_table_omits_comment_clause_when_table_comment_is_empty():
    # Given a CREATE TABLE with no table-level comment
    action = _create_table(Column("id", Integer()))

    # When compiling the create
    statement = _compile_single(action)

    # Then no empty COMMENT '' clause is emitted
    assert "COMMENT" not in statement
    assert statement == "CREATE TABLE IF NOT EXISTS `cat`.`sch`.`tbl` (`id` INT) USING delta"


def test_create_table_renders_partition_clause():
    # Given a CREATE TABLE partitioned by a column
    action = _create_table(
        Column("id", Integer()),
        Column("ds", String()),
        partitioned_by=("ds",),
    )

    # When compiling the create
    statement = _compile_single(action)

    # Then a PARTITIONED BY clause names the partition column
    assert statement.endswith("PARTITIONED BY (`ds`)")


def test_add_column_rejects_non_nullable_column():
    # Given an AddColumn carrying a NOT NULL column -- validation (NonNullableColumnAdd)
    # blocks this before execution, so the compiler must never silently emit an
    # ADD COLUMN that drops the NOT NULL constraint
    action = AddColumn(Column("age", Integer(), nullable=False))

    # Then compiling it fails loudly rather than producing nullable-by-stealth SQL
    with pytest.raises(AssertionError, match="age"):
        _compile_single(action)


def test_drop_column_renders_alter_drop():
    # When compiling a DropColumn
    statement = _compile_single(DropColumn("legacy"))

    # Then it renders an ALTER TABLE ... DROP COLUMN
    assert statement == "ALTER TABLE `cat`.`sch`.`tbl` DROP COLUMN `legacy`"


def test_set_property_renders_alter_set_tblproperties():
    # When compiling a SetProperty
    statement = _compile_single(SetProperty(name="delta.appendOnly", value="true"))

    # Then it renders an ALTER TABLE ... SET TBLPROPERTIES
    assert statement == (
        "ALTER TABLE `cat`.`sch`.`tbl` SET TBLPROPERTIES ('delta.appendOnly'='true')"
    )


def test_set_property_escapes_single_quotes_in_value_end_to_end():
    # Given a property value containing a single quote (the escaping edge case)
    statement = _compile_single(SetProperty(name="owner", value="O'Reilly"))

    # Then the quote is doubled in the emitted SQL, all the way through compile
    assert statement == "ALTER TABLE `cat`.`sch`.`tbl` SET TBLPROPERTIES ('owner'='O''Reilly')"


def test_set_column_comment_renders_alter_alter_column_comment():
    # When compiling a SetColumnComment
    statement = _compile_single(SetColumnComment(column_name="id", comment="primary key"))

    # Then it renders an ALTER COLUMN ... COMMENT
    assert statement == ("ALTER TABLE `cat`.`sch`.`tbl` ALTER COLUMN `id` COMMENT 'primary key'")


def test_set_column_comment_escapes_single_quotes_end_to_end():
    # Given a column comment containing a single quote
    statement = _compile_single(SetColumnComment(column_name="id", comment="it's the key"))

    # Then the quote is doubled in the emitted SQL
    assert statement == ("ALTER TABLE `cat`.`sch`.`tbl` ALTER COLUMN `id` COMMENT 'it''s the key'")


def test_set_column_comment_emits_unset_when_comment_is_empty():
    # Given a SetColumnComment with an empty comment (clearing the comment)
    statement = _compile_single(SetColumnComment(column_name="id", comment=""))

    # Then it emits UNSET COMMENT rather than COMMENT '' to avoid storing an empty string
    assert statement == "ALTER TABLE `cat`.`sch`.`tbl` ALTER COLUMN `id` UNSET COMMENT"


def test_set_table_comment_renders_comment_on_table():
    # When compiling a SetTableComment
    statement = _compile_single(SetTableComment(comment="core table"))

    # Then it renders a COMMENT ON TABLE
    assert statement == "COMMENT ON TABLE `cat`.`sch`.`tbl` IS 'core table'"


def test_set_column_nullability_drops_not_null_when_made_nullable():
    # When compiling a loosening to nullable
    statement = _compile_single(SetColumnNullability(column_name="id", nullable=True))

    # Then it DROPs the NOT NULL constraint
    assert statement == "ALTER TABLE `cat`.`sch`.`tbl` ALTER COLUMN `id` DROP NOT NULL"


def test_set_column_nullability_sets_not_null_when_made_non_nullable():
    # When compiling a tightening to NOT NULL
    statement = _compile_single(SetColumnNullability(column_name="id", nullable=False))

    # Then it SETs the NOT NULL constraint
    assert statement == "ALTER TABLE `cat`.`sch`.`tbl` ALTER COLUMN `id` SET NOT NULL"


def test_every_action_type_has_a_registered_compiler():
    # Given every concrete Action subclass the domain defines
    fallback = _compile_action.dispatch(object)

    # Then each one resolves to a dedicated compiler, not the raising fallback
    # (so a newly added action cannot silently lack SQL until it hits a plan)
    unregistered = [
        action_type.__name__
        for action_type in _concrete_action_types()
        if _compile_action.dispatch(action_type) is fallback
    ]
    assert unregistered == []


def test_unsupported_change_raises_at_compile_time():
    # Given an UnsupportedChange (never meant to reach the compiler)
    action = UnsupportedChange(
        kind=UnsupportedChangeKind.COLUMN_TYPE,
        subject_name="id",
        from_repr="Integer",
        to_repr="Long",
    )
    # When / Then compiling it is an internal-invariant violation
    with pytest.raises(AssertionError):
        _compile_single(action)


def test_drop_primary_key_renders_alter_drop_primary_key():
    # When compiling a DropPrimaryKey action
    statement = _compile_single(DropPrimaryKey())

    # Then it renders ALTER TABLE ... DROP PRIMARY KEY IF EXISTS
    assert statement == "ALTER TABLE `cat`.`sch`.`tbl` DROP PRIMARY KEY IF EXISTS"


def test_set_primary_key_renders_add_constraint_primary_key():
    # Given a SetPrimaryKey with two columns (compiler derives the constraint name)
    action = SetPrimaryKey(
        columns=(
            Column(name="tenant_id", data_type=Integer(), nullable=False),
            Column(name="order_id", data_type=Integer(), nullable=False),
        ),
    )

    # When compiling the action
    statement = _compile_single(action)

    # Then it renders ALTER TABLE ... ADD CONSTRAINT ... PRIMARY KEY (...)
    assert statement == (
        "ALTER TABLE `cat`.`sch`.`tbl`"
        " ADD CONSTRAINT `tbl_pk` PRIMARY KEY (`tenant_id`, `order_id`)"
    )


def test_set_primary_key_renders_alter_add_constraint():
    # When compiling a SetPrimaryKey with one column (compiler derives the constraint name)
    action = SetPrimaryKey(
        columns=(Column("id", Integer(), nullable=False),),
    )
    statement = _compile_single(action)

    # Then it renders ALTER TABLE ... ADD CONSTRAINT ... PRIMARY KEY (...)
    assert statement == ("ALTER TABLE `cat`.`sch`.`tbl` ADD CONSTRAINT `tbl_pk` PRIMARY KEY (`id`)")


def test_set_primary_key_renders_multiple_columns():
    # When compiling a SetPrimaryKey with two columns (compiler derives the constraint name)
    action = SetPrimaryKey(
        columns=(
            Column("id", Integer(), nullable=False),
            Column("tenant_id", Integer(), nullable=False),
        ),
    )
    statement = _compile_single(action)

    # Then both columns appear in the PRIMARY KEY clause
    assert statement == (
        "ALTER TABLE `cat`.`sch`.`tbl` ADD CONSTRAINT `tbl_pk` PRIMARY KEY (`id`, `tenant_id`)"
    )


def test_create_table_inlines_primary_key_constraint():
    # Given a CREATE TABLE with a primary key column
    action = _create_table(
        Column("id", Integer(), nullable=False),
        Column("name", String()),
        primary_key=PrimaryKeyConstraint(columns=("id",)),
    )
    statement = _compile_single(action)

    # Then the constraint is inlined in the column list
    assert "CONSTRAINT `tbl_pk` PRIMARY KEY (`id`)" in statement
    # And it appears after the column definitions, inside the parentheses
    assert statement.startswith("CREATE TABLE IF NOT EXISTS `cat`.`sch`.`tbl` (")


def test_create_table_without_pk_omits_constraint_clause():
    # Given a CREATE TABLE with no primary key
    action = _create_table(Column("id", Integer()))
    statement = _compile_single(action)

    # Then no CONSTRAINT clause appears
    assert "PRIMARY KEY" not in statement
    assert "CONSTRAINT" not in statement


def test_drop_foreign_key_renders_drop_constraint_if_exists():
    # Given
    action = DropForeignKey(constraint_name="orders_customer_id_fk")

    # When
    statement = _compile_single(action)

    # Then
    assert statement == (
        "ALTER TABLE `cat`.`sch`.`tbl` DROP CONSTRAINT IF EXISTS `orders_customer_id_fk`"
    )


def test_set_foreign_key_renders_add_constraint_foreign_key():
    # Given
    fk = ForeignKeyConstraint(
        local_columns=("customer_id",),
        references="cat.sch.customers",
        referenced_columns=("id",),
    )
    action = SetForeignKey(foreign_key=fk)

    # When
    statement = _compile_single(action)

    # Then
    assert statement == (
        "ALTER TABLE `cat`.`sch`.`tbl`"
        " ADD CONSTRAINT `tbl_customer_id_fk`"
        " FOREIGN KEY (`customer_id`) REFERENCES `cat`.`sch`.`customers` (`id`)"
    )


def test_set_foreign_key_renders_composite_fk():
    # Given a composite FK (two local columns, two referenced columns)
    fk = ForeignKeyConstraint(
        local_columns=("tenant_id", "customer_id"),
        references="cat.sch.customers",
        referenced_columns=("tenant_id", "id"),
    )
    action = SetForeignKey(foreign_key=fk)

    # When
    statement = _compile_single(action)

    # Then both column pairs appear
    assert statement == (
        "ALTER TABLE `cat`.`sch`.`tbl`"
        " ADD CONSTRAINT `tbl_tenant_id_customer_id_fk`"
        " FOREIGN KEY (`tenant_id`, `customer_id`)"
        " REFERENCES `cat`.`sch`.`customers` (`tenant_id`, `id`)"
    )


def test_derive_constraint_name_for_primary_key():
    # Given a table name and no local columns (the primary-key case)
    # Then the derived name is {table}_pk
    assert derive_constraint_name("orders", None) == "orders_pk"


def test_derive_constraint_name_for_single_column_foreign_key():
    # Given a table name and one local column
    # Then the derived name joins the column between the table name and _fk
    assert derive_constraint_name("orders", ("customer_id",)) == "orders_customer_id_fk"


def test_derive_constraint_name_for_composite_foreign_key():
    # Given a table name and several local columns
    # Then every local column appears, in order, joined by underscores
    assert (
        derive_constraint_name("orders", ("tenant_id", "customer_id"))
        == "orders_tenant_id_customer_id_fk"
    )
