import inspect

import pytest

from delta_engine.adapters.databricks.sql.compile import _compile_action, compile_plan
from delta_engine.domain.model import (
    DesiredColumn,
    DesiredTable,
    Integer,
    Long,
    ObservedColumn,
    QualifiedName,
    String,
    Struct,
    StructField,
    Variant,
)
from delta_engine.domain.model.constraints import ForeignKeyConstraint, PrimaryKeyConstraint
import delta_engine.domain.plan.actions as actions_module
from delta_engine.domain.plan.actions import (
    Action,
    ActionPlan,
    AddColumn,
    AlterClustering,
    AlterColumnType,
    CreateTable,
    DropColumn,
    DropForeignKey,
    DropPrimaryKey,
    RenameColumn,
    SetColumnComment,
    SetColumnNullability,
    SetColumnTag,
    SetForeignKey,
    SetPrimaryKey,
    SetProperty,
    SetTableComment,
    SetTableTag,
    UnsetColumnTag,
    UnsetProperty,
    UnsetTableTag,
)

_TARGET = QualifiedName("cat", "sch", "tbl")
_REFERENCED_TABLE = QualifiedName("cat", "sch", "customers")


def _observed_column(name: str) -> ObservedColumn:
    return ObservedColumn(name, Integer())


def _primary_key(
    columns: tuple[str, ...] = ("id",), constraint_name: str = "tbl_pk"
) -> PrimaryKeyConstraint:
    return PrimaryKeyConstraint(columns, constraint_name)


def _foreign_key(
    *,
    local_columns: tuple[str, ...] = ("customer_id",),
    referenced_table: QualifiedName = _REFERENCED_TABLE,
    referenced_columns: tuple[str, ...] = ("id",),
    constraint_name: str = "orders_customer_id_fk",
) -> ForeignKeyConstraint:
    return ForeignKeyConstraint(
        local_columns,
        referenced_table,
        referenced_columns,
        constraint_name,
    )


def _create_table(
    *columns: DesiredColumn,
    comment: str = "",
    properties: dict[str, str | None] | None = None,
    partitioned_by: tuple[str, ...] = (),
    clustered_by: tuple[str, ...] = (),
    primary_key: PrimaryKeyConstraint | None = None,
) -> CreateTable:
    return CreateTable(
        table=DesiredTable(
            qualified_name=_TARGET,
            columns=columns,
            comment=comment,
            properties=properties or {},
            partitioned_by=partitioned_by,
            clustered_by=clustered_by,
            primary_key=primary_key,
        )
    )


def _compile_single(action: Action) -> str:
    (statement,) = compile_plan(_TARGET, ActionPlan(actions=(action,)))
    return statement


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


def test_compile_empty_plan_returns_empty_tuple():
    assert compile_plan(_TARGET, ActionPlan(actions=())) == ()


def test_compile_plan_compiles_each_action_in_action_plan_order():
    # Given an ActionPlan containing three actions
    plan = ActionPlan(
        actions=(
            SetTableComment(desired_comment="first", observed_comment=""),
            SetProperty(name="second", desired_value="true", observed_value=None),
            DropColumn(column=_observed_column("third")),
        )
    )

    # When compiling the plan
    statements = compile_plan(_TARGET, plan)

    # Then each action in the normalized ActionPlan is compiled to its SQL statement
    assert statements == tuple(_compile_single(action) for action in plan)


def test_compile_backticks_table_and_column_identifiers():
    # Given identifiers that need quoting
    target = QualifiedName("cat-alog", "sch ema", "select")
    plan = ActionPlan(actions=(AddColumn(DesiredColumn("weird column", Integer())),))

    # When compiling
    (statement,) = compile_plan(target, plan)

    # Then table and column identifiers are backticked
    assert statement == ("ALTER TABLE `cat-alog`.`sch ema`.`select` ADD COLUMN `weird column` INT")


def test_alter_column_type_compiles_to_alter_column_type_statement():
    # Given a validated widening Integer → Long
    action = AlterColumnType(column_name="id", desired_type=Long(), observed_type=Integer())

    # Then only the desired type reaches the SQL
    assert _compile_single(action) == "ALTER TABLE `cat`.`sch`.`tbl` ALTER COLUMN `id` TYPE BIGINT"


def test_add_column_with_comment_includes_comment_clause():
    # Given a new column with a comment
    action = AddColumn(DesiredColumn("age", Integer(), comment="user age"))

    # When compiling
    statement = _compile_single(action)

    # Then the comment is included
    assert statement == ("ALTER TABLE `cat`.`sch`.`tbl` ADD COLUMN `age` INT COMMENT 'user age'")


def test_add_column_without_comment_omits_comment_clause():
    # Given a new column with no comment
    action = AddColumn(DesiredColumn("age", Integer()))

    # When compiling
    statement = _compile_single(action)

    # Then no empty COMMENT clause is emitted
    assert statement == "ALTER TABLE `cat`.`sch`.`tbl` ADD COLUMN `age` INT"


def test_add_column_rejects_non_nullable_column():
    # Given an AddColumn action carrying a NOT NULL column
    action = AddColumn(DesiredColumn("age", Integer(), nullable=False))

    # When / Then compiling fails loudly rather than silently dropping NOT NULL
    with pytest.raises(AssertionError, match="age"):
        _compile_single(action)


def test_create_table_renders_columns_nullability_comments_table_comment_and_properties():
    # Given a CREATE TABLE with column metadata, table comment, and property
    action = _create_table(
        DesiredColumn("id", Integer(), nullable=False),
        DesiredColumn("name", String(), comment="customer"),
        comment="core table",
        properties={"delta.appendOnly": "true"},
    )

    # When compiling
    statement = _compile_single(action)

    # Then all CREATE TABLE clauses are rendered
    assert statement == (
        "CREATE TABLE IF NOT EXISTS `cat`.`sch`.`tbl`"
        " (`id` INT NOT NULL, `name` STRING COMMENT 'customer')"
        " USING delta"
        " COMMENT 'core table'"
        " TBLPROPERTIES ('delta.appendOnly'='true')"
    )


def test_create_table_omits_comment_clause_when_table_comment_is_empty():
    # Given a CREATE TABLE with no table-level comment
    action = _create_table(DesiredColumn("id", Integer()))

    # When compiling
    statement = _compile_single(action)

    # Then no empty table COMMENT clause is emitted
    assert statement == "CREATE TABLE IF NOT EXISTS `cat`.`sch`.`tbl` (`id` INT) USING delta"


def test_create_table_renders_partition_clause():
    # Given a partitioned table
    action = _create_table(
        DesiredColumn("id", Integer()),
        DesiredColumn("ds", String()),
        partitioned_by=("ds",),
    )

    # When compiling
    statement = _compile_single(action)

    # Then the partition clause is rendered
    assert statement.endswith("PARTITIONED BY (`ds`)")


def test_create_table_renders_cluster_by_clause():
    # Given a clustered table
    action = _create_table(
        DesiredColumn("id", Integer()),
        DesiredColumn("region", String()),
        clustered_by=("region",),
    )
    # When compiling
    statement = _compile_single(action)
    # Then the clustering clause is rendered
    assert statement.endswith("CLUSTER BY (`region`)")


def test_alter_clustering_renders_cluster_by():
    action = AlterClustering(desired_clustering=("region", "day"), observed_clustering=())
    assert _compile_single(action) == ("ALTER TABLE `cat`.`sch`.`tbl` CLUSTER BY (`region`, `day`)")


def test_alter_clustering_with_no_columns_renders_cluster_by_none():
    action = AlterClustering(desired_clustering=(), observed_clustering=("region",))
    assert _compile_single(action) == "ALTER TABLE `cat`.`sch`.`tbl` CLUSTER BY NONE"


def test_create_table_renders_properties_in_sorted_order_and_filters_none_values():
    # Given a desired table with valued properties and absence assertions
    action = _create_table(
        DesiredColumn("id", Integer()),
        properties={
            "z": "last",
            "delta.logRetentionDuration": None,
            "a": "first",
        },
    )

    # When compiling
    statement = _compile_single(action)

    # Then valued properties are deterministic and None values are omitted
    assert "TBLPROPERTIES ('a'='first', 'z'='last')" in statement
    assert "delta.logRetentionDuration" not in statement


def test_create_table_inlines_primary_key_constraint():
    # Given a CREATE TABLE with a primary key
    action = _create_table(
        DesiredColumn("id", Integer(), nullable=False),
        DesiredColumn("name", String()),
        primary_key=PrimaryKeyConstraint.generate(
            table_name=_TARGET.name,
            columns=("id",),
        ),
    )

    # When compiling
    statement = _compile_single(action)

    # Then the primary key constraint is inlined in the column list
    assert statement == (
        "CREATE TABLE IF NOT EXISTS `cat`.`sch`.`tbl`"
        " (`id` INT NOT NULL, `name` STRING, CONSTRAINT `tbl_pk` PRIMARY KEY (`id`))"
        " USING delta"
    )


def test_create_table_without_primary_key_omits_constraint_clause():
    # Given a CREATE TABLE with no primary key
    action = _create_table(DesiredColumn("id", Integer()))

    # When compiling
    statement = _compile_single(action)

    # Then no constraint clause appears
    assert "PRIMARY KEY" not in statement
    assert "CONSTRAINT" not in statement


def test_create_table_backticks_struct_field_names_and_renders_variant():
    # Given a CREATE TABLE with a struct column whose field name needs column
    # mapping, and a variant column
    action = _create_table(
        DesiredColumn("payload", Struct((StructField("order id", Integer()),))),
        DesiredColumn("attributes", Variant()),
        properties={"delta.columnMapping.mode": "name"},
    )

    # When compiling
    statement = _compile_single(action)

    # Then the struct field name is backtick-quoted and VARIANT is rendered
    assert statement == (
        "CREATE TABLE IF NOT EXISTS `cat`.`sch`.`tbl`"
        " (`payload` STRUCT<`order id`: INT>, `attributes` VARIANT)"
        " USING delta"
        " TBLPROPERTIES ('delta.columnMapping.mode'='name')"
    )


@pytest.mark.parametrize(
    ("action", "expected"),
    [
        (
            DropColumn(column=_observed_column("legacy")),
            "ALTER TABLE `cat`.`sch`.`tbl` DROP COLUMN `legacy`",
        ),
        (
            SetProperty(name="delta.appendOnly", desired_value="true", observed_value=None),
            "ALTER TABLE `cat`.`sch`.`tbl` SET TBLPROPERTIES ('delta.appendOnly'='true')",
        ),
        (
            UnsetProperty(name="delta.enableChangeDataFeed", observed_value="true"),
            "ALTER TABLE `cat`.`sch`.`tbl` UNSET TBLPROPERTIES IF EXISTS "
            "('delta.enableChangeDataFeed')",
        ),
        (
            SetTableComment(desired_comment="core table", observed_comment=""),
            "COMMENT ON TABLE `cat`.`sch`.`tbl` IS 'core table'",
        ),
        (
            SetColumnComment(column_name="id", desired_comment="primary key", observed_comment=""),
            "ALTER TABLE `cat`.`sch`.`tbl` ALTER COLUMN `id` COMMENT 'primary key'",
        ),
        (
            SetColumnComment(column_name="id", desired_comment="", observed_comment="old"),
            "ALTER TABLE `cat`.`sch`.`tbl` ALTER COLUMN `id` UNSET COMMENT",
        ),
        (
            SetColumnNullability(column_name="id", desired_nullable=True, observed_nullable=False),
            "ALTER TABLE `cat`.`sch`.`tbl` ALTER COLUMN `id` DROP NOT NULL",
        ),
        (
            SetColumnNullability(column_name="id", desired_nullable=False, observed_nullable=True),
            "ALTER TABLE `cat`.`sch`.`tbl` ALTER COLUMN `id` SET NOT NULL",
        ),
        (
            DropPrimaryKey(primary_key=_primary_key(), referencing_foreign_keys=()),
            "ALTER TABLE `cat`.`sch`.`tbl` DROP PRIMARY KEY IF EXISTS",
        ),
        (
            DropForeignKey(constraint=_foreign_key()),
            "ALTER TABLE `cat`.`sch`.`tbl` DROP CONSTRAINT IF EXISTS `orders_customer_id_fk`",
        ),
        (
            SetTableTag(name="env", value="prod"),
            "ALTER TABLE `cat`.`sch`.`tbl` SET TAGS ('env'='prod')",
        ),
        (
            UnsetTableTag(name="env"),
            "ALTER TABLE `cat`.`sch`.`tbl` UNSET TAGS ('env')",
        ),
        (
            SetColumnTag(column_name="email", name="pii", value="true"),
            "ALTER TABLE `cat`.`sch`.`tbl` ALTER COLUMN `email` SET TAGS ('pii'='true')",
        ),
        (
            UnsetColumnTag(column_name="email", name="pii"),
            "ALTER TABLE `cat`.`sch`.`tbl` ALTER COLUMN `email` UNSET TAGS ('pii')",
        ),
    ],
)
def test_simple_actions_compile_to_expected_sql(action: Action, expected: str):
    assert _compile_single(action) == expected


def test_set_primary_key_renders_composite_primary_key():
    # Given a composite primary-key action
    action = SetPrimaryKey(primary_key=_primary_key(("tenant_id", "order_id"), "tbl_pk"))

    # When compiling
    statement = _compile_single(action)

    # Then all columns are rendered in order
    assert statement == (
        "ALTER TABLE `cat`.`sch`.`tbl`"
        " ADD CONSTRAINT `tbl_pk` PRIMARY KEY (`tenant_id`, `order_id`)"
    )


def test_set_foreign_key_renders_single_column_fk():
    # Given a single-column foreign-key action
    action = SetForeignKey(constraint=_foreign_key(constraint_name="tbl_customer_id_fk"))

    # When compiling
    statement = _compile_single(action)

    # Then it renders ALTER TABLE ... ADD CONSTRAINT ... FOREIGN KEY
    assert statement == (
        "ALTER TABLE `cat`.`sch`.`tbl`"
        " ADD CONSTRAINT `tbl_customer_id_fk`"
        " FOREIGN KEY (`customer_id`) REFERENCES `cat`.`sch`.`customers` (`id`)"
    )


def test_set_foreign_key_renders_composite_fk():
    # Given a composite foreign-key action
    action = SetForeignKey(
        constraint=_foreign_key(
            local_columns=("tenant_id", "customer_id"),
            referenced_columns=("tenant_id", "id"),
            constraint_name="tbl_tenant_id_customer_id_fk",
        )
    )

    # When compiling
    statement = _compile_single(action)

    # Then the complete domain constraint's canonical pair order is rendered
    assert statement == (
        "ALTER TABLE `cat`.`sch`.`tbl`"
        " ADD CONSTRAINT `tbl_tenant_id_customer_id_fk`"
        " FOREIGN KEY (`customer_id`, `tenant_id`)"
        " REFERENCES `cat`.`sch`.`customers` (`id`, `tenant_id`)"
    )


@pytest.mark.parametrize(
    ("action", "expected"),
    [
        (
            SetProperty(name="owner", desired_value="O'Reilly", observed_value=None),
            "ALTER TABLE `cat`.`sch`.`tbl` SET TBLPROPERTIES ('owner'='O''Reilly')",
        ),
        (
            SetColumnComment(column_name="id", desired_comment="it's the key", observed_comment=""),
            "ALTER TABLE `cat`.`sch`.`tbl` ALTER COLUMN `id` COMMENT 'it''s the key'",
        ),
        (
            SetTableTag(name="o'k", value="v'x"),
            "ALTER TABLE `cat`.`sch`.`tbl` SET TAGS ('o''k'='v''x')",
        ),
        (
            SetColumnTag(column_name="email", name="o'k", value="v'x"),
            "ALTER TABLE `cat`.`sch`.`tbl` ALTER COLUMN `email` SET TAGS ('o''k'='v''x')",
        ),
    ],
)
def test_string_literals_escape_single_quotes(action: Action, expected: str):
    assert _compile_single(action) == expected


def test_set_property_sql_ignores_observed_value():
    # Given two SetProperty actions differing only in observed_value
    first_write = SetProperty(
        name="delta.enableChangeDataFeed",
        desired_value="true",
        observed_value=None,
    )
    update = SetProperty(
        name="delta.enableChangeDataFeed",
        desired_value="true",
        observed_value="false",
    )

    # When compiling both
    (first_statement,) = compile_plan(_TARGET, ActionPlan(actions=(first_write,)))
    (update_statement,) = compile_plan(_TARGET, ActionPlan(actions=(update,)))

    # Then observed_value has no effect on rendered SQL
    assert first_statement == update_statement


def test_every_action_type_has_a_registered_compiler():
    # Given every concrete domain action type
    fallback = _compile_action.dispatch(object)

    # When checking the singledispatch registry
    unregistered = [
        action_type.__name__
        for action_type in _concrete_action_types()
        if _compile_action.dispatch(action_type) is fallback
    ]

    # Then every action has a specific compiler
    assert unregistered == []


def test_compile_rename_column():
    plan = ActionPlan((RenameColumn(old_name="customer_nm", new_name="customer_name"),))
    statements = compile_plan(QualifiedName("dev", "silver", "customers"), plan)
    assert statements == (
        "ALTER TABLE `dev`.`silver`.`customers` RENAME COLUMN `customer_nm` TO `customer_name`",
    )
