import inspect

from hypothesis import given
import pytest

from delta_engine.adapters.databricks.sql.compile import compile_plan
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
    TableFeature,
    TableKind,
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
    EnableTableFeature,
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
from tests.adapters.databricks.sql.strategies import MANAGED_PROPERTY_MAPS

_TARGET = QualifiedName("cat", "sch", "tbl")
_REFERENCED_TABLE = QualifiedName("cat", "sch", "customers")


def _observed_column(name: str) -> ObservedColumn:
    return ObservedColumn(name, Integer())


def _primary_key(
    columns: tuple[str, ...] = ("id",), name: str | None = "tbl_pk"
) -> PrimaryKeyConstraint:
    return PrimaryKeyConstraint(columns, name)


def _foreign_key(
    *,
    local_columns: tuple[str, ...] = ("customer_id",),
    referenced_table: QualifiedName = _REFERENCED_TABLE,
    referenced_columns: tuple[str, ...] = ("id",),
    name: str | None = "orders_customer_id_fk",
) -> ForeignKeyConstraint:
    return ForeignKeyConstraint(
        local_columns,
        referenced_table,
        referenced_columns,
        name,
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


def _plan(
    *plan_actions: Action,
    target: QualifiedName = _TARGET,
    kind: TableKind = TableKind.TABLE,
) -> ActionPlan:
    return ActionPlan(target=target, actions=plan_actions, kind=kind)


def _compile_single(action: Action, kind: TableKind = TableKind.TABLE) -> str:
    (statement,) = compile_plan(_plan(action, kind=kind)).statements
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


def test_compile_empty_plan_returns_an_empty_compiled_plan():
    plan = _plan()

    compiled = compile_plan(plan)

    assert compiled.plan is plan
    assert compiled.statements == ()


def test_compile_plan_compiles_each_action_in_action_plan_order():
    # Given an ActionPlan containing three actions
    plan = _plan(
        SetTableComment(desired_comment="first", observed_comment=""),
        SetProperty(name="second", desired_value="true", observed_value=None),
        DropColumn(column=_observed_column("third")),
    )

    # When compiling the plan
    compiled = compile_plan(plan)

    # Then each action in the normalized ActionPlan is compiled to its SQL statement
    assert compiled.plan is plan
    assert compiled.statements == tuple(_compile_single(action) for action in plan)


def test_compile_backticks_table_and_column_identifiers():
    # Given identifiers that need quoting
    target = QualifiedName("cat-alog", "sch ema", "select")
    plan = _plan(AddColumn(DesiredColumn("weird column", Integer())), target=target)

    # When compiling
    (statement,) = compile_plan(plan).statements

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


def test_create_table_renders_all_state_embedded_in_creation():
    # Given a CREATE TABLE with structure, comments, properties, partitioning, and a key
    action = _create_table(
        DesiredColumn("id", Integer(), nullable=False, comment="identifier"),
        DesiredColumn("day", String(), comment="partition date"),
        comment="core table",
        properties={"delta.appendOnly": "true"},
        partitioned_by=("day",),
        primary_key=_primary_key(),
    )

    # When compiling
    statement = _compile_single(action)

    # Then all state embedded in the creation aggregate is rendered
    assert statement == (
        "CREATE TABLE `cat`.`sch`.`tbl`"
        " (`id` INT NOT NULL COMMENT 'identifier', `day` STRING COMMENT 'partition date',"
        " CONSTRAINT `tbl_pk` PRIMARY KEY (`id`))"
        " USING delta"
        " COMMENT 'core table'"
        " TBLPROPERTIES ('delta.appendOnly'='true')"
        " PARTITIONED BY (`day`)"
    )


def test_create_table_omits_comment_clause_when_table_comment_is_empty():
    # Given a CREATE TABLE with no table-level comment
    action = _create_table(DesiredColumn("id", Integer()))

    # When compiling
    statement = _compile_single(action)

    # Then no empty table COMMENT clause is emitted
    assert statement == "CREATE TABLE `cat`.`sch`.`tbl` (`id` INT) USING delta"


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
        primary_key=PrimaryKeyConstraint(columns=("id",), name="tbl_pk"),
    )

    # When compiling
    statement = _compile_single(action)

    # Then the primary key constraint is inlined in the column list
    assert statement == (
        "CREATE TABLE `cat`.`sch`.`tbl`"
        " (`id` INT NOT NULL, `name` STRING, CONSTRAINT `tbl_pk` PRIMARY KEY (`id`))"
        " USING delta"
    )


def test_create_table_lets_databricks_name_an_unnamed_primary_key():
    action = _create_table(
        DesiredColumn("id", Integer(), nullable=False),
        primary_key=PrimaryKeyConstraint(columns=("id",)),
    )

    statement = _compile_single(action)

    assert statement == (
        "CREATE TABLE `cat`.`sch`.`tbl` (`id` INT NOT NULL, PRIMARY KEY (`id`)) USING delta"
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
        "CREATE TABLE `cat`.`sch`.`tbl`"
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
            "ALTER TABLE `cat`.`sch`.`tbl` ALTER COLUMN `id` COMMENT ''",
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
            DropPrimaryKey("tbl_pk"),
            "ALTER TABLE `cat`.`sch`.`tbl` DROP PRIMARY KEY IF EXISTS",
        ),
        (
            DropForeignKey(name="orders_customer_id_fk"),
            "ALTER TABLE `cat`.`sch`.`tbl` DROP CONSTRAINT IF EXISTS `orders_customer_id_fk`",
        ),
        (
            SetTableTag(name="env", desired_value="prod", observed_value=None),
            "ALTER TABLE `cat`.`sch`.`tbl` SET TAGS ('env'='prod')",
        ),
        (
            UnsetTableTag(name="env"),
            "ALTER TABLE `cat`.`sch`.`tbl` UNSET TAGS ('env')",
        ),
        (
            SetColumnTag(
                column_name="email", name="pii", desired_value="true", observed_value=None
            ),
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
    # Given a composite primary-key action declared in non-canonical order
    action = SetPrimaryKey(primary_key=_primary_key(("tenant_id", "order_id"), "tbl_pk"))

    # When compiling
    statement = _compile_single(action)

    # Then columns render in the constraint's canonical (sorted) order
    assert statement == (
        "ALTER TABLE `cat`.`sch`.`tbl`"
        " ADD CONSTRAINT `tbl_pk` PRIMARY KEY (`order_id`, `tenant_id`)"
    )


def test_set_primary_key_lets_databricks_choose_the_constraint_name():
    action = SetPrimaryKey(primary_key=_primary_key(("id",), None))

    assert _compile_single(action) == "ALTER TABLE `cat`.`sch`.`tbl` ADD PRIMARY KEY (`id`)"


def test_set_foreign_key_renders_single_column_fk():
    # Given a single-column foreign-key action
    action = SetForeignKey(constraint=_foreign_key(name="tbl_customer_id_fk"))

    # When compiling
    statement = _compile_single(action)

    # Then it renders ALTER TABLE ... ADD CONSTRAINT ... FOREIGN KEY
    assert statement == (
        "ALTER TABLE `cat`.`sch`.`tbl`"
        " ADD CONSTRAINT `tbl_customer_id_fk`"
        " FOREIGN KEY (`customer_id`) REFERENCES `cat`.`sch`.`customers` (`id`)"
    )


def test_set_foreign_key_lets_databricks_choose_the_constraint_name():
    action = SetForeignKey(constraint=_foreign_key(name=None))

    assert _compile_single(action) == (
        "ALTER TABLE `cat`.`sch`.`tbl`"
        " ADD FOREIGN KEY (`customer_id`) REFERENCES `cat`.`sch`.`customers` (`id`)"
    )


def test_set_foreign_key_renders_composite_fk():
    # Given a composite foreign-key action
    action = SetForeignKey(
        constraint=_foreign_key(
            local_columns=("tenant_id", "customer_id"),
            referenced_columns=("tenant_id", "id"),
            name="tbl_tenant_id_customer_id_fk",
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
            SetTableTag(name="o'k", desired_value="v'x", observed_value=None),
            "ALTER TABLE `cat`.`sch`.`tbl` SET TAGS ('o''k'='v''x')",
        ),
        (
            SetColumnTag(column_name="email", name="o'k", desired_value="v'x", observed_value=None),
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
    (first_statement,) = compile_plan(_plan(first_write)).statements
    (update_statement,) = compile_plan(_plan(update)).statements

    # Then observed_value has no effect on rendered SQL
    assert first_statement == update_statement


@pytest.mark.parametrize(
    ("addition", "replacement"),
    [
        (
            SetTableTag(name="env", desired_value="prod", observed_value=None),
            SetTableTag(name="env", desired_value="prod", observed_value="dev"),
        ),
        (
            SetColumnTag(
                column_name="email",
                name="pii",
                desired_value="true",
                observed_value=None,
            ),
            SetColumnTag(
                column_name="email",
                name="pii",
                desired_value="true",
                observed_value="false",
            ),
        ),
    ],
)
def test_set_tag_sql_ignores_observed_value(addition: Action, replacement: Action):
    assert _compile_single(addition) == _compile_single(replacement)


_SAMPLE_ACTIONS: dict[type[Action], Action] = {
    AddColumn: AddColumn(DesiredColumn("added", Integer())),
    AlterClustering: AlterClustering(desired_clustering=("id",), observed_clustering=()),
    AlterColumnType: AlterColumnType("id", Long(), Integer()),
    CreateTable: _create_table(DesiredColumn("id", Integer())),
    DropColumn: DropColumn(ObservedColumn("legacy", Integer())),
    DropForeignKey: DropForeignKey(name="orders_customer_id_fk"),
    DropPrimaryKey: DropPrimaryKey("tbl_pk"),
    EnableTableFeature: EnableTableFeature(feature=TableFeature.TIMESTAMP_NTZ),
    RenameColumn: RenameColumn("old", "new"),
    SetColumnComment: SetColumnComment("id", "new", "old"),
    SetColumnNullability: SetColumnNullability("id", False, True),
    SetColumnTag: SetColumnTag("id", "pii", "low", None),
    SetForeignKey: SetForeignKey(constraint=_foreign_key()),
    SetPrimaryKey: SetPrimaryKey(primary_key=_primary_key()),
    SetProperty: SetProperty("k", "v", None),
    SetTableComment: SetTableComment("new", "old"),
    SetTableTag: SetTableTag("env", "dev", None),
    UnsetColumnTag: UnsetColumnTag("id", "pii"),
    UnsetProperty: UnsetProperty("k", "v"),
    UnsetTableTag: UnsetTableTag("env"),
}


def test_every_action_type_compiles_through_the_public_compiler():
    # Given a sample instance of every concrete domain action type
    missing = [t.__name__ for t in _concrete_action_types() if t not in _SAMPLE_ACTIONS]
    assert missing == [], f"add a sample action for: {missing}"

    # Then each compiles to a statement — no action type can reach execution
    # without a registered compiler
    for action_type, action in _SAMPLE_ACTIONS.items():
        assert _compile_single(action), action_type.__name__


def test_compile_rename_column():
    plan = _plan(
        RenameColumn(old_name="customer_nm", new_name="customer_name"),
        target=QualifiedName("dev", "silver", "customers"),
    )
    statements = compile_plan(plan).statements
    assert statements == (
        "ALTER TABLE `dev`.`silver`.`customers` RENAME COLUMN `customer_nm` TO `customer_name`",
    )


@given(MANAGED_PROPERTY_MAPS)
def test_create_table_properties_are_mapping_order_independent_and_omit_absent_keys(
    properties: dict[str, str | None],
) -> None:
    # Given the same declared properties in two mapping insertion orders
    reversed_properties = dict(reversed(tuple(properties.items())))
    column = DesiredColumn("id", Integer())

    # When compiling a CREATE TABLE for each
    statement = _compile_single(_create_table(column, properties=properties))
    reversed_statement = _compile_single(_create_table(column, properties=reversed_properties))

    # Then the statements are identical, and None-declared keys never render
    assert statement == reversed_statement
    absent = [name for name, value in properties.items() if value is None]
    for name in absent:
        assert name not in statement
    if len(absent) == len(properties):
        assert "TBLPROPERTIES" not in statement


@pytest.mark.parametrize(
    ("action", "expected"),
    [
        (
            SetTableTag(name="owner", desired_value="gov", observed_value=None),
            "ALTER STREAMING TABLE `cat`.`sch`.`tbl` SET TAGS ('owner'='gov')",
        ),
        (
            UnsetTableTag(name="owner"),
            "ALTER STREAMING TABLE `cat`.`sch`.`tbl` UNSET TAGS ('owner')",
        ),
        (
            SetColumnTag(column_name="id", name="pii", desired_value="low", observed_value=None),
            "ALTER STREAMING TABLE `cat`.`sch`.`tbl` ALTER COLUMN `id` SET TAGS ('pii'='low')",
        ),
        (
            UnsetColumnTag(column_name="id", name="pii"),
            "ALTER STREAMING TABLE `cat`.`sch`.`tbl` ALTER COLUMN `id` UNSET TAGS ('pii')",
        ),
    ],
    ids=["set-table-tag", "unset-table-tag", "set-column-tag", "unset-column-tag"],
)
def test_tag_statements_compile_with_the_streaming_table_dialect(action, expected):
    assert _compile_single(action, kind=TableKind.STREAMING_TABLE) == expected


def test_ordinary_tables_keep_the_alter_table_dialect():
    statement = _compile_single(SetTableTag(name="owner", desired_value="gov", observed_value=None))

    assert statement == "ALTER TABLE `cat`.`sch`.`tbl` SET TAGS ('owner'='gov')"


def test_compile_enable_table_feature():
    # Given a canonical table-feature enablement action
    action = EnableTableFeature(feature=TableFeature.TIMESTAMP_NTZ)

    # When compiling it for Databricks
    statement = _compile_single(action)

    # Then the feature is enabled through its complete table-property assignment
    assert statement == (
        "ALTER TABLE `cat`.`sch`.`tbl` SET TBLPROPERTIES ('delta.feature.timestampNtz'='supported')"
    )


def test_compile_enable_table_feature_uses_documented_variant_key():
    # Given a canonical VARIANT enablement action
    action = EnableTableFeature(feature=TableFeature.VARIANT)

    # When compiling it for Databricks
    statement = _compile_single(action)

    # Then the adapter uses the currently documented preview property spelling
    assert statement == (
        "ALTER TABLE `cat`.`sch`.`tbl` SET TBLPROPERTIES"
        " ('delta.feature.variantType-preview'='supported')"
    )


def test_set_primary_key_emits_the_exact_bound_spelling():
    action = SetPrimaryKey(primary_key=PrimaryKeyConstraint(columns=("requestId",), name="tbl_pk"))
    plan = ActionPlan(target=_TARGET, actions=(action,))

    [statement] = compile_plan(plan).statements

    assert statement == (
        "ALTER TABLE `cat`.`sch`.`tbl` ADD CONSTRAINT `tbl_pk` PRIMARY KEY (`requestId`)"
    )


def test_create_table_emits_declared_spelling_for_columns_and_inline_key():
    table = DesiredTable(
        qualified_name=_TARGET,
        columns=(DesiredColumn("requestId", String(), nullable=False),),
        primary_key=PrimaryKeyConstraint(columns=("requestId",), name="tbl_pk"),
        clustered_by=("requestId",),
    )
    plan = ActionPlan(target=_TARGET, actions=(CreateTable(table),))

    [statement] = compile_plan(plan).statements

    # Then both the column definition and the inline key carry the declared spelling
    assert "`requestId` STRING NOT NULL" in statement
    assert "PRIMARY KEY (`requestId`)" in statement
    assert "CLUSTER BY (`requestId`)" in statement


def test_foreign_key_emits_exact_spelling_on_both_sides():
    constraint = ForeignKeyConstraint(
        local_columns=("orderRef",),
        referenced_table=_REFERENCED_TABLE,
        referenced_columns=("OrderId",),
        name="tbl_orderref_fk",
    )
    plan = ActionPlan(target=_TARGET, actions=(SetForeignKey(constraint=constraint),))

    [statement] = compile_plan(plan).statements

    # Then both sides carry their exact declared spelling
    assert "FOREIGN KEY (`orderRef`)" in statement
    assert "(`OrderId`)" in statement


def test_drop_foreign_key_emits_the_exact_observed_constraint_name():
    plan = ActionPlan(target=_TARGET, actions=(DropForeignKey(name="Legacy_FK_Name"),))

    [statement] = compile_plan(plan).statements

    assert "DROP CONSTRAINT IF EXISTS `Legacy_FK_Name`" in statement
