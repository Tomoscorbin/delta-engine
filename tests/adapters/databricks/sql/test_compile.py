import inspect

from delta_engine.adapters.databricks.sql.compile import _compile_action, compile_plan
from delta_engine.domain.model import Column, DesiredTable, Integer, QualifiedName, String
import delta_engine.domain.plan.actions as actions_module
from delta_engine.domain.plan.actions import (
    Action,
    ActionPlan,
    AddColumn,
    CreateTable,
    DropColumn,
    SetColumnComment,
    SetColumnNullability,
    SetProperty,
    SetTableComment,
)

_TARGET = QualifiedName("cat", "sch", "tbl")


def _create_table(*columns: Column, comment: str = "", properties=None, partitioned_by=()):
    """Wrap columns in a CreateTable action for the target table."""
    return CreateTable(
        table=DesiredTable(
            qualified_name=_TARGET,
            columns=columns,
            comment=comment,
            properties=properties or {},
            partitioned_by=partitioned_by,
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
    assert statement == (
        "ALTER TABLE `cat`.`sch`.`tbl` ALTER COLUMN `id` COMMENT 'primary key'"
    )


def test_set_column_comment_escapes_single_quotes_end_to_end():
    # Given a column comment containing a single quote
    statement = _compile_single(SetColumnComment(column_name="id", comment="it's the key"))

    # Then the quote is doubled in the emitted SQL
    assert statement == (
        "ALTER TABLE `cat`.`sch`.`tbl` ALTER COLUMN `id` COMMENT 'it''s the key'"
    )


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
