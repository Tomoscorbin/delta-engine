from delta_engine.adapters.databricks.sql.compile import compile_plan
from delta_engine.domain.model import Column, Integer, QualifiedName
from delta_engine.domain.plan.actions import ActionPlan, AddColumn

_TARGET = QualifiedName("cat", "sch", "tbl")


def _compile_single(action) -> str:
    """Compile a one-action plan and return the single SQL statement."""
    (statement,) = compile_plan(ActionPlan(target=_TARGET, actions=(action,)))
    return statement


def test_add_column_with_comment_includes_comment_clause():
    # Given a new column that carries a comment
    action = AddColumn(Column("age", Integer(), comment="user age"))

    # When compiling the add
    statement = _compile_single(action)

    # Then the ADD COLUMN carries the comment
    assert statement == (
        "ALTER TABLE `cat`.`sch`.`tbl` ADD COLUMN `age` INT COMMENT 'user age'"
    )


def test_add_column_without_comment_omits_comment_clause():
    # Given a new column with no comment (the common case)
    action = AddColumn(Column("age", Integer()))

    # When compiling the add
    statement = _compile_single(action)

    # Then no empty COMMENT clause is emitted
    assert statement == "ALTER TABLE `cat`.`sch`.`tbl` ADD COLUMN `age` INT"
