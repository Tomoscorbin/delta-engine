from delta_engine.domain.model.column import Column
from delta_engine.domain.model.data_type import Integer, String
from delta_engine.domain.plan.actions import AddColumn, DropColumn, SetColumnComment
from delta_engine.domain.services.column_diff import (
    diff_columns,
    diff_columns_for_adds,
    diff_columns_for_drops,
)


def test_diff_columns_happy_path() -> None:
    desired = (Column("id", Integer()), Column("age", Integer()))
    observed = (Column("id", Integer()), Column("nickname", String()))
    actions = diff_columns(desired, observed)
    assert actions == (
        AddColumn(Column("age", Integer())),
        DropColumn("nickname"),
        SetColumnComment(column_name="age", comment=""),
    )


def test_diff_columns_for_adds_returns_missing_desired_columns() -> None:
    desired = (Column("id", Integer()), Column("name", String()), Column("age", Integer()))
    observed = (Column("id", Integer()), Column("name", String()))
    adds = diff_columns_for_adds(desired, observed)
    assert adds == (AddColumn(Column("age", Integer())),)


def test_diff_columns_for_drops_returns_columns_missing_from_desired() -> None:
    desired = (Column("id", Integer()),)
    observed = (Column("id", Integer()), Column("nickname", String()))
    drops = diff_columns_for_drops(desired, observed)
    assert drops == (DropColumn("nickname"),)


def test_diff_columns_is_case_insensitive_via_column_normalization() -> None:
    desired = (Column("ID", Integer()),)
    observed = (Column("id", Integer()),)
    assert diff_columns(desired, observed) == ()
