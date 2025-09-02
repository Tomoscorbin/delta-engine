from delta_engine.domain.model.column import Column
from delta_engine.domain.model.data_type import Integer, String
from delta_engine.domain.plan.actions import AddColumn, DropColumn, SetColumnComment
from delta_engine.domain.services.column_diff import (
    diff_column_comments,
    diff_columns,
    diff_columns_for_adds,
    diff_columns_for_drops,
)

# --- diff_columns (integration of adds + drops + comments) --------------------


def test_diff_columns_happy_path_add_drop_and_comment_update() -> None:
    desired = (Column("id", Integer()), Column("age", Integer(), comment="years"))
    observed = (Column("id", Integer()), Column("nickname", String()))
    actions = diff_columns(desired, observed)

    adds = [a for a in actions if isinstance(a, AddColumn)]
    drops = [a for a in actions if isinstance(a, DropColumn)]
    comments = [a for a in actions if isinstance(a, SetColumnComment)]

    assert {a.column.name for a in adds} == {"age"}
    assert {d.column_name for d in drops} == {"nickname"}
    assert {(c.column_name, c.comment) for c in comments} == {("age", "years")}


def test_diff_columns_no_changes_returns_empty_tuple() -> None:
    desired = (Column("id", Integer(), comment="primary key"),)
    observed = (Column("id", Integer(), comment="primary key"),)

    assert diff_columns(desired, observed) == ()


def test_diff_columns_handles_empty_inputs() -> None:
    desired = ()
    observed = (Column("id", Integer()),)
    actions = diff_columns(desired, observed)

    assert {type(a) for a in actions} == {DropColumn}  # only drops


def test_diff_columns_is_case_insensitive_via_column_normalization() -> None:
    desired = (Column("ID", Integer()),)
    observed = (Column("id", Integer()),)

    assert diff_columns(desired, observed) == ()


# --- diff_columns_for_adds ----------------------------------------------------


def test_diff_columns_for_adds_returns_missing_desired_columns() -> None:
    desired = (Column("id", Integer()), Column("name", String()), Column("age", Integer()))
    observed = (Column("id", Integer()), Column("name", String()))
    adds = diff_columns_for_adds(desired, observed)

    assert {a.column.name for a in adds} == {"age"}


def test_diff_columns_for_adds_empty_when_all_present() -> None:
    desired = (Column("id", Integer()),)
    observed = (Column("id", Integer()),)

    assert diff_columns_for_adds(desired, observed) == ()


# --- diff_columns_for_drops ---------------------------------------------------


def test_diff_columns_for_drops_returns_columns_missing_from_desired() -> None:
    desired = (Column("id", Integer()),)
    observed = (Column("id", Integer()), Column("nickname", String()))
    drops = diff_columns_for_drops(desired, observed)

    assert {d.column_name for d in drops} == {"nickname"}


def test_diff_columns_for_drops_empty_when_no_extras() -> None:
    desired = (Column("id", Integer()), Column("name", String()))
    observed = (Column("id", Integer()), Column("name", String()))

    assert diff_columns_for_drops(desired, observed) == ()


# --- diff_column_comments -----------------------------------------------------


def test_diff_column_comments_updates_when_comment_differs() -> None:
    desired = (Column("id", Integer(), comment="primary"),)
    observed = (Column("id", Integer(), comment=""),)

    actions = diff_column_comments(desired, observed)
    assert {(a.column_name, a.comment) for a in actions} == {("id", "primary")}


def test_diff_column_comments_noop_when_comments_match() -> None:
    desired = (Column("age", Integer(), comment="years"),)
    observed = (Column("age", Integer(), comment="years"),)

    assert diff_column_comments(desired, observed) == ()
