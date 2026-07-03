from delta_engine.application.rendering import action_diff_line
from delta_engine.domain.model import TableAspect
from delta_engine.domain.plan.actions import (
    SetColumnTag,
    SetTableTag,
    TargetColumnMissing,
    TargetTableMissing,
    UnenforceablePrimaryKey,
    UnsetColumnTag,
    UnsetTableTag,
)

# ---------- tag diff lines ----------


def test_set_table_tag_renders_a_tilde_tag_line():
    # Given a SetTableTag action
    line = action_diff_line(SetTableTag(name="env", value="prod"))

    # Then it renders as a change line naming the tag and its value
    assert line == "~ tag env = 'prod'"


def test_unset_table_tag_renders_a_minus_tag_line():
    # Given an UnsetTableTag action
    line = action_diff_line(UnsetTableTag(name="env"))

    # Then it renders as a removal line naming the tag
    assert line == "- tag env"


# ---------- column tag diff lines ----------


def test_set_column_tag_renders_a_tilde_column_tag_line():
    # Given a SetColumnTag action
    line = action_diff_line(SetColumnTag(column_name="email", name="pii", value="true"))

    # Then it renders as a change line naming the column, tag, and value
    assert line == "~ column tag email.pii = 'true'"


def test_unset_column_tag_renders_a_minus_column_tag_line():
    # Given an UnsetColumnTag action
    line = action_diff_line(UnsetColumnTag(column_name="email", name="pii"))

    # Then it renders as a removal line naming the column and tag
    assert line == "- column tag email.pii"


# ---------- broken metadata target diff lines ----------


def test_target_table_missing_renders_a_bang_line():
    # Given a missing table this definition cannot create
    line = action_diff_line(TargetTableMissing())

    # Then it renders as a problem line, not a change line
    assert line == "! table does not exist (this definition cannot create it)"


def test_target_column_missing_renders_the_column_and_reasons():
    # Given a declared column absent live, targeted by comments and tags
    action = TargetColumnMissing(
        column_name="email",
        reasons=(TableAspect.COLUMN_COMMENTS, TableAspect.COLUMN_TAGS),
    )

    line = action_diff_line(action)

    # Then it names the column and why it matters
    expected = (
        "! column email missing from live table"
        " (targeted by: column comments, column tags)"
    )
    assert line == expected


def test_unenforceable_primary_key_renders_the_nullable_columns():
    # Given a planned PK over live-nullable columns
    line = action_diff_line(UnenforceablePrimaryKey(nullable_columns=("id", "region")))

    # Then it names the offending columns
    assert line == "! primary key over nullable live columns: id, region"
