from delta_engine.application.rendering import action_diff_line
from delta_engine.domain.plan.actions import (
    SetColumnTag,
    SetTableTag,
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
