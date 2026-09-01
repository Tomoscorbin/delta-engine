"""Behavioural tests for the built-in lint rules: facts only, no severity."""

from collections.abc import Mapping

import pytest

from delta_engine.domain.model import (
    DesiredColumn,
    DesiredTable,
    PrimaryKeyConstraint,
    QualifiedName,
    String,
)
from delta_engine.lint.rules import (
    ColumnCommentRule,
    NamingConventionRule,
    PrimaryKeyRule,
    RequiredTagRule,
    TableCommentRule,
)

_COMMENTED_COLUMNS = (
    DesiredColumn("id", String(), nullable=False, comment="Row identifier"),
    DesiredColumn("status", String(), comment="Order status"),
)


def build_table(
    *,
    columns: tuple[DesiredColumn, ...] = _COMMENTED_COLUMNS,
    comment: str = "Orders placed by customers",
    tags: Mapping[str, str] | None = None,
    primary_key: PrimaryKeyConstraint | None = None,
) -> DesiredTable:
    """Build a desired table that satisfies every rule unless overridden."""
    return DesiredTable(
        qualified_name=QualifiedName("dev", "silver", "orders"),
        columns=columns,
        comment=comment,
        tags=tags or {},
        primary_key=primary_key,
    )


class TestTableCommentRule:
    def test_table_with_comment_produces_no_messages(self) -> None:
        # Given a table with a comment
        table = build_table(comment="Orders placed by customers")

        # When the rule evaluates it
        messages = TableCommentRule().evaluate(table)

        # Then nothing is reported
        assert messages == ()

    @pytest.mark.parametrize("comment", ["", "   "], ids=["empty", "whitespace-only"])
    def test_blank_table_comment_is_reported(self, comment: str) -> None:
        # Given a table whose comment is blank
        table = build_table(comment=comment)

        # When the rule evaluates it
        messages = TableCommentRule().evaluate(table)

        # Then the blank comment counts as missing
        assert messages == ("table has no comment",)


class TestColumnCommentRule:
    def test_table_with_all_columns_commented_produces_no_messages(self) -> None:
        # Given a table whose columns all carry comments
        table = build_table(columns=_COMMENTED_COLUMNS)

        # When the rule evaluates it
        messages = ColumnCommentRule().evaluate(table)

        # Then nothing is reported
        assert messages == ()

    def test_each_uncommented_column_is_reported_by_name(self) -> None:
        # Given one commented column, one bare, and one whitespace-only
        table = build_table(
            columns=(
                DesiredColumn("id", String(), comment="Row identifier"),
                DesiredColumn("status", String()),
                DesiredColumn("amount", String(), comment="   "),
            )
        )

        # When the rule evaluates it
        messages = ColumnCommentRule().evaluate(table)

        # Then each blank column is reported by name
        assert messages == (
            "column 'status' has no comment",
            "column 'amount' has no comment",
        )


class TestPrimaryKeyRule:
    def test_table_with_primary_key_produces_no_messages(self) -> None:
        # Given a table declaring a primary key
        table = build_table(primary_key=PrimaryKeyConstraint(("id",)))

        # When the rule evaluates it
        messages = PrimaryKeyRule().evaluate(table)

        # Then nothing is reported
        assert messages == ()

    def test_table_without_primary_key_is_reported(self) -> None:
        # Given a table without a primary key
        table = build_table(primary_key=None)

        # When the rule evaluates it
        messages = PrimaryKeyRule().evaluate(table)

        # Then the missing key is reported
        assert messages == ("table has no primary key",)


class TestNamingConventionRule:
    def test_snake_case_table_and_columns_produce_no_messages(self) -> None:
        # Given a table and columns that are all snake_case
        table = build_table(
            columns=(
                DesiredColumn("id", String(), comment="Row identifier"),
                DesiredColumn("created_at", String(), comment="When the row was created"),
            )
        )

        # When the rule evaluates it
        messages = NamingConventionRule().evaluate(table)

        # Then nothing is reported
        assert messages == ()

    def test_a_table_name_that_is_not_snake_case_is_reported(self) -> None:
        # Given a table whose name contains a hyphen (which survives lowercasing)
        table = DesiredTable(
            qualified_name=QualifiedName("dev", "silver", "order-items"),
            columns=_COMMENTED_COLUMNS,
            comment="Orders placed by customers",
            tags={},
            primary_key=None,
        )

        # When the rule evaluates it
        messages = NamingConventionRule().evaluate(table)

        # Then the table name is reported with the pattern it broke
        assert messages == (
            "table name 'order-items' does not match naming convention '[a-z][a-z0-9_]*'",
        )

    def test_each_column_name_that_breaks_the_convention_is_reported(self) -> None:
        # Given columns with a capitalised name and a hyphenated name
        table = build_table(
            columns=(
                DesiredColumn("id", String(), comment="Row identifier"),
                DesiredColumn("CreatedAt", String(), comment="When the row was created"),
                DesiredColumn("unit-price", String(), comment="Price per unit"),
            )
        )

        # When the rule evaluates it
        messages = NamingConventionRule().evaluate(table)

        # Then each offending column is reported by name
        assert messages == (
            "column name 'CreatedAt' does not match naming convention '[a-z][a-z0-9_]*'",
            "column name 'unit-price' does not match naming convention '[a-z][a-z0-9_]*'",
        )

    def test_a_custom_pattern_overrides_the_default(self) -> None:
        # Given a pattern that permits capitals
        table = build_table(
            columns=(DesiredColumn("CreatedAt", String(), comment="When the row was created"),)
        )

        # When a rule built with that pattern evaluates it
        messages = NamingConventionRule(pattern=r"[A-Za-z][A-Za-z0-9]*").evaluate(table)

        # Then the CamelCase column now passes
        assert messages == ()

    def test_a_name_that_only_matches_the_pattern_as_a_prefix_is_reported(self) -> None:
        # Given a column name that matches the default pattern up to a trailing symbol
        table = build_table(columns=(DesiredColumn("total_$", String(), comment="Total amount"),))

        # When the rule evaluates it
        messages = NamingConventionRule().evaluate(table)

        # Then the whole name must match, so the trailing '$' fails it
        assert messages == (
            "column name 'total_$' does not match naming convention '[a-z][a-z0-9_]*'",
        )

    def test_a_blank_pattern_is_rejected_at_construction(self) -> None:
        # Given a blank pattern
        pattern = "   "

        # When the rule is constructed
        # Then construction fails
        with pytest.raises(ValueError):
            NamingConventionRule(pattern=pattern)

    def test_an_invalid_regular_expression_is_rejected_at_construction(self) -> None:
        # Given a pattern that is not a valid regular expression
        pattern = "[unclosed"

        # When the rule is constructed
        # Then construction fails
        with pytest.raises(ValueError):
            NamingConventionRule(pattern=pattern)


class TestRequiredTagRule:
    def test_table_carrying_all_required_tags_produces_no_messages(self) -> None:
        # Given a table carrying every required tag
        table = build_table(tags={"owner": "dse", "domain": "sales"})

        # When the rule evaluates it
        messages = RequiredTagRule(keys=("owner", "domain")).evaluate(table)

        # Then nothing is reported
        assert messages == ()

    def test_each_missing_required_tag_is_reported_by_key(self) -> None:
        # Given a table carrying one of three required tags
        table = build_table(tags={"owner": "dse"})

        # When the rule evaluates it
        messages = RequiredTagRule(keys=("owner", "domain", "steward")).evaluate(table)

        # Then each absent key is reported
        assert messages == (
            "missing required tag 'domain'",
            "missing required tag 'steward'",
        )

    def test_tag_values_are_not_checked(self) -> None:
        # Given a required tag present with an empty value
        table = build_table(tags={"owner": ""})

        # When the rule evaluates it
        messages = RequiredTagRule(keys=("owner",)).evaluate(table)

        # Then the key's presence alone satisfies the rule
        assert messages == ()

    def test_empty_keys_are_rejected_at_construction(self) -> None:
        # Given no keys
        # Then construction fails
        with pytest.raises(ValueError):
            RequiredTagRule(keys=())

    def test_a_bare_string_for_keys_is_rejected_at_construction(self) -> None:
        # Given a bare string where a list of keys is required
        # Then construction fails
        with pytest.raises(ValueError):
            RequiredTagRule(keys="owner")

    def test_blank_keys_are_rejected_at_construction(self) -> None:
        # Given a whitespace-only key
        # Then construction fails
        with pytest.raises(ValueError):
            RequiredTagRule(keys=("owner", "   "))
