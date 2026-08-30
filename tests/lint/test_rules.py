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
        # Given
        table = build_table(comment="Orders placed by customers")

        # When
        messages = TableCommentRule().evaluate(table)

        # Then
        assert messages == ()

    def test_blank_table_comment_is_reported(self) -> None:
        # Given tables whose comments are empty or whitespace-only
        tables = (build_table(comment=""), build_table(comment="   "))

        # When / Then each counts as missing
        for table in tables:
            assert TableCommentRule().evaluate(table) == ("table has no comment",)


class TestColumnCommentRule:
    def test_table_with_all_columns_commented_produces_no_messages(self) -> None:
        # Given
        table = build_table(columns=_COMMENTED_COLUMNS)

        # When
        messages = ColumnCommentRule().evaluate(table)

        # Then
        assert messages == ()

    def test_each_uncommented_column_is_reported_by_name(self) -> None:
        # Given
        table = build_table(
            columns=(
                DesiredColumn("id", String(), comment="Row identifier"),
                DesiredColumn("status", String()),
                DesiredColumn("amount", String(), comment="   "),
            )
        )

        # When
        messages = ColumnCommentRule().evaluate(table)

        # Then
        assert messages == (
            "column 'status' has no comment",
            "column 'amount' has no comment",
        )


class TestPrimaryKeyRule:
    def test_table_with_primary_key_produces_no_messages(self) -> None:
        # Given
        table = build_table(primary_key=PrimaryKeyConstraint(("id",)))

        # When
        messages = PrimaryKeyRule().evaluate(table)

        # Then
        assert messages == ()

    def test_table_without_primary_key_is_reported(self) -> None:
        # Given
        table = build_table(primary_key=None)

        # When
        messages = PrimaryKeyRule().evaluate(table)

        # Then
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

        # When
        messages = NamingConventionRule().evaluate(table)

        # Then
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

        # When
        messages = NamingConventionRule().evaluate(table)

        # Then
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

        # When
        messages = NamingConventionRule().evaluate(table)

        # Then
        assert messages == (
            "column name 'CreatedAt' does not match naming convention '[a-z][a-z0-9_]*'",
            "column name 'unit-price' does not match naming convention '[a-z][a-z0-9_]*'",
        )

    def test_a_custom_pattern_overrides_the_default(self) -> None:
        # Given a pattern that permits capitals
        table = build_table(
            columns=(DesiredColumn("CreatedAt", String(), comment="When the row was created"),)
        )

        # When
        messages = NamingConventionRule(pattern=r"[A-Za-z][A-Za-z0-9]*").evaluate(table)

        # Then the CamelCase column now passes
        assert messages == ()

    def test_a_name_that_only_matches_the_pattern_as_a_prefix_is_reported(self) -> None:
        # Given a column name that matches the default pattern up to a trailing symbol
        table = build_table(
            columns=(DesiredColumn("total_$", String(), comment="Total amount"),)
        )

        # When
        messages = NamingConventionRule().evaluate(table)

        # Then the whole name must match, so the trailing '$' fails it
        assert messages == (
            "column name 'total_$' does not match naming convention '[a-z][a-z0-9_]*'",
        )

    def test_a_blank_pattern_is_rejected_at_construction(self) -> None:
        # Given / When / Then
        with pytest.raises(ValueError):
            NamingConventionRule(pattern="   ")

    def test_an_invalid_regular_expression_is_rejected_at_construction(self) -> None:
        # Given / When / Then
        with pytest.raises(ValueError):
            NamingConventionRule(pattern="[unclosed")


class TestRequiredTagRule:
    def test_table_carrying_all_required_tags_produces_no_messages(self) -> None:
        # Given
        table = build_table(tags={"owner": "dse", "domain": "sales"})

        # When
        messages = RequiredTagRule(keys=("owner", "domain")).evaluate(table)

        # Then
        assert messages == ()

    def test_each_missing_required_tag_is_reported_by_key(self) -> None:
        # Given
        table = build_table(tags={"owner": "dse"})

        # When
        messages = RequiredTagRule(keys=("owner", "domain", "steward")).evaluate(table)

        # Then
        assert messages == (
            "missing required tag 'domain'",
            "missing required tag 'steward'",
        )

    def test_tag_values_are_not_checked(self) -> None:
        # Given a required tag present with an empty value
        table = build_table(tags={"owner": ""})

        # When
        messages = RequiredTagRule(keys=("owner",)).evaluate(table)

        # Then
        assert messages == ()

    def test_empty_keys_are_rejected_at_construction(self) -> None:
        # Given / When / Then
        with pytest.raises(ValueError):
            RequiredTagRule(keys=())

    def test_a_bare_string_for_keys_is_rejected_at_construction(self) -> None:
        # Given / When / Then
        with pytest.raises(ValueError):
            RequiredTagRule(keys="owner")

    def test_blank_keys_are_rejected_at_construction(self) -> None:
        # Given / When / Then
        with pytest.raises(ValueError):
            RequiredTagRule(keys=("owner", "   "))
