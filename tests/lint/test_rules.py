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

    def test_table_without_comment_is_reported(self) -> None:
        # Given
        table = build_table(comment="")

        # When
        messages = TableCommentRule().evaluate(table)

        # Then
        assert messages == ("table has no comment",)

    def test_whitespace_only_table_comment_is_reported(self) -> None:
        # Given
        table = build_table(comment="   ")

        # When
        messages = TableCommentRule().evaluate(table)

        # Then
        assert messages == ("table has no comment",)


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
