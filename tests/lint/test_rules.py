"""Behavioural tests for the built-in lint rules: facts only, no severity."""

from collections.abc import Mapping

from delta_engine.domain.model import (
    DesiredColumn,
    DesiredTable,
    PrimaryKeyConstraint,
    QualifiedName,
    String,
)
from delta_engine.lint import (
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
    def test_table_with_comment_produces_no_violations(self) -> None:
        # Given
        table = build_table(comment="Orders placed by customers")

        # When
        violations = TableCommentRule().evaluate(table)

        # Then
        assert violations == ()

    def test_table_without_comment_is_reported(self) -> None:
        # Given
        table = build_table(comment="")

        # When
        violations = TableCommentRule().evaluate(table)

        # Then
        assert len(violations) == 1
        assert violations[0].rule == "table-comment"
        assert violations[0].table == QualifiedName("dev", "silver", "orders")

    def test_whitespace_only_table_comment_is_reported(self) -> None:
        # Given
        table = build_table(comment="   ")

        # When
        violations = TableCommentRule().evaluate(table)

        # Then
        assert len(violations) == 1


class TestColumnCommentRule:
    def test_table_with_all_columns_commented_produces_no_violations(self) -> None:
        # Given
        table = build_table(columns=_COMMENTED_COLUMNS)

        # When
        violations = ColumnCommentRule().evaluate(table)

        # Then
        assert violations == ()

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
        violations = ColumnCommentRule().evaluate(table)

        # Then
        assert len(violations) == 2
        assert all(violation.rule == "column-comment" for violation in violations)
        assert "status" in violations[0].message
        assert "amount" in violations[1].message


class TestPrimaryKeyRule:
    def test_table_with_primary_key_produces_no_violations(self) -> None:
        # Given
        table = build_table(primary_key=PrimaryKeyConstraint(("id",)))

        # When
        violations = PrimaryKeyRule().evaluate(table)

        # Then
        assert violations == ()

    def test_table_without_primary_key_is_reported(self) -> None:
        # Given
        table = build_table(primary_key=None)

        # When
        violations = PrimaryKeyRule().evaluate(table)

        # Then
        assert len(violations) == 1
        assert violations[0].rule == "primary-key"


class TestRequiredTagRule:
    def test_table_carrying_all_required_tags_produces_no_violations(self) -> None:
        # Given
        table = build_table(tags={"owner": "dse", "domain": "sales"})

        # When
        violations = RequiredTagRule(keys=("owner", "domain")).evaluate(table)

        # Then
        assert violations == ()

    def test_each_missing_required_tag_is_reported_by_key(self) -> None:
        # Given
        table = build_table(tags={"owner": "dse"})

        # When
        violations = RequiredTagRule(keys=("owner", "domain", "steward")).evaluate(table)

        # Then
        assert len(violations) == 2
        assert all(violation.rule == "required-tag" for violation in violations)
        assert "domain" in violations[0].message
        assert "steward" in violations[1].message

    def test_tag_values_are_not_checked(self) -> None:
        # Given a required tag present with an empty value
        table = build_table(tags={"owner": ""})

        # When
        violations = RequiredTagRule(keys=("owner",)).evaluate(table)

        # Then
        assert violations == ()
