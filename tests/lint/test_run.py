"""Behavioural tests for lint_tables: severity attachment, report shape, ordering."""

from delta_engine.lint import Severity, lint_tables, parse_lint_config
from delta_engine.schema import Column, DeltaTable, String


def build_declared_table(
    name: str = "orders",
    *,
    comment: str = "Orders placed by customers",
    tags: dict[str, str] | None = None,
) -> DeltaTable:
    """Build a declaration that satisfies every default rule unless overridden."""
    return DeltaTable(
        "dev",
        "silver",
        name,
        columns=(Column("id", String(), nullable=False, comment="Row identifier"),),
        comment=comment,
        tags=tags or {},
        primary_key=("id",),
    )


class TestCleanRun:
    def test_compliant_tables_produce_no_findings(self) -> None:
        # Given
        table = build_declared_table()

        # When
        report = lint_tables(table)

        # Then
        assert report.findings == ()
        assert report.tables_checked == 1
        assert not report.has_errors


class TestSeverityAttachment:
    def test_findings_carry_the_severity_configured_for_their_rule(self) -> None:
        # Given
        table = build_declared_table(comment="")
        policy = parse_lint_config(
            {"table-comment": "warning", "column-comment": "off", "primary-key": "off"}
        )

        # When
        report = lint_tables(table, policy=policy)

        # Then
        assert [finding.severity for finding in report.findings] == [Severity.WARNING]
        assert not report.has_errors

    def test_default_policy_reports_missing_comment_as_an_error(self) -> None:
        # Given
        table = build_declared_table(comment="")

        # When
        report = lint_tables(table)

        # Then
        assert report.has_errors
        assert "table-comment" in [finding.rule for finding in report.findings]


class TestOrdering:
    def test_findings_are_ordered_by_qualified_table_name(self) -> None:
        # Given tables passed out of name order
        zeta = build_declared_table("zeta", comment="")
        alpha = build_declared_table("alpha", comment="")

        # When
        report = lint_tables(zeta, alpha)

        # Then
        assert [str(finding.table) for finding in report.findings] == [
            "dev.silver.alpha",
            "dev.silver.zeta",
        ]


class TestReportProjection:
    def test_report_projects_findings_and_counts_as_plain_data(self) -> None:
        # Given
        table = build_declared_table(comment="", tags={})
        policy = parse_lint_config(
            {
                "column-comment": "off",
                "primary-key": "off",
                "required-tag": {"keys": ["owner"], "severity": "warning"},
            }
        )

        # When
        data = lint_tables(table, policy=policy).to_dict()

        # Then
        assert data == {
            "tables_checked": 1,
            "error_count": 1,
            "warning_count": 1,
            "findings": [
                {
                    "rule": "table-comment",
                    "severity": "error",
                    "table": "dev.silver.orders",
                    "message": "table has no comment",
                },
                {
                    "rule": "required-tag",
                    "severity": "warning",
                    "table": "dev.silver.orders",
                    "message": "missing required tag 'owner'",
                },
            ],
        }
