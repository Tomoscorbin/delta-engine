"""Behavioural tests for lint_tables: severity attachment, report shape, ordering."""

from delta_engine.domain.model import QualifiedName
from delta_engine.lint import Finding, LintReport, Severity, lint_tables, parse_lint_config
from delta_engine.schema import Column, DeltaTable, String


def build_declared_table(
    name: str = "orders",
    *,
    schema: str = "silver",
    comment: str = "Orders placed by customers",
    tags: dict[str, str] | None = None,
    primary_key: tuple[str, ...] | None = ("id",),
) -> DeltaTable:
    """Build a declaration that satisfies every default rule unless overridden."""
    return DeltaTable(
        "dev",
        schema,
        name,
        columns=(Column("id", String(), nullable=False, comment="Row identifier"),),
        comment=comment,
        tags=tags or {},
        primary_key=primary_key,
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


class TestPerTableOverrides:
    def test_an_override_changes_the_policy_only_for_matching_tables(self) -> None:
        # Given a bronze and a silver table, neither declaring a primary key,
        # and a policy that turns primary-key off for bronze tables
        bronze = build_declared_table("raw_events", schema="bronze", primary_key=None)
        silver = build_declared_table("orders", schema="silver", primary_key=None)
        policy = parse_lint_config(
            {"overrides": [{"tables": ["dev.bronze.*"], "primary-key": "off"}]}
        )

        # When
        report = lint_tables(bronze, silver, policy=policy)

        # Then only the silver table is reported
        assert [(str(finding.table), finding.rule) for finding in report.findings] == [
            ("dev.silver.orders", "primary-key")
        ]


class TestOrdering:
    def test_every_table_is_checked_regardless_of_input_order(self) -> None:
        # Given tables passed out of name order
        zeta = build_declared_table("zeta", comment="")
        alpha = build_declared_table("alpha", comment="")

        # When
        report = lint_tables(zeta, alpha)

        # Then both are checked and their findings arrive in name order
        assert report.tables_checked == 2
        assert [str(finding.table) for finding in report.findings] == [
            "dev.silver.alpha",
            "dev.silver.zeta",
        ]

    def test_a_report_orders_findings_by_table_however_it_is_constructed(self) -> None:
        # Given findings interleaved across two tables
        alpha = QualifiedName("dev", "silver", "alpha")
        zeta = QualifiedName("dev", "silver", "zeta")
        interleaved = (
            Finding("table-comment", zeta, "table has no comment", Severity.ERROR),
            Finding("table-comment", alpha, "table has no comment", Severity.ERROR),
            Finding("primary-key", zeta, "table has no primary key", Severity.ERROR),
        )

        # When
        report = LintReport(findings=interleaved, tables_checked=2)

        # Then each table's findings are contiguous, in their given order
        assert [(str(finding.table), finding.rule) for finding in report.findings] == [
            ("dev.silver.alpha", "table-comment"),
            ("dev.silver.zeta", "table-comment"),
            ("dev.silver.zeta", "primary-key"),
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
