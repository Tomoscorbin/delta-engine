"""Behavioural tests for lint_tables: severity attachment, report shape, ordering."""

from delta_engine.lint import Severity, lint_tables, parse_lint_config
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
        # Given a fully governed declaration
        table = build_declared_table()

        # When linting it under the default policy
        report = lint_tables(table)

        # Then the report is clean but still counts the table
        assert report.findings == ()
        assert report.tables_checked == 1
        assert not report.has_errors


class TestSeverityAttachment:
    def test_findings_carry_the_severity_configured_for_their_rule(self) -> None:
        # Given a table without a comment and a policy downgrading that rule
        table = build_declared_table(comment="")
        policy = parse_lint_config(
            {"table-comment": "warning", "column-comment": "off", "primary-key": "off"}
        )

        # When linting under that policy
        report = lint_tables(table, policy=policy)

        # Then the finding carries the downgraded severity and the run is clean
        assert [finding.severity for finding in report.findings] == [Severity.WARNING]
        assert not report.has_errors

    def test_default_policy_reports_missing_comment_as_an_error(self) -> None:
        # Given a table without a comment
        table = build_declared_table(comment="")

        # When linting under the default policy
        report = lint_tables(table)

        # Then the missing comment is an error
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

        # When linting both under that policy
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

        # When linting them
        report = lint_tables(zeta, alpha)

        # Then both are checked and their findings arrive in name order
        assert report.tables_checked == 2
        assert [str(finding.table) for finding in report.findings] == [
            "dev.silver.alpha",
            "dev.silver.zeta",
        ]


class TestReportProjection:
    def test_report_projects_findings_and_counts_as_plain_data(self) -> None:
        # Given a table breaking one error-severity and one warning-severity rule
        table = build_declared_table(comment="", tags={})
        policy = parse_lint_config(
            {
                "column-comment": "off",
                "primary-key": "off",
                "required-tag": {"keys": ["owner"], "severity": "warning"},
            }
        )

        # When projecting the lint report as plain data
        data = lint_tables(table, policy=policy).to_dict()

        # Then the counts and every finding's fields are serialisable values
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
