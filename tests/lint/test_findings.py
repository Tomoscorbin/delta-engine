"""Behavioural tests for lint findings and the report they aggregate into."""

from delta_engine.domain.model import QualifiedName
from delta_engine.lint import Finding, LintReport, Severity


def test_a_report_orders_findings_by_table_however_it_is_constructed() -> None:
    # Given findings interleaved across two tables
    alpha = QualifiedName("dev", "silver", "alpha")
    zeta = QualifiedName("dev", "silver", "zeta")
    interleaved = (
        Finding("table-comment", zeta, "table has no comment", Severity.ERROR),
        Finding("table-comment", alpha, "table has no comment", Severity.ERROR),
        Finding("primary-key", zeta, "table has no primary key", Severity.ERROR),
    )

    # When constructing a report from them directly
    report = LintReport(findings=interleaved, tables_checked=2)

    # Then each table's findings are contiguous, in their given order
    assert [(str(finding.table), finding.rule) for finding in report.findings] == [
        ("dev.silver.alpha", "table-comment"),
        ("dev.silver.zeta", "table-comment"),
        ("dev.silver.zeta", "primary-key"),
    ]
