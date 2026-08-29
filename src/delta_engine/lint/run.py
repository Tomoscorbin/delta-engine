"""Run a lint policy over table declarations and assemble the report."""

from delta_engine.application.engine import lower_desired_tables
from delta_engine.application.ports import DesiredTableSource
from delta_engine.lint.config import DEFAULT_POLICY, LintPolicy
from delta_engine.lint.findings import Finding, LintReport


def lint_tables(
    *tables: DesiredTableSource,
    policy: LintPolicy = DEFAULT_POLICY,
) -> LintReport:
    """
    Evaluate every enabled rule against every table and report the findings.

    Tables are checked in qualified-name order, so the report never depends on
    the order they were passed. Each violation a rule states is paired with the
    severity the policy configures for that rule.

    Args:
        tables: Declarations to lint; anything with ``to_desired_table()``.
        policy: The enabled rules and their severities; defaults to every
            parameter-free rule at error severity.

    Raises:
        DuplicateTableDefinitionError: If one qualified table is declared twice.

    """
    desired_tables = lower_desired_tables(*tables)
    findings = tuple(
        Finding.from_violation(violation, configured.severity)
        for table in desired_tables
        for configured in policy.rules
        for violation in configured.rule.evaluate(table)
    )
    return LintReport(findings=findings, tables_checked=len(desired_tables))
