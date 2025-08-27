"""Application-level error types."""

from __future__ import annotations

from typing import Mapping
from collections import OrderedDict


from delta_engine.application.validation import ValidationFailure


class ValidationFailedError(Exception):
    """Raised after validating all tables when one or more tables failed validation.
       Engine must NOT execute any plans if this is raised.
    """
    __slots__ = ("report",)

    def __init__(self, report: ValidationRunReport) -> None:
        self.report = report
        super().__init__(self._format(report))

    @staticmethod
    def _format(report: ValidationRunReport) -> str:
        tables_with_failures: list[tuple[str, TableValidationReport]] = [
            (name, tbl) for name, tbl in report.validations.items()
            if tbl.failures
        ]

        tables_with_failures.sort(key=lambda x: x[0])
        total_tables = len(tables_with_failures)
        total_failures = sum(len(tbl.failures) for _, tbl in tables_with_failures)

        lines: list[str] = [
            f"Validation failed: {total_tables} tables, {total_failures} failures",
            "",
        ]

        for table_name, table_report in tables_with_failures:
            lines.append(table_name)
            for failure in table_report.failures:
                rule_name = getattr(failure, "rule_name", "UNKNOWN_RULE")
                message = str(getattr(failure, "message", "")).replace("\n", " ").strip()
                lines.append(f"    -- [{rule_name}] {message}")
            lines.append("")  # blank line between tables

        return "\n".join(lines).rstrip()


class ExecutionFailedError(Exception):
    """Raised after executing plans when one or more tables failed execution.

    Engine should attempt all tables before raising this.
    """
    __slots__ = ("report",)

    def __init__(self, report: RunReport) -> None:
        self.report = report
        super().__init__(self._format(report))

    @staticmethod
    def _format(report: RunReport) -> str:
        # Gather and group failures by table
        failures = list(report.failures())
        failures_by_table: dict[str, list[ExecutionFailure]] = {}
        for failure in failures:
            table = getattr(failure, "fully_qualified_name", "UNKNOWN_TABLE")
            failures_by_table.setdefault(table, []).append(failure)

        table_names = sorted(failures_by_table.keys())
        total_tables = len(table_names)
        total_failures = len(failures)

        lines: list[str] = [
            f"Execution failed: {total_tables} tables, {total_failures} failures",
            "",
        ]

        for table_name in table_names:
            lines.append(table_name)

            items = sorted(
                failures_by_table[table_name],
                key=lambda f: (getattr(f, "action_index", -1), getattr(f, "exception_type", "")),
            )

            for f in items:
                action_index = getattr(f, "action_index", -1)
                exception_type = getattr(f, "exception_type", "UNKNOWN_EXCEPTION")
                message = str(getattr(f, "message", "")).replace("\n", " ").strip()
                statement_preview = str(getattr(f, "statement_preview", "")).replace("\n", " ").strip()

                lines.append(
                    f'    -- action #{action_index} | type="{exception_type}" | details="{message}"'
                )

                lines.append(f"       statement: {statement_preview}")

            # Blank line between tables
            lines.append("")

        return "\n".join(lines).rstrip()
