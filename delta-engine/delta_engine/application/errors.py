"""Application-level error types."""

from __future__ import annotations

from delta_engine.application.validation import RunReport, ValidationFailure


class ValidationFailedError(Exception):
    __slots__ = ("report",)

    def __init__(self, failures_by_table: dict[str, tuple[ValidationFailure,...]]) -> None:
        self.failures_by_table = failures_by_table
        super().__init__(self._format(failures_by_table))

    @staticmethod
    def _format(failures_by_table: dict[str, tuple[ValidationFailure,...]]) -> str:
        items = sorted(failures_by_table.items(), key=lambda kv: kv[0])
        total_tables = len(failures_by_table)
        total_failures = sum(len(failures) for _, failures in failures_by_table.items())

        lines = [
            f"Validation failed: {total_tables} tables, {total_failures} failures",
            "",
        ]

        for table_name, failures in items:
            lines.append(table_name)
            for failure in failures:
                rule_name = getattr(failure, "rule_name", "UNKNOWN_RULE")
                message = str(getattr(failure, "message", "")).replace("\n", " ").strip()
                lines.append(f"    -- [{rule_name}] {message}")
            lines.append("")  # blank line between tables

        return "\n".join(lines).rstrip()


class ExecutionFailedError(Exception):
    __slots__ = ("report",)

    def __init__(self, report: RunReport) -> None:
        self.report = report
        super().__init__(self._format(report))

    @staticmethod
    def _format(report: RunReport) -> str:
        failures_by_table = report.failures_by_table
        items = sorted(failures_by_table.items(), key=lambda kv: kv[0])
        total_tables = len(items)
        total_failures = sum(len(fails) for _, fails in items)

        lines: list[str] = [
            f"Execution failed: {total_tables} tables, {total_failures} failures",
            "",
        ]

        for table_name, failures in items:
            lines.append(table_name)
            for f in sorted(
                failures,
                key=lambda f: (getattr(f, "action_index", -1), getattr(f, "exception_type", "")),
            ):
                action_index = getattr(f, "action_index", -1)
                exception_type = getattr(f, "exception_type", "UNKNOWN_EXCEPTION")
                message = str(getattr(f, "message", "")).replace("\n", " ").strip()
                statement_preview = str(getattr(f, "statement_preview", "")).replace("\n", " ").strip()

                lines.append(
                    f'    -- action #{action_index} | type="{exception_type}" | details="{message}"'
                )
                if statement_preview:
                    lines.append(f"       statement: {statement_preview}")

            lines.append("")  # blank line between tables

        return "\n".join(lines).rstrip()
