"""Application-level error types."""

from __future__ import annotations

from typing import Mapping
from collections import OrderedDict


from delta_engine.application.validation import ValidationFailure
from delta_engine.application.formatting import format_run_report, format_validation_run_report


class ValidationFailedError(Exception):
    """Raised after validating all tables when one or more tables failed validation.
       Engine must NOT execute any plans if this is raised.
    """
    __slots__ = ("report",)

    def __init__(self, report: ValidationRunReport) -> None:
        self.report = report
        # super().__init__(self._format(report.failures()))
        super().__init__(format_validation_run_report(report, max_width=100))

    @staticmethod
    def _format(failures: Iterable[ValidationFailure]) -> str:
        groups = OrderedDict()
        for f in failures:
            groups.setdefault(f.fully_qualified_name, []).append(f)

        blocks: list[str] = []
        for table, items in groups.items():
            lines = [table]
            for f in items:
                lines += [
                    f"- {f.rule_name}",
                    f"  {f.message}",
                ]
            blocks.append("\n".join(lines))
        return "\n----\n\n".join(blocks)


class ExecutionFailedError(Exception):
    __slots__ = ("report",)

    def __init__(self, report: RunReport) -> None:
        self.report = report
        # super().__init__(self._format(report.failures()))
        super().__init__(format_run_report(report, max_width=100))


    @staticmethod
    def _format(failures: Iterable[ExecutionFailure]) -> str:
        groups = OrderedDict()
        for f in failures:
            groups.setdefault(f.fully_qualified_name, []).append(f)

        blocks: list[str] = []
        for table, items in groups.items():
            lines = [table]
            for f in items:
                lines += [
                    f"- action #{f.action_index} | {f.exception_type}",
                    f"  {f.message}",
                    f"  {f.statement_preview}",
                ]
            blocks.append("\n".join(lines))
        return "\n----\n\n".join(blocks)
