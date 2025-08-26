"""Application-level error types."""

from __future__ import annotations

from collections.abc import Mapping

from tabula.application.results import SyncReport
from tabula.application.validation import ValidationFailure


class ValidationFailedError(Exception):
    """Raised after validating all tables when one or more tables failed validation.
    Engine must NOT execute any plans if this is raised.
    """

    __slots__ = ("failures_by_table", "report")

    def __init__(
        self,
        *,
        failures_by_table: Mapping[str, tuple[ValidationFailure, ...]],
        report: SyncReport | None = None,
    ) -> None:
        self.failures_by_table = dict(failures_by_table)
        self.report = report
        n = len(self.failures_by_table)
        head = ", ".join(sorted(self.failures_by_table)[:5]) + ("…" if n > 5 else "")
        super().__init__(f"Validation failed for {n} table(s): {head}.")


class ExecutionFailedError(Exception):
    """Raised after executing all plans when one or more tables failed execution.
    Validation was already clean at this point.
    """

    __slots__ = ("failures_by_table", "report")

    def __init__(
        self,
        *,
        failures_by_table: Mapping[str, str],
        report: SyncReport | None = None,
    ) -> None:
        self.failures_by_table = dict(failures_by_table)
        self.report = report
        n = len(self.failures_by_table)
        head = ", ".join(sorted(self.failures_by_table)[:5]) + ("…" if n > 5 else "")
        super().__init__(f"Execution failed for {n} table(s): {head}.")
