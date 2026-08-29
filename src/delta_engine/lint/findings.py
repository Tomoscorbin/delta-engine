"""Lint findings: facts stated by rules, severities attached by the runner."""

from dataclasses import dataclass
from enum import StrEnum
from typing import Any

from delta_engine.domain.collection_types import ListOrTuple
from delta_engine.domain.model import QualifiedName


class Severity(StrEnum):
    """How a lint finding counts toward the run's outcome."""

    ERROR = "error"
    WARNING = "warning"


@dataclass(frozen=True, slots=True)
class Finding:
    """One fact a rule stated about one table, at the severity the policy assigns."""

    rule: str
    table: QualifiedName
    message: str
    severity: Severity


@dataclass(frozen=True, slots=True)
class LintReport:
    """Aggregate outcome of one lint run across all tables."""

    findings: ListOrTuple[Finding]
    tables_checked: int

    def __post_init__(self) -> None:
        object.__setattr__(self, "findings", tuple(self.findings))

    @property
    def error_count(self) -> int:
        """Number of error-severity findings."""
        return sum(1 for finding in self.findings if finding.severity is Severity.ERROR)

    @property
    def warning_count(self) -> int:
        """Number of warning-severity findings."""
        return sum(1 for finding in self.findings if finding.severity is Severity.WARNING)

    @property
    def has_errors(self) -> bool:
        """Whether any finding carries error severity."""
        return self.error_count > 0

    def to_dict(self) -> dict[str, Any]:
        """Project the report as plain, JSON-serialisable data."""
        return {
            "tables_checked": self.tables_checked,
            "error_count": self.error_count,
            "warning_count": self.warning_count,
            "findings": [
                {
                    "rule": finding.rule,
                    "severity": finding.severity.value,
                    "table": str(finding.table),
                    "message": finding.message,
                }
                for finding in self.findings
            ],
        }
