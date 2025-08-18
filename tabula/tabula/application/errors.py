"""Application-layer exceptions: stable, informative, and easy to catch."""

from __future__ import annotations

from dataclasses import dataclass


class ApplicationError(RuntimeError):
    """Base class for all application-layer errors."""


@dataclass(slots=True)
class IdentityMismatch(ApplicationError):
    """Desired and observed identities differ (safety check before planning/execution)."""

    expected: str
    actual: str

    def __str__(self) -> str:
        return f"Identity mismatch: expected {self.expected!r}, actual {self.actual!r}"


@dataclass(slots=True)
class ExecutionFailed(ApplicationError):
    """Plan execution failed. Carries context for logging/alerting."""

    qualified_name: str
    messages: tuple[str, ...]
    executed_count: int = 0

    def __str__(self) -> str:
        details = "; ".join(self.messages) if self.messages else "no message"
        return f"Execution failed for {self.qualified_name}: {details} (executed_count={self.executed_count})"
