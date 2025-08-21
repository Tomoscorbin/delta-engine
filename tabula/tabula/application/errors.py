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


@dataclass(slots=True)
class ExecutionFailed(ApplicationError):
    """Plan execution failed. Carries context for logging/alerting."""

    qualified_name: str
    messages: tuple[str, ...]
    executed_count: int = 0
