from __future__ import annotations
from dataclasses import dataclass

class ApplicationError(RuntimeError):
    """Base class for all application-layer errors."""

@dataclass(slots=True)
class IdentityMismatch(ApplicationError):
    expected: str
    actual: str

    def __str__(self) -> str:
        return f"Identity mismatch: expected={self.expected!r} actual={self.actual!r}"

@dataclass(slots=True)
class ExecutionFailed(ApplicationError):
    qualified_name: str
    messages: tuple[str, ...]
    executed_count: int = 0

    def __str__(self) -> str:
        head = f"Execution failed for {self.qualified_name} (executed_count={self.executed_count})"
        if not self.messages:
            return head
        return f"{head}: " + "; ".join(self.messages)
