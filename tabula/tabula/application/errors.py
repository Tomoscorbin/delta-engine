"""Application-level error types."""

from __future__ import annotations


class IdentityMismatchError(RuntimeError):
    """Raised when an expected identifier differs from the observed one."""

    def __init__(self, *, expected: str, actual: str) -> None:
        """Initialize the error.

        Args:
            expected: Expected identifier string.
            actual: Observed identifier string.
        """

        self.expected = expected
        self.actual = actual
        msg = f"Identity mismatch: expected={expected!r} actual={actual!r}"
        super().__init__(msg)


class ExecutionFailedError(RuntimeError):
    """Raised when plan execution reports failure."""

    def __init__(
        self, *, qualified_name: str, messages: tuple[str, ...], executed_count: int = 0
    ) -> None:
        """Initialize the error.

        Args:
            qualified_name: Fully qualified table name.
            messages: Execution messages from the executor.
            executed_count: Number of statements successfully executed.
        """

        self.qualified_name = qualified_name
        self.messages = messages
        self.executed_count = executed_count

        head = f"Execution failed for {qualified_name} (executed_count={executed_count})"
        msg = head if not messages else f"{head}: " + "; ".join(messages)
        super().__init__(msg)


class ValidationError(Exception):
    """Raised when a plan violates a single validation rule."""

    __slots__ = ("code", "message", "target")

    def __init__(self, code: str, message: str, target) -> None:
        """Create a validation error.

        Args:
            code: Short validation code.
            message: Human-readable message.
            target: Object that failed validation.
        """

        self.code = code
        self.message = message
        self.target = target
        super().__init__(code, message, str(target))

    def __str__(self) -> str:
        return f"{self.code}: {self.message}\nTable: {self.target}"
