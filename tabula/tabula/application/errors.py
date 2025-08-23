from __future__ import annotations


class IdentityMismatch(RuntimeError):
    def __init__(self, *, expected: str, actual: str) -> None:
        self.expected = expected
        self.actual = actual
        msg = f"Identity mismatch: expected={expected!r} actual={actual!r}"
        super().__init__(msg)


class ExecutionFailed(RuntimeError):
    def __init__(self, *, qualified_name: str, messages: tuple[str, ...], executed_count: int = 0) -> None:
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
        self.code = code
        self.message = message
        self.target = target
        super().__init__(code, message, str(target))

    def __str__(self) -> str:
        return f"{self.code}: {self.message}\nTable: {self.target}"
