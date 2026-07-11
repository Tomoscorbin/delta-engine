"""
Translate backend exceptions into neutral details for typed failures.

Both Databricks backends reduce heterogeneous backend exceptions to the same
two facts a failure report needs: the most informative type name and a
bounded message. This module is the backend-free core; backend-specific
refinements (the Spark backend prefers the underlying Java class name for
py4j errors) layer their own ``translate_exception`` on top of it.
"""

from dataclasses import dataclass


@dataclass(frozen=True, slots=True)
class ExceptionDetails:
    """The two facts a typed failure records about a backend exception."""

    type_name: str
    message: str


def translate_exception(exception: Exception) -> ExceptionDetails:
    """Translate an exception into its Python class name and bounded message."""
    return ExceptionDetails(type(exception).__name__, bounded_message(exception))


def bounded_message(exception: Exception) -> str:
    """Return the first lines of an exception message, bounded for reports."""
    return "\n".join(str(exception).splitlines()[:5])
