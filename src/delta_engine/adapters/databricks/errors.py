"""
Render exceptions safely for failure records.

One total rendering policy covers both backends. Most PySpark failures arrive
already converted to PySpark's own exception hierarchy
(``AnalysisException`` and friends), and every databricks-sql-connector
failure is a plain Python exception — for all of those the Python class
name is the informative fact. The residual shape is py4j's
``Py4JJavaError``, wrapping a JVM exception PySpark did not recognise;
there the Java class (e.g. ``org.apache.spark.sql.AnalysisException``) is
preferred over the uninformative wrapper name.

The wrapper is detected by its ``java_exception`` attribute rather than an
``isinstance`` check: importing py4j here would make it a hard dependency
of the warehouse backend (which must run without PySpark installed — see
the import-linter contracts), and the duck-typing matches how this layer
already treats catalog rows and connections.
"""

import re
from typing import Final

_MESSAGE_UNAVAILABLE = "<exception message unavailable>"

_MISSING_RELATION_CONDITIONS: Final[frozenset[str]] = frozenset(
    {"TABLE_OR_VIEW_NOT_FOUND", "SCHEMA_NOT_FOUND", "CATALOG_NOT_FOUND"}
)
_CONDITION_PREFIX: Final = re.compile(r"\s*\[([A-Z0-9_.]+)\]")


def exception_type_name(exception: Exception) -> str:
    """
    Return the most informative type name available for ``exception``.

    Falls back to the Python class name when the exception wraps no JVM
    exception, and when the JVM gateway cannot be reached (``getClass`` /
    ``getName`` are remote calls; a dead gateway must not turn a failure
    record into a second exception).
    """
    java_exception = getattr(exception, "java_exception", None)
    if java_exception is not None:
        try:
            return str(java_exception.getClass().getName())
        except Exception:
            pass
    return type(exception).__name__


def exception_message(exception: BaseException) -> str:
    """
    Return ``exception``'s message without allowing rendering to raise.

    ``Py4JJavaError.__str__`` makes another gateway call to render the wrapped
    Java exception. If the gateway itself failed, that secondary call can
    raise and break the readers' and executor's total boundary while they are
    trying to record the original failure. A stable fallback keeps failure
    construction non-throwing even when the backend can no longer describe
    its exception.
    """
    try:
        return str(exception)
    except Exception:
        return _MESSAGE_UNAVAILABLE


def _error_condition(exception: BaseException) -> str | None:
    """Extract the catalog error condition from getCondition() or message prefix."""
    getter = getattr(exception, "getCondition", None)
    if callable(getter):
        try:
            condition = getter()
        except Exception:
            condition = None
        if isinstance(condition, str):
            return condition
    match = _CONDITION_PREFIX.match(exception_message(exception))
    return match.group(1) if match else None


def is_missing_relation(exception: BaseException) -> bool:
    """Whether ``exception`` reports that the described table/schema/catalog does not exist."""
    return _error_condition(exception) in _MISSING_RELATION_CONDITIONS
