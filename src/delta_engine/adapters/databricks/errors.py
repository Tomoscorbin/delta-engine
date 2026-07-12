"""
Name exceptions for failure records: prefer the underlying Java class.

One total naming policy covers both backends. Most PySpark failures arrive
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
