"""
Spark-specific exception type naming: prefer the underlying Java class name.

Failure records name the exception type that produced them. On
Databricks/Spark the primary failure shape is ``Py4JJavaError``, where the
JVM exception class is the informative fact — e.g.
``org.apache.spark.sql.AnalysisException`` rather than ``Py4JJavaError`` —
so this backend injects its own type naming into the shared boundaries.
"""


def exception_type_name(exception: Exception) -> str:
    """
    Return the most informative exception class name available.

    For Py4JJavaError (the primary failure shape on Databricks, where JVM
    exceptions surface through py4j), the underlying Java class is preferred
    over the py4j wrapper — e.g. 'org.apache.spark.sql.AnalysisException'
    rather than 'Py4JJavaError'. Falls back to the Python class name for all
    other exceptions.
    """
    try:
        from py4j.protocol import Py4JJavaError  # type: ignore[import]

        if isinstance(exception, Py4JJavaError):
            try:
                return exception.java_exception.getClass().getName()
            except (AttributeError, TypeError):
                return "Py4JJavaError"
    except ImportError:
        pass
    return type(exception).__name__
