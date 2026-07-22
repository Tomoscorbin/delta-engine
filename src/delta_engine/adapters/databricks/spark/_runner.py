"""Physical SQL invocation through a Spark session."""

from pyspark.sql import DataFrame, SparkSession

_VARIABLE_SUBSTITUTION = "spark.sql.variable.substitute"


class SparkSqlRunner:
    """Run SQL while containing Spark session-wide transport quirks."""

    def __init__(self, spark: SparkSession) -> None:
        self._spark = spark

    def run(self, statement: str) -> DataFrame:
        """Run one statement without allowing Spark to rewrite ``${...}``."""
        if "${" not in statement:
            return self._spark.sql(statement)

        substitution = self._spark.conf.get(_VARIABLE_SUBSTITUTION)
        if substitution is None:
            raise RuntimeError(
                f"Spark did not expose the {_VARIABLE_SUBSTITUTION} setting;"
                " refusing to execute SQL containing ${...}"
            )
        if substitution.casefold() == "false":
            return self._spark.sql(statement)

        # Spark substitutes variables before parsing, including inside quoted
        # literals. Setting the session option is therefore the only reliable
        # place to protect comments, tags, and properties. Let a set failure
        # propagate before executing so declared text is never silently changed.
        self._spark.conf.set(_VARIABLE_SUBSTITUTION, "false")
        try:
            return self._spark.sql(statement)
        finally:
            self._spark.conf.set(_VARIABLE_SUBSTITUTION, substitution)
