from tabula.adapters.databricks.sql.dialects import SqlDialect
from tabula.adapters.databricks.sql.dialects.spark_sql import SPARK_SQL


def test_spark_sql_conforms_to_protocol() -> None:
    assert isinstance(SPARK_SQL, SqlDialect)
