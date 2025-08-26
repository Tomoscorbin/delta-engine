from delta_engine.adapters.databricks.sql.dialects import SqlDialect
from delta_engine.adapters.databricks.sql.dialects.ansi_sql import ANSI_SQL


def test_spark_sql_conforms_to_protocol() -> None:
    assert isinstance(ANSI_SQL, SqlDialect)
