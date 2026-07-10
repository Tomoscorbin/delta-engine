import pytest

from delta_engine.adapters.databricks.spark.reader import DatabricksReader


@pytest.fixture(autouse=True)
def local_spark_databricks_reader_compat(monkeypatch):
    def _table_exists(self, qualified_name):
        # Local Spark fallback for existence checks.
        return self.spark.catalog.tableExists(f"{qualified_name.schema}.{qualified_name.name}")

    monkeypatch.setattr(DatabricksReader, "_table_exists", _table_exists, raising=True)
