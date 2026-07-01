"""
Live-catalog inspection helpers for the delta-engine walkthrough notebook.

``CatalogInspector`` reads table state back from Unity Catalog so the notebook
can assert what actually landed after each sync, using the same catalog surfaces
the engine's own reader uses. It runs on Databricks: the active ``SparkSession``
is imported from the Databricks runtime, so the notebook never has to pass it in.
Keep this file in the same workspace folder as the notebook so the import
resolves.
"""

from databricks.sdk.runtime import spark
from pyspark.sql.types import StructField


class CatalogInspector:
    """Reads live table state from Unity Catalog for one ``catalog.schema``."""

    def __init__(self, catalog: str, schema: str) -> None:
        self.catalog = catalog
        self.schema = schema

    def fqname(self, table: str) -> str:
        """Return the fully qualified name for a table in this schema."""
        return f"{self.catalog}.{self.schema}.{table}"

    def fields_of(self, table: str) -> dict[str, StructField]:
        """Return a map of column name to StructField for the live table."""
        return {field.name: field for field in spark.table(self.fqname(table)).schema.fields}

    def partitions_of(self, table: str) -> tuple[str, ...]:
        """Return the partition column names, in catalog order."""
        columns = spark.catalog.listColumns(self.fqname(table))
        return tuple(column.name for column in columns if column.isPartition)

    def properties_of(self, table: str) -> dict[str, str]:
        """Return the Delta table properties as a plain dict (from DESCRIBE DETAIL)."""
        row = spark.sql(f"DESCRIBE DETAIL {self.fqname(table)}").first()
        return dict(row["properties"]) if row else {}

    def tags_of(self, table: str) -> dict[str, str]:
        """Return the Unity Catalog tags as a plain dict, read from information_schema."""
        rows = spark.sql(
            f"SELECT tag_name, tag_value FROM {self.catalog}.information_schema.table_tags"
            f" WHERE schema_name = '{self.schema}' AND table_name = '{table}'"
        ).collect()
        return {row.tag_name: row.tag_value for row in rows}

    def column_tags_of(self, table: str) -> dict[tuple[str, str], str]:
        """Return {(column, tag): value} for the live table, from information_schema."""
        rows = spark.sql(
            f"SELECT column_name, tag_name, tag_value"
            f" FROM {self.catalog}.information_schema.column_tags"
            f" WHERE schema_name = '{self.schema}' AND table_name = '{table}'"
        ).collect()
        return {(row.column_name, row.tag_name): row.tag_value for row in rows}

    def has_primary_key(self, table: str) -> bool:
        """Return True if the live table has a primary key constraint."""
        rows = spark.sql(
            f"SELECT 1 FROM {self.catalog}.information_schema.table_constraints"
            f" WHERE table_schema = '{self.schema}' AND table_name = '{table}'"
            f" AND constraint_type = 'PRIMARY KEY'"
        ).collect()
        return len(rows) > 0

    def has_foreign_key(self, table: str) -> bool:
        """Return True if the live table is the child of a foreign key constraint."""
        rows = spark.sql(
            f"SELECT 1 FROM {self.catalog}.information_schema.referential_constraints AS rc"
            f" JOIN {self.catalog}.information_schema.key_column_usage AS kcu"
            f" USING (constraint_catalog, constraint_schema, constraint_name)"
            f" WHERE kcu.table_schema = '{self.schema}' AND kcu.table_name = '{table}'"
        ).collect()
        return len(rows) > 0

    def table_comment(self, table: str) -> str:
        """Return the live table comment, or empty string."""
        return spark.catalog.getTable(self.fqname(table)).description or ""

    def column_comment(self, table: str, column: str) -> str:
        """Return the live comment on one column, or empty string."""
        return self.fields_of(table)[column].metadata.get("comment", "")
