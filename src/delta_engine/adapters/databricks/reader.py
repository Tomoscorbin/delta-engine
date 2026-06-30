"""Reader adapter for Databricks Unity Catalog."""

from __future__ import annotations

from dataclasses import dataclass
import logging
from types import MappingProxyType

from pyspark.errors.exceptions.base import AnalysisException
from pyspark.sql import SparkSession
from pyspark.sql.catalog import Column as SparkColumn

from delta_engine.adapters.databricks.sql import (
    backtick,
    backtick_qualified_name,
    domain_type_from_spark,
    error_preview,
    exception_type_name,
    quote_literal,
)
from delta_engine.application.results import (
    CatalogState,
    ReadFailed,
    ReadFailure,
    TableAbsent,
    TablePresent,
)
from delta_engine.domain.model import Column as DomainColumn, ObservedTable, QualifiedName
from delta_engine.domain.model.foreign_key import ForeignKeyConstraint

logger = logging.getLogger(__name__)


@dataclass(frozen=True, slots=True)
class _ColumnMapping:
    column: DomainColumn
    is_partition: bool


def _to_column_mapping(
    spark_column: SparkColumn, qualified_name: QualifiedName
) -> _ColumnMapping | None:
    """
    Convert a Spark catalog column into a domain ``Column`` and its partition flag.

    Returns ``None`` for columns whose Spark type has no domain mapping yet,
    logging a warning so operators can track gaps as new Spark types are released.

    The column name is lowercased here: the domain model requires lowercase
    identifiers, and case-preserving catalogs (e.g. Hive Metastore) can return
    mixed-case names. Normalising at the adapter boundary keeps that impedance
    mismatch out of the domain. The partition name in ``_ColumnMapping`` is
    therefore derived from the already-normalised domain column name, not from
    the raw Spark object.
    """
    domain_data_type = domain_type_from_spark(spark_column.dataType)
    if domain_data_type is None:
        logger.warning(
            "Skipping column %r in %s: unrecognised Spark type %r"
            " — column will be unmanaged until support is added",
            spark_column.name,
            qualified_name,
            spark_column.dataType,
        )
        return None

    nullable = bool(getattr(spark_column, "nullable", True))
    comment = spark_column.description if spark_column.description else ""

    return _ColumnMapping(
        column=DomainColumn(
            name=spark_column.name.casefold(),
            data_type=domain_data_type,
            nullable=nullable,
            comment=comment,
        ),
        is_partition=bool(getattr(spark_column, "isPartition", False)),
    )


class DatabricksReader:
    """Catalog state reader backed by a Databricks/Spark session."""

    def __init__(self, spark: SparkSession) -> None:
        """Initialize the reader with a `SparkSession`."""
        self.spark = spark

    def fetch_state(self, qualified_name: QualifiedName) -> CatalogState:
        """
        Fetch the current state of a table: present, absent, or unreadable.

        Returns ``TablePresent`` carrying the current columns, properties, and
        table comment; ``TableAbsent`` when the table doesn't exist; or
        ``ReadFailed`` if catalog access raised an exception.

        Every failure mode is contained: anything that goes wrong reading this
        table -- a failing existence probe, an unsupported column type, a Spark
        error mid-read -- becomes a ``ReadFailed`` for this table rather than an
        exception that aborts the whole sync. The ``CatalogStateReader`` contract
        promises a ``CatalogState``, so the boundary must be total.
        """
        try:
            return self._read(qualified_name)
        except Exception as exception:
            failure = ReadFailure(exception_type_name(exception), error_preview(exception))
            return ReadFailed(failure=failure)

    def _read(self, qualified_name: QualifiedName) -> CatalogState:
        """Read current state, letting any failure propagate to ``fetch_state``."""
        if not self._table_exists(qualified_name):
            return TableAbsent()

        all_mappings = (
            _to_column_mapping(c, qualified_name)
            for c in self.spark.catalog.listColumns(str(qualified_name))
        )
        mappings = tuple(m for m in all_mappings if m is not None)
        columns = tuple(m.column for m in mappings)
        partition_columns = tuple(m.column.name for m in mappings if m.is_partition)
        observed = ObservedTable(
            qualified_name=qualified_name,
            columns=columns,
            comment=self._fetch_table_comment(qualified_name),
            properties=self._fetch_properties(qualified_name),
            partitioned_by=partition_columns,
            primary_key=self._fetch_primary_key(qualified_name),
            foreign_keys=self._fetch_foreign_keys(qualified_name),
        )
        return TablePresent(table=observed)

    def _table_exists(self, qualified_name: QualifiedName) -> bool:
        """
        Return `True` if the table exists, else `False`.

        Uses the catalog's own existence check rather than a hand-rolled
        information_schema query. Note: ``tableExists`` also reports ``True`` for
        a session temporary view registered under this name; that is an unusual
        collision for a three-part ``catalog.schema.table`` and, if it happened,
        the subsequent read would surface as a ``ReadFailed`` rather than corrupt
        state.
        """
        return self.spark.catalog.tableExists(str(qualified_name))

    def _fetch_properties(self, qualified_name: QualifiedName) -> MappingProxyType[str, str]:
        """Return all catalog table properties as a read-only mapping."""
        # The name is interpolated into SQL text here, so it must be backtick-quoted
        # to stay an identifier (and escape any embedded backtick). This differs
        # deliberately from the catalog.* calls, which take the plain ``str()`` form
        # because they parse the dot-separated parts themselves. Don't unify the two.
        query = f"DESCRIBE DETAIL {backtick_qualified_name(qualified_name)}"
        row = self.spark.sql(query).first()
        if not row:
            return MappingProxyType({})
        return MappingProxyType(dict(row["properties"]))

    def _fetch_primary_key(self, qualified_name: QualifiedName) -> tuple[str, ...]:
        """
        Return the primary key column names from Unity Catalog information_schema.

        Returns an empty tuple when no primary key is defined. Column names are
        normalised to lowercase at the adapter boundary.
        """
        catalog = backtick(qualified_name.catalog)
        query = (
            f"SELECT constraint_columns.column_name"
            f" FROM {catalog}.information_schema.constraint_column_usage"
            f" AS constraint_columns"
            f" JOIN {catalog}.information_schema.table_constraints"
            f" AS table_constraints_info"
            f" USING (constraint_catalog, constraint_schema, constraint_name)"
            f" WHERE constraint_columns.table_schema ="
            f" {quote_literal(qualified_name.schema)}"
            f" AND constraint_columns.table_name ="
            f" {quote_literal(qualified_name.name)}"
            f" AND table_constraints_info.constraint_type = 'PRIMARY KEY'"
        )
        try:
            rows = self.spark.sql(query).collect()
        except AnalysisException:
            # information_schema is only available in Unity Catalog. On plain
            # Spark (e.g. local tests), the table does not exist and there are
            # no PK constraints to observe.
            return ()
        return tuple(row["column_name"].casefold() for row in rows)

    def _fetch_foreign_keys(
        self, qualified_name: QualifiedName
    ) -> tuple[ForeignKeyConstraint, ...]:
        """
        Return foreign key constraints from Unity Catalog information_schema.

        Returns an empty tuple when no FKs are defined, or on AnalysisException
        (plain Spark / non-Unity Catalog environment).

        Constraint names are read from the catalog so the differ can compare them
        directly without re-deriving them. Column names are lowercased at the
        adapter boundary, consistent with how primary key and column names are
        normalised throughout this reader.
        """
        catalog = backtick(qualified_name.catalog)
        query = (
            f"SELECT rc.constraint_name,"
            f" kcu.column_name AS local_column,"
            f" kcu.ordinal_position,"
            f" ccu.table_catalog AS ref_catalog,"
            f" ccu.table_schema AS ref_schema,"
            f" ccu.table_name AS ref_table,"
            f" ccu.column_name AS ref_column"
            f" FROM {catalog}.information_schema.referential_constraints AS rc"
            f" JOIN {catalog}.information_schema.key_column_usage AS kcu"
            f" USING (constraint_catalog, constraint_schema, constraint_name)"
            f" JOIN {catalog}.information_schema.constraint_column_usage AS ccu"
            f" ON rc.unique_constraint_name = ccu.constraint_name"
            f" AND rc.unique_constraint_catalog = ccu.constraint_catalog"
            f" AND rc.unique_constraint_schema = ccu.constraint_schema"
            f" WHERE kcu.table_schema = {quote_literal(qualified_name.schema)}"
            f" AND kcu.table_name = {quote_literal(qualified_name.name)}"
            f" ORDER BY rc.constraint_name, kcu.ordinal_position"
        )
        try:
            rows = self.spark.sql(query).collect()
        except AnalysisException:
            # information_schema is only available in Unity Catalog. On plain
            # Spark (e.g. local tests without UC), the view does not exist and
            # there are no FK constraints to observe.
            return ()

        # Group rows by constraint_name, preserving ordinal order from the query.
        # Using a plain dict keeps insertion order (Python 3.7+), so constraints
        # appear in the same order as the first row for each name.
        grouped: dict[str, dict] = {}
        for row in rows:
            constraint_name = row.constraint_name
            if constraint_name not in grouped:
                grouped[constraint_name] = {
                    "local_columns": [],
                    "ref_catalog": row.ref_catalog,
                    "ref_schema": row.ref_schema,
                    "ref_table": row.ref_table,
                    "referenced_columns": [],
                }
            grouped[constraint_name]["local_columns"].append(row.local_column.casefold())
            grouped[constraint_name]["referenced_columns"].append(row.ref_column.casefold())

        return tuple(
            ForeignKeyConstraint(
                local_columns=tuple(data["local_columns"]),
                references=f"{data['ref_catalog']}.{data['ref_schema']}.{data['ref_table']}".casefold(),
                referenced_columns=tuple(data["referenced_columns"]),
                constraint_name=constraint_name,
            )
            for constraint_name, data in grouped.items()
        )

    def _fetch_table_comment(self, qualified_name: QualifiedName) -> str:
        """Return the table comment (empty string when not set)."""
        return self.spark.catalog.getTable(str(qualified_name)).description or ""
