"""SparkReader drives the shared catalog read through spark.sql."""

import pytest

from delta_engine.adapters.databricks.spark._runner import SparkSqlRunner
from delta_engine.adapters.databricks.spark.reader import SparkReader
from delta_engine.adapters.databricks.sql import describe_json_query, schema_exists_query
from delta_engine.application.errors import ReadError
from delta_engine.application.ports import TableAbsent, TablePresent
from delta_engine.domain.model import Integer, QualifiedName
from tests.adapters.databricks.fakes import build_catalog_responses

QN = QualifiedName("cat", "sch", "tbl")


class FakeAnalysisError(Exception):
    def __init__(self, condition):
        super().__init__(condition)
        self._condition = condition

    def getCondition(self):
        return self._condition


class FakeDataFrame:
    def __init__(self, rows):
        self._rows = rows

    def first(self):
        return self._rows[0] if self._rows else None

    def collect(self):
        return list(self._rows)


class FakeSpark:
    """Routes spark.sql() by exact query text; the AS JSON result is one 1-col row."""

    def __init__(self, responses):
        self._responses = responses
        self.queries = []

    def sql(self, query):
        self.queries.append(query)
        if query not in self._responses:
            pytest.fail(f"unexpected SQL query: {query}", pytrace=False)
        value = self._responses[query]
        if isinstance(value, Exception):
            raise value
        return FakeDataFrame(value)


def test_present_table_reads_via_as_json():
    # Given a described table reachable through a Spark session
    spark = FakeSpark(build_catalog_responses(QN))

    state = SparkReader(SparkSqlRunner(spark)).fetch_state(QN)

    # Then the described state reaches the observed table through spark.sql
    assert isinstance(state, TablePresent)
    assert state.table.columns[0].data_type == Integer()


def test_missing_table_is_absent_after_confirming_the_schema_exists():
    # Given a missing table inside an existing schema
    spark = FakeSpark(
        {
            describe_json_query(QN): FakeAnalysisError("TABLE_OR_VIEW_NOT_FOUND"),
            schema_exists_query(QN): [("sch",)],
        }
    )

    # Then the table reads as absent, probing the schema second
    assert isinstance(SparkReader(SparkSqlRunner(spark)).fetch_state(QN), TableAbsent)
    assert spark.queries == [describe_json_query(QN), schema_exists_query(QN)]


def test_other_error_is_translated_to_read_error():
    # Given a describe failing for a reason other than a missing relation
    spark = FakeSpark({describe_json_query(QN): FakeAnalysisError("INSUFFICIENT_PERMISSIONS")})

    with pytest.raises(ReadError) as exc_info:
        SparkReader(SparkSqlRunner(spark)).fetch_state(QN)

    # Then the failure surfaces as a read error naming the exception
    assert exc_info.value.exception_type == "FakeAnalysisError"
