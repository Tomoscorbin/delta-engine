"""
Shell tests for WarehouseReader: sequencing, normalization at the boundary,
and fetch_state's totality.

The fake connection routes cursor.execute() by EXACT query text (keyed by the
same builders the reader uses), so no fake ever parses SQL. Rows are
SimpleNamespace/dict stand-ins for databricks-sql Row objects, which support
attribute access, item access, and asDict().
"""

from types import SimpleNamespace

from delta_engine.adapters.databricks.sql import (
    column_tags_query,
    columns_query,
    describe_detail_query,
    foreign_keys_query,
    primary_key_query,
    referencing_foreign_keys_query,
    table_row_query,
    table_tags_query,
)
from delta_engine.adapters.databricks.warehouse.reader import WarehouseReader
from delta_engine.application.ports import ReadFailed, TableAbsent, TablePresent
from delta_engine.domain.model import QualifiedName
from delta_engine.domain.model.data_type import Integer, String

QN = QualifiedName("cat", "sch", "tbl")


def column_row(
    name: str,
    full_data_type: str = "int",
    is_nullable: str = "YES",
    comment: str | None = None,
    partition_index: int | None = None,
):
    return SimpleNamespace(
        column_name=name,
        full_data_type=full_data_type,
        is_nullable=is_nullable,
        comment=comment,
        partition_index=partition_index,
    )


class DetailRow(dict):
    """Duck-typed DESCRIBE DETAIL row: attribute access plus asDict()."""

    def __getattr__(self, name):
        try:
            return self[name]
        except KeyError as exc:  # pragma: no cover - defensive
            raise AttributeError(name) from exc

    def asDict(self):
        return dict(self)


def detail_row(properties: str | None = "{}", **extra) -> DetailRow:
    return DetailRow(properties=properties, **extra)


class RoutedCursor:
    """Cursor fake answering fetchall() from an exact query-text table."""

    def __init__(self, responses: dict):
        self._responses = responses
        self._current = None
        self.queries: list[str] = []

    def __enter__(self):
        return self

    def __exit__(self, *exc_info):
        return False

    def execute(self, query: str) -> None:
        self.queries.append(query)
        if query not in self._responses:
            raise AssertionError(f"unexpected query: {query}")
        value = self._responses[query]
        if isinstance(value, Exception):
            raise value
        self._current = value

    def fetchall(self):
        return list(self._current)


class RoutedConnection:
    def __init__(self, responses: dict):
        self.cursor_fake = RoutedCursor(responses)

    def cursor(self):
        return self.cursor_fake


def routed_connection(
    *,
    table_rows=None,
    columns=None,
    detail=None,
    pk=(),
    fks=(),
    referencing=(),
    table_tags=(),
    column_tags=(),
) -> RoutedConnection:
    """Wire a fake connection for one table read with sensible defaults."""
    if table_rows is None:
        table_rows = [SimpleNamespace(comment=None)]
    if columns is None:
        # A table with zero columns is not a valid domain object (`TableSnapshot`
        # requires at least one column), so tests that don't care about column
        # shape still need a placeholder column to get a valid ObservedTable.
        columns = [column_row("id")]
    responses = {
        table_row_query(QN): table_rows,
        columns_query(QN): list(columns),
        describe_detail_query(QN): [detail if detail is not None else detail_row()],
        primary_key_query(QN): list(pk),
        foreign_keys_query(QN): list(fks),
        referencing_foreign_keys_query(QN): list(referencing),
        table_tags_query(QN): list(table_tags),
        column_tags_query(QN): list(column_tags),
    }
    return RoutedConnection(responses)


def fetch_present(connection) -> TablePresent:
    state = WarehouseReader(connection).fetch_state(QN)
    assert isinstance(state, TablePresent), state
    return state


def test_no_tables_row_means_absent():
    connection = RoutedConnection({table_row_query(QN): []})
    assert isinstance(WarehouseReader(connection).fetch_state(QN), TableAbsent)


def test_maps_columns_with_types_nullability_and_comments():
    connection = routed_connection(
        columns=[
            column_row("ID", "int", is_nullable="NO", comment="pk"),
            column_row("name", "string", is_nullable="YES"),
        ],
    )

    observed = fetch_present(connection).table

    assert [column.name for column in observed.columns] == ["id", "name"]
    assert observed.columns[0].data_type == Integer()
    assert observed.columns[0].nullable is False
    assert observed.columns[0].comment == "pk"
    assert observed.columns[1].data_type == String()
    assert observed.columns[1].nullable is True
    assert observed.columns[1].comment == ""


def test_table_comment_read_from_tables_row():
    connection = routed_connection(table_rows=[SimpleNamespace(comment="orders table")])
    assert fetch_present(connection).table.comment == "orders table"


def test_partition_columns_ordered_by_partition_index():
    connection = routed_connection(
        columns=[
            column_row("a", partition_index=2),
            column_row("b"),
            column_row("c", partition_index=1),
        ],
    )
    assert fetch_present(connection).table.partitioned_by == ("c", "a")


def test_unmappable_column_type_is_skipped():
    connection = routed_connection(
        columns=[column_row("ok"), column_row("weird", full_data_type="geography")],
    )
    observed = fetch_present(connection).table
    assert [column.name for column in observed.columns] == ["ok"]


def test_unmappable_partition_column_type_fails_the_read():
    connection = routed_connection(
        columns=[column_row("p", full_data_type="geography", partition_index=1)],
    )
    state = WarehouseReader(connection).fetch_state(QN)
    assert isinstance(state, ReadFailed)
    assert "partition" in state.failure.message.casefold()


def test_properties_parsed_from_json_and_filtered_to_registry():
    properties = '{"delta.columnMapping.mode": "name", "delta.internal.noise": "x"}'
    connection = routed_connection(detail=detail_row(properties=properties))
    observed = fetch_present(connection).table
    assert dict(observed.properties) == {"delta.columnMapping.mode": "name"}


def test_null_properties_field_means_no_properties():
    connection = routed_connection(detail=detail_row(properties=None))
    observed = fetch_present(connection).table
    assert dict(observed.properties) == {}


def test_clustering_columns_parsed_from_json_and_casefolded():
    connection = routed_connection(
        columns=[column_row("region"), column_row("city")],
        detail=detail_row(clusteringColumns='["Region", "City"]'),
    )
    assert fetch_present(connection).table.clustered_by == ("region", "city")


def test_missing_clustering_field_means_unclustered():
    connection = routed_connection(detail=detail_row())
    assert fetch_present(connection).table.clustered_by == ()


def test_empty_describe_detail_fails_the_read():
    connection = routed_connection()
    connection.cursor_fake._responses[describe_detail_query(QN)] = []
    assert isinstance(WarehouseReader(connection).fetch_state(QN), ReadFailed)


def test_primary_key_and_tags_are_wired_through_the_shared_mappers():
    connection = routed_connection(
        columns=[column_row("id", is_nullable="NO")],
        pk=[{"constraint_name": "PK_TBL", "column_name": "ID"}],
        table_tags=[SimpleNamespace(tag_name="Owner", tag_value="Data")],
        column_tags=[
            SimpleNamespace(column_name="ID", tag_name="pii", tag_value="low"),
        ],
    )

    observed = fetch_present(connection).table

    assert observed.primary_key is not None
    assert observed.primary_key.constraint_name == "pk_tbl"
    assert observed.primary_key.columns == ("id",)
    assert dict(observed.tags) == {"Owner": "Data"}
    assert dict(observed.columns[0].tags) == {"pii": "low"}


def test_any_backend_exception_becomes_read_failed():
    connection = RoutedConnection({table_row_query(QN): RuntimeError("warehouse gone")})
    state = WarehouseReader(connection).fetch_state(QN)
    assert isinstance(state, ReadFailed)
    assert state.failure.exception_type == "RuntimeError"
    assert "warehouse gone" in state.failure.message
