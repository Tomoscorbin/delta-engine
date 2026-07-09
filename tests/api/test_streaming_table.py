from delta_engine.api.delta_table import TAG_ASPECTS
from delta_engine.domain.model import TableAspect
from delta_engine.schema import (
    Column,
    DeltaTable,
    ForeignKey,
    Integer,
    Property,
    StreamingTable,
    String,
)


def test_streaming_table_manages_only_table_and_column_tags() -> None:
    table = StreamingTable(
        catalog="dev",
        schema="silver",
        name="orders",
        columns=[
            Column("id", Integer(), nullable=False, primary_key=True),
            Column("email", String(), comment="PII", tags={"pii": "true"}),
        ],
        comment="Streaming orders",
        properties={Property.CHANGE_DATA_FEED: "true"},
        tags={"domain": "sales"},
        partitioned_by=["id"],
    )

    desired = table.to_desired_table()

    assert desired.managed_aspects == TAG_ASPECTS
    assert desired.managed_aspects == frozenset(
        {TableAspect.TABLE_TAGS, TableAspect.COLUMN_TAGS}
    )
    assert desired.comment == "Streaming orders"
    assert desired.properties == {Property.CHANGE_DATA_FEED: "true"}
    assert desired.partitioned_by == ("id",)
    assert desired.primary_key_columns == ("id",)
    assert desired.tags == {"domain": "sales"}
    assert desired.columns[1].comment == "PII"
    assert desired.columns[1].tags == {"pii": "true"}


def test_streaming_table_carries_foreign_keys_without_managing_them() -> None:
    customers = DeltaTable(
        catalog="dev",
        schema="silver",
        name="customers",
        columns=[Column("id", Integer(), nullable=False, primary_key=True)],
    )

    table = StreamingTable(
        catalog="dev",
        schema="silver",
        name="orders",
        columns=[
            Column("id", Integer(), nullable=False, primary_key=True),
            Column("customer_id", Integer()),
        ],
        foreign_keys=[ForeignKey(local_columns=("customer_id",), references=customers)],
    )

    desired = table.to_desired_table()

    assert len(desired.foreign_keys) == 1
    assert TableAspect.FOREIGN_KEYS not in desired.managed_aspects


def test_streaming_table_mirrors_special_character_columns_without_column_mapping() -> None:
    table = StreamingTable(
        catalog="dev",
        schema="silver",
        name="events",
        columns=[Column("order id", Integer())],
    )

    assert table.to_desired_table().columns[0].name == "order id"


def test_streaming_table_mirrors_cdf_reserved_columns_with_cdf_declared() -> None:
    table = StreamingTable(
        catalog="dev",
        schema="silver",
        name="events",
        columns=[Column("id", Integer()), Column("_change_type", String())],
        properties={Property.CHANGE_DATA_FEED: "true"},
    )

    assert tuple(column.name for column in table.to_desired_table().columns) == (
        "id",
        "_change_type",
    )
