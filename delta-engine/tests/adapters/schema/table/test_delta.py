import pytest

from delta_engine.adapters.schema import Column
from delta_engine.adapters.schema.delta.properties import Property
from delta_engine.adapters.schema.delta.table import DeltaTable
from delta_engine.domain.model.data_type import Integer


def test_rejects_unknown_property_keys() -> None:
    with pytest.raises(ValueError) as exc:
        DeltaTable(
            catalog="dev",
            schema="silver",
            name="things",
            columns=(Column("id", Integer()),),
            properties={"foo": "bar"},
        )
    assert "Unknown Delta table properties" in str(exc.value)
    assert "foo" in str(exc.value)


def test_accepts_known_property_keys() -> None:
    # Should not raise for supported property keys
    t = DeltaTable(
        catalog="dev",
        schema="silver",
        name="things",
        columns=(Column("id", Integer()),),
        properties={Property.CHANGE_DATA_FEED.value: "true"},
    )
    assert Property.CHANGE_DATA_FEED.value in t.properties


def test_partition_column_must_exist() -> None:
    with pytest.raises(ValueError) as exc:
        DeltaTable(
            catalog="dev",
            schema="silver",
            name="things",
            columns=(Column("id", Integer()),),
            partitioned_by=("created_at",),
        )
    assert "Partition column not found: created_at" in str(exc.value)
