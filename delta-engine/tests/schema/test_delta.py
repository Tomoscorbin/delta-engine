import pytest

from delta_engine.domain.model.data_type import Integer, String
from delta_engine.schema.column import Column as UserColumn
from delta_engine.schema.delta import DeltaTable


def test_delta_table_rejects_duplicate_column_names_case_insensitive() -> None:
    cols = [UserColumn("ID", Integer(), is_nullable=False), UserColumn("id", Integer())]
    with pytest.raises(ValueError, match="Duplicate column name"):
        DeltaTable(catalog="dev", schema="silver", name="people", columns=cols)


def test_delta_table_validates_identifiers_via_normalize_rules() -> None:
    with pytest.raises(ValueError):
        DeltaTable(
            catalog="dev.", schema="silver", name="people", columns=[UserColumn("id", Integer())]
        )


def test_delta_table_accepts_valid_identifiers() -> None:
    t = DeltaTable(
        catalog="Dev",  # case allowed here; we only validate, normalize happens later
        schema="Silver",
        name="People",
        columns=[UserColumn("id", Integer(), is_nullable=False), UserColumn("name", String())],
    )
    assert t.name == "People"
