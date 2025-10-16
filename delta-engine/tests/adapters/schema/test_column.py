from dataclasses import FrozenInstanceError

import pytest

from delta_engine.adapters.schema.column import Column as UserColumn
from delta_engine.adapters.schema.types import Integer


def test_user_column_defaults_and_frozen() -> None:
    c = UserColumn(name="id", data_type=Integer(), comment="primary key")
    assert c.nullable is True
    assert c.comment == "primary key"
    with pytest.raises(FrozenInstanceError):
        c.name = "other"  # type: ignore[misc]
