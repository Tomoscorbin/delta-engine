import pytest

from delta_engine.application.scopes import table_scope_for
from delta_engine.domain.model import TableScope


@pytest.mark.parametrize(
    ("name", "expected"),
    [
        ("full", TableScope.FULL),
        ("metadata", TableScope.METADATA),
        ("annotations", TableScope.ANNOTATIONS),
        ("tags", TableScope.TAGS),
    ],
)
def test_each_public_name_resolves_to_its_domain_scope(name, expected):
    assert table_scope_for(name) is expected


def test_unknown_scope_is_rejected():
    with pytest.raises(ValueError):
        table_scope_for("everything")
