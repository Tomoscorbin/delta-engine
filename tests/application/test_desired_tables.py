from hypothesis import given, strategies as st
import pytest

from delta_engine.application.desired_tables import prepare_desired_tables
from delta_engine.domain.model import Column, DesiredTable, Integer, QualifiedName
from delta_engine.schema import DeltaTable, String


@st.composite
def _distinct_qualified_names(
    draw: st.DrawFn, min_size: int = 1, max_size: int = 8
) -> list[QualifiedName]:
    """Draw a list of distinct QualifiedName instances."""
    part = st.from_regex(r"[a-z][a-z0-9]{0,9}", fullmatch=True)
    names: list[QualifiedName] = []
    seen: set[str] = set()
    size = draw(st.integers(min_value=min_size, max_value=max_size))
    attempts = 0
    while len(names) < size and attempts < size * 20:
        attempts += 1
        catalog, schema, name = draw(part), draw(part), draw(part)
        key = f"{catalog}.{schema}.{name}"
        if key not in seen:
            seen.add(key)
            names.append(QualifiedName(catalog, schema, name))
    return names


class _StubSource:
    """Minimal DesiredTableSource — no DeltaTable overhead."""

    def __init__(self, qualified_name: QualifiedName) -> None:
        self._qualified_name = qualified_name

    def to_desired_table(self) -> DesiredTable:
        return DesiredTable(
            qualified_name=self._qualified_name,
            columns=(Column("id", Integer()),),
        )


def _tbl(fqn: str, **kwargs) -> DeltaTable:
    """Build a table definition from 'catalog.schema.name' plus overrides."""
    catalog, schema, name = fqn.split(".")
    defaults = dict(
        columns=(
            Column("id", Integer(), nullable=False, comment="PK"),
            Column("name", String()),
        ),
    )
    defaults.update(kwargs)
    return DeltaTable(catalog=catalog, schema=schema, name=name, **defaults)


def test_rejects_duplicate_qualified_name_in_one_call():
    # Given two specs with the same qualified name
    t1 = _tbl("cat.a.users")
    t2 = _tbl("cat.a.users")

    # When/Then preparing them raises ValueError
    with pytest.raises(ValueError):
        prepare_desired_tables(t1, t2)


def test_orders_result_by_qualified_name():
    # Given specs passed out of order
    # When preparing them
    desired = prepare_desired_tables(
        _tbl("cat.z.last"),
        _tbl("cat.a.first"),
        _tbl("cat.m.middle"),
    )

    # Then the result is deterministic name-sorted order
    names = [str(d.qualified_name) for d in desired]
    assert names == ["cat.a.first", "cat.m.middle", "cat.z.last"]


def test_lowers_sources_to_domain_desired_tables():
    # Given a table definition
    # When preparing it
    desired = prepare_desired_tables(_tbl("cat.a.customers"))

    # Then it is a domain DesiredTable carrying the qualified name
    assert len(desired) == 1
    assert isinstance(desired[0], DesiredTable)
    assert str(desired[0].qualified_name) == "cat.a.customers"


def test_no_tables_yields_empty_tuple():
    # Given no table specifications
    # When preparing them
    # Then the result is an empty tuple (a valid no-op sync input)
    assert prepare_desired_tables() == ()


@given(_distinct_qualified_names(min_size=1, max_size=8))
def test_result_is_always_sorted_by_qualified_name_string(
    qualified_names: list[QualifiedName],
) -> None:
    # Given N distinct qualified names in arbitrary order
    sources = [_StubSource(qn) for qn in qualified_names]

    # When preparing them
    observed_order = [str(t.qualified_name) for t in prepare_desired_tables(*sources)]

    # Then the output is always in lexicographic str(qualified_name) order
    assert observed_order == sorted(observed_order)
