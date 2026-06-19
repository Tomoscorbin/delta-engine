import pytest
from hypothesis import given
from hypothesis import strategies as st

from delta_engine.application.registry import Registry
from delta_engine.domain.model import Column, DesiredTable, Integer, QualifiedName
from delta_engine.schema import Column as SchemaColumn, DeltaTable, Integer as SchemaInteger, String


@st.composite
def _distinct_qualified_names(draw: st.DrawFn, min_size: int = 1, max_size: int = 8) -> list[QualifiedName]:
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
    """Minimal DesiredTableSource for registry tests — no DeltaTable overhead."""

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


def test_register_rejects_duplicate_qualified_name_within_same_call():
    # Given an empty registry and two specs with the same qualified name
    reg = Registry()
    t1 = _tbl("cat.a.users")
    t2 = _tbl("cat.a.users")  # duplicate in same call

    # When/Then registering raises ValueError
    with pytest.raises(ValueError):
        reg.register(t1, t2)


def test_register_rejects_duplicate_qualified_name_across_calls():
    # Given a registry with one table already registered
    reg = Registry()
    reg.register(_tbl("cat.a.users"))

    # When attempting to register the same qualified name again (separate call)
    # Then a clear duplicate error is raised
    with pytest.raises(ValueError):
        reg.register(_tbl("cat.a.users"))


def test_iteration_is_sorted_by_qualified_name():
    # Given tables registered out of order
    reg = Registry()
    reg.register(
        _tbl("cat.z.last"),
        _tbl("cat.a.first"),
        _tbl("cat.m.middle"),
    )

    # When iterating
    names = [str(d.qualified_name) for d in reg]

    # Then iteration yields deterministic name-sorted order
    assert names == ["cat.a.first", "cat.m.middle", "cat.z.last"]


def test_registered_tables_are_yielded_as_domain_desired_tables():
    # Given a registered table definition
    reg = Registry()
    reg.register(_tbl("cat.a.customers"))

    # When retrieving the registered entry
    desired = next(iter(reg))

    # Then it is a domain DesiredTable carrying the qualified name
    assert isinstance(desired, DesiredTable)
    assert str(desired.qualified_name) == "cat.a.customers"


def test_len_reflects_number_of_registered_tables_even_after_multiple_calls():
    # Given a registry and multiple registration calls
    reg = Registry()
    reg.register(_tbl("cat.a.t1"))
    reg.register(_tbl("cat.a.t2"), _tbl("cat.a.t3"))

    # When checking len
    # Then it reflects total registered
    assert len(reg) == 3


# ---------- property: iteration order ----------


@given(_distinct_qualified_names(min_size=1, max_size=8))
def test_registry_iteration_is_always_sorted_by_qualified_name_string(
    qualified_names: list[QualifiedName],
) -> None:
    # Given: N distinct qualified names registered in arbitrary order
    reg = Registry()
    for qn in qualified_names:
        reg.register(_StubSource(qn))

    # When: iterating
    observed_order = [str(t.qualified_name) for t in reg]

    # Then: output is always in lexicographic str(qualified_name) order
    assert observed_order == sorted(observed_order)


# ---------- property: batch registration is atomic ----------


@given(_distinct_qualified_names(min_size=2, max_size=6))
def test_registry_batch_with_duplicate_leaves_registry_unchanged(
    qualified_names: list[QualifiedName],
) -> None:
    # Given: a registry with the first name pre-registered
    reg = Registry()
    reg.register(_StubSource(qualified_names[0]))
    size_before = len(reg)

    # When: a batch containing a duplicate of the already-registered name is submitted
    duplicate = qualified_names[0]
    batch = [_StubSource(qn) for qn in qualified_names[1:]] + [_StubSource(duplicate)]
    try:
        reg.register(*batch)
    except ValueError:
        pass

    # Then: the registry is unchanged — the batch was rejected atomically
    assert len(reg) == size_before
