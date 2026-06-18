import pytest

from delta_engine.application.registry import Registry
from delta_engine.domain.model import DesiredTable
from delta_engine.schema import Column, DeltaTable, Integer, String


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
