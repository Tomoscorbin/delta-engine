import pytest

from delta_engine.application.registry import Registry
from delta_engine.domain.model import Column as DomainColumn, TableFormat

# --------- fakes


class _FakeColumn:
    def __init__(self, name: str, data_type: str, is_nullable: bool, comment: str | None = None):
        self.name = name
        self.data_type = data_type
        self.is_nullable = is_nullable
        self.comment = comment


class _FakeTable:
    def __init__(
        self,
        *,
        catalog: str,
        schema: str,
        name: str,
        columns: tuple[_FakeColumn, ...],
        comment: str | None = None,
        effective_properties: dict[str, str] | None = None,
        partitioned_by: tuple[str, ...] | None = None,
    ):
        self.catalog = catalog
        self.schema = schema
        self.name = name
        self.columns = columns
        self.comment = comment
        self.effective_properties = effective_properties or {}
        self.partitioned_by = partitioned_by
        self.format = TableFormat.DELTA


def _col(
    name: str, dt: str = "string", nullable: bool = True, comment: str | None = None
) -> _FakeColumn:
    return _FakeColumn(name=name, data_type=dt, is_nullable=nullable, comment=comment)


def _tbl(fqn: str, **kwargs) -> _FakeTable:
    """Build a table spec from 'cat.sch.name' plus kwargs."""
    catalog, schema, name = fqn.split(".")
    defaults = dict(
        columns=(_col("id", "int", False, "PK"), _col("name", "string", True)),
        comment=None,
        effective_properties={"delta.appendOnly": "true"},
        partitioned_by=None,
    )
    defaults.update(kwargs)
    return _FakeTable(catalog=catalog, schema=schema, name=name, **defaults)


# --------- tests


def test_register_rejects_duplicate_fqn_within_same_call():
    # Given an empty registry and two specs with same FQN
    reg = Registry()
    t1 = _tbl("cat.a.users")
    t2 = _tbl("cat.a.users")  # duplicate in same call

    # When/Then registering raises ValueError
    with pytest.raises(ValueError):
        reg.register(t1, t2)


def test_register_rejects_duplicate_fqn_across_calls():
    # Given a registry with one table already registered
    reg = Registry()
    reg.register(_tbl("cat.a.users"))

    # When attempting to register the same FQN again (separate call)
    # Then a clear duplicate error is raised
    with pytest.raises(ValueError):
        reg.register(_tbl("cat.a.users"))


def test_iteration_is_sorted_by_fully_qualified_name():
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


def test_columns_are_converted_to_domain_columns_with_attributes_preserved():
    # Given a table spec with explicit column metadata
    columns = (
        _col("id", "int", False, "primary key"),
        _col("name", "string", True, "customer name"),
    )
    reg = Registry()
    reg.register(_tbl("cat.a.customers", columns=columns))

    # When retrieving the DesiredTable
    desired = next(iter(reg))

    # Then columns are DomainColumn with same attributes in order
    assert all(isinstance(c, DomainColumn) for c in desired.columns)
    assert [c.name for c in desired.columns] == ["id", "name"]
    assert [str(c.data_type) for c in desired.columns] == ["int", "string"]
    assert [c.is_nullable for c in desired.columns] == [False, True]
    assert [c.comment for c in desired.columns] == ["primary key", "customer name"]


def test_properties_and_comment_and_partitioning_are_carried_through():
    # Given a table with properties, comment, and partitioning
    reg = Registry()
    reg.register(
        _tbl(
            "cat.sales.fact_orders",
            comment="Daily aggregated orders",
            columns=(_col("ds", "string"),),
            effective_properties={
                "delta.appendOnly": "false",
                "delta.autoOptimize.optimizeWrite": "true",
            },
            partitioned_by=("ds",),
        )
    )

    # When retrieving the DesiredTable
    desired = next(iter(reg))

    # Then metadata is preserved
    assert desired.comment == "Daily aggregated orders"
    assert desired.properties == {
        "delta.appendOnly": "false",
        "delta.autoOptimize.optimizeWrite": "true",
    }
    assert desired.partitioned_by == ("ds",)


def test_none_partitioning_becomes_empty_tuple_not_none():
    # Given a table with no partition spec (None)
    reg = Registry()
    reg.register(_tbl("cat.core.dim_date", partitioned_by=None))

    # When retrieving
    desired = next(iter(reg))

    # Then partitioned_by is a stable empty tuple
    assert desired.partitioned_by == ()


def test_len_reflects_number_of_registered_tables_even_after_multiple_calls():
    # Given a registry and multiple registration calls
    reg = Registry()
    reg.register(_tbl("cat.a.t1"))
    reg.register(_tbl("cat.a.t2"), _tbl("cat.a.t3"))

    # When checking len
    # Then it reflects total registered
    assert len(reg) == 3
