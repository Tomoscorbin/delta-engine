import pytest

from tabula.application.change_target import load_change_target
from tabula.domain.model.change_target import ChangeTarget


class FakeCatalogReader:
    """Records calls and returns a preconfigured observed."""
    def __init__(self, observed):
        self._observed = observed
        self.calls: list[str] = []

    def fetch_state(self, qualified_name: str):
        self.calls.append(qualified_name)
        return self._observed


class FakeDesiredTable:
    """Minimal duck-typed desired with only the attribute we need."""
    def __init__(self, qualified_name: str):
        self.qualified_name = qualified_name


class FakeObservedTable:
    """Minimal duck-typed observed with only the attributes ChangeTarget may touch."""
    def __init__(self, qualified_name: str, is_empty: bool = True):
        self.qualified_name = qualified_name
        self.is_empty = is_empty


def test_load_change_target_calls_reader_with_desired_qualified_name():
    desired = FakeDesiredTable("cat.schema.table")
    reader = FakeCatalogReader(observed=None)

    _ = load_change_target(reader, desired)

    assert reader.calls == ["cat.schema.table"]


def test_load_change_target_returns_change_target_with_expected_parts():
    desired = FakeDesiredTable("cat.schema.table")
    observed = FakeObservedTable("cat.schema.table", is_empty=True)
    reader = FakeCatalogReader(observed=observed)

    subject = load_change_target(reader, desired)

    assert isinstance(subject, ChangeTarget)
    assert subject.desired is desired
    assert subject.observed is observed
    # qualified_name should be the desired's name
    assert str(subject.qualified_name) == "cat.schema.table" or subject.qualified_name == "cat.schema.table"


def test_load_change_target_handles_table_absent_observed_none():
    desired = FakeDesiredTable("cat.schema.table")
    reader = FakeCatalogReader(observed=None)

    subject = load_change_target(reader, desired)

    assert isinstance(subject, ChangeTarget)
    assert subject.observed is None
    # If ChangeTarget exposes is_new, this should be True; otherwise omit this assertion.
    assert getattr(subject, "is_new", subject.observed is None)


def test_load_change_target_raises_on_qualified_name_mismatch():
    desired = FakeDesiredTable("cat.schema.table")
    # Observed has a different qualified name -> ChangeTarget should raise
    observed = FakeObservedTable("other.schema.table", is_empty=True)
    reader = FakeCatalogReader(observed=observed)

    with pytest.raises(ValueError):
        _ = load_change_target(reader, desired)
