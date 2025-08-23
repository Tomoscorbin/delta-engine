import pytest

from tabula.domain.model.change_target import ChangeTarget


class DesiredStub:
    def __init__(self, qualified_name: str):
        self.qualified_name = qualified_name


class ObservedStub:
    def __init__(self, qualified_name: str, is_empty: bool):
        self.qualified_name = qualified_name
        self.is_empty = is_empty


def test_constructs_with_none_observed_and_marks_new() -> None:
    desired = DesiredStub("cat.schema.table")
    subject = ChangeTarget(desired=desired, observed=None)

    assert subject.is_new is True
    # is_existing_and_non_empty should be False when observed is None
    assert subject.is_existing_and_non_empty is False


def test_forwards_qualified_name_from_desired() -> None:
    desired = DesiredStub("cat.schema.table")
    subject = ChangeTarget(desired=desired, observed=None)

    assert subject.qualified_name == "cat.schema.table"


def test_raises_on_qualified_name_mismatch() -> None:
    desired = DesiredStub("cat.schema.table")
    observed = ObservedStub("other.schema.table", is_empty=True)

    with pytest.raises(ValueError, match="qualified_name must match"):
        ChangeTarget(desired=desired, observed=observed)


def test_is_existing_and_non_empty_true_when_observed_not_empty() -> None:
    desired = DesiredStub("cat.schema.table")
    observed = ObservedStub("cat.schema.table", is_empty=False)

    subject = ChangeTarget(desired=desired, observed=observed)
    assert subject.is_existing_and_non_empty is True
    assert subject.is_new is False


def test_is_existing_and_non_empty_false_when_observed_empty() -> None:
    desired = DesiredStub("cat.schema.table")
    observed = ObservedStub("cat.schema.table", is_empty=True)

    subject = ChangeTarget(desired=desired, observed=observed)
    assert subject.is_existing_and_non_empty is False
    assert subject.is_new is False
