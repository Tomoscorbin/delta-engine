import pytest

from tabula.application.errors import ExecutionFailedError, ValidationFailedError


class DummyReport:
    """Minimal stand-in for SyncReport to assert passthrough storage."""

    pass


def test_validation_failed_error_stores_copy_and_message_single_key():
    original = {"b": ("issue-b-1",)}
    err = ValidationFailedError(failures_by_table=original, report=DummyReport())

    # Failures are copied (not the same object) and type is dict
    assert isinstance(err.failures_by_table, dict)
    assert err.failures_by_table == {"b": ("issue-b-1",)}
    assert err.failures_by_table is not original

    # Report is preserved
    assert isinstance(err.report, DummyReport)

    # Message includes count and sorted head (single key, no ellipsis)
    assert str(err) == "Validation failed for 1 table(s): b."


def test_execution_failed_error_stores_copy_and_message_single_key():
    original = {"b": "boom-b"}
    err = ExecutionFailedError(failures_by_table=original, report=DummyReport())

    assert isinstance(err.failures_by_table, dict)
    assert err.failures_by_table == {"b": "boom-b"}
    assert err.failures_by_table is not original

    assert isinstance(err.report, DummyReport)

    assert str(err) == "Execution failed for 1 table(s): b."


@pytest.mark.parametrize(
    "exc_cls,prefix",
    [
        (ValidationFailedError, "Validation"),
        (ExecutionFailedError, "Execution"),
    ],
)
def test_message_lists_keys_sorted_no_ellipsis_when_exactly_five(exc_cls, prefix):
    # Unsorted on purpose; head must be sorted alphabetically
    mapping = {"z": None, "b": None, "e": None, "a": None, "d": None}
    err = exc_cls(failures_by_table=mapping, report=None)

    # Exactly five → no ellipsis
    assert str(err) == f"{prefix} failed for 5 table(s): a, b, d, e, z."


@pytest.mark.parametrize(
    "exc_cls,prefix",
    [
        (ValidationFailedError, "Validation"),
        (ExecutionFailedError, "Execution"),
    ],
)
def test_message_includes_ellipsis_when_more_than_five(exc_cls, prefix):
    # 6 keys, intentionally scrambled to test sorting + truncation
    mapping = {"z": None, "b": None, "e": None, "a": None, "d": None, "c": None}
    err = exc_cls(failures_by_table=mapping, report=None)

    # First five sorted keys, then ellipsis
    assert str(err) == f"{prefix} failed for 6 table(s): a, b, c, d, e…."


@pytest.mark.parametrize("exc_cls,expected_slots", [
    (ValidationFailedError, ("failures_by_table", "report")),
    (ExecutionFailedError, ("failures_by_table", "report")),
])
def test_exception_classes_declare_expected_slots(exc_cls, expected_slots):
    # Exceptions in CPython still have a __dict__, so __slots__ doesn't prevent
    # setting arbitrary attributes. We assert the slots are declared as intended.
    assert hasattr(exc_cls, "__slots__")
    assert tuple(exc_cls.__slots__) == expected_slots


@pytest.mark.parametrize("exc_cls", [ValidationFailedError, ExecutionFailedError])
def test_mutating_original_mapping_does_not_affect_stored_copy(exc_cls):
    # Pass a real dict so we can mutate it after construction
    original = {"a": ("x",)} if exc_cls is ValidationFailedError else {"a": "x"}
    err = exc_cls(failures_by_table=original, report=None)

    original["b"] = ("y",) if exc_cls is ValidationFailedError else "y"

    # Stored copy should remain unchanged
    assert set(err.failures_by_table.keys()) == {"a"}
