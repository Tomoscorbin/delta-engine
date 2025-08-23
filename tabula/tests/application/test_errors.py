from __future__ import annotations
import pytest

from tabula.application.errors import (
    IdentityMismatch,
    ExecutionFailed,
    ValidationError,
)


def test_identity_mismatch_has_clear_message_and_is_exception():
    err = IdentityMismatch(expected="cat.sch.tbl", actual="cat.sch.other")
    assert str(err) == "Identity mismatch: expected='cat.sch.tbl' actual='cat.sch.other'"
    assert isinstance(err, Exception)
    with pytest.raises(IdentityMismatch):
        raise err


def test_execution_failed_formats_with_messages():
    err = ExecutionFailed(qualified_name="cat.sch.tbl", messages=("boom", "bad"), executed_count=3)
    assert str(err) == "Execution failed for cat.sch.tbl (executed_count=3): boom; bad"
    assert isinstance(err, Exception)


def test_execution_failed_formats_without_messages_cleanly():
    err = ExecutionFailed(qualified_name="cat.sch.tbl", messages=(), executed_count=0)
    assert str(err) == "Execution failed for cat.sch.tbl (executed_count=0)"
    assert isinstance(err, Exception)


def test_validation_error_formats_and_is_exception():
    err = ValidationError(
        code="NO_ADD_ON_NON_EMPTY_TABLE", message="Not allowed", target="cat.sch.tbl"
    )
    s = str(err)
    assert "NO_ADD_ON_NON_EMPTY_TABLE: Not allowed" in s
    assert "Table: cat.sch.tbl" in s
    assert isinstance(err, Exception)
