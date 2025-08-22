from __future__ import annotations
import pytest
from tabula.application.errors import ApplicationError, IdentityMismatch, ExecutionFailed

def test_identity_mismatch_has_clear_message_and_base_type():
    err = IdentityMismatch(expected="cat.sch.tbl", actual="cat.sch.other")
    assert str(err) == "Identity mismatch: expected='cat.sch.tbl' actual='cat.sch.other'"
    assert isinstance(err, ApplicationError)
    with pytest.raises(IdentityMismatch):
        raise err  # ensure it’s an Exception subclass

def test_execution_failed_formats_with_messages():
    err = ExecutionFailed(qualified_name="cat.sch.tbl", messages=("boom", "bad"), executed_count=3)
    assert str(err) == "Execution failed for cat.sch.tbl (executed_count=3): boom; bad"
    assert isinstance(err, ApplicationError)

def test_execution_failed_formats_without_messages_cleanly():
    err = ExecutionFailed(qualified_name="cat.sch.tbl", messages=(), executed_count=0)
    assert str(err) == "Execution failed for cat.sch.tbl (executed_count=0)"
