from tabula.application.errors import ApplicationError, ExecutionFailed, IdentityMismatch


def test_identity_mismatch_str_includes_expected_and_actual():
    err = IdentityMismatch(expected="cat.sch.tbl", actual="cat.sch.other")
    s = str(err)
    assert "expected" in s and "actual" in s
    assert "cat.sch.tbl" in s and "cat.sch.other" in s
    assert isinstance(err, ApplicationError)


def test_execution_failed_str_includes_context():
    err = ExecutionFailed(qualified_name="cat.sch.tbl", messages=("boom", "bad"), executed_count=3)
    s = str(err)
    assert "cat.sch.tbl" in s
    assert "boom" in s and "bad" in s
    assert "executed_count=3" in s
    assert isinstance(err, ApplicationError)
