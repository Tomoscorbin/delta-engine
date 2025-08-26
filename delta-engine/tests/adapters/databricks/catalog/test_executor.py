from __future__ import annotations

import pytest

from delta_engine.adapters.databricks.catalog import executor as exec_mod


def _mk_executor(run_sql=lambda s: None, **kwargs) -> exec_mod.UCExecutor:
    return exec_mod.UCExecutor(run_sql=run_sql, **kwargs)


def test_dry_run_echoes_and_executes_nothing(monkeypatch) -> None:
    monkeypatch.setattr(exec_mod, "compile_plan", lambda plan, dialect=None: ("S1", "S2"))
    ran: list[str] = []
    ex = _mk_executor(run_sql=ran.append, dry_run=True)

    out = ex.execute(object())

    assert out.success is True
    assert out.executed_count == 0
    assert out.executed_sql == ("S1", "S2")
    assert out.messages == ("DRY RUN 1: S1", "DRY RUN 2: S2")
    assert ran == []


def test_stop_on_first_error(monkeypatch) -> None:
    monkeypatch.setattr(exec_mod, "compile_plan", lambda plan, dialect=None: ("A", "B", "C"))

    def run_sql(sql: str) -> None:
        if sql == "B":
            raise RuntimeError("boom")

    ex = _mk_executor(run_sql, on_error="stop")
    out = ex.execute(object())

    assert out.success is False
    assert out.executed_count == 1
    assert out.executed_sql == ("A",)
    # Message numbering and formatting matter:
    assert out.messages[0] == "OK 1"
    assert out.messages[1] == "ERROR 2: RuntimeError: boom"
    # No message for 3 because we stopped


def test_continue_runs_remaining_after_error(monkeypatch) -> None:
    monkeypatch.setattr(exec_mod, "compile_plan", lambda plan, dialect=None: ("A", "B", "C"))

    def run_sql(sql: str) -> None:
        if sql == "B":
            raise ValueError("bad")

    ex = _mk_executor(run_sql, on_error="continue")
    out = ex.execute(object())

    assert out.success is False
    assert out.executed_count == 2  # A and C executed
    assert out.executed_sql == ("A", "C")
    assert out.messages == ("OK 1", "ERROR 2: ValueError: bad", "OK 3")


def test_first_statement_failure(monkeypatch) -> None:
    monkeypatch.setattr(exec_mod, "compile_plan", lambda plan, dialect=None: ("X", "Y"))

    def run_sql(sql: str) -> None:
        raise RuntimeError("kaboom")

    ex = _mk_executor(run_sql, on_error="stop")
    out = ex.execute(object())

    assert out.success is False
    assert out.executed_count == 0
    assert out.executed_sql == ()
    assert out.messages == ("ERROR 1: RuntimeError: kaboom",)


def test_empty_plan_is_noop(monkeypatch) -> None:
    monkeypatch.setattr(exec_mod, "compile_plan", lambda plan, dialect=None: ())
    ex = _mk_executor(lambda s: (_ for _ in ()).throw(AssertionError("should not run")))

    out = ex.execute(object())

    assert out.success is True
    assert out.executed_count == 0
    assert out.executed_sql == ()
    assert out.messages == ()


def test_dialect_is_threaded_to_compiler_default(monkeypatch) -> None:
    seen = {}

    def _compile(plan, dialect=None):
        seen["dialect"] = dialect
        return ("ONLY",)

    monkeypatch.setattr(exec_mod, "compile_plan", _compile)
    ex = _mk_executor(lambda s: None)  # use default dialect
    out = ex.execute(object())

    assert out.success is True
    # Just check that *something* was passed; the exact object is SPARK_SQL
    assert seen["dialect"] is exec_mod.SPARK_SQL


def test_dialect_is_threaded_to_compiler_override(monkeypatch) -> None:
    seen = {}

    def _compile(plan, dialect=None):
        seen["dialect"] = dialect
        return ("ONLY",)

    class _Dialect: ...

    d = _Dialect()

    monkeypatch.setattr(exec_mod, "compile_plan", _compile)
    ex = _mk_executor(lambda s: None, dialect=d)
    out = ex.execute(object())

    assert out.success is True
    assert seen["dialect"] is d


def test_compile_exception_bubbles_up(monkeypatch) -> None:
    def _compile(plan, dialect=None):
        raise RuntimeError("compiler blew up")

    monkeypatch.setattr(exec_mod, "compile_plan", _compile)
    ex = _mk_executor(lambda s: None)

    with pytest.raises(RuntimeError, match="compiler blew up"):
        ex.execute(object())
