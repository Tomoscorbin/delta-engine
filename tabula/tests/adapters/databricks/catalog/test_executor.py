from __future__ import annotations

import pytest

from tabula.adapters.databricks.catalog import executor as exec_mod
from tabula.domain.plan.actions import ActionPlan


# --- tiny plan double --------------------------------------------------------

class _Plan(ActionPlan):
    def __init__(self):
        # executor never touches internals; compile_plan is monkeypatched
        pass


# --- helpers -----------------------------------------------------------------

def _exec(run_sql, **kw) -> exec_mod.UCExecutor:
    return exec_mod.UCExecutor(run_sql=run_sql, **kw)


# --- tests -------------------------------------------------------------------

def test_dry_run(monkeypatch) -> None:
    monkeypatch.setattr(exec_mod, "compile_plan", lambda plan, dialect=None: ("S1", "S2"))

    ran: list[str] = []
    ex = _exec(lambda s: ran.append(s), dry_run=True)

    out = ex.execute(_Plan())

    assert out.success is True
    assert out.executed_count == 0
    assert out.executed_sql == ("S1", "S2")  # echoes would-be SQL
    assert out.messages == ("DRY RUN 1: S1", "DRY RUN 2: S2")
    assert ran == []  # nothing executed


def test_stop_on_first_error(monkeypatch) -> None:
    monkeypatch.setattr(exec_mod, "compile_plan", lambda plan, dialect=None: ("A", "B", "C"))

    def run_sql(s: str) -> None:
        if s == "B":
            raise RuntimeError("boom")

    ex = _exec(run_sql, on_error="stop")
    out = ex.execute(_Plan())

    assert out.success is False
    assert out.executed_count == 1
    assert out.executed_sql == ("A",)  # only A succeeded
    assert out.messages[0] == "OK 1"
    assert out.messages[1].startswith("ERROR 2:")


def test_continue_runs_remaining(monkeypatch) -> None:
    monkeypatch.setattr(exec_mod, "compile_plan", lambda plan, dialect=None: ("A", "B", "C"))

    def run_sql(s: str) -> None:
        if s == "B":
            raise ValueError("bad")

    ex = _exec(run_sql, on_error="continue")
    out = ex.execute(_Plan())

    assert out.success is False
    assert out.executed_count == 2  # A and C
    assert out.executed_sql == ("A", "C")
    assert out.messages == ("OK 1", "ERROR 2: ValueError: bad", "OK 3")


def test_dialect_is_threaded_to_compiler(monkeypatch) -> None:
    seen = {}

    def _compile(plan, dialect=None):
        seen["dialect"] = dialect
        return ("ONLY",)

    monkeypatch.setattr(exec_mod, "compile_plan", _compile)

    class _Dialect: pass

    ex = _exec(lambda s: None, dialect=_Dialect())
    out = ex.execute(_Plan())

    assert out.success is True
    assert isinstance(seen["dialect"], _Dialect)
