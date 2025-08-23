import tabula.application.execute as exec_mod

# ---------- fakes / stubs ----------


class FakePlan:
    def __init__(self, target="cat.sch.tbl"):
        self.target = target


class FakePreview:
    def __init__(self, plan, is_noop: bool):
        self.plan = plan
        self.is_noop = is_noop

    def __bool__(self):
        return not self.is_noop


class SuccessOutcome:
    def __init__(self, messages=("ok",), executed_count=1):
        self.messages = tuple(messages)
        self.executed_count = executed_count

    def __bool__(self):
        return True


class FailureOutcome:
    def __init__(self, messages=("boom",), executed_count=0):
        self.messages = tuple(messages)
        self.executed_count = executed_count

    def __bool__(self):
        return False


class SpyExecutor:
    def __init__(self, outcome):
        self.outcome = outcome
        self.calls = []

    def execute(self, plan):
        self.calls.append(plan)
        return self.outcome


# ---------- execute_plan ----------


def test_execute_plan_success_returns_execution_result():
    plan = FakePlan(target="cat.sch.tbl")
    preview = FakePreview(plan=plan, is_noop=False)
    executor = SpyExecutor(SuccessOutcome(messages=("m1", "m2"), executed_count=3))

    result = exec_mod.execute_plan(preview, executor)

    assert result.plan is plan
    assert result.messages == ("m1", "m2")
    assert result.executed_count == 3
    assert executor.calls == [plan]




# ---------- plan_then_execute ----------


def test_plan_then_execute_noop_short_circuits(monkeypatch):
    # load_change_target -> subject (ignored), preview_plan -> noop preview
    monkeypatch.setattr(exec_mod, "load_change_target", lambda r, d: object())

    plan = FakePlan("cat.sch.tbl")
    monkeypatch.setattr(
        exec_mod, "preview_plan", lambda subject, validator: FakePreview(plan, True)
    )

    # Executor should not be called
    class NeverExecutor:
        def execute(self, plan):
            raise AssertionError("should not execute on noop")

    result = exec_mod.plan_then_execute(
        desired_table=object(), reader=object(), executor=NeverExecutor()
    )

    assert result.plan is plan
    assert result.messages == ("noop",)
    assert result.executed_count == 0


def test_plan_then_execute_runs_execute_when_not_noop(monkeypatch):
    calls = {"planned": False, "executed": False}

    monkeypatch.setattr(exec_mod, "load_change_target", lambda r, d: "SUBJECT")

    plan = FakePlan("cat.sch.tbl")

    def fake_preview_plan(subject, validator):
        calls["planned"] = subject == "SUBJECT"
        return FakePreview(plan, is_noop=False)

    monkeypatch.setattr(exec_mod, "preview_plan", fake_preview_plan)

    class Exec(SpyExecutor):
        def __init__(self):
            super().__init__(SuccessOutcome(messages=("done",), executed_count=2))

        def execute(self, plan_arg):
            calls["executed"] = plan_arg is plan
            return super().execute(plan_arg)

    executor = Exec()
    result = exec_mod.plan_then_execute(desired_table=object(), reader=object(), executor=executor)

    assert calls["planned"] is True
    assert calls["executed"] is True
    assert result.plan is plan
    assert result.messages == ("done",)
    assert result.executed_count == 2
