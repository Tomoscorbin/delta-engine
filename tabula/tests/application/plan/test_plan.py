import pytest

import tabula.application.plan.plan as plan_mod


# ---------- Tiny fakes ----------

class FakeSubject:
    """Minimal ChangeTarget stub used by _compute_plan and preview_plan."""
    def __init__(self, observed, desired):
        self.observed = observed
        self.desired = desired


class FakePlan:
    """Minimal ActionPlan stub with just what's needed by helpers."""
    def __init__(self, n_actions: int, *, target: str = "cat.sch.tbl", counts: dict | None = None):
        self._n = n_actions
        self.target = target
        self._counts = counts or {}

    def __bool__(self):
        return self._n > 0

    def __len__(self):
        return self._n

    def count_by_action(self):
        return self._counts


# ---------- _compute_plan ----------

def test_compute_plan_delegates_to_diff(monkeypatch):
    calls = {}

    def fake_diff(observed, desired):
        calls["args"] = (observed, desired)
        return "FAKE_PLAN"

    monkeypatch.setattr(plan_mod, "diff", fake_diff)

    subject = FakeSubject(observed="OBS", desired="DES")
    result = plan_mod._compute_plan(subject)

    assert result == "FAKE_PLAN"
    assert calls["args"] == ("OBS", "DES")


# ---------- _validate_plan ----------

def test_validate_plan_builds_context_and_calls_validator(monkeypatch):
    seen = {}

    class SpyValidator:
        def validate(self, ctx):
            seen["subject"] = ctx.subject
            seen["plan"] = ctx.plan
            seen["target"] = ctx.target  # proxies to plan.target

    validator = SpyValidator()
    fake_plan = FakePlan(2, target="cat.sch.tbl")
    subject = FakeSubject(observed="OBS", desired="DES")

    plan_mod._validate_plan(fake_plan, subject, validator)

    assert seen["subject"] is subject
    assert seen["plan"] is fake_plan
    assert seen["target"] == "cat.sch.tbl"


def test_validate_plan_propagates_validation_error():
    from tabula.application.errors import ValidationError

    class FailingValidator:
        def validate(self, ctx):
            raise ValidationError("CODE", "msg", ctx.target)

    with pytest.raises(ValidationError):
        plan_mod._validate_plan(FakePlan(1), FakeSubject("OBS", "DES"), FailingValidator())


# ---------- _make_preview ----------

def test_make_preview_orders_and_summarises(monkeypatch):
    ordered_sentinel = "ORDERED_PLAN"

    def fake_order(plan):
        return ordered_sentinel

    monkeypatch.setattr(plan_mod, "order_plan", fake_order)

    fake_plan = FakePlan(
        3,
        counts={"AddColumn": 2, "DropColumn": 1},
        target="cat.sch.tbl",
    )
    preview = plan_mod._make_preview(fake_plan)

    assert preview.plan == ordered_sentinel
    assert preview.is_noop is False    # since bool(fake_plan) is True
    assert preview.summary_counts == {"AddColumn": 2, "DropColumn": 1}
    assert preview.total_actions == 3


def test_make_preview_handles_noop(monkeypatch):
    monkeypatch.setattr(plan_mod, "order_plan", lambda p: "ORDERED_NOOP")
    fake_plan = FakePlan(0, counts={})
    preview = plan_mod._make_preview(fake_plan)

    assert preview.plan == "ORDERED_NOOP"
    assert preview.is_noop is True
    assert preview.summary_counts == {}
    assert preview.total_actions == 0


# ---------- preview_plan (end-to-end orchestration) ----------

def test_preview_plan_pipeline(monkeypatch):
    calls = {"diff": None, "validate": 0, "order": None}

    fake_plan = FakePlan(2, counts={"X": 2}, target="cat.sch.tbl")

    def fake_diff(observed, desired):
        calls["diff"] = (observed, desired)
        return fake_plan

    def fake_order(plan):
        calls["order"] = plan
        return "ORDERED"

    class SpyValidator:
        def __init__(self):
            self.ctx = None
        def validate(self, ctx):
            self.ctx = ctx
            calls["validate"] += 1

    validator = SpyValidator()
    subject = FakeSubject(observed="OBS", desired="DES")

    monkeypatch.setattr(plan_mod, "diff", fake_diff)
    monkeypatch.setattr(plan_mod, "order_plan", fake_order)

    preview = plan_mod.preview_plan(subject, validator)

    # diff saw correct args
    assert calls["diff"] == ("OBS", "DES")
    # validator received PlanContext with our plan/subject
    assert validator.ctx is not None
    assert validator.ctx.plan is fake_plan
    assert validator.ctx.subject is subject
    # order received the original plan
    assert calls["order"] is fake_plan

    # preview fields summarise the original plan
    assert preview.plan == "ORDERED"
    assert preview.is_noop is False
    assert preview.summary_counts == {"X": 2}
    assert preview.total_actions == 2
