from dataclasses import dataclass
import types

import delta_engine.application.plan as mod

# --- compute_plan -------------------------------------------------------------

def test_compute_plan_sorts_actions_and_returns_new_plan(monkeypatch):
    @dataclass(frozen=True)
    class FakePlan:
        actions: tuple[object, ...]
        keep: str = "unchanged"

    # Unsorted by our key: 3,1,2
    a3 = types.SimpleNamespace(k=3)
    a1 = types.SimpleNamespace(k=1)
    a2 = types.SimpleNamespace(k=2)
    original = FakePlan(actions=(a3, a1, a2))

    # diff_tables returns our unsorted plan
    monkeypatch.setattr(mod, "diff_tables", lambda *, desired, observed: original)
    # action_sort_key uses the .k attribute
    monkeypatch.setattr(mod, "action_sort_key", lambda a: a.k)

    result = mod.compute_plan(observed="OBS", desired="DES")

    # New instance of the same "shape", actions sorted and tupled
    assert isinstance(result, FakePlan)
    assert result is not original
    assert result.actions == (a1, a2, a3)
    # Original plan not mutated
    assert original.actions == (a3, a1, a2)
    # Other fields preserved through dataclasses.replace
    assert result.keep == "unchanged"


# --- plan_summary_counts ------------------------------------------------------

def test_plan_summary_counts_prefers_action_name_and_falls_back_to_class_name():
    @dataclass(frozen=True)
    class FakePlan:
        actions: tuple[object, ...]

    class Fallback:  # no action_name attribute → falls back to class name
        pass

    actions = (
        types.SimpleNamespace(action_name="CreateTable"),
        types.SimpleNamespace(action_name="CreateTable"),
        types.SimpleNamespace(action_name="AddColumn"),
        Fallback(),
    )

    counts = mod.plan_summary_counts(FakePlan(actions=actions))

    assert counts == {"CreateTable": 2, "AddColumn": 1, "Fallback": 1}


# --- make_plan_context --------------------------------------------------------

def test_make_plan_context_wires_fields_and_calls_compute_plan_keyword_only(monkeypatch):
    """This will FAIL until make_plan_context calls compute_plan with keywords."""
    @dataclass(frozen=True)
    class FakePlan:
        actions: tuple[object, ...] = ()

    calls: list[tuple[object, object]] = []

    def fake_compute_plan(observed, desired):
        calls.append((observed, desired))
        return FakePlan()

    monkeypatch.setattr(mod, "compute_plan", fake_compute_plan)

    qn = object()
    desired = types.SimpleNamespace(qualified_name=qn)
    observed = object()

    ctx = mod.make_plan_context(observed=observed, desired=desired)

    # compute_plan got called once with keyword args in correct order
    assert calls == [(observed, desired)]

    # PlanContext fields populated correctly
    assert ctx.qualified_name is qn
    assert ctx.desired is desired
    assert ctx.observed is observed
    assert isinstance(ctx.plan, FakePlan)
