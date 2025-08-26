import pytest

from delta_engine.application.plan.plan_context import PlanContext
import delta_engine.application.validation as val_mod

# ---- tiny stubs -------------------------------------------------------------


class FakeSubject:
    def __init__(self, is_existing_and_non_empty: bool):
        self.is_existing_and_non_empty = is_existing_and_non_empty


class FakePlan:
    def __init__(self, actions, target: str = "cat.sch.tbl"):
        self.actions = tuple(actions)
        self.target = target


# ---- Rule behavior ----------------------------------------------------------


def test_rule_is_ok_when_table_is_new_or_empty():
    subject = FakeSubject(is_existing_and_non_empty=False)
    plan = FakePlan(actions=())
    ctx = PlanContext(subject=subject, plan=plan)

    rule = val_mod.NoAddColumnOnNonEmptyTable()
    assert rule.fails(ctx) is False  # rule doesn't apply → OK


def test_rule_is_ok_when_non_empty_but_no_addcolumn():
    subject = FakeSubject(is_existing_and_non_empty=True)
    plan = FakePlan(actions=(object(), object()))
    ctx = PlanContext(subject=subject, plan=plan)

    rule = val_mod.NoAddColumnOnNonEmptyTable()
    assert rule.fails(ctx) is False  # no AddColumn present


def test_rule_fails_when_non_empty_and_addcolumn_present(monkeypatch):
    # Make the module-under-test see our fake AddColumn class
    class FakeAddColumn: ...

    monkeypatch.setattr(val_mod, "AddColumn", FakeAddColumn)

    subject = FakeSubject(is_existing_and_non_empty=True)
    plan = FakePlan(actions=(FakeAddColumn(), object()))
    ctx = PlanContext(subject=subject, plan=plan)

    rule = val_mod.NoAddColumnOnNonEmptyTable()
    assert rule.fails(ctx) is True


# ---- Validator behavior -----------------------------------------------------


def test_validator_raises_with_rule_code_message_and_target(monkeypatch):
    # Arrange a plan containing "AddColumn" to trigger the rule
    class FakeAddColumn: ...

    monkeypatch.setattr(val_mod, "AddColumn", FakeAddColumn)

    subject = FakeSubject(is_existing_and_non_empty=True)
    plan = FakePlan(actions=(FakeAddColumn(),), target="cat.sch.tbl")
    ctx = PlanContext(subject=subject, plan=plan)

    validator = val_mod.PlanValidator((val_mod.NoAddColumnOnNonEmptyTable(),))

    with pytest.raises(val_mod.ValidationError) as excinfo:
        validator.validate(ctx)

    err = excinfo.value
    # string format and fields come from your ValidationError implementation
    assert "NO_ADD_ON_NON_EMPTY_TABLE" in str(err)
    assert "AddColumn actions are not allowed on non-empty tables." in str(err)
    assert "cat.sch.tbl" in str(err)


def test_validator_does_not_raise_when_all_rules_pass():
    subject = FakeSubject(is_existing_and_non_empty=False)  # rule not applicable
    plan = FakePlan(actions=(), target="cat.sch.tbl")
    ctx = PlanContext(subject=subject, plan=plan)

    validator = val_mod.PlanValidator((val_mod.NoAddColumnOnNonEmptyTable(),))
    # Should not raise
    validator.validate(ctx)
