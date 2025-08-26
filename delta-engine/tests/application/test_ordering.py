import types

import pytest

import delta_engine.application.ordering as mod

# ----------------------------
# target_name
# ----------------------------

def test_target_name_returns_str_of_target_when_present():
    class FakeQualifiedName:
        def __init__(self, text: str) -> None:
            self.text = text
        def __str__(self) -> str:
            return self.text

    class FakeAction:
        pass

    action = FakeAction()
    action.target = FakeQualifiedName("cat.schema.table_a")  # type: ignore[attr-defined]
    assert mod.target_name(action) == "cat.schema.table_a"


def test_target_name_returns_empty_string_when_no_target():
    class FakeAction:  # no target attribute
        pass
    assert mod.target_name(FakeAction()) == ""


# ----------------------------
# subject_name
# ----------------------------

def test_subject_name_uses_add_column_column_name(monkeypatch):
    class FakeAddColumn:
        pass
    monkeypatch.setattr(mod, "AddColumn", FakeAddColumn)

    column = types.SimpleNamespace(name="new_col")
    action = FakeAddColumn()
    action.column = column  # type: ignore[attr-defined]
    action.name = "should_be_ignored"  # type: ignore[attr-defined]

    assert mod.subject_name(action) == "new_col"


def test_subject_name_falls_back_to_name_for_non_addcolumn():
    class NotAddColumn:
        pass
    action = NotAddColumn()
    action.name = "plain_name"  # type: ignore[attr-defined]
    assert mod.subject_name(action) == "plain_name"


def test_subject_name_empty_when_no_column_or_name():
    class Empty:
        pass
    assert mod.subject_name(Empty()) == ""


def test_phase_order_and_rank_alignment():
    # PHASE_ORDER is the source of truth; PHASE_RANK must enumerate it 0..n-1
    assert isinstance(mod.PHASE_ORDER, tuple)
    expected_rank = {cls: i for i, cls in enumerate(mod.PHASE_ORDER)}
    assert mod.PHASE_RANK == expected_rank

def test_action_sort_key_raises_for_unknown_action_type():
    class Unknown:
        pass
    with pytest.raises(ValueError) as err:
        mod.action_sort_key(Unknown())
    # Helpful message mentions the missing type and PHASE_ORDER
    assert "Place Unknown in PHASE_ORDER." in str(err.value)



def test_action_sort_key_orders_by_rank_then_target_then_subject(monkeypatch):
    class PhaseCreate: pass
    class PhaseDrop: pass
    class PhaseAdd: pass  # acts as AddColumn

    monkeypatch.setattr(mod, "AddColumn", PhaseAdd)
    monkeypatch.setattr(mod, "PHASE_ORDER", (PhaseCreate, PhaseDrop, PhaseAdd), raising=False)
    monkeypatch.setattr(mod, "PHASE_RANK", {PhaseCreate: 0, PhaseDrop: 1, PhaseAdd: 2}, raising=False)

    create_t1 = PhaseCreate(); create_t1.target = "t1"; create_t1.name = "zzz"
    create_t2 = PhaseCreate(); create_t2.target = "t2"; create_t2.name = "zzz"
    drop_t1   = PhaseDrop();   drop_t1.target   = "t1"; drop_t1.name   = "m"
    add_t2_a  = PhaseAdd();    add_t2_a.target  = "t2"; add_t2_a.column = types.SimpleNamespace(name="a")
    add_t2_b  = PhaseAdd();    add_t2_b.target  = "t2"; add_t2_b.column = types.SimpleNamespace(name="b")

    ordered = sorted([add_t2_b, drop_t1, create_t2, add_t2_a, create_t1], key=mod.action_sort_key)
    assert ordered == [create_t1, create_t2, drop_t1, add_t2_a, add_t2_b]
    assert mod.action_sort_key(add_t2_a) == (2, "t2", "a")
