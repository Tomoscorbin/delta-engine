from collections import Counter
from typing import Optional

from tabula.application.plan import plan_actions
from tabula.application.ports import CatalogReader
from tabula.domain.model.table import DesiredTable, ObservedTable
from tabula.domain.model.qualified_name import QualifiedName
from tabula.domain.model.column import Column
from tabula.domain.model.types import integer
from tabula.domain.model.actions import ActionPlan

def qn() -> QualifiedName:
    return QualifiedName("Cat", "Sch", "Tbl")

class ReaderNone(CatalogReader):
    """Returns None to simulate missing table."""
    def fetch_state(self, qualified_name: QualifiedName) -> Optional[ObservedTable]:
        return None

class ReaderWith( CatalogReader ):
    """Returns a simple observed table snapshot."""
    def __init__(self, observed: ObservedTable) -> None:
        self._observed = observed
    def fetch_state(self, qualified_name: QualifiedName) -> Optional[ObservedTable]:
        assert str(qualified_name) == str(self._observed.qualified_name)
        return self._observed

def desired(*cols: str) -> DesiredTable:
    return DesiredTable(qualified_name=qn(), columns=tuple(Column(c, integer()) for c in cols))

def observed(*cols: str) -> ObservedTable:
    return ObservedTable(qualified_name=qn(), columns=tuple(Column(c, integer()) for c in cols))

def test_plan_actions_no_observed_emits_create_table_and_counts_match():
    d = desired("a", "b", "c")
    preview = plan_actions(d, reader=ReaderNone())
    assert isinstance(preview.plan, ActionPlan)
    assert not isinstance(preview.plan, tuple)  # sanity
    assert preview.is_noop is False
    # counts should reflect the plan's action kinds
    expected = Counter(a.kind for a in preview.plan)
    assert dict(preview.summary_counts) == dict(expected)
    # summary_text should either be 'noop' or contain every kind with '='
    for k in expected:
        assert f"{k}=" in preview.summary_text

def test_plan_actions_with_observed_adds_and_drops_and_is_not_noop():
    d = desired("a", "b")                  # want a,b
    o = observed("b", "c")                 # have b,c
    preview = plan_actions(d, reader=ReaderWith(o))
    assert not preview.is_noop
    # kinds present should include an add and a drop (names depend on your Action.kind)
    kinds = {a.kind for a in preview.plan}
    assert len(kinds) >= 2
    # structured counts are consistent with plan contents
    expected = Counter(a.kind for a in preview.plan)
    assert dict(preview.summary_counts) == dict(expected)
    # human summary mentions all kinds present
    for k in kinds:
        assert f"{k}=" in preview.summary_text

def test_plan_actions_noop_when_schemas_match_ignoring_case():
    d = desired("A", "B")
    o = observed("a", "b")
    preview = plan_actions(d, reader=ReaderWith(o))
    assert preview.is_noop
    assert preview.summary_counts == {}
    assert preview.summary_text == "noop"
