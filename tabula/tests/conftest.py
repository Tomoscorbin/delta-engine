import pytest
from hypothesis import strategies as st
from tabula.domain.model.qualified_name import QualifiedName
from tabula.domain.model.data_type import DataType
from tabula.domain.model.types import integer, string, decimal
from tabula.domain.model.column import Column
from tabula.domain.model.table import DesiredTable, ObservedTable

# simple factory helpers (readable names)
def qn(cat="cat", sch="sch", name="tbl") -> QualifiedName:
    return QualifiedName(cat, sch, name)

def col(name: str, dt: DataType = None, nullable: bool = True) -> Column:
    return Column(name=name, data_type=dt or integer(), is_nullable=nullable)

def desired(qualified_name: QualifiedName, *columns: Column) -> DesiredTable:
    return DesiredTable(qualified_name=qualified_name, columns=tuple(columns))

def observed(qualified_name: QualifiedName, *columns: Column) -> ObservedTable:
    return ObservedTable(qualified_name=qualified_name, columns=tuple(columns))

# hypothesis strategies
valid_part = st.text(
    alphabet=st.characters(
        blacklist_categories=('Cs',),
        blacklist_characters='.',
    ),
    min_size=1, max_size=20,
).map(lambda s: s.strip() or "x")  # avoid empty after strip

name_cases = st.text(
    alphabet=st.characters(whitelist_categories=('Lu','Ll','Nd','Pc')),
    min_size=1, max_size=15
)

@st.composite
def qname_strat(draw):
    c = draw(valid_part)
    s = draw(valid_part)
    n = draw(valid_part)
    # sprinkle random casing
    return QualifiedName(c.swapcase(), s.upper(), n.lower())

@st.composite
def columns_strat(draw, min_size=0, max_size=8):
    # generate unique normalized names
    base = draw(st.lists(name_cases, min_size=min_size, max_size=max_size, unique_by=lambda x: x.casefold()))
    cols = [Column(name=b, data_type=integer(), is_nullable=draw(st.booleans())) for b in base]
    return cols
