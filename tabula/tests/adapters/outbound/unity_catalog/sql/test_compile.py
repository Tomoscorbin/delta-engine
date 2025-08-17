from tabula.adapters.outbound.unity_catalog.sql.compile import compile_plan
from tabula.domain.model.actions import CreateTable, AddColumn, DropColumn, ActionPlan
from tabula.domain.model.table_spec import TableSpec
from tabula.domain.model.table_state import TableState
from tabula.domain.model.qualified_name import FullName
from tabula.domain.model.column import Column
from tabula.domain.model.data_type import DataType

def col(name, t="string", nn=False):
    return Column(name, DataType(t if "(" not in t else "decimal", (18,2)) if t=="decimal(18,2)" else DataType(t)), not nn

def test_compile_create_add_drop():
    fn = FullName("dev","sales","orders")
    spec = TableSpec(fn, (Column("id", DataType("integer"), False),))
    plan = ActionPlan(
        fn,
        (
            CreateTable(columns=spec.columns),               # ← pass columns, not spec
            AddColumn(Column("b", DataType("string"))),
            DropColumn(column_name="old"),                   # ← explicit field name
        ),
    )
    stmts = compile_plan(plan)
    assert stmts[0].startswith("CREATE TABLE dev.sales.orders (id INT)")
    assert stmts[1] == "ALTER TABLE dev.sales.orders ADD COLUMNS (b STRING)"
    assert stmts[2] == "ALTER TABLE dev.sales.orders DROP COLUMN old"
