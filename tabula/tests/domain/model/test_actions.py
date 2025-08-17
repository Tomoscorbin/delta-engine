from tabula.domain.model.full_name import FullName
from tabula.domain.model.column import Column
from tabula.domain.model.data_type import DataType
from tabula.domain.model.table_spec import TableSpec
from tabula.domain.model.actions import CreateTable, AddColumn, DropColumn, ActionPlan

def test_action_strs_are_readable():
    spec = TableSpec(FullName("dev","sales","orders"), (Column("id", DataType("integer"), False),))
    assert "CreateTable(" in str(CreateTable(columns=spec.columns))
    assert "AddColumn(" in str(AddColumn(Column("amount", DataType("decimal",(18,2)), False)))
    assert "DropColumn(" in str(DropColumn("scratch"))

