import pytest

from delta_engine.adapters.databricks.sql import compile
from delta_engine.adapters.databricks.sql.compile import (
    _column_def,
    _compile_action,
    _set_properties,
    _set_table_comment,
    compile_plan,
)
from delta_engine.domain.model.column import Column
from delta_engine.domain.model.data_type import Integer, String
from delta_engine.domain.model.table import DesiredTable
from delta_engine.domain.plan.actions import (
    ActionPlan,
    AddColumn,
    CreateTable,
    DropColumn,
    SetColumnComment,
    SetProperty,
    SetTableComment,
    UnsetProperty,
)
from tests.factories import make_qualified_name

# --- helpers ------------------------------------------------------------------


@pytest.fixture(autouse=True)
def stub_sql_helpers(monkeypatch: pytest.MonkeyPatch) -> None:
    """
    Stabilise quoting and type rendering so tests validate compiler behaviour,
    not adapter details.
    """
    monkeypatch.setattr(compile, "quote_identifier", lambda s: f'"{s}"')
    monkeypatch.setattr(compile, "quote_literal", lambda s: f"'{s}'")
    monkeypatch.setattr(compile, "quote_qualified_name", lambda _qn: "T")

    def fake_type(dt) -> str:
        if isinstance(dt, Integer):
            return "INT"
        if isinstance(dt, String):
            return "STRING"
        return dt.__class__.__name__.upper()

    monkeypatch.setattr(compile, "sql_type_for_data_type", fake_type)


def make_table(
    columns, comment: str = "", properties: dict[str, str] | None = None
) -> DesiredTable:
    qn = make_qualified_name("dev", "silver", "people")
    return DesiredTable(qn, columns, comment=comment, properties=properties or {})


# --- compile_plan: emits one statement per action, uses quoted table name -----


def test_compile_plan_emits_statement_per_action_and_uses_quoted_table_name() -> None:
    table = make_table((Column("id", Integer()),))
    actions = (
        CreateTable(table),
        AddColumn(Column("age", Integer())),
        DropColumn("nickname"),
        SetProperty("owner", "data-eng"),
        UnsetProperty("ttl"),
        SetColumnComment("age", "years"),
        SetTableComment("table-comment"),
    )
    plan = ActionPlan(table.qualified_name, actions)

    statements = compile_plan(plan)

    assert len(statements) == len(actions)
    # All statements should target the same quoted table name "T"
    assert all(
        s.startswith("CREATE TABLE") or f" {'T'} " in f" {s} " or s.startswith("COMMENT ON TABLE T")
        for s in statements
    )


# --- per-action compilation (focused, exact expectations) ---------------------


def test_compile_create_table_with_columns_comment_and_properties() -> None:
    table = make_table(
        (
            Column("id", Integer()),
            Column("age", Integer(), is_nullable=False),
        ),
        comment="team-owned",
        properties={"quality": "gold", "owner": "asda"},
    )
    plan = ActionPlan(table.qualified_name, (CreateTable(table),))

    (sql,) = compile_plan(plan)

    # Structure + critical fragments; order of properties not asserted.
    assert sql.startswith('CREATE TABLE IF NOT EXISTS T ("id" INT, "age" INT NOT NULL)')
    assert "COMMENT 'team-owned'" in sql
    assert "TBLPROPERTIES (" in sql
    assert "'owner'='asda'" in sql
    assert "'quality'='gold'" in sql


def test_compile_create_table_omits_tblproperties_when_empty() -> None:
    table = make_table((Column("id", Integer()),), comment="c", properties={})
    (sql,) = compile_plan(ActionPlan(table.qualified_name, (CreateTable(table),)))
    assert "TBLPROPERTIES" not in sql


def test_compile_add_column() -> None:
    table = make_table((Column("id", Integer()),))
    (sql,) = compile_plan(ActionPlan(table.qualified_name, (AddColumn(Column("age", Integer())),)))
    assert sql == 'ALTER TABLE T ADD COLUMN "age" INT'


def test_compile_drop_column() -> None:
    table = make_table((Column("id", Integer()),))
    (sql,) = compile_plan(ActionPlan(table.qualified_name, (DropColumn("nickname"),)))
    assert sql == 'ALTER TABLE T DROP COLUMN "nickname"'


def test_compile_set_property() -> None:
    table = make_table((Column("id", Integer()),))
    (sql,) = compile_plan(ActionPlan(table.qualified_name, (SetProperty("owner", "asda"),)))
    assert sql == "ALTER TABLE T SET TBLPROPERTIES ('owner'='asda')"


def test_compile_unset_property() -> None:
    table = make_table((Column("id", Integer()),))
    (sql,) = compile_plan(ActionPlan(table.qualified_name, (UnsetProperty("ttl"),)))
    assert sql == "ALTER TABLE T UNSET TBLPROPERTIES ('ttl')"


def test_compile_set_column_comment() -> None:
    table = make_table((Column("id", Integer()),))
    (sql,) = compile_plan(ActionPlan(table.qualified_name, (SetColumnComment("id", "pk"),)))
    assert sql == "ALTER TABLE T ALTER COLUMN \"id\" COMMENT 'pk'"


def test_compile_set_table_comment() -> None:
    table = make_table((Column("id", Integer()),))
    (sql,) = compile_plan(ActionPlan(table.qualified_name, (SetTableComment("hello"),)))
    assert sql == "COMMENT ON TABLE T IS 'hello'"


# --- helper behaviour ---------------------------------------------------------


def test_column_def_renders_not_null_only_when_false() -> None:
    assert _column_def(Column("age", Integer())) == '"age" INT'
    assert _column_def(Column("age", Integer(), is_nullable=False)) == '"age" INT NOT NULL'


def test_set_table_comment_wraps_literal() -> None:
    assert _set_table_comment("hi") == "COMMENT 'hi'"


def test_set_properties_handles_none_and_empty() -> None:
    assert _set_properties({}) == ""
    assert _set_properties(None) == ""  # type: ignore[arg-type]


# --- sad path: unknown action type -------------------------------------------


def test_compile_action_raises_for_unknown_action_type() -> None:
    class UnknownAction:
        pass

    with pytest.raises(NotImplementedError) as excinfo:
        _compile_action(UnknownAction(), "T")  # type: ignore[arg-type]
    assert "No SQL compiler for action UnknownAction" in str(excinfo.value)
