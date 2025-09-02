import re

import pytest

from delta_engine.adapters.databricks.sql import compile
from delta_engine.adapters.databricks.sql.compile import (
    _column_definition,
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

# ------------------------------ helpers ---------------------------------------


@pytest.fixture(autouse=True)
def stub_sql_helpers(monkeypatch: pytest.MonkeyPatch) -> None:
    """
    Stabilise quoting and type rendering so tests validate compiler behaviour,
    not adapter specifics.
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


def normalize_sql(sql: str) -> str:
    """Collapse whitespace; keep case as-is."""
    return " ".join(sql.split())


def assert_add_column_sql(
    sql: str, table_name: str, column_name: str, data_type: str, comment: str = ""
) -> None:
    """
    Accept:
      - ADD COLUMN or ADD COLUMNS
      - With or without surrounding parentheses
      - Optional COMMENT when empty comments are allowed
    Asserts that NOT NULL is NOT present (we tighten separately).
    """
    ident = re.escape(compile.quote_identifier(column_name))
    lit = re.escape(compile.quote_literal(comment))
    # Column def with optional COMMENT
    col_def = rf"{ident}\s+{data_type}(?:\s+COMMENT\s+{lit})?"
    pattern = rf"""^ALTER\s+TABLE\s+{table_name}\s+
                   ADD\s+COLUMNS?    # COLUMN or COLUMNS
                   \s*
                   (?:\(\s*{col_def}\s*\)|{col_def})$
                """
    assert re.match(pattern, normalize_sql(sql), flags=re.IGNORECASE | re.VERBOSE), (
        f"Bad ADD COLUMN SQL: {sql}"
    )
    assert "NOT NULL" not in sql.upper(), "ADD COLUMN must not include NOT NULL"


def assert_contains_props(sql: str, props: dict[str, str]) -> None:
    normalized = normalize_sql(sql)
    assert "TBLPROPERTIES (" in normalized
    for k, v in props.items():
        piece = f"{compile.quote_literal(k)}={compile.quote_literal(v)}"
        assert piece in normalized


# ---------------- compile_plan contract ---------------------------------------


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
    # All statements should target "T" (our stubbed quoted table name)
    assert all(
        s.startswith("CREATE TABLE") or " T " in f" {s} " or s.startswith("COMMENT ON TABLE T")
        for s in statements
    )


# ---------------- per-action compilation (behavioural) ------------------------


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
    text = normalize_sql(sql)

    # Columns present and NOT NULL respected on CREATE
    assert text.startswith('CREATE TABLE IF NOT EXISTS T ("id" INT, "age" INT NOT NULL)')
    # Table comment present
    assert "COMMENT 'team-owned'" in text
    # Properties present (content asserted via helper)
    assert_contains_props(sql, {"owner": "asda", "quality": "gold"})


def test_compile_create_table_omits_tblproperties_when_empty() -> None:
    table = make_table((Column("id", Integer()),), comment="c", properties={})
    (sql,) = compile_plan(ActionPlan(table.qualified_name, (CreateTable(table),)))
    assert "TBLPROPERTIES" not in normalize_sql(sql)


def test_compile_add_column_minimal_and_nullable() -> None:
    table = make_table((Column("id", Integer()),))
    (sql,) = compile_plan(ActionPlan(table.qualified_name, (AddColumn(Column("age", Integer())),)))
    # Minimal DDL, no NOT NULL
    assert_add_column_sql(sql, table_name="T", column_name="age", data_type="INT", comment="")


def test_compile_drop_column() -> None:
    table = make_table((Column("id", Integer()),))
    (sql,) = compile_plan(ActionPlan(table.qualified_name, (DropColumn("nickname"),)))
    assert normalize_sql(sql) == 'ALTER TABLE T DROP COLUMN "nickname"'


def test_compile_set_property_and_unset_property() -> None:
    table = make_table((Column("id", Integer()),))
    (set_sql,) = compile_plan(ActionPlan(table.qualified_name, (SetProperty("owner", "asda"),)))
    (unset_sql,) = compile_plan(ActionPlan(table.qualified_name, (UnsetProperty("ttl"),)))
    assert normalize_sql(set_sql) == "ALTER TABLE T SET TBLPROPERTIES ('owner'='asda')"
    assert normalize_sql(unset_sql) == "ALTER TABLE T UNSET TBLPROPERTIES ('ttl')"


def test_compile_set_column_comment() -> None:
    table = make_table((Column("id", Integer()),))
    (sql,) = compile_plan(ActionPlan(table.qualified_name, (SetColumnComment("id", "pk"),)))
    assert normalize_sql(sql) == "ALTER TABLE T ALTER COLUMN \"id\" COMMENT 'pk'"


def test_compile_set_table_comment() -> None:
    table = make_table((Column("id", Integer()),))
    (sql,) = compile_plan(ActionPlan(table.qualified_name, (SetTableComment("hello"),)))
    assert normalize_sql(sql) == "COMMENT ON TABLE T IS 'hello'"


# ---------------- helper behaviour -------------------------------------------


def test_column_definition_renders_not_null_only_when_false() -> None:
    assert _column_definition(Column("age", Integer())) == '"age" INT'
    assert _column_definition(Column("age", Integer(), is_nullable=False)) == '"age" INT NOT NULL'


def test_set_table_comment_wraps_literal() -> None:
    assert _set_table_comment("hi") == "COMMENT 'hi'"


def test_set_properties_handles_none_and_empty() -> None:
    assert _set_properties({}) == ""
    assert _set_properties(None) == ""  # type: ignore[arg-type]


# ---------------- sad path ----------------------------------------------------


def test_compile_action_raises_for_unknown_action_type() -> None:
    class UnknownAction:
        pass

    with pytest.raises(NotImplementedError) as excinfo:
        _compile_action(UnknownAction(), "T")  # type: ignore[arg-type]
    assert "No SQL compiler for action UnknownAction" in str(excinfo.value)
