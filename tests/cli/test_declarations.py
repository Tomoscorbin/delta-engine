"""Loading one explicit ordered declaration collection."""

import sys
from textwrap import dedent

import pytest

from delta_engine.application import DuplicateTableDefinitionError
from delta_engine.cli.declarations import DeclarationRef, load_declarations
from delta_engine.cli.errors import ConfigError

_TWO_TABLES = """
    from delta_engine.schema import Column, DeltaTable, String

    orders = DeltaTable("dev", "silver", "orders", columns=(Column("id", String()),))
    customers = DeltaTable("dev", "silver", "customers", columns=(Column("id", String()),))
    all_tables = [orders, customers]
"""


def _load(text: str):
    return load_declarations(DeclarationRef.parse(text))


def _dotted_names(tables) -> list[str]:
    return [f"{table.catalog}.{table.schema}.{table.name}" for table in tables]


def test_reference_parses_and_round_trips():
    reference = DeclarationRef.parse("myproject.tables:all_tables")

    assert reference.module_name == "myproject.tables"
    assert reference.attribute == "all_tables"
    assert str(reference) == "myproject.tables:all_tables"


@pytest.mark.parametrize(
    "text",
    [
        "mod",
        "mod:",
        ":attr",
        "mod:attr:extra",
        "./tables.py:all_tables",
        ".relative:all_tables",
        "tables.py/:all_tables",
        "my-project.tables:all_tables",
        "myproject.tables:all tables",
        "myproject.tables:all_tables.first",
    ],
)
def test_malformed_references_are_configuration_errors(text):
    # Given a reference that is not an importable MODULE:ATTRIBUTE pair

    # When parsing the reference
    with pytest.raises(ConfigError) as exc_info:
        DeclarationRef.parse(text)

    # Then the diagnostic identifies the required input shape
    assert "MODULE:ATTRIBUTE" in str(exc_info.value)


def test_non_empty_ordered_sequence_loads_in_declared_order(write_module):
    module = write_module("decl_ordered", _TWO_TABLES)

    tables = _load(f"{module}:all_tables")

    assert _dotted_names(tables) == ["dev.silver.orders", "dev.silver.customers"]


def test_single_table_is_rejected_even_for_one_declaration(write_module):
    module = write_module("decl_single", _TWO_TABLES)

    with pytest.raises(ConfigError, match="one DeltaTable"):
        _load(f"{module}:orders")


def test_empty_sequence_is_a_configuration_error(write_module):
    module = write_module("decl_empty", "all_tables = []\n")

    with pytest.raises(ConfigError, match="empty"):
        _load(f"{module}:all_tables")


def test_unordered_collection_is_rejected(write_module):
    module = write_module(
        "decl_set",
        """
        from delta_engine.schema import Column, DeltaTable, String
        orders = DeltaTable("dev", "silver", "orders", columns=(Column("id", String()),))
        all_tables = {orders}
        """,
    )

    with pytest.raises(ConfigError, match="ordered sequence"):
        _load(f"{module}:all_tables")


def test_mixed_sequence_names_the_invalid_item(write_module):
    module = write_module(
        "decl_mixed",
        """
        from delta_engine.schema import Column, DeltaTable, String
        orders = DeltaTable("dev", "silver", "orders", columns=(Column("id", String()),))
        all_tables = [orders, "oops"]
        """,
    )

    with pytest.raises(ConfigError, match="item 1 is str"):
        _load(f"{module}:all_tables")


def test_wrong_attribute_type_is_a_configuration_error(write_module):
    module = write_module("decl_wrong_type", "all_tables = 42\n")

    with pytest.raises(ConfigError, match="int"):
        _load(f"{module}:all_tables")


def test_duplicate_qualified_names_raise_the_typed_engine_error(write_module):
    module = write_module(
        "decl_duplicates",
        """
        from delta_engine.schema import Column, DeltaTable, String
        first = DeltaTable("dev", "silver", "orders", columns=(Column("id", String()),))
        second = DeltaTable("dev", "silver", "orders", columns=(Column("id", String()),))
        all_tables = [first, second]
        """,
    )

    with pytest.raises(DuplicateTableDefinitionError, match=r"dev\.silver\.orders"):
        _load(f"{module}:all_tables")


def test_missing_module_is_a_configuration_error():
    with pytest.raises(ConfigError, match="no_such_module"):
        _load("no_such_module:tables")


def test_missing_attribute_is_a_configuration_error(write_module):
    module = write_module("decl_missing_attr", _TWO_TABLES)

    with pytest.raises(ConfigError, match="no_such_attr"):
        _load(f"{module}:no_such_attr")


def test_attributes_provided_by_module_getattr_load_normally(write_module):
    # A lazy declaration module (PEP 562 module __getattr__) is ordinary Python.
    module = write_module(
        "decl_module_getattr",
        """
        from delta_engine.schema import Column, DeltaTable, String

        def __getattr__(name):
            if name == "all_tables":
                orders = DeltaTable(
                    "dev", "silver", "orders", columns=(Column("id", String()),)
                )
                return [orders]
            raise AttributeError(name)
        """,
    )

    tables = _load(f"{module}:all_tables")

    assert _dotted_names(tables) == ["dev.silver.orders"]


def test_module_import_exception_propagates_unchanged(write_module):
    module = write_module("decl_raises", 'raise RuntimeError("boom in user code")\n')

    with pytest.raises(RuntimeError, match="boom in user code"):
        _load(f"{module}:tables")


def test_missing_user_dependency_propagates_module_not_found(write_module):
    module = write_module("decl_missing_dep", "import package_that_does_not_exist\n")

    with pytest.raises(ModuleNotFoundError) as excinfo:
        _load(f"{module}:tables")

    assert excinfo.value.name == "package_that_does_not_exist"


def test_loads_from_the_working_directory_without_installation(tmp_path, monkeypatch):
    (tmp_path / "decl_cwd_tables.py").write_text(
        dedent(
            """
            from delta_engine.schema import Column, DeltaTable, String
            orders = DeltaTable("dev", "silver", "orders", columns=(Column("id", String()),))
            all_tables = [orders]
            """
        )
    )
    monkeypatch.chdir(tmp_path)
    # Loading mutates sys.path in place; snapshot it so a failed assertion
    # cannot leak the temporary directory into later tests.
    monkeypatch.setattr(sys, "path", list(sys.path))

    try:
        tables = _load("decl_cwd_tables:all_tables")
        assert _dotted_names(tables) == ["dev.silver.orders"]
    finally:
        sys.modules.pop("decl_cwd_tables", None)


def test_working_directory_takes_precedence_when_already_later_on_path(tmp_path, monkeypatch):
    project = tmp_path / "project"
    shadow = tmp_path / "shadow"
    project.mkdir()
    shadow.mkdir()
    module_name = "decl_path_precedence"
    source = """
        from delta_engine.schema import Column, DeltaTable, String
        orders = DeltaTable(
            "{catalog}", "silver", "orders", columns=(Column("id", String()),)
        )
        all_tables = [orders]
    """
    (project / f"{module_name}.py").write_text(dedent(source.format(catalog="dev")))
    (shadow / f"{module_name}.py").write_text(dedent(source.format(catalog="wrong")))
    monkeypatch.chdir(project)
    monkeypatch.syspath_prepend(str(project))
    monkeypatch.syspath_prepend(str(shadow))

    try:
        tables = _load(f"{module_name}:all_tables")
        assert _dotted_names(tables) == ["dev.silver.orders"]
    finally:
        sys.modules.pop(module_name, None)
