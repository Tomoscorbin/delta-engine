"""Behaviour of module[:attr] declaration loading."""

import sys
from textwrap import dedent

import pytest

from delta_engine.cli.declarations import load_declarations
from delta_engine.cli.errors import ConfigError, DeclarationImportError

_TWO_TABLES = """
    from delta_engine.schema import Column, DeltaTable, String

    orders = DeltaTable("dev", "silver", "orders", columns=(Column("id", String()),))
    customers = DeltaTable("dev", "silver", "customers", columns=(Column("id", String()),))
    alias_of_orders = orders

    all_tables = [orders, customers]
"""


def _dotted_names(tables) -> list[str]:
    return [f"{table.catalog}.{table.schema}.{table.name}" for table in tables]


@pytest.fixture
def write_module(tmp_path, monkeypatch):
    """Write a module into an importable temp dir; clean sys.modules afterwards."""
    monkeypatch.syspath_prepend(str(tmp_path))
    created: list[str] = []

    def _write(module_name: str, source: str) -> str:
        (tmp_path / f"{module_name}.py").write_text(dedent(source))
        created.append(module_name)
        return module_name

    yield _write
    for name in created:
        sys.modules.pop(name, None)


def test_bare_module_collects_tables_in_definition_order_without_duplicates(write_module):
    module = write_module("decl_bare_module", _TWO_TABLES)

    tables = load_declarations([module])

    assert _dotted_names(tables) == ["dev.silver.orders", "dev.silver.customers"]


def test_attribute_form_accepts_an_iterable_of_tables(write_module):
    module = write_module("decl_attr_iterable", _TWO_TABLES)

    tables = load_declarations([f"{module}:all_tables"])

    assert _dotted_names(tables) == ["dev.silver.orders", "dev.silver.customers"]


def test_attribute_form_accepts_a_single_table(write_module):
    module = write_module("decl_attr_single", _TWO_TABLES)

    tables = load_declarations([f"{module}:orders"])

    assert _dotted_names(tables) == ["dev.silver.orders"]


def test_tables_repeated_across_specs_are_loaded_once(write_module):
    module = write_module("decl_repeated", _TWO_TABLES)

    tables = load_declarations([module, f"{module}:orders"])

    assert _dotted_names(tables) == ["dev.silver.orders", "dev.silver.customers"]


def test_missing_module_is_a_config_error(write_module):
    with pytest.raises(ConfigError, match="no_such_module"):
        load_declarations(["no_such_module"])


def test_missing_attribute_is_a_config_error(write_module):
    module = write_module("decl_missing_attr", _TWO_TABLES)

    with pytest.raises(ConfigError, match="no_such_attr"):
        load_declarations([f"{module}:no_such_attr"])


def test_attribute_of_wrong_type_is_a_config_error(write_module):
    module = write_module(
        "decl_wrong_type",
        """
        not_a_table = 42
        """,
    )

    with pytest.raises(ConfigError, match="not_a_table"):
        load_declarations([f"{module}:not_a_table"])


def test_iterable_containing_a_non_table_is_a_config_error(write_module):
    module = write_module(
        "decl_mixed_iterable",
        """
        from delta_engine.schema import Column, DeltaTable, String

        orders = DeltaTable("dev", "silver", "orders", columns=(Column("id", String()),))
        all_tables = [orders, "oops"]
        """,
    )

    with pytest.raises(ConfigError, match="all_tables"):
        load_declarations([f"{module}:all_tables"])


def test_module_with_no_tables_is_a_config_error(write_module):
    module = write_module("decl_empty", "x = 1\n")

    with pytest.raises(ConfigError, match="no DeltaTable declarations"):
        load_declarations([module])


def test_malformed_spec_is_a_config_error(write_module):
    with pytest.raises(ConfigError, match="spec"):
        load_declarations(["mod:attr:extra"])


def test_module_raising_on_import_is_the_users_bug(write_module):
    module = write_module(
        "decl_raises",
        """
        raise RuntimeError("boom in user code")
        """,
    )

    with pytest.raises(DeclarationImportError) as excinfo:
        load_declarations([module])

    assert excinfo.value.module_name == module
    assert isinstance(excinfo.value.__cause__, RuntimeError)


def test_module_importing_a_missing_dependency_is_the_users_bug(write_module):
    module = write_module(
        "decl_missing_dep",
        """
        import package_that_does_not_exist
        """,
    )

    with pytest.raises(DeclarationImportError):
        load_declarations([module])


def test_loads_from_the_working_directory_without_installation(tmp_path, monkeypatch):
    # Given a declarations module in the current working directory only
    (tmp_path / "decl_cwd_tables.py").write_text(
        dedent(
            """
            from delta_engine.schema import Column, DeltaTable, String

            orders = DeltaTable("dev", "silver", "orders", columns=(Column("id", String()),))
            """
        )
    )
    monkeypatch.chdir(tmp_path)

    # When loading without the directory on sys.path
    tables = load_declarations(["decl_cwd_tables"])

    # Then the CLI found it by prepending the working directory
    assert _dotted_names(tables) == ["dev.silver.orders"]
    sys.modules.pop("decl_cwd_tables", None)
    sys.path.remove(str(tmp_path))
