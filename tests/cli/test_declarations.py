"""Behaviour of explicit ``MODULE:ATTRIBUTE`` declaration loading."""

import sys
from textwrap import dedent

import pytest

from delta_engine.cli.declarations import load_declarations
from delta_engine.cli.errors import ConfigError

_TWO_TABLES = """
    from delta_engine.schema import Column, DeltaTable, String

    orders = DeltaTable("dev", "silver", "orders", columns=(Column("id", String()),))
    customers = DeltaTable("dev", "silver", "customers", columns=(Column("id", String()),))
    alias_of_orders = orders

    all_tables = [orders, customers]
"""


def _dotted_names(tables) -> list[str]:
    return [f"{table.catalog}.{table.schema}.{table.name}" for table in tables]


def test_bare_module_is_rejected(write_module):
    module = write_module("decl_bare_module", _TWO_TABLES)

    with pytest.raises(ConfigError, match="MODULE:ATTRIBUTE"):
        load_declarations([module])


def test_attribute_form_accepts_an_iterable_of_tables(write_module):
    module = write_module("decl_attr_iterable", _TWO_TABLES)

    tables = load_declarations([f"{module}:all_tables"])

    assert _dotted_names(tables) == ["dev.silver.orders", "dev.silver.customers"]


def test_attribute_form_accepts_a_single_table(write_module):
    module = write_module("decl_attr_single", _TWO_TABLES)

    tables = load_declarations([f"{module}:orders"])

    assert _dotted_names(tables) == ["dev.silver.orders"]


def test_empty_attribute_is_a_config_error(write_module):
    module = write_module("decl_empty", "all_tables = []\n")

    with pytest.raises(ConfigError, match="empty"):
        load_declarations([f"{module}:all_tables"])


def test_same_object_repeated_within_iterable_reports_both_items(write_module):
    module = write_module(
        "decl_same_object_within",
        """
        from delta_engine.schema import Column, DeltaTable, String
        orders = DeltaTable("dev", "silver", "orders", columns=(Column("id", String()),))
        all_tables = [orders, orders]
        """,
    )

    with pytest.raises(ConfigError) as excinfo:
        load_declarations([f"{module}:all_tables"])

    message = str(excinfo.value)
    assert "dev.silver.orders" in message
    assert f"{module}:all_tables[0]" in message
    assert f"{module}:all_tables[1]" in message


def test_same_name_objects_within_iterable_are_not_deduplicated(write_module):
    module = write_module(
        "decl_same_name_within",
        """
        from delta_engine.schema import Column, DeltaTable, String
        orders_a = DeltaTable("dev", "silver", "orders", columns=(Column("id", String()),))
        orders_b = DeltaTable("dev", "silver", "orders", columns=(Column("id", String()),))
        all_tables = [orders_a, orders_b]
        """,
    )

    with pytest.raises(ConfigError, match="duplicate table definition"):
        load_declarations([f"{module}:all_tables"])


def test_same_object_across_specs_reports_both_arguments(write_module):
    module = write_module("decl_same_object_across", _TWO_TABLES)

    with pytest.raises(ConfigError) as excinfo:
        load_declarations([f"{module}:orders", f"{module}:alias_of_orders"])

    message = str(excinfo.value)
    assert f"'{module}:orders' (argument 1)" in message
    assert f"'{module}:alias_of_orders' (argument 2)" in message


def test_same_name_objects_across_specs_are_not_deduplicated(write_module):
    module = write_module(
        "decl_same_name_across",
        """
        from delta_engine.schema import Column, DeltaTable, String
        orders_a = DeltaTable("dev", "silver", "orders", columns=(Column("id", String()),))
        orders_b = DeltaTable("dev", "silver", "orders", columns=(Column("id", String()),))
        """,
    )

    with pytest.raises(ConfigError) as excinfo:
        load_declarations([f"{module}:orders_a", f"{module}:orders_b"])

    message = str(excinfo.value)
    assert f"{module}:orders_a" in message
    assert f"{module}:orders_b" in message


def test_missing_module_is_a_config_error():
    with pytest.raises(ConfigError, match="no_such_module"):
        load_declarations(["no_such_module:tables"])


def test_missing_attribute_is_a_config_error(write_module):
    module = write_module("decl_missing_attr", _TWO_TABLES)

    with pytest.raises(ConfigError, match="no_such_attr"):
        load_declarations([f"{module}:no_such_attr"])


def test_attribute_of_wrong_type_is_a_config_error(write_module):
    module = write_module("decl_wrong_type", "not_a_table = 42\n")

    with pytest.raises(ConfigError, match="not_a_table"):
        load_declarations([f"{module}:not_a_table"])


def test_an_unordered_container_is_rejected(write_module):
    # A set would make item indices in duplicate-origin messages depend on
    # hash order, so only sequences are accepted.
    module = write_module(
        "decl_set",
        """
        from delta_engine.schema import Column, DeltaTable, String
        orders = DeltaTable("dev", "silver", "orders", columns=(Column("id", String()),))
        all_tables = {orders}
        """,
    )

    with pytest.raises(ConfigError, match="sequence"):
        load_declarations([f"{module}:all_tables"])


def test_iterable_containing_a_non_table_names_the_item(write_module):
    module = write_module(
        "decl_mixed_iterable",
        """
        from delta_engine.schema import Column, DeltaTable, String
        orders = DeltaTable("dev", "silver", "orders", columns=(Column("id", String()),))
        all_tables = [orders, "oops"]
        """,
    )

    with pytest.raises(ConfigError, match="item 1 is str"):
        load_declarations([f"{module}:all_tables"])


@pytest.mark.parametrize("spec", ["mod", "mod:", ":attr", "mod:attr:extra"])
def test_malformed_spec_is_a_config_error(spec):
    with pytest.raises(ConfigError, match="MODULE:ATTRIBUTE"):
        load_declarations([spec])


def test_no_specs_is_a_config_error():
    with pytest.raises(ConfigError, match="at least one"):
        load_declarations([])


def test_module_raising_on_import_propagates_the_original_exception(write_module):
    module = write_module("decl_raises", 'raise RuntimeError("boom in user code")\n')

    with pytest.raises(RuntimeError, match="boom in user code"):
        load_declarations([f"{module}:tables"])


def test_module_importing_a_missing_dependency_propagates_module_not_found(write_module):
    module = write_module("decl_missing_dep", "import package_that_does_not_exist\n")

    with pytest.raises(ModuleNotFoundError) as excinfo:
        load_declarations([f"{module}:tables"])

    assert excinfo.value.name == "package_that_does_not_exist"


def test_loads_from_the_working_directory_without_installation(tmp_path, monkeypatch):
    (tmp_path / "decl_cwd_tables.py").write_text(
        dedent(
            """
            from delta_engine.schema import Column, DeltaTable, String
            orders = DeltaTable("dev", "silver", "orders", columns=(Column("id", String()),))
            """
        )
    )
    monkeypatch.chdir(tmp_path)

    tables = load_declarations(["decl_cwd_tables:orders"])

    assert _dotted_names(tables) == ["dev.silver.orders"]
    sys.modules.pop("decl_cwd_tables", None)
    sys.path.remove(str(tmp_path))


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
    """
    (project / f"{module_name}.py").write_text(dedent(source.format(catalog="dev")))
    (shadow / f"{module_name}.py").write_text(dedent(source.format(catalog="wrong")))
    monkeypatch.chdir(project)
    monkeypatch.syspath_prepend(str(project))
    monkeypatch.syspath_prepend(str(shadow))

    tables = load_declarations([f"{module_name}:orders"])

    assert _dotted_names(tables) == ["dev.silver.orders"]
    sys.modules.pop(module_name, None)
