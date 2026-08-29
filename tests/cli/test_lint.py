"""Behaviour of the offline ``delta-engine lint`` workflow."""

import json
from textwrap import dedent

from delta_engine.cli.app import app

COMPLIANT_TABLES = """
    from delta_engine.schema import Column, DeltaTable, String

    orders = DeltaTable(
        "dev",
        "silver",
        "orders",
        columns=(Column("id", String(), nullable=False, comment="Row identifier"),),
        comment="Orders placed by customers",
        tags={"owner": "dse"},
        primary_key=("id",),
    )
    all_tables = [orders]
"""

# Violates table-comment, column-comment, and primary-key under the defaults.
UNGOVERNED_TABLES = """
    from delta_engine.schema import Column, DeltaTable, String

    orders = DeltaTable("dev", "silver", "orders", columns=(Column("id", String()),))
    all_tables = [orders]
"""

ALL_WARNINGS_CONFIG = """
    [tool.delta-engine.lint]
    table-comment = "warning"
    column-comment = "warning"
    primary-key = "warning"
"""


def write_pyproject(directory, content: str) -> None:
    (directory / "pyproject.toml").write_text(dedent(content))


def test_compliant_declarations_exit_zero_and_report_no_findings(
    runner, write_module, tmp_path, monkeypatch
):
    # Given a fully governed declaration and no lint config
    monkeypatch.chdir(tmp_path)
    module = write_module("lint_compliant", COMPLIANT_TABLES)

    # When
    result = runner.invoke(app, ["lint", f"{module}:all_tables"])

    # Then
    assert result.exit_code == 0
    assert "1 table checked: no findings" in result.stdout


def test_violations_exit_one_and_name_rule_table_and_message(
    runner, write_module, tmp_path, monkeypatch
):
    # Given a declaration breaking the default rules
    monkeypatch.chdir(tmp_path)
    module = write_module("lint_ungoverned", UNGOVERNED_TABLES)

    # When
    result = runner.invoke(app, ["lint", f"{module}:all_tables"])

    # Then
    assert result.exit_code == 1
    assert "dev.silver.orders" in result.stdout
    assert "table-comment" in result.stdout
    assert "table has no comment" in result.stdout
    assert "column 'id' has no comment" in result.stdout
    assert "table has no primary key" in result.stdout
    assert "1 table checked: 3 errors" in result.stdout


def test_warnings_alone_exit_zero(runner, write_module, tmp_path, monkeypatch):
    # Given every broken rule downgraded to a warning
    monkeypatch.chdir(tmp_path)
    write_pyproject(tmp_path, ALL_WARNINGS_CONFIG)
    module = write_module("lint_warned", UNGOVERNED_TABLES)

    # When
    result = runner.invoke(app, ["lint", f"{module}:all_tables"])

    # Then
    assert result.exit_code == 0
    assert "1 table checked: 3 warnings" in result.stdout


def test_json_output_is_machine_readable(runner, write_module, tmp_path, monkeypatch):
    # Given
    monkeypatch.chdir(tmp_path)
    module = write_module("lint_json", UNGOVERNED_TABLES)

    # When
    result = runner.invoke(app, ["lint", f"{module}:all_tables", "--output", "json"])

    # Then
    report = json.loads(result.stdout)
    assert report["tables_checked"] == 1
    assert report["error_count"] == 3
    assert {finding["rule"] for finding in report["findings"]} == {
        "table-comment",
        "column-comment",
        "primary-key",
    }


def test_invalid_config_exits_one_with_an_error_line(runner, write_module, tmp_path, monkeypatch):
    # Given a config with a bad severity
    monkeypatch.chdir(tmp_path)
    write_pyproject(
        tmp_path,
        """
        [tool.delta-engine.lint]
        table-comment = "fatal"
        """,
    )
    module = write_module("lint_bad_config", COMPLIANT_TABLES)

    # When
    result = runner.invoke(app, ["lint", f"{module}:all_tables"])

    # Then
    assert result.exit_code == 1
    assert "error:" in result.stderr
    assert "table-comment" in result.stderr


def test_declarations_target_from_config_is_used_when_argument_omitted(
    runner, write_module, tmp_path, monkeypatch
):
    # Given the target declared in config only
    monkeypatch.chdir(tmp_path)
    module = write_module("lint_config_target", COMPLIANT_TABLES)
    write_pyproject(
        tmp_path,
        f"""
        [tool.delta-engine.lint]
        declarations = "{module}:all_tables"
        """,
    )

    # When invoked bare
    result = runner.invoke(app, ["lint"])

    # Then
    assert result.exit_code == 0
    assert "no findings" in result.stdout


def test_argument_overrides_the_config_target(runner, write_module, tmp_path, monkeypatch):
    # Given a config pointing at compliant tables
    monkeypatch.chdir(tmp_path)
    compliant = write_module("lint_target_compliant", COMPLIANT_TABLES)
    ungoverned = write_module("lint_target_ungoverned", UNGOVERNED_TABLES)
    write_pyproject(
        tmp_path,
        f"""
        [tool.delta-engine.lint]
        declarations = "{compliant}:all_tables"
        """,
    )

    # When the argument names the ungoverned tables
    result = runner.invoke(app, ["lint", f"{ungoverned}:all_tables"])

    # Then the argument wins
    assert result.exit_code == 1


def test_a_non_table_tool_key_is_a_config_error(runner, write_module, tmp_path, monkeypatch):
    # Given a pyproject whose 'tool' key is not a TOML table
    monkeypatch.chdir(tmp_path)
    module = write_module("lint_bad_tool", COMPLIANT_TABLES)
    write_pyproject(tmp_path, 'tool = "not a table"')

    # When
    result = runner.invoke(app, ["lint", f"{module}:all_tables"])

    # Then the malformed file is rejected, not silently treated as defaults
    assert result.exit_code == 1
    assert "error:" in result.stderr


def test_a_non_string_declarations_target_is_a_config_error(runner, tmp_path, monkeypatch):
    # Given a declarations target that is not a string
    monkeypatch.chdir(tmp_path)
    write_pyproject(
        tmp_path,
        """
        [tool.delta-engine.lint]
        declarations = ["pkg.tables:all_tables"]
        """,
    )

    # When invoked bare
    result = runner.invoke(app, ["lint"])

    # Then
    assert result.exit_code == 1
    assert "declarations" in result.stderr


def test_missing_target_everywhere_is_a_config_error(runner, tmp_path, monkeypatch):
    # Given no argument and no config
    monkeypatch.chdir(tmp_path)

    # When
    result = runner.invoke(app, ["lint"])

    # Then
    assert result.exit_code == 1
    assert "error:" in result.stderr
    assert "declarations" in result.stderr


def test_explicit_config_path_is_read_instead_of_the_working_directory(
    runner, write_module, tmp_path, monkeypatch
):
    # Given a config outside the working directory
    monkeypatch.chdir(tmp_path)
    module = write_module("lint_explicit_config", UNGOVERNED_TABLES)
    elsewhere = tmp_path / "configs"
    elsewhere.mkdir()
    write_pyproject(elsewhere, ALL_WARNINGS_CONFIG)

    # When pointing --config at it
    result = runner.invoke(
        app,
        ["lint", f"{module}:all_tables", "--config", str(elsewhere / "pyproject.toml")],
    )

    # Then its downgrades apply
    assert result.exit_code == 0
    assert "3 warnings" in result.stdout


def test_missing_explicit_config_path_is_an_error(runner, write_module, tmp_path, monkeypatch):
    # Given
    monkeypatch.chdir(tmp_path)
    module = write_module("lint_missing_config", COMPLIANT_TABLES)

    # When
    result = runner.invoke(
        app, ["lint", f"{module}:all_tables", "--config", str(tmp_path / "absent.toml")]
    )

    # Then
    assert result.exit_code == 1
    assert "error:" in result.stderr
