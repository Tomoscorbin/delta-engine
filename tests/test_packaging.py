"""Distribution metadata and optional-dependency import boundaries."""

from importlib.metadata import entry_points, requires
import subprocess
import sys

from packaging.requirements import Requirement


def test_base_distribution_has_no_unconditional_runtime_dependencies():
    requirements = requires("delta-engine") or []

    assert requirements
    assert all("extra ==" in requirement for requirement in requirements)


def test_cli_extra_contains_the_sdk_connector_and_typer():
    requirements = requires("delta-engine") or []
    cli_requirements = [
        requirement for requirement in requirements if "extra == 'cli'" in requirement
    ]

    assert any(requirement.startswith("databricks-sdk>=0.70.0") for requirement in cli_requirements)
    assert any(
        requirement.startswith("databricks-sql-connector>=4.0.0")
        for requirement in cli_requirements
    )
    assert any(requirement.startswith("typer>=0.12") for requirement in cli_requirements)
    assert {Requirement(requirement).name for requirement in cli_requirements} == {
        "databricks-sdk",
        "databricks-sql-connector",
        "typer",
    }


def test_console_script_points_at_the_stdlib_only_shim():
    scripts = {
        entry_point.name: entry_point.value for entry_point in entry_points(group="console_scripts")
    }

    assert scripts["delta-engine"] == "delta_engine.cli:main"


def test_base_cli_shim_and_databricks_facade_load_no_optional_dependencies():
    program = """
import sys

import delta_engine
import delta_engine.cli
import delta_engine.databricks

optional_modules = ("typer", "databricks.sdk", "databricks.sql", "pyspark", "delta", "py4j")
loaded = [name for name in optional_modules if name in sys.modules]
if loaded:
    raise AssertionError(f"optional modules loaded eagerly: {loaded}")
print("ok")
"""

    result = subprocess.run([sys.executable, "-c", program], capture_output=True, text=True)

    assert result.returncode == 0, result.stderr
    assert result.stdout.strip() == "ok"
