"""Distribution metadata and optional-dependency import boundaries."""

from importlib.metadata import entry_points, metadata, requires
import subprocess
import sys

from packaging.requirements import Requirement
from packaging.specifiers import SpecifierSet


def test_distribution_requires_supported_python_floor_without_an_upper_bound():
    assert metadata("delta-engine")["Requires-Python"] == ">=3.12"


def test_base_distribution_has_no_unconditional_runtime_dependencies():
    requirements = requires("delta-engine") or []

    assert requirements
    assert all("extra ==" in requirement for requirement in requirements)


def test_distribution_exposes_only_supported_extras():
    extras = metadata("delta-engine").get_all("Provides-Extra") or []

    assert set(extras) == {"cli", "sql"}


def test_optional_dependency_ranges_match_the_supported_major_lines():
    parsed_requirements = [
        Requirement(requirement) for requirement in requires("delta-engine") or []
    ]

    def requirements_for(extra: str) -> dict[str, SpecifierSet]:
        return {
            requirement.name: requirement.specifier
            for requirement in parsed_requirements
            if requirement.marker is not None and requirement.marker.evaluate({"extra": extra})
        }

    assert requirements_for("sql") == {
        "databricks-sql-connector": SpecifierSet(">=4.0.0,<5"),
    }
    assert requirements_for("cli") == {
        "databricks-sdk": SpecifierSet(">=0.70.0,<1"),
        "databricks-sql-connector": SpecifierSet(">=4.0.0,<5"),
        "typer": SpecifierSet(">=0.15.4,<1"),
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
