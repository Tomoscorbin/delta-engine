"""
Fixtures for credentialed live tests against a real Databricks SQL warehouse.

Every test collected from this directory is automatically marked
``databricks_e2e`` — directory membership alone defines the live suite, so a
new file cannot leak into default runs by forgetting a marker line. The
addopts marker filter excludes the suite from every default pytest run; it
only runs when requested explicitly (a manual run or the live CI job):

    databricks auth login --host https://<workspace>   # once (OAuth), or set
                                                       # DATABRICKS_CONFIG_PROFILE
    export DATABRICKS_HTTP_PATH=...
    export DELTA_ENGINE_E2E_CATALOG=... DELTA_ENGINE_E2E_SCHEMA=...
    uv run pytest tests/live -m databricks_e2e --no-cov -n auto

Every test allocates uniquely named tables through the ``live_tables`` factory
and drops them afterwards, so runs are safe to repeat against a shared schema
and safe to run in parallel: ``-n auto`` fans the suite across xdist workers.
"""

from collections.abc import Callable
import os
from pathlib import Path
from uuid import uuid4

import pytest

_LIVE_REQUIRED_ENV = ("DELTA_ENGINE_E2E_CATALOG", "DELTA_ENGINE_E2E_SCHEMA")
_LIVE_DIRECTORY = Path(__file__).parent

type LiveTableFactory = Callable[[str], str]


def pytest_collection_modifyitems(items: list[pytest.Item]) -> None:
    """Mark every test in this directory as credentialed-live."""
    for item in items:
        if _LIVE_DIRECTORY in item.path.parents:
            item.add_marker(pytest.mark.databricks_e2e)


# Session-scoped so each parallel xdist worker opens one connection and reuses
# it for every test it runs, rather than reconnecting per module.
@pytest.fixture(scope="session")
def live_connection():
    missing = [name for name in _LIVE_REQUIRED_ENV if not os.environ.get(name)]
    if missing:
        pytest.skip(f"warehouse e2e env vars not set: {', '.join(missing)}")

    from tests.live.databricks_connection import open_sql_warehouse_connection

    with open_sql_warehouse_connection() as connection:
        yield connection


@pytest.fixture
def live_tables(live_connection) -> LiveTableFactory:
    from tests.live.sql_warehouse_live_helpers import execute_sql, qualified_table

    names: list[str] = []

    def allocate(label: str) -> str:
        name = f"de_live_{label}_{uuid4().hex[:8]}"
        names.append(name)
        return name

    yield allocate

    for name in reversed(names):
        execute_sql(live_connection, f"DROP TABLE IF EXISTS {qualified_table(name)}")


# Visitor-facing behaviour areas for the GitHub job summary, in report order.
# Each live test file maps to a label and a one-line statement of what it
# proves; a new live file needs an entry here to appear in the summary.
_LIVE_AREAS: dict[str, tuple[str, str]] = {
    "test_sql_warehouse_live.py": (
        "Table lifecycle",
        "create, evolve every mutable aspect, dry-run previews, idempotent resync",
    ),
    "test_sql_warehouse_live_constraints.py": (
        "Primary & foreign keys",
        "add, change, and drop keys; composite, self-referential, dependency order",
    ),
    "test_sql_warehouse_live_safety.py": (
        "Validation blocks",
        "unsafe DDL is rejected and the catalog is left unchanged",
    ),
    "test_sql_warehouse_live_scopes.py": (
        "Scoped ownership",
        "metadata-only and tags-only declarations change nothing outside their scope",
    ),
    "test_sql_warehouse_live_types_and_layout.py": (
        "Types & layout",
        "every column type round-trips; widening, partitioning, and clustering",
    ),
    "test_sql_warehouse_live_renames.py": (
        "Column renames",
        "data, tags, comments, and layout travel; keys are replaced; ambiguity is rejected",
    ),
    "test_sql_warehouse_live_platform_assumptions.py": (
        "Platform assumptions",
        "Databricks behaviours the engine's safety gates rely on",
    ),
}


def pytest_terminal_summary(terminalreporter: pytest.TerminalReporter) -> None:
    """
    Append a behaviour-area summary of the live suite to the GitHub job summary.

    Only acts in CI (``GITHUB_STEP_SUMMARY`` set), only counts this directory's
    tests, and writes once from the xdist controller rather than once per
    worker.
    """
    # Under ``-n`` parallelism xdist runs this hook on every worker as well as
    # the controller, but only the controller's stats are aggregated across
    # workers -- a worker sees just the subset it ran and would append its own
    # partial, duplicate summary. xdist sets ``workerinput`` on worker configs
    # only, so this skips workers and leaves the controller to write once.
    if hasattr(terminalreporter.config, "workerinput"):
        return

    summary_path = os.environ.get("GITHUB_STEP_SUMMARY")
    if not summary_path:
        return

    # Collapse each test's phase reports (setup/call/teardown) to one pass/fail.
    passed_by_test: dict[str, bool] = {}
    for status in ("passed", "failed", "error"):
        for report in terminalreporter.stats.get(status, []):
            filename = report.nodeid.split("::", 1)[0].rsplit("/", 1)[-1]
            if filename not in _LIVE_AREAS:
                continue
            passed_by_test[report.nodeid] = (
                passed_by_test.get(report.nodeid, True) and status == "passed"
            )

    if not passed_by_test:
        return

    counts_by_file: dict[str, list[int]] = {}
    for nodeid, passed in passed_by_test.items():
        filename = nodeid.split("::", 1)[0].rsplit("/", 1)[-1]
        counts = counts_by_file.setdefault(filename, [0, 0])
        counts[0] += int(passed)
        counts[1] += 1

    total = len(passed_by_test)
    total_passed = sum(passed_by_test.values())

    lines: list[str] = []
    if total_passed == total:
        lines.append(f"## ✅ Live Databricks tests — all {total} passed")
    else:
        lines.append(
            f"## ❌ Live Databricks tests — {total - total_passed} failed, {total_passed} passed"
        )
    lines += [
        "",
        "Verified against a real Databricks SQL warehouse (Unity Catalog).",
        "",
        "| Area | Checks | Result |",
        "| --- | --- | --- |",
    ]
    for filename, (label, description) in _LIVE_AREAS.items():
        counts = counts_by_file.get(filename)
        if counts is None:
            continue
        area_passed, area_total = counts
        mark = "✅" if area_passed == area_total else "❌"
        lines.append(f"| {label} | {description} | {mark} {area_passed}/{area_total} |")

    with open(summary_path, "a", encoding="utf-8") as summary:
        summary.write("\n".join(lines) + "\n")
