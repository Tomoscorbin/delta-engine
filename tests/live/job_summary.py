"""
Collect and render the live suite's GitHub job summary.

Backend-free and importable without a Databricks connection: the live
``conftest`` attaches each test's docstring to its report and, at the end of the
run, hands the terminal reporter to :func:`write_summary`. The hermetic tests in
``tests/test_live_job_summary.py`` drive these functions directly.

The summary keeps a high-level overview table (one row per behaviour area) and
adds a collapsible ``<details>`` drill-down per area listing the individual
checks that ran. Each check's text is the first line of its test's docstring,
falling back to a humanized test name when a test has none.
"""

from dataclasses import dataclass
import os

# Visitor-facing behaviour areas for the GitHub job summary, in report order.
# Each live test file maps to a label and a one-line statement of what it
# proves; a new live file needs an entry here to appear in the summary.
LIVE_AREAS: dict[str, tuple[str, str]] = {
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

# The user_property key under which a test's docstring rides its report from the
# xdist worker back to the controller that writes the summary.
_AREA_DOC_KEY = "area_doc"

# Acronyms to re-case when falling back to a humanized test name.
_ACRONYMS = {
    "cdf": "CDF",
    "cdc": "CDC",
    "ddl": "DDL",
    "sql": "SQL",
    "pk": "PK",
    "fk": "FK",
    "ntz": "NTZ",
}


@dataclass(frozen=True)
class CheckOutcome:
    """One live test's contribution to the summary."""

    nodeid: str
    filename: str
    passed: bool
    docstring: str  # first docstring line, or "" when the test has none
    line: int  # definition line, used only to order checks deterministically


def docstring_property(obj: object) -> tuple[str, str]:
    """Return the report ``user_property`` carrying a test's one-line docstring."""
    doc = (getattr(obj, "__doc__", None) or "").strip()
    return (_AREA_DOC_KEY, doc.splitlines()[0] if doc else "")


def collect_outcomes(stats: dict) -> list[CheckOutcome]:
    """
    Collapse a reporter's phase reports into one outcome per live test.

    A test contributes several reports (setup, call, teardown); it counts as
    passed only when every phase passed, and its docstring rides the ``call``
    report's ``user_properties``.
    """
    passed: dict[str, bool] = {}
    docstrings: dict[str, str] = {}
    lines: dict[str, int] = {}
    for status in ("passed", "failed", "error"):
        for report in stats.get(status, []):
            filename = _filename(report.nodeid)
            if filename not in LIVE_AREAS:
                continue
            nodeid = report.nodeid
            passed[nodeid] = passed.get(nodeid, True) and status == "passed"
            doc = _area_doc(report)
            if doc:
                docstrings[nodeid] = doc
            lines.setdefault(nodeid, _line_of(report))
    return [
        CheckOutcome(
            nodeid=nodeid,
            filename=_filename(nodeid),
            passed=passed[nodeid],
            docstring=docstrings.get(nodeid, ""),
            line=lines[nodeid],
        )
        for nodeid in passed
    ]


def render_summary(
    outcomes: list[CheckOutcome],
    areas: dict[str, tuple[str, str]] = LIVE_AREAS,
) -> str | None:
    """Render the whole job-summary markdown, or ``None`` when no live tests ran."""
    if not outcomes:
        return None

    checks_by_file: dict[str, list[CheckOutcome]] = {}
    for outcome in outcomes:
        checks_by_file.setdefault(outcome.filename, []).append(outcome)

    total = len(outcomes)
    total_passed = sum(1 for outcome in outcomes if outcome.passed)

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
    for filename, (label, description) in areas.items():
        checks = checks_by_file.get(filename)
        if not checks:
            continue
        area_passed = sum(1 for check in checks if check.passed)
        mark = "✅" if area_passed == len(checks) else "❌"
        lines.append(f"| {label} | {description} | {mark} {area_passed}/{len(checks)} |")

    lines.append("")
    for filename, (label, _description) in areas.items():
        checks = checks_by_file.get(filename)
        if not checks:
            continue
        area_passed = sum(1 for check in checks if check.passed)
        if area_passed == len(checks):
            summary_line = f"✅ {label} — {len(checks)} checks"
        else:
            summary_line = f"❌ {label} — {area_passed}/{len(checks)}"
        lines.append(f"<details><summary>{summary_line}</summary>")
        lines.append("")
        for check in sorted(checks, key=lambda outcome: outcome.line):
            prefix = "" if check.passed else "❌ "
            lines.append(f"- {prefix}{_describe(check)}")
        lines.append("</details>")
        lines.append("")

    return "\n".join(lines) + "\n"


def write_summary(terminalreporter) -> None:
    """
    Append the rendered summary to ``GITHUB_STEP_SUMMARY``, once, from the controller.

    Under ``-n`` parallelism xdist runs the terminal-summary hook on every worker
    as well as the controller, but only the controller aggregates stats across
    workers; a worker would append its own partial, duplicate summary. xdist sets
    ``workerinput`` on worker configs only, so this returns early on workers.
    """
    if hasattr(terminalreporter.config, "workerinput"):
        return
    summary_path = os.environ.get("GITHUB_STEP_SUMMARY")
    if not summary_path:
        return
    summary = render_summary(collect_outcomes(terminalreporter.stats))
    if summary is None:
        return
    with open(summary_path, "a", encoding="utf-8") as handle:
        handle.write(summary)


def _filename(nodeid: str) -> str:
    return nodeid.split("::", 1)[0].rsplit("/", 1)[-1]


def _area_doc(report) -> str:
    for key, value in getattr(report, "user_properties", ()):
        if key == _AREA_DOC_KEY:
            return value
    return ""


def _line_of(report) -> int:
    location = getattr(report, "location", None)
    if location and location[1] is not None:
        return location[1]
    return 0


def _describe(outcome: CheckOutcome) -> str:
    return outcome.docstring or _humanize(outcome.nodeid)


def _humanize(nodeid: str) -> str:
    name = nodeid.rsplit("::", 1)[-1].split("[", 1)[0].removeprefix("test_")
    words = [_ACRONYMS.get(word, word) for word in name.split("_")]
    sentence = " ".join(words).strip()
    if not sentence:
        return nodeid
    return sentence[0].upper() + sentence[1:]
