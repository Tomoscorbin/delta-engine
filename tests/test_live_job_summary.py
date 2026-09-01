"""
Hermetic tests for the live suite's GitHub job-summary rendering.

These exercise ``tests/live/job_summary.py`` without a Databricks connection, so
they run in default CI. The live suite itself (``tests/live/``) is credentialed
and excluded from default runs; this file lives outside that directory so it is
not auto-marked ``databricks_e2e``.
"""

from types import SimpleNamespace

from tests.live.job_summary import (
    LIVE_AREAS,
    CheckOutcome,
    collect_outcomes,
    docstring_property,
    render_summary,
    write_summary,
)

_SAFETY = "test_sql_warehouse_live_safety.py"
_LIFECYCLE = "test_sql_warehouse_live.py"


def _outcome(filename, name, *, passed=True, docstring="", line=0):
    return CheckOutcome(
        nodeid=f"tests/live/{filename}::{name}",
        filename=filename,
        passed=passed,
        docstring=docstring,
        line=line,
    )


def _report(filename, name, *, when="call", doc=None, line=0):
    return SimpleNamespace(
        nodeid=f"tests/live/{filename}::{name}",
        location=(f"tests/live/{filename}", line, name),
        user_properties=[("area_doc", doc)] if doc is not None else [],
        when=when,
    )


# --- render_summary -------------------------------------------------------


def test_render_keeps_the_overview_row_and_adds_a_drilldown_for_the_area():
    # Given two passing checks in one area
    outcomes = [
        _outcome(_SAFETY, "test_a", docstring="Narrowing a type is rejected.", line=10),
        _outcome(_SAFETY, "test_b", docstring="Adding a required column is rejected.", line=20),
    ]

    # When the summary is rendered
    summary = render_summary(outcomes)

    # Then the status line and overview row are unchanged, and a drill-down lists each check
    assert summary is not None
    assert summary.startswith("## ✅ Live Databricks tests — all 2 passed")
    label, description = LIVE_AREAS[_SAFETY]
    assert f"| {label} | {description} | ✅ 2/2 |" in summary
    assert "<details><summary>✅ Validation blocks — 2 checks</summary>" in summary
    assert "- Narrowing a type is rejected." in summary
    assert "- Adding a required column is rejected." in summary


def test_render_orders_checks_by_definition_line_and_falls_back_to_the_name():
    # Given checks defined out of report order, one without a docstring
    outcomes = [
        _outcome(_SAFETY, "test_second_defined", docstring="", line=50),
        _outcome(_SAFETY, "test_first_defined", docstring="First runs.", line=10),
    ]

    summary = render_summary(outcomes)

    # Then they render in definition order, and the docstring-less one is humanized
    body = summary.split("<summary>✅ Validation blocks", 1)[1]
    assert body.index("First runs.") < body.index("Second defined")


def test_render_flags_a_failing_area_and_only_its_failed_check():
    # Given one passing and one failing check in the same area
    outcomes = [
        _outcome(_SAFETY, "test_pass", passed=True, docstring="A passing check.", line=10),
        _outcome(_SAFETY, "test_fail", passed=False, docstring="A failing check.", line=20),
    ]

    # When the summary is rendered
    summary = render_summary(outcomes)

    # Then the headline, the area row, and only the failed check carry the failure mark
    assert summary.startswith("## ❌ Live Databricks tests — 1 failed, 1 passed")
    label, description = LIVE_AREAS[_SAFETY]
    assert f"| {label} | {description} | ❌ 1/2 |" in summary
    assert "<details><summary>❌ Validation blocks — 1/2</summary>" in summary
    assert "- A passing check." in summary
    assert "- ❌ A failing check." in summary


def test_render_groups_checks_under_their_area_in_report_order():
    # Given checks from two areas, reported in the opposite of their LIVE_AREAS order
    outcomes = [
        _outcome(_SAFETY, "test_s", docstring="A safety check.", line=10),
        _outcome(_LIFECYCLE, "test_l", docstring="A lifecycle check.", line=10),
    ]

    # When the summary is rendered
    summary = render_summary(outcomes)

    # Then each check lands under its own area, ordered by LIVE_AREAS
    # (Table lifecycle precedes Validation blocks), not by report order
    assert summary.index("Table lifecycle —") < summary.index("Validation blocks —")
    assert "- A lifecycle check." in summary
    assert "- A safety check." in summary


def test_render_returns_none_when_no_live_tests_ran():
    # Then an empty run produces no summary rather than an empty table
    assert render_summary([]) is None


# --- collect_outcomes -----------------------------------------------------


def test_collect_collapses_phase_reports_and_reads_the_docstring_from_the_report():
    # Given one test's setup, call, and teardown reports, all passed
    stats = {
        "passed": [
            _report(_SAFETY, "test_a", when="setup", line=12),
            _report(_SAFETY, "test_a", when="call", doc="Alpha is rejected.", line=12),
            _report(_SAFETY, "test_a", when="teardown", line=12),
        ],
    }

    # When the reports are collected
    [outcome] = collect_outcomes(stats)

    # Then they collapse into one passing outcome carrying the call report's docstring
    assert outcome.nodeid.endswith("::test_a")
    assert outcome.filename == _SAFETY
    assert outcome.passed is True
    assert outcome.docstring == "Alpha is rejected."
    assert outcome.line == 12


def test_collect_marks_a_test_failed_when_any_phase_did_not_pass():
    # Given a test whose setup passed but whose call failed
    stats = {
        "passed": [_report(_SAFETY, "test_a", when="setup", line=5)],
        "failed": [_report(_SAFETY, "test_a", when="call", doc="A.", line=5)],
    }

    # When the reports are collected
    [outcome] = collect_outcomes(stats)

    # Then the single outcome is failed, still carrying its docstring
    assert outcome.passed is False
    assert outcome.docstring == "A."


def test_collect_ignores_tests_outside_the_live_areas():
    # Given a report from a file with no LIVE_AREAS entry
    stats = {"passed": [_report("test_unrelated.py", "test_x", doc="x", line=1)]}

    # Then it contributes nothing to the summary
    assert collect_outcomes(stats) == []


# --- docstring_property ---------------------------------------------------


def test_docstring_property_takes_only_the_first_line():
    # Given a test function with a multi-paragraph docstring
    def fn():
        """
        First line.

        Second paragraph is ignored.
        """

    # Then only the first line rides the report as the area_doc property
    assert docstring_property(fn) == ("area_doc", "First line.")


def test_docstring_property_is_empty_without_a_docstring():
    # Given a test function with no docstring
    def fn(): ...

    # Then the property is empty, leaving rendering to the humanized fallback
    assert docstring_property(fn) == ("area_doc", "")


# --- write_summary --------------------------------------------------------


def test_write_summary_appends_the_rendered_summary_once(tmp_path, monkeypatch):
    # Given a controller reporter (no workerinput) and a step-summary file
    summary_file = tmp_path / "summary.md"
    monkeypatch.setenv("GITHUB_STEP_SUMMARY", str(summary_file))
    reporter = SimpleNamespace(
        config=SimpleNamespace(),
        stats={
            "passed": [
                _report(_SAFETY, "test_a", when="setup", line=10),
                _report(_SAFETY, "test_a", when="call", doc="Alpha is rejected.", line=10),
            ],
        },
    )

    # When the summary is written
    write_summary(reporter)

    # Then it lands once — not once per phase report
    text = summary_file.read_text(encoding="utf-8")
    assert "Alpha is rejected." in text
    assert text.count("## ") == 1


def test_write_summary_skips_on_an_xdist_worker(tmp_path, monkeypatch):
    # Given a worker reporter (workerinput set) and a step-summary file
    summary_file = tmp_path / "summary.md"
    monkeypatch.setenv("GITHUB_STEP_SUMMARY", str(summary_file))
    reporter = SimpleNamespace(
        config=SimpleNamespace(workerinput={}),
        stats={"passed": [_report(_SAFETY, "test_a", doc="A.", line=1)]},
    )

    # When the summary is written
    write_summary(reporter)

    # Then the worker writes nothing — only the controller aggregates
    assert not summary_file.exists()


def test_write_summary_is_a_noop_without_the_step_summary_env(monkeypatch):
    # Given no GITHUB_STEP_SUMMARY in the environment
    monkeypatch.delenv("GITHUB_STEP_SUMMARY", raising=False)
    reporter = SimpleNamespace(config=SimpleNamespace(), stats={"passed": []})

    # Then a local run writes nothing and does not raise
    write_summary(reporter)
