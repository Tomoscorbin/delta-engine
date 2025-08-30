from datetime import datetime, timedelta

from delta_engine.application.errors import SyncFailedError
from delta_engine.application.results import (
    ActionStatus,
    ExecutionResult,
    ReadResult,
    SyncReport,
    TableRunReport,
    ValidationFailure,
    ValidationResult,
)
from delta_engine.domain.model.column import Column
from delta_engine.domain.model.data_type import Integer
from tests.factories import make_observed_table, make_qualified_name


def _now_pair():
    start = datetime.now()
    return start, start + timedelta(seconds=1)


def _obs_fqn(cat="dev", sch="silver", name="people"):
    qn = make_qualified_name(cat, sch, name)
    return make_observed_table(qn, (Column("id", Integer()),)), f"{cat}.{sch}.{name}"


def test_sync_failed_error_carries_report_and_stringifies() -> None:
    # One validation failure + one success
    obs, fqn = _obs_fqn()
    start, end = _now_pair()

    failing = TableRunReport(
        fully_qualified_name=fqn,
        started_at=start,
        ended_at=end,
        read=ReadResult.create_present(obs),
        validation=ValidationResult(failures=(ValidationFailure("RuleX", "bad"),)),
        execution_results=(),
    )

    success = TableRunReport(
        fully_qualified_name="dev.silver.ok",
        started_at=start,
        ended_at=end,
        read=ReadResult.create_present(obs),
        validation=ValidationResult(failures=()),
        execution_results=(
            ExecutionResult(
                action="CreateTable",
                action_index=0,
                status=ActionStatus.OK,
                statement_preview="CREATE TABLE ...",
            ),
        ),
    )

    report = SyncReport(started_at=start, ended_at=end, table_reports=(failing, success))
    err = SyncFailedError(report)

    assert err.report is report
    s = str(err)
    assert isinstance(s, str) and len(s) > 0
