import pytest

from delta_engine.application.failures import ExecutionFailure
from delta_engine.application.ports import (
    CatalogSpellings,
    ExecutionSucceeded,
    ExecutionSummary,
)
from delta_engine.domain.model import (
    DesiredColumn,
    DesiredTable,
    ObservedColumn,
    ObservedTable,
    QualifiedName,
    String,
)

_ORDERS = QualifiedName("dev", "silver", "orders")


def _ok_exec(idx=0, preview="ALTER TABLE ..."):
    return ExecutionSucceeded(statement_index=idx, statement=preview)


def _failed_exec(idx=0, preview="ALTER TABLE ...", exc="ValueError", msg="boom"):
    return ExecutionFailure(
        statement_index=idx,
        exception_type=exc,
        message=msg,
        statement=preview,
    )


def test_execution_summary_reports_no_failure_when_every_statement_succeeds():
    # Given a run whose statements all executed
    summary = ExecutionSummary((_ok_exec(0), _ok_exec(1)))

    # Then the summary reports success with no failures
    assert summary.failed is False
    assert summary.failures == ()
    assert summary.failed_count == 0
    assert summary.applied_count == 2


def test_execution_summary_exposes_the_failures_among_mixed_results():
    # Given a run of two statements, the second of which failed
    summary = ExecutionSummary((_ok_exec(0), _failed_exec(1, msg="bang")))

    # Then the summary surfaces the single failure and the applied count
    assert summary.failed is True
    assert summary.failed_count == 1
    assert summary.applied_count == 1
    assert tuple(f.message for f in summary.failures) == ("bang",)


def test_execution_summary_defaults_to_an_empty_unattempted_run():
    # Given no execution happened (e.g. an earlier phase short-circuited)
    summary = ExecutionSummary()

    # Then it is an empty, non-failing summary
    assert summary.results == ()
    assert summary.failed is False
    assert summary.failed_count == 0
    assert summary.applied_count == 0


def test_execution_summary_rejects_non_contiguous_statement_indexes():
    with pytest.raises(ValueError, match="contiguous"):
        ExecutionSummary((_ok_exec(1),))


def test_execution_summary_rejects_results_after_a_failure():
    with pytest.raises(ValueError, match="after a failure"):
        ExecutionSummary((_failed_exec(0), _ok_exec(1)))


def _desired_table(*column_names: str) -> DesiredTable:
    return DesiredTable(
        qualified_name=_ORDERS,
        columns=tuple(DesiredColumn(name, String()) for name in column_names),
    )


def _observed_table(*column_names: str) -> ObservedTable:
    return ObservedTable(
        qualified_name=_ORDERS,
        columns=tuple(ObservedColumn(name, String()) for name in column_names),
    )


def test_catalog_spellings_prefer_the_observed_spelling_across_casing():
    # Given a column declared "CustomerID" that the catalog spells "customerid"
    spellings = CatalogSpellings(((_desired_table("CustomerID"), _observed_table("customerid")),))

    # Then the effective spelling is the catalog's, however the caller cases the lookup
    assert str(spellings.spelling(_ORDERS, "CustomerID")) == "customerid"
    assert str(spellings.spelling(_ORDERS, "CUSTOMERID")) == "customerid"


def test_catalog_spellings_fall_back_to_the_declared_spelling_for_unobserved_columns():
    # Given a declared column the catalog has not seen yet
    spellings = CatalogSpellings(((_desired_table("newCol"), _observed_table("other")),))

    # Then the declared spelling is returned verbatim
    assert str(spellings.spelling(_ORDERS, "newCol")) == "newCol"


def test_catalog_spellings_keep_the_declared_spelling_when_the_table_is_absent():
    # Given a registered table with no observed counterpart (created this sync)
    spellings = CatalogSpellings(((_desired_table("OrderId"), None),))

    # Then the declared spelling is the only spelling there is
    assert str(spellings.spelling(_ORDERS, "orderid")) == "OrderId"


def test_catalog_spellings_return_the_given_spelling_for_unknown_tables_and_columns():
    # Given an empty lookup
    spellings = CatalogSpellings(())

    # Then any spelling passes through unchanged — the rule is total
    assert str(spellings.spelling(_ORDERS, "Whatever")) == "Whatever"
