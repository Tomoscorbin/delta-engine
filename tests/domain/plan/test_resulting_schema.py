from delta_engine.domain.model import (
    DesiredColumn,
    DesiredTable,
    ObservedColumn,
    ObservedTable,
    QualifiedName,
    String,
)
from delta_engine.domain.plan import TableDiff, diff_table, resulting_column_spellings

_NAME = QualifiedName("cat", "sch", "t")


def _desired(*columns: DesiredColumn) -> DesiredTable:
    return DesiredTable(qualified_name=_NAME, columns=columns)


def _observed(*columns: ObservedColumn) -> ObservedTable:
    return ObservedTable(qualified_name=_NAME, columns=columns)


def _spellings(diff: TableDiff) -> dict[str, str]:
    """Extract each resolved column's exact spelling, keyed by its identity."""
    return {key: value.spelling for key, value in resulting_column_spellings(diff).items()}


def test_a_missing_table_resolves_every_column_to_its_desired_spelling():
    diff = diff_table(_desired(DesiredColumn("request_id", String())), None)

    assert _spellings(diff) == {"request_id": "request_id"}


def test_a_matched_column_resolves_to_the_observed_spelling():
    diff = diff_table(
        _desired(DesiredColumn("request_id", String())),
        _observed(ObservedColumn("request_id", String())),
    )

    assert _spellings(diff) == {"request_id": "request_id"}


def test_an_added_column_resolves_to_the_desired_spelling():
    diff = diff_table(
        _desired(DesiredColumn("request_id", String()), DesiredColumn("extra", String())),
        _observed(ObservedColumn("request_id", String())),
    )

    assert resulting_column_spellings(diff)["extra"].spelling == "extra"


def test_a_renamed_column_resolves_to_the_rename_target_spelling():
    diff = diff_table(
        _desired(DesiredColumn("customer_name", String(), renamed_from="customer_nm")),
        _observed(ObservedColumn("customer_nm", String())),
    )

    assert _spellings(diff) == {"customer_name": "customer_name"}
    assert "customer_nm" not in resulting_column_spellings(diff)


def test_a_removed_column_does_not_appear():
    diff = diff_table(
        _desired(DesiredColumn("keep", String())),
        _observed(ObservedColumn("keep", String()), ObservedColumn("drop_me", String())),
    )

    assert "drop_me" not in resulting_column_spellings(diff)


def test_mixed_case_matched_column_resolves_to_the_observed_spelling():
    diff = diff_table(
        _desired(DesiredColumn("requestid", String())),
        _observed(ObservedColumn("requestId", String())),
    )

    assert _spellings(diff) == {"requestid": "requestId"}
