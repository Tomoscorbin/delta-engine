from delta_engine.domain.model import (
    DesiredColumn,
    DesiredTable,
    ObservedColumn,
    ObservedTable,
    QualifiedName,
    String,
)
from delta_engine.domain.plan import diff_table, resulting_column_spellings

_NAME = QualifiedName("cat", "sch", "t")


def _desired(*columns: DesiredColumn) -> DesiredTable:
    return DesiredTable(qualified_name=_NAME, columns=columns)


def _observed(*columns: ObservedColumn) -> ObservedTable:
    return ObservedTable(qualified_name=_NAME, columns=columns)


def test_a_missing_table_resolves_every_column_to_its_desired_spelling():
    diff = diff_table(_desired(DesiredColumn("request_id", String())), None)

    assert resulting_column_spellings(diff) == {"request_id": "request_id"}


def test_a_matched_column_resolves_to_the_observed_spelling():
    diff = diff_table(
        _desired(DesiredColumn("request_id", String())),
        _observed(ObservedColumn("request_id", String())),
    )

    assert resulting_column_spellings(diff) == {"request_id": "request_id"}


def test_an_added_column_resolves_to_the_desired_spelling():
    diff = diff_table(
        _desired(DesiredColumn("request_id", String()), DesiredColumn("extra", String())),
        _observed(ObservedColumn("request_id", String())),
    )

    assert resulting_column_spellings(diff)["extra"] == "extra"


def test_a_renamed_column_resolves_to_the_rename_target_spelling():
    diff = diff_table(
        _desired(DesiredColumn("customer_name", String(), renamed_from="customer_nm")),
        _observed(ObservedColumn("customer_nm", String())),
    )

    spellings = resulting_column_spellings(diff)
    assert spellings == {"customer_name": "customer_name"}
    assert "customer_nm" not in spellings


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

    assert resulting_column_spellings(diff) == {"requestid": "requestId"}
