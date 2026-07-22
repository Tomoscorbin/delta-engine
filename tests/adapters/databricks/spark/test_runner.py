import pytest

from delta_engine.adapters.databricks.spark._runner import SparkSqlRunner

_VARIABLE_SUBSTITUTION = "spark.sql.variable.substitute"


class _FakeConfig:
    def __init__(self, value: str | None = "true", *, fail_to_disable: bool = False) -> None:
        self.value = value
        self.fail_to_disable = fail_to_disable
        self.gets: list[str] = []
        self.sets: list[tuple[str, str]] = []

    def get(self, name: str) -> str | None:
        self.gets.append(name)
        return self.value

    def set(self, name: str, value: str) -> None:
        self.sets.append((name, value))
        if self.fail_to_disable and value == "false":
            raise RuntimeError("configuration is read-only")
        self.value = value


class _FakeSpark:
    def __init__(self, *, config: _FakeConfig | None = None, failure: Exception | None = None):
        self.conf = config or _FakeConfig()
        self.failure = failure
        self.statements: list[str] = []
        self.result = object()

    def sql(self, statement: str):
        self.statements.append(statement)
        if self.failure is not None:
            raise self.failure
        return self.result


def test_clean_sql_does_not_touch_session_configuration():
    spark = _FakeSpark()

    result = SparkSqlRunner(spark).run("SELECT 1")

    assert result is spark.result
    assert spark.statements == ["SELECT 1"]
    assert spark.conf.gets == []
    assert spark.conf.sets == []


def test_variable_expression_is_run_with_substitution_disabled_and_restored():
    spark = _FakeSpark()
    statement = "SELECT '${env:HOME}'"

    result = SparkSqlRunner(spark).run(statement)

    assert result is spark.result
    assert spark.statements == [statement]
    assert spark.conf.gets == [_VARIABLE_SUBSTITUTION]
    assert spark.conf.sets == [
        (_VARIABLE_SUBSTITUTION, "false"),
        (_VARIABLE_SUBSTITUTION, "true"),
    ]
    assert spark.conf.value == "true"


def test_already_disabled_substitution_is_not_mutated():
    spark = _FakeSpark(config=_FakeConfig("false"))

    SparkSqlRunner(spark).run("SELECT '${name}'")

    assert spark.statements == ["SELECT '${name}'"]
    assert spark.conf.sets == []


def test_substitution_setting_is_restored_when_sql_fails():
    spark = _FakeSpark(failure=RuntimeError("query failed"))

    with pytest.raises(RuntimeError, match="query failed"):
        SparkSqlRunner(spark).run("SELECT '${name}'")

    assert spark.conf.value == "true"
    assert spark.conf.sets == [
        (_VARIABLE_SUBSTITUTION, "false"),
        (_VARIABLE_SUBSTITUTION, "true"),
    ]


def test_sql_is_not_run_when_substitution_cannot_be_disabled():
    spark = _FakeSpark(config=_FakeConfig(fail_to_disable=True))

    with pytest.raises(RuntimeError, match="configuration is read-only"):
        SparkSqlRunner(spark).run("SELECT '${name}'")

    assert spark.statements == []


def test_sql_is_not_run_when_substitution_setting_is_unavailable():
    spark = _FakeSpark(config=_FakeConfig(None))

    with pytest.raises(RuntimeError, match="did not expose"):
        SparkSqlRunner(spark).run("SELECT '${name}'")

    assert spark.statements == []


def test_variable_expression_round_trips_through_spark_without_substitution(spark):
    original = spark.conf.get(_VARIABLE_SUBSTITUTION)

    [row] = SparkSqlRunner(spark).run("SELECT '${env:HOME}' AS value").collect()

    assert row.value == "${env:HOME}"
    assert spark.conf.get(_VARIABLE_SUBSTITUTION) == original
