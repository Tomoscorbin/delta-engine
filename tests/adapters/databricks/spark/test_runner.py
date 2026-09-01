import pytest

from delta_engine.adapters.databricks.spark._runner import SparkSqlRunner

_VARIABLE_SUBSTITUTION = "spark.sql.variable.substitute"


class _FakeConfig:
    def __init__(self, value: str | None = "true", *, set_failure: Exception | None = None) -> None:
        self.value = value
        self.set_failure = set_failure
        self.gets: list[str] = []
        self.sets: list[tuple[str, str]] = []

    def get(self, name: str) -> str | None:
        self.gets.append(name)
        return self.value

    def set(self, name: str, value: str) -> None:
        self.sets.append((name, value))
        if self.set_failure is not None and value == "false":
            raise self.set_failure
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
    # Given a statement containing no variable expression
    spark = _FakeSpark()

    result = SparkSqlRunner(spark).run("SELECT 1")

    # Then it runs directly, leaving the session configuration unread and unset
    assert result is spark.result
    assert spark.statements == ["SELECT 1"]
    assert spark.conf.gets == []
    assert spark.conf.sets == []


def test_variable_expression_is_run_with_substitution_disabled_and_restored():
    # Given a statement carrying a ${...} variable expression
    spark = _FakeSpark()
    statement = "SELECT '${env:HOME}'"

    result = SparkSqlRunner(spark).run(statement)

    # Then substitution is disabled around the run and restored afterwards
    assert result is spark.result
    assert spark.statements == [statement]
    assert spark.conf.gets == [_VARIABLE_SUBSTITUTION]
    assert spark.conf.sets == [
        (_VARIABLE_SUBSTITUTION, "false"),
        (_VARIABLE_SUBSTITUTION, "true"),
    ]
    assert spark.conf.value == "true"


def test_already_disabled_substitution_is_not_mutated():
    # Given a session where substitution is already disabled
    spark = _FakeSpark(config=_FakeConfig("false"))

    SparkSqlRunner(spark).run("SELECT '${name}'")

    # Then the statement runs without touching the setting
    assert spark.statements == ["SELECT '${name}'"]
    assert spark.conf.sets == []


def test_substitution_setting_is_restored_when_sql_fails():
    # Given a variable-carrying statement whose SQL fails
    spark = _FakeSpark(failure=RuntimeError("query failed"))

    with pytest.raises(RuntimeError) as exc_info:
        SparkSqlRunner(spark).run("SELECT '${name}'")

    # Then the SQL failure propagates and the setting is still restored
    assert exc_info.value is spark.failure
    assert spark.conf.value == "true"
    assert spark.conf.sets == [
        (_VARIABLE_SUBSTITUTION, "false"),
        (_VARIABLE_SUBSTITUTION, "true"),
    ]


def test_sql_is_not_run_when_substitution_cannot_be_disabled():
    # Given a session whose substitution setting cannot be written
    set_failure = RuntimeError("configuration is read-only")
    spark = _FakeSpark(config=_FakeConfig(set_failure=set_failure))

    with pytest.raises(RuntimeError) as exc_info:
        SparkSqlRunner(spark).run("SELECT '${name}'")

    # Then that failure propagates and the SQL never runs — running it would
    # corrupt the statement's literals
    assert exc_info.value is set_failure
    assert spark.statements == []


def test_sql_is_not_run_when_substitution_setting_is_unavailable():
    # Given a session that does not expose the substitution setting
    spark = _FakeSpark(config=_FakeConfig(None))

    with pytest.raises(RuntimeError):
        SparkSqlRunner(spark).run("SELECT '${name}'")

    # Then the SQL never runs
    assert spark.statements == []


def test_variable_expression_round_trips_through_spark_without_substitution(spark):
    original = spark.conf.get(_VARIABLE_SUBSTITUTION)

    [row] = SparkSqlRunner(spark).run("SELECT '${env:HOME}' AS value").collect()

    assert row.value == "${env:HOME}"
    assert spark.conf.get(_VARIABLE_SUBSTITUTION) == original
