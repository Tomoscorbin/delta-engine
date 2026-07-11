---
name: add-tests
description: Use when writing tests, improving coverage, adding regression tests, testing new behaviour, or reviewing whether existing tests are too coupled to implementation details — applies classical, black-box unit testing principles (real objects over mocks, behaviour-focused assertions) to Python tests.
---

# Add Tests

Add or improve tests for the current change using classical unit testing principles.

The goal is not just to increase coverage. The goal is to make behaviour clear, protect important logic, and expose awkward design.

## Testing Philosophy

Prefer black-box testing over white-box testing: verify behaviour through the public interface, not by inspecting or asserting on internal steps.

Prefer:

- Behaviour-focused tests.
- Real objects over mocks.
- Simple setup.
- Clear given / when / then structure.
- Tests that read like examples of the public API.
- Tests that make regressions obvious.

Avoid:

- Testing private implementation details.
- Mocking internal collaborators unnecessarily.
- Tests that duplicate the implementation.
- Excessive patching.
- Brittle assertions against incidental structure.
- Coverage-only tests with no behavioural value.

Mocks are acceptable only when testing outgoing interactions, such as:

- A call to an external service.
- A filesystem boundary.
- A network boundary.
- A Databricks/Spark adapter boundary.
- A clock, random generator, or other nondeterministic dependency.

Do not mock domain objects just to isolate small classes. If real objects are cheap and deterministic, use them.

## Workflow

When adding tests, follow this process.

### 1. Understand the Behaviour

Before writing tests:

- Inspect the relevant production code.
- Inspect nearby tests.
- Identify the public behaviour being protected.
- Identify whether the change is a new behaviour, regression fix, refactor, or edge case.
- Prefer testing through the most meaningful public interface.

Do not immediately write a test against the first function you see.

### 2. Choose the Right Test Level

Prefer the lowest useful level that still tests meaningful behaviour.

Use:

- Unit tests for pure domain logic and small deterministic behaviours.
- Contract-style tests for adapter boundaries.
- Integration tests only when behaviour depends on real interaction between components.
- Regression tests for bugs that have already happened.

Avoid using an integration test when a simple unit test would express the behaviour better.

### 3. Write Behaviour-First Test Names

Test names should describe behaviour, not implementation.

Good:

```python
def test_preserves_unmanaged_properties_when_merging_desired_properties():
    ...
```

```python
def test_fails_when_column_names_duplicate():
    ...
```

```python
def test_metadata_only_table_manages_metadata_aspects():
    ...
```

Bad:

```python
def test_property_manager():
    ...
```

```python
def test_evaluate_method():
    ...
```

```python
def test_mock_called_once():
    ...
```

The test name does not need to include the system-under-test name if the behaviour is already clear.

### 4. Use Given / When / Then Comments

Structure non-trivial tests like this:

```python
def test_fails_when_column_names_duplicate():
    # given
    columns = (Column("id", Integer()), Column("id", String()))

    # when / then
    with pytest.raises(ValueError, match="Duplicate column name"):
        DesiredTable(qualified_name=QualifiedName("catalog", "schema", "table"), columns=columns)
```

Use comments when they improve readability. Do not force them into tiny one-line tests where they add noise.

### 5. Keep Setup Small

If setup becomes large, first ask whether the design is awkward.

Prefer:

- Small builders or fixtures for common domain objects.
- Explicit data in the test when it matters.
- Helper functions only when they hide irrelevant detail.

Avoid:

- Giant fixtures that obscure the important behaviour.
- Magic builders with too many defaults.
- Shared mutable test state.
- Autouse fixtures unless there is a strong reason.

### 6. Assert Outcomes, Not Implementation

Prefer assertions about observable behaviour:

```python
assert plan.actions == (SetTableComment("new comment"),)
```

Avoid assertions about internal steps unless the step is the actual behaviour:

```python
mock_property_manager.merge.assert_called_once()
```

Mock call assertions are only appropriate for outgoing interactions.

### 7. Add Regression Tests for Bugs

For bug fixes:

- Write a failing regression test first when practical.
- Name the test after the broken behaviour.
- Keep the test focused on the bug.
- Do not overfit to the exact implementation mistake.

Good:

```python
def test_metadata_only_declaration_carries_properties_without_deploying_them():
    ...
```

Bad:

```python
def test_fix_properties_bug():
    ...
```

### 8. Cover Edge Cases Through the General Behaviour

Add edge cases when they clarify the rule.

Prefer testing:

- Empty input.
- Missing optional values.
- Duplicate names.
- Case sensitivity (e.g. this project rejects non-lowercase column names outright, rather than casefolding them).
- Ordering rules.
- Conflicting declarations.
- Managed versus unmanaged properties.
- Metadata-only versus full table sync scope.

Avoid testing edge cases by adding special-case tests for every internal branch.

### 9. Review Test Design

After writing tests, check:

- Would a reader understand the behaviour from the tests?
- Are the tests coupled to implementation details?
- Is there unnecessary mocking?
- Is setup proportionate to the behaviour?
- Did the tests reveal an awkward public API?
- Is the test protecting important logic or just increasing coverage?

If the test is hard to write, consider whether the production design needs improving.

## Verification

After adding or changing tests:

1. Run the focused test file first.

```bash
uv run pytest path/to/test_file.py
```

2. Run the relevant package or directory if the change is broader.

```bash
uv run pytest tests/path/to/package
```

3. Run formatting and linting.

```bash
uv run ruff format .
uv run ruff check .
```

4. If type checking is configured, run it.

```bash
uv run pyright
```

Do not claim the tests pass unless the commands were actually run.

## Output Format

When finished, report:

### Tests Added or Changed

Briefly list the behaviours covered.

### Design Feedback

Mention any API or design awkwardness revealed by the tests.

### Verification

List the exact commands run and whether they passed.

### Remaining Gaps

Mention important behaviours not covered, if any.

## Examples

### Behaviour-First Domain Test

```python
def test_fails_when_column_names_duplicate():
    # given
    columns = (Column("id", Integer()), Column("id", String()))

    # when / then
    with pytest.raises(ValueError, match="Duplicate column name"):
        DesiredTable(qualified_name=QualifiedName("catalog", "schema", "table"), columns=columns)
```

### Regression Test

```python
def test_metadata_only_declaration_carries_properties_without_deploying_them():
    # given a metadata-only declaration of a full table, properties included —
    # the flag scopes deployment, not what may be declared
    table = DeltaTable(
        catalog="dev",
        schema="silver",
        name="orders",
        columns=[Column("id", Integer())],
        properties={"delta.enableChangeDataFeed": "true"},
        metadata_only=True,
    )

    # when
    desired = table.to_desired_table()

    # then
    assert desired.properties == {"delta.enableChangeDataFeed": "true"}
    assert TableAspect.PROPERTIES not in desired.managed_aspects
```

### Outgoing Interaction Test

Use a fake only when the behaviour is the outgoing interaction — here, the executor handing compiled SQL to Spark. This project's own adapter tests use exactly this pattern (see `tests/adapters/databricks/spark/test_executor.py`).

```python
class _FakeSpark:
    """Minimal stand-in for Spark that records executed statements."""

    def __init__(self) -> None:
        self.executed: list[str] = []

    def sql(self, statement: str):
        self.executed.append(statement)


def test_executor_runs_the_compiled_statement_against_spark():
    # given
    spark = _FakeSpark()
    executor = SparkExecutor(spark)
    plan = ActionPlan((SetTableComment("new comment"),))

    # when
    statements = executor.compile(QualifiedName("catalog", "schema", "table"), plan)
    executor.execute(statements)

    # then
    assert len(spark.executed) == 1
    assert "COMMENT ON TABLE" in spark.executed[0]
    assert "new comment" in spark.executed[0]
```

### Avoid This Style

This test is too coupled to implementation:

```python
def test_property_merger_calls_merge_method():
    # given
    merger = Mock()
    service = PropertyService(merger)

    # when
    service.apply(properties={"a": "b"})

    # then
    merger.merge.assert_called_once()
```

Prefer testing the resulting behaviour instead.
