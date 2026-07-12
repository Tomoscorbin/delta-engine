---
paths:
  - "tests/**/*.py"
---

# Testing rules

Use existing tests as the source of style.

Prefer behaviour-focused tests through public or stable module boundaries.

Prefer black box testing over white box testing unless otherwise necessary or helpful.

Tests should verify outcomes, state, reports, failures, plans, or generated SQL. Avoid testing implementation details that would change during a reasonable refactor.

Use classical unit testing by default: real objects, real state, and observable outcomes.

Do not mock internal collaborators.

Use mocks only for outgoing interactions at boundaries, such as Spark, Databricks, file systems, external services, databases, HTTP clients, message queues, or time.

Use `# Given`, `# When`, and `# Then` comments when they make the test easier to read. Do not add them mechanically when the test is already obvious.

Test names should describe the behaviour or business rule being verified, not the implementation mechanism.

Prefer tests that survive reasonable refactoring.

Do not assert internal call sequences unless the behaviour under test is itself an outgoing interaction.

## What to assert

For domain model changes, assert backend-free values and behaviour.

For diff changes, assert the produced change facts.

For validation changes, assert typed validation failures and successful validation results.

For planning changes, assert `ActionPlan` contents and ordering.

For dependency-resolution changes, assert table ordering, blocked dependents, and typed dependency failures.

For report changes, assert report status, table outcomes, failures, and rendered output where relevant.

For Databricks SQL compiler changes, assert generated SQL strings.

For Databricks reader changes, assert parsed observed state and backend metadata normalization.

For Databricks executor changes, assert execution summaries, successful action results, failed action results, and translated backend failures.

## Spark and Databricks tests

Do not require real Databricks for ordinary unit tests.

This repo carries no tests that need Databricks credentials. Do not add tests that require a real workspace; live verification against Unity Catalog happens in a separate project outside this repo.

Local Spark/Delta end-to-end tests should stay marked with `local_e2e`.

When a test uses Spark, keep the Spark-specific setup isolated in fixtures or adapter-level tests. Do not leak Spark assumptions into domain or application tests.

## Running tests

Use the narrowest useful test command first:

```bash
uv run pytest tests/path/to/test_file.py::test_name
```

Then broaden when needed:

```bash
uv run pytest tests/path/to/test_file.py
uv run pytest
```

To skip local Spark/Delta end-to-end tests:

```bash
uv run pytest -m "not local_e2e"
```

Every test in this repo runs locally without Databricks credentials.
