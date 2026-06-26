# Contributing

## Set up

Install [uv](https://docs.astral.sh/uv/), then:

```bash
uv sync --group dev
```

This installs all development dependencies (pytest, ruff, mypy, pyspark, delta-spark, hypothesis) into a local virtual environment.

## Run tests

```bash
uv run pytest
```

Tests require a local Spark session. The test suite spins one up automatically via the `spark` fixture in `tests/conftest.py`. Expect the first run to take 30–60 seconds while Spark initializes.

Coverage must stay above 90%. The build fails if it drops below.

## Lint and format

```bash
uv run ruff check src tests
uv run ruff format src tests
```

## Type check

```bash
uv run mypy src
```

## Build docs

```bash
uv sync --group docs
uv run --group docs sphinx-build -b html docs docs/_build/html -W
```

Open `docs/_build/html/index.html` to preview.

## Before opening a pull request

1. All tests pass: `uv run pytest`
2. No lint errors: `uv run ruff check src tests`
3. No type errors: `uv run mypy src`
4. Docs build without warnings: `uv run --group docs sphinx-build -b html docs docs/_build/html -W`

Pull requests target `main`. CI runs mypy, ruff, and pytest automatically.
