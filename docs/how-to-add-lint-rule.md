---
tags:
  - how-to
---

# How to add a lint rule

This guide walks through adding a rule to `delta-engine lint` — for example,
`max-columns` to cap how many columns a table declares.

Everything a rule needs lives in one file: `src/delta_engine/lint/rules.py`.
The config parser, the known-settings check, and the rule's default severity
all derive from the registry in that file, so no other production code
changes.

## 1. Define the rule

Add a frozen dataclass to `src/delta_engine/lint/rules.py`:

```python
@dataclass(frozen=True, slots=True)
class MaxColumnsRule:
    """Every table stays within a column budget."""

    limit: int
    name: ClassVar[str] = "max-columns"

    def __post_init__(self) -> None:
        if self.limit < 1:
            raise ValueError("'limit' must be a positive integer")

    def evaluate(self, table: DesiredTable) -> tuple[str, ...]:
        """Report the table when it declares more columns than the limit."""
        if len(table.columns) <= self.limit:
            return ()
        return (f"table declares {len(table.columns)} columns; the limit is {self.limit}",)
```

> Note: `max-columns` is a hypothetical example showing the pattern.

A rule satisfies the `LintRule` protocol: a `name` `ClassVar[str]` — the id
used in config keys and in output — and
`evaluate(table: DesiredTable) -> tuple[str, ...]`. Messages state facts
only, one per violation; the runner pairs each message with the rule id, the
table, and the configured severity. This is the same separation the safety
rules keep: rules state what is wrong, policy decides how much it matters.

The dataclass fields are the rule's config parameters. Validate them in
`__post_init__` and raise `ValueError`; the config parser reports it as a
configuration error prefixed with the rule name
(`max-columns: 'limit' must be a positive integer`). A rule with no
parameters — most rules — needs no fields and no `__post_init__`.

## 2. Register it

Add the class to `ALL_RULES` at the bottom of the same file:

```python
ALL_RULES: Final = (
    TableCommentRule,
    ColumnCommentRule,
    PrimaryKeyRule,
    RequiredTagRule,
    MaxColumnsRule,
)
```

Registration is all the wiring. From the registry, config parsing derives:

- `max-columns` is now a valid key in `[tool.delta-engine.lint]`; unknown
  keys stay configuration errors.
- A bare value sets the severity: `max-columns = "warning"`.
- An inline table is `severity` plus constructor keyword arguments:
  `max-columns = { limit = 50, severity = "warning" }`. `severity` is a
  reserved name, so no rule may have a field called `severity`.
- The default when the key is absent: enabled at `error` if the rule
  constructs without arguments, off otherwise. `max-columns` requires
  `limit`, so it stays off until a limit is configured.

## 3. Write tests

Add tests in:

- `tests/lint/test_rules.py` — the rule's facts against `DesiredTable`s
  built with the file's `build_table` helper: a compliant table yields no
  messages, each violation yields its message. If the rule has parameters,
  invalid ones raise `ValueError` at construction.
- `tests/lint/test_config.py` — if the rule constructs without arguments,
  add it to the default-policy expectation in `TestDefaults`. That mapping
  is pinned exactly on purpose: a new default-on rule changes what a bare
  `delta-engine lint` enforces, and the test makes that a conscious choice.

Run:

```bash
uv run pytest tests/lint tests/cli
```

## 4. Document it

Add a row to the rule table in the [CLI reference](reference-cli.md).
