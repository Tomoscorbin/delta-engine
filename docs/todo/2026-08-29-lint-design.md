# Declaration linting: `delta-engine lint`

Status: **implemented**
Date: 2026-08-29
Branch: `claude/lint-command`

## Goal

Add a `lint` command that checks `DeltaTable` declarations against governance rules
and fails CI when an enabled rule is broken. Four built-in rules to start:

- every table has a comment
- every column has a comment
- every table has a primary key
- every table carries the required tag keys

Lint is offline: it imports the declarations exactly like `plan` does, but never
opens a connection. It can run first in CI, before any credentials exist.

## Non-goals (v1)

- **No live-catalog auditing.** Rules evaluate desired state only. They operate on
  `DesiredTable`, so an observed-table mode can be added later without redesigning
  the rule model.
- **No user-defined rules.** The rule set is built in. Rules sit behind a small
  protocol, so adding one later is adding a class; a public plugin API is deferred.
- **No per-table exemptions.** Severity per rule (`error` / `warning` / `off`) is
  the only knob. If one legacy table cannot satisfy a rule, the rule drops to
  `warning` for everything; finer-grained escapes wait for evidence they are needed.
- **No numbered rule codes.** Kebab-case names (`table-comment`) are the ids.
  Codes (`DE001`) earn their keep at flake8 scale with plugin namespaces; with four
  curated rules, the config should read as policy without a lookup table.
- **No repo crawling.** Lint never discovers declarations by importing modules it
  was not pointed at. The declared collection is the estate — the same universe
  `plan` and `apply` manage.
- **No coverage check** for a stray `DeltaTable` defined somewhere but never added
  to the collection. Real gap, different feature.

## Decisions (resolved during brainstorming)

| Decision       | Choice                                        | Why                                                                                                                       |
| -------------- | --------------------------------------------- | ------------------------------------------------------------------------------------------------------------------------- |
| Lint target    | **Declarations only**                         | All four rules are statically checkable; fast, credential-free, runs before `plan` in CI.                                 |
| Package home   | **`src/delta_engine/lint/`**, not `cli/`      | Pure logic over `DesiredTable`; usable as a pytest gate without typer. `cli/` keeps only the thin command.                |
| Config home    | **`pyproject.toml` `[tool.delta-engine.lint]`** | Sits next to the declarations; shared by CI and local runs; no new file format. Flags override per invocation.            |
| Extensibility  | **Built-in rules behind a protocol**          | Mirrors `application/validation.py`'s rule idiom; adding a rule is adding a class. No public plugin API yet.              |
| Exemptions     | **Severity levels only**                      | One knob. Per-table exemption lists deferred.                                                                              |
| Rule ids       | **Kebab-case names**                          | Four rules, one curated namespace; `table-comment = "error"` reads as policy in review.                                    |
| Default target | **`declarations` key in config**              | Bare `delta-engine lint` works in an estate repo. Discovery by config, not by import sweep.                                |
| Severity owner | **The runner, never the rule**                | Rules state facts; severity is policy. Same separation the differ already keeps.                                           |

## Rule and finding model

New package `src/delta_engine/lint/`:

```python
class Severity(StrEnum):
    ERROR = "error"
    WARNING = "warning"

class LintRule(Protocol):
    name: str                  # the rule id used in config and output
    def evaluate(self, table: DesiredTable) -> tuple[str, ...]: ...
```

Rules are small frozen classes, one per policy, mirroring `validation.py`. A rule
returns messages — facts only. The runner already knows which rule and table it is
evaluating, so a rule states only what is wrong (e.g. `"column 'customer_id' has
no comment"`). The runner pairs each message with its rule id, table, and the
configured severity, producing a `Finding`, and collects findings into a
`LintReport` with `has_errors` and `to_dict()` (the same report/render split as
`SyncReport`).

The entry point matches the engine's lowering contract:

```python
def lint_tables(*tables: DesiredTableSource, policy: LintPolicy) -> LintReport: ...
```

It accepts anything with `to_desired_table()` — so `DeltaTable` works directly —
and lowers via `lower_desired_tables`, inheriting the duplicate-name rejection and
deterministic qualified-name ordering. Rules see `DesiredTable` only.

### Built-in rules

| Rule id          | Emits                                                                       |
| ---------------- | ---------------------------------------------------------------------------- |
| `table-comment`  | one message when the table `comment` is blank                               |
| `column-comment` | one message **per uncommented column**, naming the column                    |
| `primary-key`    | one message when `primary_key` is `None`                                    |
| `required-tag`   | one message per configured tag key missing from `tags` (key presence only)  |

`required-tag` is the only parameterized rule — constructed with the keys from
config. Tag *values* are not checked in v1.

### Deliberate choices

- **Per-table signature.** `evaluate(table)`, not `evaluate(tables)`. Every
  governance rule in sight is per-table; the runner owns the loop. A future
  collection-level rule is a signature change, accepted.
- **Scope is ignored.** A `tags`-scoped declaration still declares the full table
  shape (the existing scope contract), so lint checks the declared shape
  uniformly. If that proves noisy for annotations-scoped mirrors of
  pipeline-owned tables, a later refinement can skip rules for unmanaged aspects.

## Configuration

`[tool.delta-engine.lint]` in `pyproject.toml`. One key per rule, value is the
severity; the parameterized rule takes an inline table. `declarations` is the one
reserved non-rule key:

```toml
[tool.delta-engine.lint]
declarations = "myproject.tables:all_tables"
table-comment = "error"        # "error" | "warning" | "off"
column-comment = "warning"
primary-key = "error"
required-tag = { keys = ["owner"], severity = "error" }
```

Defaults with no file or no section — the linter is useful bare:

| Rule             | Default                    |
| ---------------- | -------------------------- |
| `table-comment`  | error                      |
| `column-comment` | error                      |
| `primary-key`    | error                      |
| `required-tag`   | off (cannot run without keys) |

Parsing rules (`lint/config.py`, a pure function from a mapping to `LintPolicy`;
the CLI owns reading the file and the reserved `declarations` key, which carries
no policy):

- `"off"` exists only in config space. Parsing yields a `LintPolicy` holding only
  enabled rules, each paired with its severity. Nothing downstream branches on
  enablement.
- Unknown rule ids and invalid severity strings raise `LintConfigError` — a
  silently ignored `primary_key = "error"` typo would be a policy hole.
- `required-tag` with an empty `keys` list is a `LintConfigError`, not a no-op.
  `severity` inside its inline table is optional and defaults to `"error"`.

## CLI

```
delta-engine lint [MODULE:ATTRIBUTE] [--output text|json] [--config PATH]
```

Same shape as `plan`, minus the connection. Flow: read config section →
parse policy → resolve target → `load_declarations` → `lint_tables` →
render → exit code.

- **Target resolution:** positional argument wins; otherwise the `declarations`
  config key; neither present is a `ConfigError` naming both fixes.
- **Config discovery:** `./pyproject.toml` in the working directory, `--config`
  to point elsewhere. No walking up the tree — `load_declarations` already
  establishes "run from the project root" as the CLI convention.
- **Exit codes:** `0` when no error-severity findings (warnings alone never
  fail); `1` for any error finding or `ConfigError`. No `--strict` flag —
  promoting a rule to `error` is what severity config is for.
- **Stdout discipline:** importing user modules can print; loading runs under
  `redirect_stdout(sys.stderr)` like `_sync`, so stdout carries only the report
  and `--output json` stays machine-consumable.
- Errors surface through the existing `_anticipated_errors` context manager.

Text output, grouped per table, summary line always printed:

```
catalog.schema.orders
  error    table-comment   table has no comment
  warning  column-comment  column 'customer_id' has no comment

3 tables checked: 1 error, 1 warning
```

A clean run prints `3 tables checked: no findings`. JSON output is
`json.dumps(report.to_dict(), indent=2)`: findings with `rule`, `severity`,
`table`, `message`, plus summary counts.

The text renderer lives in `cli/rendering.py` next to `render_sync`; `to_dict()`
lives on `LintReport` — the same split `SyncReport` uses today.

### Programmatic use

Because the logic lives outside `cli/`, an estate repo can gate in pytest without
the CLI:

```python
def test_tables_pass_lint() -> None:
    report = lint_tables(*all_tables, policy=policy)
    assert not report.has_errors
```

## Module layout

```
src/delta_engine/lint/
    __init__.py     public surface: lint_tables, parse_lint_config, LintPolicy,
                    LintReport, Finding, Severity, LintConfigError
    findings.py     Severity, Finding, LintReport
    rules.py        LintRule protocol, four built-in rules
    config.py       mapping -> LintPolicy (pure; LintConfigError on bad input)
    run.py          lint_tables(): lower, evaluate, attach severity, report
src/delta_engine/cli/
    app.py          lint command (target resolution, config read, exit code)
    rendering.py    render_lint()
```

## Testing

Black-box through public entry points, Given/When/Then, behaviour-named, no mocks
(nothing here has outgoing I/O except the config file read, which stays in the CLI):

- `tests/lint/test_rules.py` — each rule against `DesiredTable`s built with the
  existing `tests/builders.py` helpers: commented vs uncommented table, per-column
  violations name the column, primary key present/absent, tag key
  present/missing/multiple. Facts only — no severity assertions here.
- `tests/lint/test_config.py` — mapping → `LintPolicy`: absent section yields
  defaults, `"off"` removes a rule, unknown rule id / bad severity / empty `keys`
  each raise `LintConfigError`, the reserved `declarations` key carries no policy.
- `tests/lint/test_run.py` — `lint_tables`: severity attached per policy,
  `has_errors`, `to_dict()` shape, clean run yields no findings, accepts
  `DeltaTable` sources directly.
- `tests/cli/test_lint.py` — typer `CliRunner` end-to-end in the style of
  `tests/cli/conftest.py`: exit 0 clean, exit 0 warnings-only, exit 1 with errors,
  exit 1 on config error, `--output json` parses, argument overrides config
  `declarations`, bare invocation uses config.

Verification gate as usual: pytest, ruff, mypy.

## Future work (parked, not designed)

- Live-catalog lint mode (rules over observed tables; governance audit of
  unmanaged tables).
- User-defined rules with a dotted namespace (`myteam.pk-naming`).
- Per-table exemptions if severity-only proves too blunt.
- Coverage check for `DeltaTable`s never added to the declared collection.
- Scope-aware rule skipping for annotations-scoped declarations.
- Required tag *values* (e.g. `owner` must be a known team).
