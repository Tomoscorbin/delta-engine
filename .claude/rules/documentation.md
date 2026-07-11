---

paths:

- "src/**/*.py"
- "tests/**/*.py"
- "docs/**/*.md"
- "docs/**/*.rst"
- "README.md"

---

# Documentation rules

When changing source code, tests, public behaviour, architecture, validation policy, action types, adapter behaviour, failure semantics, or examples, consider whether docs need updating.

Documentation should explain the project’s public behaviour, architecture, workflows, and design rationale. Do not duplicate implementation details that are better read directly from code.

Keep docs aligned with the actual code and tests. If code and docs disagree, treat the code and tests as the source of truth, then update the docs.

Prefer clear, practical explanations over marketing language.

Use examples that reflect the real public API.

Do not invent Databricks, Delta Lake, Spark, or Unity Catalog behaviour. If behaviour depends on Databricks specifics, verify it against adapter code, tests, or official Databricks documentation before documenting it.

## When docs should change

Consider updating docs when changing:

* public API declarations
* `DeltaTable`, `ForeignKey`, `Property`, or schema declaration behaviour
* sync lifecycle behaviour
* validation policy or safe-change rules
* action planning or action ordering
* Databricks SQL generation
* adapter behaviour
* error handling or failure reporting
* metadata-only deployment behaviour
* table properties, tags, comments, primary keys, or foreign keys
* project architecture or layer boundaries

Do not update docs for purely internal refactors unless the conceptual model or user-visible behaviour changes.

## Style

Write for a technical user who understands Python and data engineering, but may not know the internal engine design yet.

Prefer concrete examples over abstract explanation.

Keep examples small and focused.

Use consistent terminology:

* desired state
* observed state
* drift
* validation
* action plan
* dependency resolution
* execution summary
* sync report
* managed aspects

Do not introduce new names for existing concepts.

When explaining architecture, preserve the distinction between:

* domain facts
* application policy
* adapter behaviour
* public API declarations

When explaining failures, make clear whether a failure happens during read, validation, dependency resolution, execution, or report handling.

## Code examples

Use public import paths in user-facing examples:

```python
from delta_engine.schema import DeltaTable
from delta_engine.databricks import build_spark_engine
```

Avoid examples that import from internal modules unless the doc is explicitly for contributors.

Keep examples executable in principle, even when shortened.

Prefer simple names such as `customers`, `orders`, and `order_items` unless the surrounding doc needs a more specific domain.

## Building docs

Build docs with:

```bash
uv sync --group docs
uv run --group docs sphinx-build -b html docs docs/_build/html -W
```

If documentation changes include code examples, also run the most relevant tests for the behaviour being documented.
