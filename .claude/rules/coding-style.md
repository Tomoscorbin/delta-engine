---

paths:

- "src/**/*.py"

---

# Code style rules

Follow the existing project style before introducing a new style.

Use clear, descriptive names. Avoid abbreviations unless they are widely understood, such as `id`, `url`, `http`, `sql`, `api`, `pk`, or `fk`.

Use absolute imports only. Relative imports are banned by project tooling.

Use type hints on function signatures.

Prefer explicit control flow when logic is non-trivial.

Avoid clever one-liners when a small block is easier to read.

Prefer immutable value objects for domain concepts when practical.

Keep functions focused, but do not split code into shallow helpers that only rename, forward, or obscure the flow.

Comments should explain non-obvious why, constraints, invariants, or trade-offs. Do not add comments that merely restate what the code says.

Do not swallow errors silently.

Do not use bare `except`.

Do not introduce new dependencies without explicit approval.

## Design style

Prefer deep modules: simple interfaces that hide meaningful internal complexity.

Pull complexity downward into modules so callers do not need to understand sequencing, internal data structures, backend quirks, algorithms, or implementation details.

Prefer composition over inheritance.

Prioritise readability and maintainability over optimisation. Optimise only when there is evidence that performance, cost, scale, or reliability requires it.

Avoid shallow abstractions: wrappers, pass-through methods, and layers that merely rename or forward concepts without hiding complexity.

Avoid pass-through methods and wrappers that do not add behaviour, hide complexity, protect a boundary, or provide a stable public import path.

Avoid scattering special cases across callers. Prefer designs where the general case naturally handles edge inputs such as empty collections, missing optional values, zero values, or absent drift.

Design invalid states out of existence when practical. Prefer types, constructors, and APIs that make misuse difficult rather than detecting invalid states late.

Keep information hidden. Internal data structures, algorithms, backend quirks, and sequencing decisions should not leak through public interfaces.

Prefer the simplest general-purpose interface that covers the current real use cases over a collection of narrow special-purpose methods.

Do not add speculative abstractions for imagined future use cases.

Do not trade a clean design for a quick patch unless the task is explicitly an emergency fix. If a tactical fix is necessary, call out the design debt and the follow-up.

## Layering

Keep infrastructure concerns at the edges.

Do not leak Spark, Databricks, Delta Lake, Py4J, SQL execution details, or backend metadata shapes into domain or application code.

Shared domain vocabulary is fine across layers. Implementation details, infrastructure types, persistence models, and backend-specific assumptions should not cross inward.

Thin public facades are allowed when they provide stable user-facing import paths. Avoid accidental pass-through layers elsewhere.

## Error handling

Prefer explicit, typed failures where the project already uses them.

Backend exceptions should be translated at the adapter boundary.

Do not hide unexpected failures behind vague messages.

Error messages should help the caller understand what failed and, where appropriate, what to inspect next.

## Formatting

Do not manually fight the formatter.

If a style question is already enforced by Ruff, mypy, import-linter, or pytest, prefer satisfying the tool over adding a new convention.
