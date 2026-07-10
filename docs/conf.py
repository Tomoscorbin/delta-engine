from importlib.metadata import version as _version
from pathlib import Path

from sphinx.application import Sphinx

project = "delta-engine"
author = "Tomos Corbin"
release = _version("delta-engine")

extensions = [
    "autoapi.extension",
    "sphinx.ext.napoleon",
    "sphinx_copybutton",
    "sphinxcontrib.mermaid",
    "myst_parser",
]

myst_enable_extensions = ["colon_fence"]
myst_heading_anchors = 3
myst_fence_as_directive = ["mermaid"]

html_theme = "furo"
html_title = "delta-engine"

# Show object titles as short names (build_engine, class Engine) rather than
# the fully-qualified definition path. This does not shorten type annotations
# that resolve to hidden internal modules; those still show their full path.
add_module_names = False

# sphinx-autoapi documents the package by static analysis: nothing is
# imported at build time, so pyspark/delta need no mocking.
autoapi_dirs = [str(Path(__file__).parent.parent / "src" / "delta_engine")]
autoapi_root = "autoapi"
autoapi_add_toctree_entry = False
autoapi_options = [
    "members",
    "undoc-members",
    "show-inheritance",
    "show-module-summary",
    # The public surfaces are re-export facades; without this their pages
    # would be empty and the docs would only exist on internal module pages.
    "imported-members",
]
autoapi_member_order = "bysource"
autoapi_python_class_content = "class"

# The published reference covers only the public import surfaces. Internal
# layers (domain, application, adapters, api) are implementation detail;
# documenting them would contradict the two-facade API story.
_PUBLIC_MODULES = {
    "delta_engine",
    "delta_engine.schema",
    "delta_engine.databricks",
}

# Public methods that are internal wiring, not user API.
_HIDDEN_MEMBERS = {"DeltaTable.to_desired_table"}


def _skip_non_public(
    app: Sphinx,
    what: str,
    name: str,
    obj: object,
    skip: bool,
    options: object,
) -> bool | None:
    if what in ("module", "package") and name not in _PUBLIC_MODULES:
        return True
    if any(name.endswith(hidden) for hidden in _HIDDEN_MEMBERS):
        return True
    return None


def setup(app: Sphinx) -> None:
    app.connect("autoapi-skip-member", _skip_non_public)


napoleon_use_ivar = True

exclude_patterns = ["_build", "superpowers", "todo"]
