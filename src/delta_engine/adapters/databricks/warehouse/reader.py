"""
Reader adapter for Databricks SQL warehouses.

Unity Catalog only. Reads one table's state through the shared
``read_catalog_state`` (one table description containing protocol features,
then five information_schema queries). The backend-private SQL runner owns
physical invocation and cursor cleanup. It acquires one cursor lazily on the
first query, reuses it for the describe and information_schema follow-ups, then
closes it when the read finishes. Lazy acquisition keeps a dead connection
inside the shared ``ReadError`` translation boundary.
"""

from delta_engine.adapters.databricks.read import read_catalog_state
from delta_engine.adapters.databricks.warehouse._runner import WarehouseSqlRunner
from delta_engine.application.ports import CatalogState
from delta_engine.domain.model import QualifiedName


class WarehouseReader:
    """Catalog state reader backed by a Databricks SQL warehouse connection."""

    def __init__(self, runner: WarehouseSqlRunner) -> None:
        self._runner = runner

    def fetch_state(self, qualified_name: QualifiedName) -> CatalogState:
        """Return the known catalog state, raising ``ReadError`` when it cannot be read."""
        with self._runner.query_batch() as run_query:
            return read_catalog_state(run_query, qualified_name)
