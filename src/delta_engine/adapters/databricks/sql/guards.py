"""
Read-boundary representability guards for the Databricks readers.

Both readers call these before mapping a catalog object into an
``ObservedTable``: an object the engine cannot reconcile as an ordinary
managed Delta table is failed at the boundary — raising
``UnsupportedCatalogRelationError``, which the readers' ``except Exception``
boundary turns into ``ReadFailed`` — rather than admitted as observed state.

The guards decide *representability* (is this an ordinary managed Delta table
the engine can model), not *safety* (is a change to it allowed), so they live
in the adapter, not application validation. PySpark-free like the rest of the
shared SQL core.
"""

from typing import Final

from delta_engine.domain.model import QualifiedName


class UnsupportedCatalogRelationError(Exception):
    """A catalog object delta-engine does not manage as an ordinary Delta table."""


# The engine's action set is ``ALTER TABLE`` DDL, and its CREATE path emits only
# managed ``CREATE TABLE ... USING DELTA`` (no LOCATION) — so a managed Delta
# table is the one relation kind it can both create and reconcile. Every other
# kind is rejected here rather than read and mismanaged:
#   - views, materialized views, streaming tables, foreign tables, and shallow
#     clones have their own DDL surfaces and restricted capabilities;
#   - EXTERNAL is reconcilable in principle (Delta ALTERs work on it) but is
#     unverified and not expressible as a desired declaration, so it waits for
#     a live test before joining the allowlist.
# An allowlist, not a blocklist, so unknown future kinds fail closed.
_SUPPORTED_RELATION_KINDS: Final[frozenset[str]] = frozenset({"MANAGED"})


def require_supported_relation(table_type: str, qualified_name: QualifiedName) -> None:
    """
    Raise unless ``table_type`` is a relation kind the engine can reconcile.

    A representability guard, not a safety rule: an unsupported kind is not a
    table the engine can diff, so it fails at the read boundary (turning into
    ``ReadFailed``) rather than being admitted as observed state. A streaming
    table or materialized view reports Delta format, so only the relation kind
    — not the format guard — catches it.
    """
    if table_type.upper() not in _SUPPORTED_RELATION_KINDS:
        raise UnsupportedCatalogRelationError(
            f"{qualified_name} has relation kind {table_type};"
            " delta-engine manages MANAGED Delta tables only"
        )


def require_delta_format(table_format: str, qualified_name: QualifiedName) -> None:
    """
    Raise unless a table's storage format is Delta.

    A managed relation can still be non-Delta (Iceberg); ``DESCRIBE DETAIL.format``
    is the documented delta/iceberg discriminator, which the reader reads from the
    detail row and passes here. Like the relation-kind guard, this fails closed to
    ``ReadFailed``.
    """
    if table_format.casefold() != "delta":
        raise UnsupportedCatalogRelationError(
            f"{qualified_name} has format {table_format!r}; delta-engine manages Delta tables only"
        )
