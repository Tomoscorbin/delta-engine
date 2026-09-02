"""Delta table features the engine can represent in observed state and plans."""

from enum import StrEnum


class TableFeature(StrEnum):
    """Closed identity vocabulary for table features managed by the engine."""

    TIMESTAMP_NTZ = "timestampNtz"
    VARIANT = "variantType"
