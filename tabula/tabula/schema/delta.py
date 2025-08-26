from collections.abc import Sequence
from dataclasses import dataclass

from tabula.schema.column import Column


@dataclass(frozen=True, slots=True)
class DeltaTable:
    catalog: str
    schema: str
    name: str
    columns: Sequence[Column]
    comment: str | None = None
