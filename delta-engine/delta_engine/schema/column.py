from dataclasses import dataclass

from delta_engine.domain.model import DataType


@dataclass(frozen=True, slots=True)
class Column:
    name: str
    data_type: DataType
    is_nullable: bool = True
    comment: str | None = None
