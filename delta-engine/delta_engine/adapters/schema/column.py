"""User-facing column spec used when declaring schemas."""

from dataclasses import dataclass

from delta_engine.domain.model import DataType


@dataclass(frozen=True, slots=True)
class Column:
    """User-facing column specification for schema authoring APIs."""

    name: str
    data_type: DataType
    is_nullable: bool = True
    comment: str = ""
