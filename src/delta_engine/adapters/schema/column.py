"""User-facing column spec used when declaring schemas."""

from dataclasses import dataclass

from delta_engine.domain.model import Column as DomainColumn, DataType


@dataclass(frozen=True, slots=True)
class Column:
    """User-facing column specification for schema authoring APIs."""

    name: str
    data_type: DataType
    nullable: bool = True
    comment: str = ""

    def to_domain_column(self) -> DomainColumn:
        """Convert this user-facing column into a domain :class:`Column`."""
        return DomainColumn(
            name=self.name,
            data_type=self.data_type,
            nullable=self.nullable,
            comment=self.comment,
        )
