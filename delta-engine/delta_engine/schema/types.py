"""Convenience singletons for common domain data types."""

from delta_engine.domain.model.data_type import (
    Boolean,
    Date,
    Double,
    Float,
    Integer,
    Long,
    String,
    Timestamp,
)

# optional singletons for users
INTEGER = Integer()
LONG = Long()
FLOAT = Float()
DOUBLE = Double()
BOOLEAN = Boolean()
STRING = String()
DATE = Date()
TIMESTAMP = Timestamp()
