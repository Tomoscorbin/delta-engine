from __future__ import annotations

"""Factory functions for common ``DataType`` instances."""

from typing import Literal

from tabula.domain.model.data_type.data_type import DataType

# ---- Singleton instances
_BIGINT = DataType("bigint")
_INT = DataType("int")
_SMALLINT = DataType("smallint")
_BOOLEAN = DataType("boolean")
_STRING = DataType("string")
_DATE = DataType("date")
_TIMESTAMP = DataType("timestamp")
_DOUBLE = DataType("double")
_FLOAT = DataType("float")


# ---- Scalar factories
def bigint() -> DataType:
    """Return a BIGINT data type."""

    return _BIGINT


def integer() -> DataType:
    """Return an INT data type."""

    return _INT


def smallint() -> DataType:
    """Return a SMALLINT data type."""

    return _SMALLINT


def boolean() -> DataType:
    """Return a BOOLEAN data type."""

    return _BOOLEAN


def string() -> DataType:
    """Return a STRING data type."""

    return _STRING


def date() -> DataType:
    """Return a DATE data type."""

    return _DATE


def timestamp() -> DataType:
    """Return a TIMESTAMP data type."""

    return _TIMESTAMP


def double() -> DataType:
    """Return a DOUBLE data type."""

    return _DOUBLE


def float32() -> DataType:
    """Return a 32-bit floating point type."""

    return _FLOAT


def float64() -> DataType:
    """Return a 64-bit floating point type."""

    return _DOUBLE


# Preferred generic
def floating_point(bits: Literal[32, 64] = 64) -> DataType:
    """Return a floating point type of the specified width."""

    if bits == 32:
        return float32()
    if bits == 64:
        return float64()
    raise ValueError(f"floating_point bits must be 32 or 64, got {bits}")


# Parameterized
def decimal(precision: int, scale: int) -> DataType:
    """Return a DECIMAL data type with precision and scale."""

    return DataType("decimal", (precision, scale))
