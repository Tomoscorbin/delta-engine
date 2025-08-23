from __future__ import annotations

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
    return _BIGINT


def integer() -> DataType:
    return _INT


def smallint() -> DataType:
    return _SMALLINT


def boolean() -> DataType:
    return _BOOLEAN


def string() -> DataType:
    return _STRING


def date() -> DataType:
    return _DATE


def timestamp() -> DataType:
    return _TIMESTAMP


def double() -> DataType:
    return _DOUBLE


def float32() -> DataType:
    return _FLOAT


def float64() -> DataType:
    return _DOUBLE


# Preferred generic
def floating_point(bits: Literal[32, 64] = 64) -> DataType:
    if bits == 32:
        return float32()
    if bits == 64:
        return float64()
    raise ValueError(f"floating_point bits must be 32 or 64, got {bits}")


# Parameterized
def decimal(precision: int, scale: int) -> DataType:
    return DataType("decimal", (precision, scale))
