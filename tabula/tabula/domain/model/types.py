from __future__ import annotations
from typing import Literal
from tabula.domain.model.data_type import DataType

# Scalars
def bigint() -> DataType: return DataType("bigint")
def integer() -> DataType: return DataType("int")
def smallint() -> DataType: return DataType("smallint")
def boolean() -> DataType: return DataType("boolean")
def string() -> DataType: return DataType("string")
def date() -> DataType: return DataType("date")
def timestamp() -> DataType: return DataType("timestamp")
def double() -> DataType: return DataType("double")
def float32() -> DataType: return DataType("float")
def float64() -> DataType: return DataType("double")

# Preferred generic
def floating_point(bits: Literal[32, 64] = 64) -> DataType:
    return float32() if bits == 32 else float64()

# Parameterized
def decimal(precision: int, scale: int) -> DataType:
    return DataType("decimal", (precision, scale))
