from tabula.domain.model.data_type import DataType

# Scalar builders
def big_integer() -> DataType: return DataType("big_integer")
def boolean() -> DataType: return DataType("boolean")
def date() -> DataType: return DataType("date")
def timestamp() -> DataType: return DataType("timestamp")
def double() -> DataType: return DataType("double")
def float_() -> DataType: return DataType("float")
def integer() -> DataType: return DataType("integer")
def small_integer() -> DataType: return DataType("small_integer")
def string() -> DataType: return DataType("string")

# Parameterised builders
def decimal(precision: int, scale: int) -> DataType:
    return DataType("decimal", (precision, scale))

def varchar(length: int) -> DataType:
    return DataType("varchar", (length,))
