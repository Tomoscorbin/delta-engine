class Identifier(str):
    def __new__(cls, value: str):
        v = value.strip()
        if not v or " " in v or "." in v or not v.isascii():
            raise ValueError(f"Invalid identifier: {value!r}")
        return str.__new__(cls, v.casefold())
