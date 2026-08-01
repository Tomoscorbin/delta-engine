"""
Collection input types that avoid the ``Sequence[str]`` ambiguity.

A bare string satisfies ``Sequence[str]``, so that annotation cannot distinguish
one string from an ordered collection of strings. ``ListOrTuple`` names the
built-in ordered containers accepted by immutable model constructors and lets
type checkers reject a bare string before the values are copied into a tuple.
"""

type ListOrTuple[T] = list[T] | tuple[T, ...]
