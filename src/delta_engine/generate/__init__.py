"""Generate importable declaration modules from observed tables."""

from delta_engine.generate.declaration import GeneratedModule, GenerationError, generate_module

__all__ = [
    "GeneratedModule",
    "GenerationError",
    "generate_module",
]
