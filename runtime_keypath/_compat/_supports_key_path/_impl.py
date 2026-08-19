from __future__ import annotations

__all__ = [
    "supports_key_path",
]

######

from typing_extensions import TypeVar, deprecated

######

Class_t0 = TypeVar("Class_t0", bound="type")

######


@deprecated("The decorator `supports_key_path` now does nothing and can be safely removed.")
def supports_key_path(clazz: Class_t0, /) -> Class_t0:
    return clazz
