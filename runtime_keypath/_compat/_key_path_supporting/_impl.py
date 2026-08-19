from __future__ import annotations

__all__ = [
    "KeyPathSupporting",
    "key_path_supporting",
]

######

from typing_extensions import TypeVar, deprecated

######

Class_t0 = TypeVar("Class_t0", bound="type")

######


@deprecated("The superclass `KeyPathSupporting` now does nothing and can be safely removed.")
class KeyPathSupporting:
    pass


@deprecated("The decorator `key_path_supporting` now does nothing and can be safely removed.")
def key_path_supporting(clazz: Class_t0, /) -> Class_t0:
    return clazz
