from __future__ import annotations

__all__ = ["KeyPath"]

######

from collections.abc import Sequence
from typing import Any, Final, Generic, TypeVar

from . import _sugarful, _sugarless

######

_V_co = TypeVar("_V_co", covariant=True)
_V_0 = TypeVar("_V_0")

######


class KeyPathMeta(_sugarful._MixinMeta, _sugarless._MixinMeta):
    pass


class KeyPath(
    _sugarful._Mixin, _sugarless._Mixin, Generic[_V_co], metaclass=KeyPathMeta
):
    """
    An object that stands for a member chain from a base object.
    """

    __base: Final[Any]
    __keys: Final[tuple[str, ...]]

    def __init__(self, /, target: Any, keys: str | Sequence[str]) -> None:
        self.__base = target

        # This initializer will seldom be called by user, so we don't need a sanity
        # check here.
        if isinstance(keys, str):
            keys = tuple(keys.split("."))
        else:
            keys = tuple(keys)
        self.__keys = keys

    @property
    def base(self, /) -> Any:
        return self.__base

    @property
    def keys(self, /) -> tuple[str, ...]:
        return self.__keys

    def get(self, /) -> _V_co:
        """
        Get value from the end-point of this key-path.
        """

        value = self.__base
        for key in self.__keys:
            value = getattr(value, key)
        return value

    __call__ = get

    def unsafe_set(self: KeyPath[_V_0], value: _V_0, /) -> None:
        """
        Set a value to the end-point of this key-path.

        WARNING
        -------
        This method is unsafe, primarily in two ways:

        1.  It may raise exceptions if any key in the key-path doesn't allow writing.
        2.  It breaks Liskov substitution principle.
        """

        target = self.__base
        keys = self.__keys
        i_last_key = len(keys) - 1
        for i in range(i_last_key):
            target = getattr(target, keys[i])
        setattr(target, keys[i_last_key], value)

    def __hash__(self, /) -> int:
        return hash((id(self.__base), self.__keys))

    def __eq__(self, other, /) -> bool:
        return (
            isinstance(other, KeyPath)
            and self.__base is other.__base
            and self.__keys == other.__keys
        )
