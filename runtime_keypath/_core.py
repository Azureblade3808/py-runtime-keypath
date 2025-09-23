from __future__ import annotations

__all__ = [
    "KeyPath",
    "KeyPathSupporting",
    "key_path_supporting",
]

import threading
from collections.abc import Sequence
from typing import (
    TYPE_CHECKING,
    Any,
    Final,
    Generic,
    Protocol,
    TypeVar,
    cast,
    final,
)

try:
    from typing_extensions import deprecated, override
except ImportError:
    if TYPE_CHECKING:
        assert False
    else:
        deprecated = lambda *args, **kwargs: lambda x: x
        override = lambda x: x

_T = TypeVar("_T")

_Value_co = TypeVar("_Value_co", covariant=True)
_Value_0 = TypeVar("_Value_0")

_MISSING = cast("Any", object())


@final
class _KeyPathRecorder:
    __slots__ = ("busy", "start", "end", "key_list")

    busy: bool
    start: Any
    end: Any
    key_list: list[str]

    def __init__(self, /) -> None:
        self.busy = False
        self.start = _MISSING
        self.end = _MISSING
        self.key_list = []


class _ThreadLocalProtocol(Protocol):
    recorder: _KeyPathRecorder
    """
    The active key-path recorder for this thread. May not exist.
    """


_thread_local = cast("_ThreadLocalProtocol", threading.local())


@final
class _KeyPathMeta(type):
    """
    The metaclass for class `KeyPath`.

    It exists mainly to provide `KeyPath.of` as a property.
    """

    # ! `of` is provided as a property here, so that whenever `KeyPath.of` gets
    # ! accessed, we can do something before it actually gets called.
    @property
    def of(self, /) -> _KeyPathOfFunction:
        # ! Docstring here is for Pylance hint.
        """
        Returns the key-path for accessing a certain value from a target
        object with a key sequence such as `a.b.c`.

        The target object and all intermediate objects, except for the
        final value, are expected to subclass `KeyPathSupporting`.

        Parameters
        ----------
        `value`
            A value that is accessed with chained keys such as `a.b.c`.

        Returns
        -------
        A key-path that indicates the target object and the key sequence
        to access the given value.

        Raises
        ------
        `RuntimeError`
            Typically occurs when the target or an intermediate object
            isn't subclassing `KeyPathSupporting`. Check the error
            message for more details.

        Example
        -------
        >>> class A(KeyPathSupporting):
        ...     def __init__(self) -> None:
        ...         self.b = B()
        >>> @key_path_supporting
        ... class B:
        ...     def __init__(self) -> None:
        ...         self.c = C()
        >>> class C:
        ...     pass
        >>> a = A()
        >>> key_path = KeyPath.of(a.b.c)
        >>> assert key_path.base is a
        >>> assert key_path.keys == ("b", "c")
        """

        try:
            _ = _thread_local.recorder
        except AttributeError:
            pass
        else:
            raise RuntimeError(
                " ".join(
                    [
                        "An unfinished key-path recorder has been found.",
                        "Check if `KeyPath.of` is always called immediatelly.",
                    ]
                )
            )

        recorder = _KeyPathRecorder()
        _thread_local.recorder = recorder

        func = _KeyPathOfFunction()
        return func


@final
class KeyPath(Generic[_Value_co], metaclass=_KeyPathMeta):
    """
    An object that stands for a member chain from a base object.
    """

    __base: Final[Any]
    __keys: Final[Sequence[str]]

    def __init__(self, /, target: Any, keys: str | Sequence[str]) -> None:
        self.__base = target

        if isinstance(keys, str):
            keys = tuple(keys.split("."))
        else:
            keys = tuple(keys)
        self.__keys = keys

    @property
    def base(self, /) -> Any:
        return self.__base

    @property
    def keys(self, /) -> Sequence[str]:
        return self.__keys

    def get(self, /) -> _Value_co:
        value = self.__base
        for key in self.__keys:
            value = getattr(value, key)
        return value

    def unsafe_set(self: KeyPath[_Value_0], value: _Value_0, /) -> None:
        target = self.__base
        keys = self.__keys
        i_last_key = len(keys) - 1
        for i in range(i_last_key):
            target = getattr(target, keys[i])
        setattr(target, keys[i_last_key], value)

    @deprecated("`KeyPath.set` is deprecated. Use `KeyPath.unsafe_set` instead.")
    def set(self: KeyPath[_Value_0], value: _Value_0, /) -> None:
        return self.unsafe_set(value)

    @override
    def __hash__(self, /) -> int:
        return hash((self.base, self.keys))

    @override
    def __eq__(self, other: object, /) -> bool:
        return (
            isinstance(other, KeyPath)
            and self.base is other.base
            and self.keys == other.keys
        )

    @override
    def __repr__(self, /) -> str:
        type_name = type(self).__name__
        base = self.base
        keys = self.keys
        return f"{type_name}({base=!r}, {keys=!r})"

    def __call__(self, /) -> _Value_co:
        return self.get()


# ! We implement the result of `KeyPath.of` as a callable object, so that when an
# ! exception occurred during the key-path access, there would still be a chance to
# ! perform some finalization.
class _KeyPathOfFunction:
    # ! Docstring here is for runtime help.
    """
    Returns the key-path for accessing a certain value from a target
    object with a key sequence such as `a.b.c`.

    The target object and all intermediate objects, except for the
    final value, are expected to subclass `KeyPathSupporting`.

    Parameters
    ----------
    `value`
        A value that is accessed with chained keys such as `a.b.c`.

    Returns
    -------
    A key-path that indicates the target object and the key sequence to
    access the given value.

    Raises
    ------
    `RuntimeError`
        Typically occurs when the target or an intermediate object isn't
        subclassing `KeyPathSupporting`. Check the error message for
        more details.

    Example
    -------
    >>> class A(KeyPathSupporting):
    ...     def __init__(self) -> None:
    ...         self.b = B()
    >>> class B(KeyPathSupporting):
    ...     def __init__(self) -> None:
    ...         self.c = C()
    >>> class C:
    ...     pass
    >>> a = A()
    >>> key_path = KeyPath.of(a.b.c)
    >>> assert key_path.base is a
    >>> assert key_path.keys == ("b", "c")
    """

    __invoked: bool = False

    def __call__(self, value: _Value_0, /) -> KeyPath[_Value_0]:
        self.__invoked = True

        try:
            recorder = _thread_local.recorder
        except AttributeError:
            raise RuntimeError(
                " ".join(
                    [
                        "`KeyPath.of` must be accessed and then called immediatedly",
                        "and should NOT be called more than once.",
                    ]
                )
            )

        del _thread_local.recorder

        assert not recorder.busy

        start = recorder.start
        key_list = recorder.key_list
        if start is _MISSING:
            assert len(key_list) == 0

            raise RuntimeError("No key has been recorded.")
        else:
            assert len(key_list) > 0

            if recorder.end is not value:
                raise RuntimeError(
                    " ".join(
                        [
                            "Key-path is broken. Check if there is something that does",
                            "NOT support key-paths in the member chain.",
                        ]
                    )
                )

        key_path = KeyPath(start, key_list)
        return key_path

    def __del__(self, /) -> None:
        # ! If an exception had occured during the key-path access, or this function
        # ! were just discarded without being finally called, we would do some cleaning
        # ! here.
        if not self.__invoked:
            del _thread_local.recorder


class KeyPathSupporting:
    """
    A base class that supports key-paths.

    Examples
    --------
    >>> class C(KeyPathSupporting):
    ...     v = 0
    >>> c = C()
    >>> key_path = KeyPath.of(c.v)
    >>> assert key_path.base is c
    >>> assert key_path.keys == ("v",)
    """

    # ! This method is intentially not named as `__getattribute__`. See below for
    # ! reason.
    def _(self, key: str, /) -> Any:
        try:
            recorder = _thread_local.recorder
        except AttributeError:
            # There is no recorder, which means that `KeyPath.of` is not being called.
            # So we don't need to record this key.
            return super().__getattribute__(key)

        if recorder.busy:
            # The recorder is busy, which means that another member is being accessed,
            # typically because the computation of that member is dependent on this one.
            # So we don't need to record this key.
            return super().__getattribute__(key)

        recorder.busy = True

        if recorder.start is not _MISSING and recorder.end is not self:
            raise RuntimeError(
                " ".join(
                    [
                        "Key-path is broken. Check if there is something that does NOT",
                        "support key-paths in the member chain.",
                    ]
                )
            )

        value = super().__getattribute__(key)

        recorder.busy = False
        if recorder.start is _MISSING:
            recorder.start = self
        recorder.end = value
        recorder.key_list.append(key)

        return value

    # ! `__getattribute__(...)` is declared against `TYPE_CHECKING`, so that unknown
    # ! attributes on conforming classes won't be treated as known by type-checkers.
    if not TYPE_CHECKING:
        __getattribute__ = _

    del _


def key_path_supporting(clazz: type[_T], /) -> type[_T]:
    """
    Patch on a class so that it can support key-paths.

    Examples
    --------
    >>> @key_path_supporting
    ... class C:
    ...     v = 0
    >>> c = C()
    >>> key_path = KeyPath.of(c.v)
    >>> assert key_path.base is c
    >>> assert key_path.keys == ("v",)
    """

    old_getattribute = clazz.__getattribute__

    def __getattribute__(self: _T, key: str) -> Any:
        try:
            recorder = _thread_local.recorder
        except AttributeError:
            # There is no recorder, which means that `KeyPath.of` is not being called.
            # So we don't need to record this key.
            return old_getattribute(self, key)

        if recorder.busy:
            # The recorder is busy, which means that another member is being accessed,
            # typically because the computation of that member is dependent on this one.
            # So we don't need to record this key.
            return old_getattribute(self, key)

        recorder.busy = True

        if recorder.start is not _MISSING and recorder.end is not self:
            raise RuntimeError(
                " ".join(
                    [
                        "Key-path is broken. Check if there is something that does NOT",
                        "support key-paths in the member chain.",
                    ]
                )
            )

        value = old_getattribute(self, key)

        recorder.busy = False
        if recorder.start is _MISSING:
            recorder.start = self
        recorder.end = value
        recorder.key_list.append(key)

        return value

    clazz.__getattribute__ = __getattribute__

    return clazz
