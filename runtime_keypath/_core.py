# pyright: reportImplicitOverride=false


from __future__ import annotations

__all__ = [
    "KeyPath",
    "KeyPathSupporting",
]

# region[Keywords]

from typing import TYPE_CHECKING, Final, Generic, Protocol, TypeVar, cast, final

# endregion[Keywords]

# region[Types]

if TYPE_CHECKING:
    from typing import Any, Sequence

# endregion[Types]

import threading
from dataclasses import dataclass, field
from typing import NamedTuple

_Value_t = TypeVar("_Value_t")
_Value_co = TypeVar("_Value_co", covariant=True)


class _ThreadLocalProtocol(Protocol):
    recorder: _KeyPathRecorder
    """
    The active key-path recorder for this thread. May not exist.
    """


_thread_local = cast("_ThreadLocalProtocol", threading.local())


@final
class _Terminals(NamedTuple):
    start: Any
    end: Any


@final
@dataclass
class _KeyPathRecorder:
    terminals: _Terminals | None = None
    key_list: list[str] = field(default_factory=list)
    busy: bool = False


# ! A metaclass is made for class `KeyPath`, and `KeyPath.of` is
# ! provided as a property on class `KeyPath`, so that whenever
# ! `KeyPath.of` gets accessed, we can do something before it actually
# ! gets called.
@final
class _KeyPathMeta(type):
    @property
    def of(self, /) -> _KeyPathOfFunction:
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
        ...     def __repr__(self) -> str:
        ...         return "a"

        >>> class B(KeyPathSupporting):
        ...     def __init__(self) -> None:
        ...         self.c = C()

        >>> class C:
        ...     pass

        >>> a = A()
        >>> KeyPath.of(a.b.c)
        KeyPath(target=a, keys=('b', 'c'))
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


# ! We implement the result of `KeyPath.of` as a stand-alone class, so
# ! that when an exception occurred during the key-path access, there
# ! would still be a chance to perform some finalization.
class _KeyPathOfFunction:
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
    ...     def __repr__(self) -> str:
    ...         return "a"

    >>> class B(KeyPathSupporting):
    ...     def __init__(self) -> None:
    ...         self.c = C()

    >>> class C:
    ...     pass

    >>> a = A()
    >>> KeyPath.of(a.b.c)
    KeyPath(target=a, keys=('b', 'c'))
    """

    __invoked: bool = False

    def __call__(self, value: _Value_t, /) -> KeyPath[_Value_t]:
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

        terminals = recorder.terminals
        key_list = recorder.key_list
        if terminals is None:
            assert len(key_list) == 0

            raise RuntimeError("No key has been recorded.")
        else:
            assert len(key_list) > 0

            if terminals.end is not value:
                raise RuntimeError(
                    " ".join(
                        [
                            "Key-path is broken. Check if there is something that does",
                            "NOT support key-paths in the member chain.",
                        ]
                    )
                )

        key_path = KeyPath(terminals.start, key_list)
        return key_path

    def __del__(self, /) -> None:
        # ! If an exception had occured during the key-path access, or
        # ! this function were just discarded without being finally
        # ! called, we would do some cleaning here.
        if not self.__invoked:
            del _thread_local.recorder


@final
class KeyPath(Generic[_Value_co], metaclass=_KeyPathMeta):
    __target: Final[object]
    __keys: Final[Sequence[str]]

    def __init__(self, /, target: object, keys: str | Sequence[str]) -> None:
        self.__target = target

        if isinstance(keys, str):
            keys = tuple(keys.split("."))
        else:
            keys = tuple(keys)
        self.__keys = keys

    @property
    def target(self, /) -> object:
        return self.__target

    @property
    def keys(self, /) -> Sequence[str]:
        return self.__keys

    def __hash__(self, /) -> int:
        return hash((self.target, self.keys))

    def __eq__(self, other: object, /) -> bool:
        return (
            isinstance(other, KeyPath)
            and self.target is other.target
            and self.keys == other.keys
        )

    def __repr__(self, /) -> str:
        return f"{KeyPath.__name__}(target={self.target!r}, keys={self.keys!r})"

    def __call__(self, /) -> _Value_co:
        value = self.__target
        for key in self.__keys:
            value = getattr(value, key)
        return cast("_Value_co", value)


class KeyPathSupporting:
    # ! This method is purposedly not named as `__getattribute__`. See below for reason.
    def __getattribute_0__(self, key: str, /) -> Any:
        try:
            recorder = _thread_local.recorder
        except AttributeError:
            # There is no recorder, which means that `KeyPath.of` is not
            # being called. So we don't need to record this key.
            return super().__getattribute__(key)

        if recorder.busy:
            # The recorder is busy, which means that another member is
            # being accessed, typically because the computation of that
            # member is dependent on this one. So we don't need to
            # record this key.
            return super().__getattribute__(key)

        recorder.busy = True

        terminals = recorder.terminals
        if terminals is not None and terminals.end is not self:
            raise RuntimeError(
                " ".join(
                    [
                        "Key-path is broken. Check if there is something that does NOT",
                        "support key-paths in the member chain.",
                    ]
                )
            )

        value = super().__getattribute__(key)

        if terminals is None:
            terminals = _Terminals(self, value)
        else:
            terminals = terminals._replace(end=value)

        recorder.terminals = terminals
        recorder.key_list.append(key)
        recorder.busy = False

        return value

    # ! `__getattribute__(...)` is declared against `TYPE_CHECKING`, so that unknown
    # ! attributes on conforming classes won't be regarded as known by type-checkers.
    if not TYPE_CHECKING:
        __getattribute__ = __getattribute_0__

    del __getattribute_0__
