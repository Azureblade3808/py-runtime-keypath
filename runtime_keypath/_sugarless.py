from __future__ import annotations

__all__ = [
    "_Mixin",
    "KeyPathSupporting",
    "key_path_supporting",
]

######

import threading
from typing import TYPE_CHECKING, Any, Final, Protocol, TypeVar, cast, final

######

# ! We are performing some conditional imports here, so that the imported items would be
# ! regarded as potentially unbound and could be used in type annotations but not in
# ! code to run.
if 0:
    from ._core import KeyPath

######

_V_t = TypeVar("_V_t")
_V_0 = TypeVar("_V_0")

######

_MISSING = cast("Any", object())

######


class _MixinMeta(type):
    """
    The metaclass for class `_Mixin`.

    It exists mainly to provide `KeyPath.of` as a property.
    """

    # ! `of` is provided as a property here, so that whenever `KeyPath.of` gets
    # ! accessed, we can do something before it actually gets called.
    @property
    def of(
        # ! `_MixinMeta` is the metaclass of only `_Mixin`, and `_Mixin` is directly
        # ! inherited by only `KeyPath`, so the typing is safe here.
        self: type[KeyPath],  # pyright: ignore[reportGeneralTypeIssues]
        /,
    ) -> _KeyPathOfFunction:
        # ! The docstring here is in fact for `_MixinMeta._KeyPathOfFunction`, but it
        # ! will help Pylance to display some nice hint for `KeyPath.of`.
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
            __ = _thread_local.recorder
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

        func = _MixinMeta._KeyPathOfFunction(self)
        return func

    class _KeyPathOfFunction:
        """
        The class of `_MixinMeta.of`.

        We implement the result of `KeyPath.of` as a callable object, so that when an
        exception occurred during the key-path access, there would still be a chance to
        perform some finalization.

        Note
        ----
        This docstring will be overwritten soon.
        """

        __key_path_type: Final[type[KeyPath]]

        def __init__(self, key_path_type: type[KeyPath], /) -> None:
            self.__key_path_type = key_path_type

        __called: bool = False

        def __call__(self, value: _V_0, /) -> KeyPath[_V_0]:
            KeyPath = self.__key_path_type

            self.__called = True

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
            if not self.__called:
                del _thread_local.recorder

    # Copy docstring so that it can be displayed upon calling `help(KeyPath.of)`.
    _KeyPathOfFunction.__doc__ = of.__doc__


class _Mixin(metaclass=_MixinMeta):
    """
    A mixin class that allows `KeyPath` to be used in a sugarless form.

    Examples
    --------
    >>> class A:
    ...     b: B
    ...     def __init__(self, /) -> None:
    ...         self.b = B()

    >>> class B:
    ...     c: int
    ...     def __init__(self, /) -> None:
    ...         self.c = 0

    >>> a = A()

    >>> key_path = KeyPath.of(a.b.c)

    >>> assert key_path.base is a

    >>> assert key_path.keys == ("b", "c")

    """


######


class _ThreadLocalProtocol(Protocol):
    recorder: _KeyPathRecorder
    """
    The active key-path recorder for this thread. May not exist.
    """


_thread_local = cast("_ThreadLocalProtocol", threading.local())


######


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


######


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

    # ! This method is intentially not named as `__getattribute__`. See reason below.
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
    # ! attributes on subclasses won't be treated as known by type-checkers.
    if not TYPE_CHECKING:
        __getattribute__ = _

    del _


def key_path_supporting(clazz: type[_V_t], /) -> type[_V_t]:
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

    def __getattribute__(self: _V_t, key: str) -> Any:
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
