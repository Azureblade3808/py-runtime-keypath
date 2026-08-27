from __future__ import annotations

__all__ = [
    "KeyPath",
]

######

import dis
import inspect
from bisect import bisect_right
from collections.abc import Sequence
from threading import current_thread
from typing import TYPE_CHECKING, Any, Final, Generic, Protocol, TypeVar, cast, overload

from runtime_keypath._utils import invoke

######

MISSING = cast(Any, object())

######

Value_co = TypeVar("Value_co", covariant=True)
Value_t0 = TypeVar("Value_t0")

Default_t0 = TypeVar("Default_t0")

######


class KeyPathOfFunction(Protocol):
    def __call__(self, value: Value_t0, /) -> KeyPath[Value_t0]: ...


######


class KeyPathMeta(type):
    """
    The metaclass for class `KeyPath`.

    It exists mainly to provide `KeyPath.of` as a property.
    """

    # ! `of` is provided as a property here, so that whenever `KeyPath.of` gets accessed, we can do something before it
    # ! actually gets called.
    @property
    def of(self, /) -> KeyPathOfFunction:
        frame = inspect.currentframe()
        if frame is not None:
            frame = frame.f_back
        if frame is None:
            raise ValueError("Cannot find the caller frame.")

        local_dict = frame.f_locals
        global_dict = frame.f_globals
        builtin_dict = frame.f_builtins

        ######

        @invoke
        def base() -> Any:
            instructions = tuple(dis.get_instructions(frame.f_code))
            pending_instructions = instructions[
                bisect_right([instruction.offset for instruction in instructions], frame.f_lasti) :
            ]

            for instruction in pending_instructions:
                opname = instruction.opname

                if "CALL" in opname:
                    return MISSING

                if "LOAD" in opname:
                    argval = instruction.argval

                    if opname == "LOAD_GLOBAL":
                        dicts = [global_dict, builtin_dict]
                        key = argval
                    elif opname in (
                        "LOAD_CLOSURE",
                        "LOAD_DEREF",
                        "LOAD_FAST",
                        "LOAD_FAST_AND_CLEAR",
                        "LOAD_FAST_BORROW",
                        "LOAD_FAST_CHECK",
                    ):
                        dicts = [local_dict]
                        key = argval
                    elif opname == "STORE_FAST_LOAD_FAST":
                        dicts = [local_dict]
                        key = argval[1]
                    elif opname == "LOAD_NAME":
                        dicts = [local_dict, global_dict, builtin_dict]
                        key = argval
                    else:
                        return MISSING

                    for dict_ in dicts:
                        value = dict_.get(key, MISSING)
                        if value is not MISSING:
                            return value
                    else:
                        return MISSING
            else:
                return MISSING

        if base is MISSING:
            raise ValueError("Unsupported access pattern.")

        ######

        base_class = base.__class__

        thread = current_thread()
        key_list = []

        class KeyRecorder(base_class):
            __slots__ = ()

            def __getattribute__(self, name: str) -> Any:
                if current_thread() is not thread:
                    raise ValueError("`KeyPath.of` argument gets accessed from a different thread.")

                key_list.append(name)
                return self

            def __getattr__(self, name: str) -> Any:
                raise NotImplementedError

            def __setattr__(self, name: str, value: Any) -> None:
                raise NotImplementedError

            def __delattr__(self, name: str) -> None:
                raise NotImplementedError

        if base_class.__module__ == KeyRecorder.__module__ and base_class.__qualname__ == KeyRecorder.__qualname__:
            raise ValueError("`KeyPath.of` argument gets accessed from a different thread.")

        base.__class__ = KeyRecorder

        ######

        # ! `key_path_of_function` is a callable object instead of a plain function, so that we can still do some clean
        # ! up even if it doesn't actually get called (e.g. a keyboard interrupt just in time).
        @invoke
        class key_path_of_function:
            __has_cleaned_up: bool = False

            def __clean_up_if_needed(self, /) -> None:
                if not self.__has_cleaned_up:
                    object.__setattr__(base, "__class__", base_class)

            def __call__(self, value: Any, /) -> KeyPath[Any]:
                key_path = KeyPath(base, key_list)
                self.__clean_up_if_needed()
                return key_path

            def __del__(self, /) -> None:
                self.__clean_up_if_needed()

        ######

        return key_path_of_function


######


class KeyPath(Generic[Value_co], metaclass=KeyPathMeta):
    """
    An object that stands for a member chain from a base object.
    """

    ######

    # NOTE: This method is actually provided as a property of `KeyPathMeta`.
    @classmethod
    def of(cls, value: Value_t0, /) -> KeyPath[Value_t0]:
        # ! The docstring here is in fact for `KeyPathMeta.of`, but it will help Pylance to display some nice hint for
        # ! `KeyPath.of`.
        """
        Returns the key-path for accessing a certain value from a target
        object with a key sequence such as `a.b.c`.

        Parameters
        ----------
        `value`
            A value that is accessed with chained keys such as `a.b.c`.

        Returns
        -------
        A key-path that indicates the target object and the key sequence
        to access the given value.

        Example
        -------
        >>> class A:
        ...     def __init__(self, /) -> None:
        ...         self.b = B()
        >>> class B:
        ...     def __init__(self, /) -> None:
        ...         self.c = C()
        >>> class C:
        ...     pass
        >>> a = A()
        >>> key_path = KeyPath.of(a.b.c)
        >>> assert key_path.base is a
        >>> assert key_path.keys == ("b", "c")

        Warning
        -------
        The base object will get temporarily polluted (for its
        `__class__` is replaced) during the key-path evaluation. Just
        before the key-path object is returned, the base object will be
        restored to its original state.

        It is important that the base object not be used in another
        thread during the process, or an exception may be raised.
        Single-threaded usage is always safe though.
        """

        ...

    KeyPathMeta.of.__doc__ = of.__doc__

    if not TYPE_CHECKING:
        del of

    ######

    __base: Final[Any]
    __keys: Final[tuple[str, ...]]

    def __init__(self, /, base: Any, keys: str | Sequence[str]) -> None:
        keys = tuple(keys.split(".") if isinstance(keys, str) else keys)
        if len(keys) == 0:
            raise ValueError("Empty `keys`.")

        self.__base = base
        self.__keys = keys

    ######

    @property
    def base(self, /) -> Any:
        return self.__base

    @property
    def keys(self, /) -> tuple[str, ...]:
        return self.__keys

    ######

    @overload
    def get(self, /) -> Value_co: ...
    @overload
    def get(self, /, *, default: Default_t0) -> Value_co | Default_t0: ...

    def get(self, /, *, default: Any = MISSING) -> Any:
        """
        Returns the value from the end-point of this key-path.

        When any key in the key path does not exist, the `default`
        argument is returned if it is provided, otherwise an
        `AttributeError` is raised.
        """

        if default is MISSING:
            return self.__get()
        else:
            return self.__get_with_default(default)

    def __get(self, /) -> Any:
        value = self.__base
        for key in self.__keys:
            value = getattr(value, key)
        return value

    def __get_with_default(self, default: Any, /) -> Any:
        value = self.__base
        for key in self.__keys:
            value = getattr(value, key, MISSING)
            if value is MISSING:
                return default
        return value

    # A convenient alias for `__get` (non-default version of `get`).
    __call__ = __get

    ######

    def unsafe_set(self: KeyPath[Value_t0], value: Value_t0, /) -> None:
        """
        Set a value to the end-point of this key-path.

        WARNING
        -------
        This method is unsafe, primarily in two ways:

        1.  It may raise exceptions if any key in the key-path doesn't allow writing.
        2.  It breaks Liskov substitution principle.
        """

        base = self.__base
        keys = self.__keys

        parent = base
        for key in keys[:-1]:
            parent = getattr(parent, key)

        key = keys[-1]
        setattr(parent, key, value)

    ######

    def __hash__(self, /) -> int:
        return hash((id(self.__base), self.__keys))

    def __eq__(self, other, /) -> bool:
        return isinstance(other, KeyPath) and self.__base is other.__base and self.__keys == other.__keys
