from __future__ import annotations

__all__ = [
    "KeyPath",
]

######

import dis
import inspect
from bisect import bisect_right
from collections.abc import Sequence
from itertools import islice
from typing import TYPE_CHECKING, Any, Final, Generic, Protocol, TypeVar

from .._utils import invoke

######

Value_co = TypeVar("Value_co", covariant=True)
Value_t0 = TypeVar("Value_t0")

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
        """

        ######

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
        def key_path() -> KeyPath[Any] | None:
            instructions = list(dis.get_instructions(frame.f_code, first_line=frame.f_lineno))
            islice_instructions = islice(
                instructions,
                bisect_right(instructions, frame.f_lasti, key=(lambda instruction: instruction.offset)),
                None,
            )

            instruction = next(islice_instructions, None)
            while instruction is not None and instruction.opname == "PUSH_NULL":
                instruction = next(islice_instructions, None)
            if instruction is None:
                return None
            opname = instruction.opname
            argval = instruction.argval
            if opname == "LOAD_NAME":
                for dict_ in (local_dict, global_dict, builtin_dict):
                    try:
                        base = dict_[argval]
                    except KeyError:
                        pass
                    else:
                        break
                else:
                    return None
            elif opname in ("LOAD_GLOBAL"):
                base = global_dict[argval]
            elif opname in ("LOAD_FAST", "LOAD_FAST_BORROW", "LOAD_DEREF"):
                base = local_dict[argval]
            elif opname == "STORE_FAST_LOAD_FAST":
                base = local_dict[argval[1]]
            else:
                return None

            keys = []
            while True:
                instruction = next(islice_instructions, None)
                if instruction is None:
                    return None
                opname = instruction.opname
                if opname == "LOAD_ATTR":
                    keys.append(instruction.argval)
                elif opname in ("LOAD_FAST_BORROW", "PUSH_NULL", "STORE_FAST_LOAD_FAST"):
                    pass
                elif opname in ("CALL", "CALL_METHOD"):
                    break
                else:
                    return None

            key_path = KeyPath(base, keys)
            return key_path

        if key_path is None:
            raise ValueError("Unsupported access pattern.")

        ######

        result = key_path

        def key_path_of_function(value: Value_t0, /) -> KeyPath[Value_t0]:
            return result

        key_path_of_function.__doc__ = KeyPathMeta.of.__doc__

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
        """

        ...

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

    def get(self, /) -> Value_co:
        """
        Get the value from the end-point of this key-path.
        """

        value = self.__base
        for key in self.__keys:
            value = getattr(value, key)
        return value

    # A convenient alias for `get`.
    __call__ = get

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
