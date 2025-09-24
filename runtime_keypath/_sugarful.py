from __future__ import annotations

__all__ = ["_Mixin"]

######

from typing import TYPE_CHECKING, TypeVar

######

# ! We are performing some conditional imports here, so that the imported items would be
# ! regarded as potentially unbound and could be used in type annotations but not in
# ! code to run.
if 0:
    from typing_extensions import TypeForm

    from ._core import KeyPath

######

_T = TypeVar("_T")

######


class _MixinMeta(type):
    if TYPE_CHECKING:

        def __getitem__(self, key: TypeForm[_T], /) -> KeyPath[_T]:
            """
            Makes `KeyPath[...]` evaluate into a `KeyPath` object
            instead of a generic alias, so that `KeyPath[...] or ...`
            will have the desired type.
            """

            ...


class _Mixin(metaclass=_MixinMeta):
    """
    A mixin class that allows `KeyPath` to be used in a sugarful form.

    Examples
    --------
    >>> class A:
    ...     def __init__(self) -> None:
    ...         self.b = B()
    >>> class B:
    ...     def __init__(self) -> None:
    ...         self.c = C()
    >>> class C:
    ...     pass
    >>> a = A()
    >>> key_path = KeyPath[int] or a.b.c
    >>> assert key_path == KeyPath(base=a, keys=("b", "c"))
    ```
    """

    # TODO
