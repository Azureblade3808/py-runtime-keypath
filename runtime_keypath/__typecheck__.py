"""
Makes sure that type-checking works as expected.
"""

# pyright: reportUnnecessaryTypeIgnoreComment = true

######

from __future__ import annotations

__all__ = []

######

from typing import Any

from typing_extensions import assert_type

from . import *

######


class Check__KeyPath:
    @staticmethod
    def check__sugarful(any_: Any, /) -> None:
        class A:
            b: B

        class B:
            c: C

        class C:
            pass

        a: A = any_
        __ = assert_type(KeyPath[C] or a.b.c, "KeyPath[C]")

    @staticmethod
    def check__sugarless(any_: Any, /) -> None:
        class A:
            b: B

        class B:
            c: C

        class C:
            pass

        a: A = any_
        __ = assert_type(KeyPath.of(a.b.c), "KeyPath[C]")
