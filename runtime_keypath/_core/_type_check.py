# pyright: reportUninitializedInstanceVariable = false
# pyright: reportUnnecessaryTypeIgnoreComment = true

######

from __future__ import annotations

__all__ = []

######

from typing_extensions import assert_type

from . import KeyPath

######


class Check__KeyPath:
    @staticmethod
    def check() -> None:
        class A:
            b: B

        class B:
            c: C

        class C:
            pass

        a = A()
        assert_type(KeyPath.of(a.b.c), KeyPath[C])
