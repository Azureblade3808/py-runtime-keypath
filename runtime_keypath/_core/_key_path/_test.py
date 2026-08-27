from __future__ import annotations

__all__ = []

######

import attrs
import pytest
from typing_extensions import Any, cast

from . import KeyPath

######


class Test:
    @staticmethod
    def test() -> None:
        class A:
            b: B

            def __init__(self, /) -> None:
                self.b = B()

        class B:
            c: int

            def __init__(self) -> None:
                self.c = 0

        a = A()
        key_path = KeyPath.of(a.b.c)
        assert key_path == KeyPath(base=a, keys=("b", "c"))
        assert key_path() == 0

        a.b.c = 1
        assert key_path() == 1

    @staticmethod
    def test__should_work_for_cycle_references() -> None:
        class A:
            a: A
            b: B

            def __init__(self) -> None:
                self.a = self
                self.b = B()

        class B:
            b: B
            c: C

            def __init__(self) -> None:
                self.b = self
                self.c = C()

        class C:
            pass

        a = A()
        assert KeyPath.of(a.a.b.b.c) == KeyPath(base=a, keys=("a", "b", "b", "c"))

    @staticmethod
    def test__should_raise_exceptions_for_common_mistakes() -> None:
        class A:
            b: B

            def __init__(self) -> None:
                self.b = B()

        class B:
            c: C

            def __init__(self) -> None:
                self.c = C()

        class C:
            pass

        a = A()

        with pytest.raises(Exception):
            # Not even accessed a single member.
            KeyPath.of(a)

        with pytest.raises(Exception):
            # Using something that is not a member chain.
            KeyPath.of(id(a.b.c))

        with pytest.raises(Exception):
            # Calling the same `KeyPath.of` more than once.
            of = KeyPath.of
            of(a.b.c)
            of(a.b.c)

    @staticmethod
    def test__should_not_record_keys_from_internal_references() -> None:
        class C:
            @property
            def v0(self) -> int:
                return self.v1.v2

            @property
            def v1(self) -> C:
                return self

            @property
            def v2(self) -> int:
                return 0

        c = C()
        assert KeyPath.of(c.v0) == KeyPath(base=c, keys=("v0",))

    @staticmethod
    def test__should_work_even_if_intermediate_values_are_missing() -> None:
        class A:
            b: B  # pyright: ignore[reportUninitializedInstanceVariable]

        class B:
            c: int  # pyright: ignore[reportUninitializedInstanceVariable]

        a = A()
        with pytest.raises(Exception):
            a.b.c

        key_path = KeyPath.of(a.b.c)
        assert key_path == KeyPath(base=a, keys=("b", "c"))

    @staticmethod
    def test__should_work_with_data_classes() -> None:
        @attrs.mutable
        class A:
            b: B

        @attrs.frozen
        class B:
            c: C

        class C:
            pass

        a = A(B(C()))
        assert KeyPath.of(a.b.c) == KeyPath(base=a, keys=("b", "c"))

    class Test__get:
        @staticmethod
        def test() -> None:
            MISSING = cast(Any, object())

            class A:
                b: B = MISSING

            class B:
                c: C = MISSING

            class C:
                v: int = MISSING

            a = A()
            b = B()
            c = C()

            key_path_0 = KeyPath.of(a.b)
            assert key_path_0.get() is MISSING
            a.b = b
            assert key_path_0.get() is b

            key_path_1 = KeyPath.of(a.b.c)
            assert key_path_1.get() is MISSING
            a.b.c = c
            assert key_path_1.get() is c

            key_path_2 = KeyPath.of(a.b.c.v)
            assert key_path_2.get() is MISSING
            a.b.c.v = 12345
            assert key_path_2.get() == 12345

        @staticmethod
        def test__should_work_with_default() -> None:
            MISSING = cast(Any, object())

            class A:
                b: B  # pyright: ignore[reportUninitializedInstanceVariable]

            class B:
                c: C  # pyright: ignore[reportUninitializedInstanceVariable]

            class C:
                v: int  # pyright: ignore[reportUninitializedInstanceVariable]

            a = A()
            b = B()
            c = C()

            key_path = KeyPath.of(a.b.c.v)
            assert key_path.get(default=MISSING) is MISSING

            a.b = b
            assert key_path.get(default=MISSING) is MISSING

            b.c = c
            assert key_path.get(default=MISSING) is MISSING

            c.v = 42
            assert key_path.get(default=MISSING) == 42

    class Test__unsafe_set:
        @staticmethod
        def test() -> None:
            MISSING = cast(Any, object())

            class A:
                b: B = MISSING

            class B:
                c: C = MISSING

            class C:
                v: int = MISSING

            a = A()
            b = B()
            c = C()

            assert a.b is MISSING
            key_path_0 = KeyPath.of(a.b)
            key_path_0.unsafe_set(b)
            assert a.b is b

            assert a.b.c is MISSING
            key_path_1 = KeyPath.of(a.b.c)
            key_path_1.unsafe_set(c)
            assert a.b.c is c

            assert a.b.c.v is MISSING
            key_path_2 = KeyPath.of(a.b.c.v)
            key_path_2.unsafe_set(12345)
            assert a.b.c.v == 12345
