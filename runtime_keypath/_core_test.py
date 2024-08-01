from __future__ import annotations

import time
from threading import Thread
from typing import Any, cast

import pytest

from ._core import *


class Tests__KeyPathSupporting:
    @staticmethod
    def test__normal() -> None:
        class A(KeyPathSupporting):
            b: B

            def __init__(self) -> None:
                self.b = B()

        class B(KeyPathSupporting):
            c: int

            def __init__(self) -> None:
                self.c = 0

        a = A()
        key_path = KeyPath.of(a.b.c)
        assert key_path == KeyPath(target=a, keys=("b", "c"))
        assert key_path() == 0

        a.b.c = 1
        assert key_path() == 1

    @staticmethod
    def test__cycle_reference() -> None:
        class A(KeyPathSupporting):
            a: A
            b: B

            def __init__(self) -> None:
                self.a = self
                self.b = B()

        class B(KeyPathSupporting):
            b: B
            c: C

            def __init__(self) -> None:
                self.b = self
                self.c = C()

        class C:
            pass

        a = A()
        assert KeyPath.of(a.a.b.b.c) == KeyPath(target=a, keys=("a", "b", "b", "c"))

    @staticmethod
    def test__common_mistakes() -> None:
        class A(KeyPathSupporting):
            b: B

            def __init__(self) -> None:
                self.b = B()

        class B(KeyPathSupporting):
            c: C

            def __init__(self) -> None:
                self.c = C()

        class C:
            pass

        a = A()

        with pytest.raises(Exception):
            # Not even accessed a single member.
            _ = KeyPath.of(a)

        with pytest.raises(Exception):
            # Using something that is not a member chain.
            _ = KeyPath.of(id(a.b.c))

        with pytest.raises(Exception):
            # Calling the same `KeyPath.of` more than once.
            of = KeyPath.of
            _ = of(a.b.c)
            _ = of(a.b.c)

    @staticmethod
    def test__error_handling() -> None:
        class A(KeyPathSupporting):
            b: B

            def __init__(self) -> None:
                self.b = B()

        class B(KeyPathSupporting):
            c: C

            def __init__(self) -> None:
                self.c = C()

        class C:
            pass

        a = A()

        with pytest.raises(AttributeError):
            # Accessing something that doesn't exist.
            _ = KeyPath.of(a.b.c.d)  # type: ignore

        # With above exception caught, normal code should run correctly.
        key_path = KeyPath.of(a.b.c)
        assert key_path == KeyPath(target=a, keys=("b", "c"))

    @staticmethod
    def test__threading() -> None:
        class A(KeyPathSupporting):
            b: B

            def __init__(self) -> None:
                self.b = B()

        class B(KeyPathSupporting):
            c: C

            def __init__(self) -> None:
                self.c = C()

        class C:
            pass

        a = A()
        key_path_list: list[KeyPath] = []

        def f() -> None:
            # Sleeping for a short while so that the influence of starting a thread
            # could be minimal.
            time.sleep(1)

            key_path = KeyPath.of(a.b.c)
            key_path_list.append(key_path)

        threads = [Thread(target=f) for _ in range(1000)]
        for thread in threads:
            thread.start()
        for thread in threads:
            thread.join()

        assert len(key_path_list) == 1000
        assert all(
            key_path == KeyPath(target=a, keys=("b", "c")) for key_path in key_path_list
        )

    @staticmethod
    def test__internal_reference() -> None:
        class C(KeyPathSupporting):
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
        assert KeyPath.of(c.v0) == KeyPath(target=c, keys=("v0",))

    @staticmethod
    def test__get() -> None:
        MISSING = cast(Any, object())

        class A(KeyPathSupporting):
            b: B = MISSING

        class B(KeyPathSupporting):
            c: C = MISSING

        class C(KeyPathSupporting):
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
    def test__unsafe_set() -> None:
        MISSING = cast(Any, object())

        class A(KeyPathSupporting):
            b: B = MISSING

        class B(KeyPathSupporting):
            c: C = MISSING

        class C(KeyPathSupporting):
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


class Tests__key_path_supporting:
    @staticmethod
    def test__normal() -> None:
        @key_path_supporting
        class A:
            b: B

            def __init__(self) -> None:
                self.b = B()

        @key_path_supporting
        class B:
            c: int

            def __init__(self) -> None:
                self.c = 0

        a = A()
        key_path = KeyPath.of(a.b.c)
        assert key_path == KeyPath(target=a, keys=("b", "c"))
        assert key_path() == 0

        a.b.c = 1
        assert key_path() == 1

    @staticmethod
    def test__cycle_reference() -> None:
        @key_path_supporting
        class A:
            a: A
            b: B

            def __init__(self) -> None:
                self.a = self
                self.b = B()

        @key_path_supporting
        class B:
            b: B
            c: C

            def __init__(self) -> None:
                self.b = self
                self.c = C()

        class C:
            pass

        a = A()
        assert KeyPath.of(a.a.b.b.c) == KeyPath(target=a, keys=("a", "b", "b", "c"))

    @staticmethod
    def test__common_mistakes() -> None:
        @key_path_supporting
        class A:
            b: B

            def __init__(self) -> None:
                self.b = B()

        @key_path_supporting
        class B(KeyPathSupporting):
            c: C

            def __init__(self) -> None:
                self.c = C()

        class C:
            pass

        a = A()

        with pytest.raises(Exception):
            # Not even accessed a single member.
            _ = KeyPath.of(a)

        with pytest.raises(Exception):
            # Using something that is not a member chain.
            _ = KeyPath.of(id(a.b.c))

        with pytest.raises(Exception):
            # Calling the same `KeyPath.of` more than once.
            of = KeyPath.of
            _ = of(a.b.c)
            _ = of(a.b.c)

    @staticmethod
    def test__error_handling() -> None:
        @key_path_supporting
        class A:
            b: B

            def __init__(self) -> None:
                self.b = B()

        @key_path_supporting
        class B(KeyPathSupporting):
            c: C

            def __init__(self) -> None:
                self.c = C()

        class C:
            pass

        a = A()

        with pytest.raises(AttributeError):
            # Accessing something that doesn't exist.
            _ = KeyPath.of(a.b.c.d)  # type: ignore

        # With above exception caught, normal code should run correctly.
        key_path = KeyPath.of(a.b.c)
        assert key_path == KeyPath(target=a, keys=("b", "c"))

    @staticmethod
    def test__threading() -> None:
        @key_path_supporting
        class A:
            b: B

            def __init__(self) -> None:
                self.b = B()

        @key_path_supporting
        class B:
            c: C

            def __init__(self) -> None:
                self.c = C()

        class C:
            pass

        a = A()
        key_path_list: list[KeyPath] = []

        def f() -> None:
            # Sleeping for a short while so that the influence of starting a thread
            # could be minimal.
            time.sleep(1)

            key_path = KeyPath.of(a.b.c)
            key_path_list.append(key_path)

        threads = [Thread(target=f) for _ in range(1000)]
        for thread in threads:
            thread.start()
        for thread in threads:
            thread.join()

        assert len(key_path_list) == 1000
        assert all(
            key_path == KeyPath(target=a, keys=("b", "c")) for key_path in key_path_list
        )

    @staticmethod
    def test__internal_reference() -> None:
        @key_path_supporting
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
        assert KeyPath.of(c.v0) == KeyPath(target=c, keys=("v0",))

    @staticmethod
    def test__get() -> None:
        MISSING = cast(Any, object())

        @key_path_supporting
        class A:
            b: B = MISSING

        @key_path_supporting
        class B:
            c: C = MISSING

        @key_path_supporting
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
    def test__unsafe_set() -> None:
        MISSING = cast(Any, object())

        @key_path_supporting
        class A:
            b: B = MISSING

        @key_path_supporting
        class B:
            c: C = MISSING

        @key_path_supporting
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
