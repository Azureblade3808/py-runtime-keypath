# Python Key-path

Supports runtime key-path recording/accessing for Python.

```python
from __future__ import annotations

from runtime_keypath import KeyPath, KeyPathSupporting

class A(KeyPathSupporting):
    def __init__(self) -> None:
        self.__b = B()

    @property
    def b(self) -> B:
        return self.__b

class B(KeyPathSupporting):
    def __init__(self) -> None:
        self.__c = C()

    @property
    def c(self) -> C:
        return self.__c

class C:
    pass

a = A()
key_path = KeyPath.of(a.b.c)
assert key_path.target is a and key_path.keys == ("b", "c")
assert key_path() is a.b.c
```
