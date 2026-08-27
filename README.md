# Python Key-path

Supports runtime key-path recording/accessing for Python.

```python
from __future__ import annotations

from runtime_keypath import KeyPath, KeyPathSupporting

class A:
    b: B

class B:
    c: C

class C:
    pass

a = A()

# Note that although `a.b.c` will be an error, `KeyPath.of(a.b.c)` still works.
key_path = KeyPath.of(a.b.c)  
assert key_path == KeyPath(base=a, keys=["b", "c"])

b = B()
a.b = b
c = C()
b.c = c

assert key_path() is c
```
