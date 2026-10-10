# runtime-keypath

[![PyPI - Python Version](https://img.shields.io/pypi/pyversions/runtime-keypath)](https://pypi.org/project/runtime-keypath/)
[![License](https://img.shields.io/badge/license-MIT-blue.svg)](LICENSE)

Supports runtime key-path recording and accessing for Python.

English | [中文](README_zh.md)

## Overview

A **key-path** is a chain of member accesses such as `a.b.c`. `runtime-keypath` lets you capture that chain at runtime — without relying on string literals — and re-apply it later to read or write the target value.

This is useful for scenarios where you need to refer to a "location" inside an object structure, for example when building declarative APIs, validation rules, or any situation where you want to decouple _what_ to access from _how_ to access it.

## Installation

```bash
pip install runtime-keypath
```

Requires Python 3.8+.

## Quick Start

```python
>>> from runtime_keypath import KeyPath

>>> class C:
...     pass

>>> class B:
...     def __init__(self):
...         self.c = C()

>>> class A:
...     def __init__(self):
...         self.b = B()

>>> a = A()

>>> key_path = KeyPath.of(a.b.c)

>>> key_path.base is a
True

>>> key_path.keys == ("b", "c")
True

>>> key_path() is a.b.c
True

>>> key_path.get() is a.b.c
True

>>> key_path.unsafe_set(None)

>>> a.b.c is None
True

```

## Features

- **Minimal API** — a single `KeyPath` class is all you need.
- **No string literals** — use the natural `a.b.c` syntax; the library figures out the path by inspecting bytecode. This means IDE refactorings (renaming an attribute, moving a class, etc.) work out of the box — rename `c` to `d` and your `KeyPath.of(a.b.d)` call updates automatically, which would be impossible if you had written `"b.c"` as a string.

## How It Works

`KeyPath.of` is defined as a **property on the metaclass**. This makes the call `KeyPath.of(a.b.c)` evaluate in two distinct phases:

1. **Phase 1 — capture setup (before argument evaluation)**
   Accessing `KeyPath.of` fires the metaclass property getter. It inspects the caller's bytecode, locates the `LOAD_*` instruction that loaded the base (`a`), and resolves the actual object from locals/globals/builtins. It then temporarily replaces `a.__class__` with a `KeyRecorder` — a class whose `__getattribute__` records every attribute name and returns itself, so no real attribute values are ever read. Finally, the property returns a callable.

2. **Phase 2 — argument evaluation + finalization**
   Python now evaluates `a.b.c`. Since `a.__class__` is `KeyRecorder`, each `.b`, `.c` just appends a string to the recorded key list. The callable returned in Phase 1 is then invoked, producing `KeyPath(base=a, keys=("b", "c"))` and restoring `a.__class__` to its original value.

Because the recorder is installed and cleaned up in the same thread, single-threaded usage is safe.

## API Reference

### `KeyPath.of(value)`

Capture the member chain used to reach `value`.

| Parameter | Description                                                 |
| --------- | ----------------------------------------------------------- |
| `value`   | A value obtained via chained member access such as `a.b.c`. |

Returns a `KeyPath` instance representing the base object and the key sequence.

### `KeyPath(base, keys)`

Construct a `KeyPath` explicitly. `keys` may be a tuple/list of strings or a dot-separated string like `"b.c"`.

### Properties

- `key_path.base` — the root object from which the path starts.
- `key_path.keys` — a `tuple[str, ...]` of member names forming the chain.

### Methods

- `key_path.get(*, default=MISSING)` — walks `base` through `keys` and returns the final value. If any key is missing and `default` is provided, returns `default`; otherwise raises `AttributeError`.
- `key_path()` — shorthand for `.get()` (no default).
- `key_path.unsafe_set(value)` — walks the path up to the second-to-last key and sets the final attribute to `value`.

## Warnings

### Thread-safety

`KeyPath.of` temporarily mutates the base object's `__class__` during recording. If the same object is accessed from another thread at that moment, an exception may be raised. **Single-threaded usage is always safe**, and this is the expected and recommended way to use the library.

### Limitations

- `KeyPath.of(a)` — with no member access at all — is not supported.
- `KeyPath.of(some_function(a.b.c))` — anything that isn't a pure member chain will raise an error.
- `KeyPath.of` relies on CPython bytecode details. It works on CPython 3.8+ and may work on other Python implementations, but this is not guaranteed.

## Project Links

- [Homepage](https://github.com/Azureblade3808/py-runtime-keypath)
- [Bug Tracker](https://github.com/Azureblade3808/py-runtime-keypath/issues)

## License

[MIT](LICENSE)

---

_README drafted by Seed-Code. Reviewed by Chris Fu._
