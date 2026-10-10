# runtime-keypath

[![PyPI - Python Version](https://img.shields.io/pypi/pyversions/runtime-keypath)](https://pypi.org/project/runtime-keypath/)
[![License](https://img.shields.io/badge/license-MIT-blue.svg)](LICENSE)

[English](README.md) | 中文

为 Python 提供运行时 key-path 的记录与访问能力。

## 概述

**Key-path**（键路径）就是一连串的成员访问，例如 `a.b.c`。`runtime-keypath` 让你在运行时捕获这条访问链——而不依赖字符串字面量——之后还能重新应用它来读取或写入目标值。

在需要引用对象结构中某个"位置"的场景下，这会很有用，比如构建声明式 API、编写校验规则，或者任何希望把 _访问什么_ 与 _如何访问_ 解耦的场合。

## 安装

```bash
pip install runtime-keypath
```

要求 Python 3.8+。

## 快速开始

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

>>> key_path.unsafe_set(C())

>>> isinstance(a.b.c, C)
True

```

## 特性

- **极简 API** — 只需一个 `KeyPath` 类即可。
- **无需字符串字面量** — 直接用自然的 `a.b.c` 语法，库会通过检查字节码自动推断路径。这意味着 IDE 重构（重命名属性、移动类等）可以无缝生效——把 `c` 改成 `d`，`KeyPath.of(a.b.d)` 调用就会自动更新；如果当初写的是 `"b.c"` 这个字符串，就做不到了。

## 原理

`KeyPath.of` 定义在 metaclass 上，是一个 **property**。这使得 `KeyPath.of(a.b.c)` 的调用过程分成两个独立阶段：

1. **阶段一 — 捕获准备（在参数求值之前）**
   访问 `KeyPath.of` 会触发 metaclass 的 property getter。它检查调用者的字节码，定位到加载 base（即 `a`）的那条 `LOAD_*` 指令，并从 locals/globals/builtins 中解析出实际对象。接着它临时把 `a.__class__` 替换为 `KeyRecorder`——这个类的 `__getattribute__` 只记录属性名然后返回自身，因此不会真的读取任何实际属性值。最后 property 返回一个可调用对象。

2. **阶段二 — 参数求值 + 收尾**
   Python 现在才开始求值 `a.b.c`。由于 `a.__class__` 是 `KeyRecorder`，每次 `.b`、`.c` 都只是往记录的 key 列表里追加一个字符串。阶段一返回的可调用对象随之被调用，产生 `KeyPath(base=a, keys=("b", "c"))`，并将 `a.__class__` 恢复为原来的值。

因为 recorder 的安装和清理都在同一个线程内完成，单线程使用是安全的。

## API 参考

### `KeyPath.of(value)`

捕获用于访问 `value` 的成员链。

| 参数    | 描述                                         |
| ------- | -------------------------------------------- |
| `value` | 通过链式成员访问（如 `a.b.c`）得到的一个值。 |

返回一个 `KeyPath` 实例，表示 base 对象和 key 序列。

### `KeyPath(base, keys)`

显式构造一个 `KeyPath`。`keys` 可以是字符串元组/列表，也可以是点分隔的字符串，如 `"b.c"`。

### 属性

- `key_path.base` — 路径起点的根对象。
- `key_path.keys` — 由成员名组成的 `tuple[str, ...]`。

### 方法

- `key_path.get(*, default=MISSING)` — 沿着 `keys` 从 `base` 逐级取值并返回最终值。如果中间某个 key 不存在：提供了 `default` 则返回它，否则抛出 `AttributeError`。
- `key_path()` — `.get()` 无默认值的简写。
- `key_path.unsafe_set(value)` — 沿路径走到倒数第二个 key，然后把最终属性设为 `value`。

## 注意事项

### 线程安全

`KeyPath.of` 在录制期间会临时修改 base 对象的 `__class__`。如果同一时刻有另一个线程在访问这个对象，可能会抛出异常。**单线程使用始终安全**，这也是库的预期和推荐用法。

### 限制

- `KeyPath.of(a)` — 完全没有成员访问的情况——不支持。
- `KeyPath.of(some_function(a.b.c))` — 任何不是纯成员链的写法都会报错。
- `KeyPath.of` 依赖 CPython 的字节码细节。它能在 CPython 3.8+ 上工作，在其他 Python 实现上或许也能跑，但不保证。

## 项目链接

- [主页](https://github.com/Azureblade3808/py-runtime-keypath)
- [Bug 追踪](https://github.com/Azureblade3808/py-runtime-keypath/issues)

## 许可证

[MIT](LICENSE)

---

_README中文版由 Seed-Code 基于英文版翻译，由 傅立业 审阅。_
