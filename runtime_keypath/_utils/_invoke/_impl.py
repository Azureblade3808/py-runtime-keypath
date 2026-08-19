from __future__ import annotations

__all__ = [
    "invoke",
]

######

from collections.abc import Callable
from typing import TypeVar

######

Result_t0 = TypeVar("Result_t0")

######


def invoke(func: Callable[[], Result_t0], /) -> Result_t0:
    return func()