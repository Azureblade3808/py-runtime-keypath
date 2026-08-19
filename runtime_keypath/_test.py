# pyright: reportDeprecated = false
# pyright: reportUnusedImport = false

######

from __future__ import annotations

__all__ = []

######


class Test:
    @staticmethod
    def test() -> None:
        from . import KeyPath, KeyPathSupporting, supports_key_path
