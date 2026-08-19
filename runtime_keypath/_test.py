# pyright: reportDeprecated = false
# pyright: reportUnusedImport = false

######

from __future__ import annotations

__all__ = []

######


class Test:
    @staticmethod
    def test() -> None:
        global_dict = {}
        exec("from runtime_keypath import *", global_dict)
        assert "KeyPath" in global_dict
        assert "KeyPathSupporting" in global_dict
        assert "key_path_supporting" in global_dict
