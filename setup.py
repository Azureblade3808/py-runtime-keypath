# pyright: basic

import setuptools

setuptools.setup(
    packages=["runtime_keypath"],
    package_data={
        "runtime_keypath": ["py.typed"],
    },
)
