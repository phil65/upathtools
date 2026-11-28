"""Lazy re-exports for package ``__init__`` modules.

A barrel that imports every filesystem eagerly loads all of their modules (and the
SDK imports in them) for one ``import upathtools``. ``lazy_exports`` gives a package a
module ``__getattr__`` instead: each name loads its submodule on first access. Unlike
PEP 810's ``__lazy_modules__`` this also works on Python 3.14, and the
``package:Name`` entry points (fsspec, universal_pathlib) resolve through it as well.
"""

import functools
import importlib
import operator
from typing import TYPE_CHECKING, Any


if TYPE_CHECKING:
    from collections.abc import Callable


def lazy_exports(
    package: str,
    exports: dict[str, str],
    unions: dict[str, tuple[str, ...]] | None = None,
) -> tuple[Callable[[str], Any], Callable[[], list[str]]]:
    """Return ``__getattr__`` and ``__dir__`` for ``package``.

    Args:
        package: The package's ``__name__``.
        exports: Exported name -> submodule, relative to ``package`` (``".x.y"``).
        unions: Exported name -> names of other exports, joined with ``|`` on access
            (type aliases such as ``SandboxFilesystem``).
    """
    unions = unions or {}
    namespace = importlib.import_module(package).__dict__

    def __getattr__(name: str) -> Any:  # noqa: N807
        if name in exports:
            value = getattr(importlib.import_module(exports[name], package), name)
        elif name in unions:
            value = functools.reduce(operator.or_, (__getattr__(n) for n in unions[name]))
        else:
            msg = f"module {package!r} has no attribute {name!r}"
            raise AttributeError(msg)
        namespace[name] = value
        return value

    def __dir__() -> list[str]:  # noqa: N807
        return sorted({*namespace, *exports, *unions})

    return __getattr__, __dir__
