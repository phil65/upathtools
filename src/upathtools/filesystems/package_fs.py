"""Filesystem implementation for browsing a single Python package."""

from __future__ import annotations

import importlib.util
import os
import pkgutil
from typing import TYPE_CHECKING, Any, Literal, overload

import fsspec

from upathtools.filesystems.base import BaseFileSystem, BaseUPath


if TYPE_CHECKING:
    from collections.abc import Sequence
    import types
    from types import ModuleType


class PackagePath(BaseUPath):
    """UPath implementation for browsing Python packages."""

    __slots__ = ()

    def iterdir(self):
        if not self.is_dir():
            raise NotADirectoryError(str(self))
        yield from super().iterdir()

    @property
    def path(self) -> str:
        path = super().path
        return "/" if path == "." else path


class PackageFS(BaseFileSystem[PackagePath]):
    """Filesystem for browsing a single package's structure."""

    protocol = "pkg"
    upath_cls = PackagePath

    def __init__(
        self,
        package: str | types.ModuleType = "",
        **kwargs: Any,
    ) -> None:
        """Initialize the filesystem.

        Args:
            package: Name of the package to browse (e.g., "requests")
            kwargs: Additional keyword arguments for the filesystem
        """
        super().__init__(**kwargs)
        if not package:
            msg = "Package name required"
            raise ValueError(msg)

        self.package = package if isinstance(package, str) else package.__name__
        self._module_cache: dict[str, ModuleType] = {}

    @staticmethod
    def _get_kwargs_from_urls(path):
        path = path.removeprefix("pkg://")
        return {"package": path}

    def _get_module(self, module_name: str) -> ModuleType:
        """Get or import a module."""
        if module_name in self._module_cache:
            return self._module_cache[module_name]

        try:
            module = importlib.import_module(module_name)
            self._module_cache[module_name] = module
        except ImportError as exc:
            msg = f"Module {module_name} not found"
            raise FileNotFoundError(msg) from exc
        return module

    @overload
    def ls(
        self, path: str, detail: Literal[True] = True, **kwargs: Any
    ) -> list[dict[str, Any]]: ...

    @overload
    def ls(self, path: str, detail: Literal[False], **kwargs: Any) -> list[str]: ...

    def ls(
        self,
        path: str,
        detail: bool = True,
        **kwargs: Any,
    ) -> Sequence[str | dict[str, Any]]:
        """List contents of a path within the package."""
        path = path.removesuffix(".py")
        path = self._strip_protocol(path).strip("/")  # type: ignore

        # Construct full module name
        module_name = self.package
        if path:
            module_name = f"{module_name}.{path.replace('/', '.')}"

        try:
            module = self._get_module(module_name)
            contents: list[str | dict[str, Any]] = []

            if hasattr(module, "__path__"):
                # List submodules if it's a package
                for item in pkgutil.iter_modules(module.__path__):
                    if not detail:
                        contents.append(item.name)
                    else:
                        sub_name = f"{module_name}.{item.name}"
                        sub_module = self._get_module(sub_name)
                        contents.append({
                            "name": item.name,
                            "type": "package" if item.ispkg else "module",
                            "size": 0 if item.ispkg else 1,
                            "mtime": self._get_mtime(sub_module),
                            "doc": sub_module.__doc__,
                        })
        except ImportError as exc:
            msg = f"Cannot access {path}"
            raise FileNotFoundError(msg) from exc
        else:
            return contents

    def _get_mtime(self, module: ModuleType) -> float | None:
        """Get modification time of a module."""
        try:
            if hasattr(module, "__file__") and module.__file__:
                return os.path.getmtime(module.__file__)  # noqa: PTH204
        except (OSError, AttributeError):
            pass
        return None

    def info(self, path: str, **kwargs: Any) -> dict[str, Any]:
        """Get info about a path."""
        path = self._strip_protocol(path).strip("/")  # type: ignore

        # Construct full module name
        module_name = self.package
        if path:
            module_name = f"{module_name}.{path.replace('/', '.')}"

        try:
            module = self._get_module(module_name)
            type_ = "package" if hasattr(module, "__path__") else "module"

            return {
                "name": module_name.split(".")[-1],
                "type": type_,
                "size": 0 if type_ == "package" else 1,
                "mtime": self._get_mtime(module),
                "doc": module.__doc__,
            }
        except ImportError as exc:
            msg = f"Path {path} not found"
            raise FileNotFoundError(msg) from exc

    def cat(self, path: str) -> bytes:
        """Get module file content."""
        path = path.removesuffix(".py")
        path = self._strip_protocol(path).strip("/")  # type: ignore

        # Construct full module name
        module_name = self.package
        if path:
            module_name = f"{module_name}.{path.replace('/', '.')}"

        try:
            module = self._get_module(module_name)
            if not module.__file__:
                msg = f"No source file for {path}"
                raise FileNotFoundError(msg)

            with fsspec.open(module.__file__, "rb") as f:
                return f.read()  # type: ignore
        except ImportError as exc:
            msg = f"Cannot read source of {path}"
            raise FileNotFoundError(msg) from exc


if __name__ == "__main__":
    # Create a filesystem instance
    from upath import UPath

    fs = UPath("pkg://pydantic")
    print(list(fs.iterdir()))
