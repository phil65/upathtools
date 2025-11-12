"""Filesystem implementation for browsing Python packages hierarchically."""

from __future__ import annotations

import importlib.metadata
import importlib.util
import os
import pkgutil
from typing import TYPE_CHECKING, Any, Literal, overload

import fsspec

from upathtools.filesystems.base import BaseAsyncFileSystem, BaseUPath


if TYPE_CHECKING:
    from collections.abc import Sequence
    from types import ModuleType


def _get_mtime(module: ModuleType) -> float | None:
    """Get modification time of a module."""
    try:
        if hasattr(module, "__file__") and module.__file__:
            return os.path.getmtime(module.__file__)  # noqa: PTH204
    except (OSError, AttributeError):
        pass
    return None


class DistributionPath(BaseUPath):
    """UPath implementation for browsing Python distributions."""

    __slots__ = ()

    def iterdir(self):
        if not self.is_dir():
            raise NotADirectoryError(str(self))
        yield from super().iterdir()

    @property
    def path(self) -> str:
        path = super().path
        return "/" if path == "." else path


class DistributionFS(BaseAsyncFileSystem[DistributionPath]):
    """Hierarchical filesystem for browsing Python packages."""

    protocol = "distribution"
    upath_cls = DistributionPath

    def __init__(self, *args: Any, **storage_options: Any) -> None:
        """Initialize the filesystem."""
        super().__init__(*args, **storage_options)
        self._module_cache: dict[str, ModuleType] = {}

    def _normalize_path(self, path: str) -> str:
        """Convert any path format to internal path format."""
        clean_path = self._strip_protocol(path).strip("/")  # type: ignore
        if not clean_path:
            return ""
        return clean_path.replace(".", "/")

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
        """List contents of a path."""
        norm_path = self._normalize_path(path)

        if not norm_path:
            return self._list_packages(detail)

        # Convert path to module name
        module_name = norm_path.replace("/", ".")
        try:
            module = self._get_module(module_name)
            contents: list[str | dict[str, Any]] = []

            if hasattr(module, "__path__"):
                # List submodules if it's a package
                for item in pkgutil.iter_modules(module.__path__):
                    full_name = f"{module_name}.{item.name}"
                    if not detail:
                        contents.append(item.name)
                    else:
                        sub_module = self._get_module(full_name)
                        contents.append({
                            "name": item.name,
                            "type": "package" if item.ispkg else "module",
                            "size": 0 if item.ispkg else 1,
                            "mtime": _get_mtime(sub_module),
                            "doc": sub_module.__doc__,
                        })
        except ImportError as exc:
            msg = f"Cannot access {path}"
            raise FileNotFoundError(msg) from exc
        else:
            return contents

    def _list_packages(self, detail: bool) -> list[dict[str, Any]] | list[str]:
        """List all installed packages."""
        packages = list(importlib.metadata.distributions())

        if not detail:
            return [pkg.metadata["Name"] for pkg in packages]

        return [
            {
                "name": pkg.metadata["Name"],
                "type": "package",
                "size": 0,
                "version": pkg.version,
                "mtime": None,
            }
            for pkg in packages
        ]

    def info(self, path: str, **kwargs: Any) -> dict[str, Any]:
        """Get info about a path."""
        norm_path = self._normalize_path(path)

        if not norm_path:
            return {"name": "", "type": "directory", "size": 0}

        module_name = norm_path.replace("/", ".")
        try:
            module = self._get_module(module_name)
            type_ = "package" if hasattr(module, "__path__") else "module"

            return {
                "name": module_name.split(".")[-1],
                "type": type_,
                "size": 0 if type_ == "package" else 1,
                "mtime": _get_mtime(module),
                "doc": module.__doc__,
            }
        except ImportError as exc:
            msg = f"Path {path} not found"
            raise FileNotFoundError(msg) from exc

    def cat(self, path: str) -> bytes:
        """Get module file content."""
        norm_path = self._normalize_path(path)
        if not norm_path:
            msg = "Cannot read source of root directory"
            raise FileNotFoundError(msg)

        module_name = norm_path.replace("/", ".")
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
    fs = DistributionFS()

    # List root
    print("Root level (installed packages):")
    for item in fs.ls("/", detail=True):
        print(f"- {item['name']} ({item['type']})")

    # Explore a package
    print("\nContents of requests:")
    for item in fs.ls("requests", detail=True):
        print(f"- {item})")
