"""Runtime-based filesystem for browsing Python module contents."""

from __future__ import annotations

from dataclasses import dataclass
import inspect
from io import BytesIO
import os
import sys
from types import ModuleType
from typing import Any, Literal, overload

import fsspec
from fsspec.spec import AbstractFileSystem
from upath import UPath


NodeType = Literal["function", "class"]


@dataclass
class ModuleMember:
    """A module-level member (function or class)."""

    name: str
    type: NodeType
    doc: str | None = None


class ModulePath(UPath):
    """UPath implementation for browsing Python modules."""

    __slots__ = ()

    def iterdir(self):
        if not self.is_dir():
            raise NotADirectoryError(str(self))
        yield from super().iterdir()

    @property
    def path(self) -> str:
        path = super().path
        return "/" if path == "." else path


class ModuleFS(AbstractFileSystem):
    """Runtime-based filesystem for browsing a single Python module."""

    protocol = "mod"

    def __init__(
        self,
        fo: str = "",
        target_protocol: str | None = None,
        target_options: dict[str, Any] | None = None,
        **kwargs: Any,
    ) -> None:
        super().__init__(**kwargs)

        if not fo:
            msg = "Path to Python file required"
            raise ValueError(msg)

        self.source_path = fo if fo.endswith(".py") else f"{fo}.py"
        self._module: ModuleType | None = None
        self.target_protocol = target_protocol
        self.target_options = target_options or {}

    def _make_path(self, path: str) -> UPath:
        """Create a path object from string."""
        return ModulePath(path)

    def _load(self) -> None:
        """Load the module if not already loaded."""
        if self._module is not None:
            return

        # Read and compile the source
        with fsspec.open(
            self.source_path,
            "r",
            protocol=self.target_protocol,
            **self.target_options,
        ) as f:
            source = f.read()  # type: ignore
        code = compile(source, self.source_path, "exec")

        # Create proper module name
        module_name = os.path.splitext(os.path.basename(self.source_path))[0]  # noqa: PTH119, PTH122

        # Create module and set up its attributes
        module = ModuleType(module_name)
        module.__file__ = str(self.source_path)
        module.__loader__ = None
        module.__package__ = None

        # Register in sys.modules

        sys.modules[module_name] = module

        # Execute in the module's namespace
        exec(code, module.__dict__)

        # Set __module__ for all classes and functions
        for obj in module.__dict__.values():
            if inspect.isclass(obj) or inspect.isfunction(obj):
                obj.__module__ = module_name

        self._module = module

    @overload
    def ls(
        self,
        path: str = "",
        detail: Literal[True] = True,
        **kwargs: Any,
    ) -> list[dict[str, Any]]: ...

    @overload
    def ls(
        self,
        path: str = "",
        detail: Literal[False] = False,
        **kwargs: Any,
    ) -> list[str]: ...

    def ls(
        self,
        path: str = "",
        detail: bool = True,
        **kwargs: Any,
    ) -> list[dict[str, Any]] | list[str]:
        """List module contents (functions and classes)."""
        self._load()
        assert self._module is not None

        members: list[ModuleMember] = []
        for name, obj in vars(self._module).items():
            if name.startswith("_"):
                continue

            if inspect.isfunction(obj):
                member = ModuleMember(name=name, type="function", doc=obj.__doc__)
                members.append(member)
            elif inspect.isclass(obj):
                member = ModuleMember(name=name, type="class", doc=obj.__doc__)
                members.append(member)

        if not detail:
            return [m.name for m in members]

        return [{"name": m.name, "type": m.type, "doc": m.doc} for m in members]

    def cat(self, path: str = "") -> bytes:
        """Get source code of whole module or specific member."""
        self._load()
        assert self._module is not None

        path = self._strip_protocol(path).strip("/")  # type: ignore
        if not path:
            # Return whole module source
            with fsspec.open(
                self.source_path,
                "rb",
                protocol=self.target_protocol,
                **self.target_options,
            ) as f:
                return f.read()  # type: ignore

        # Get specific member
        obj = getattr(self._module, path, None)
        if obj is None:
            msg = f"Member {path} not found"
            raise FileNotFoundError(msg)

        source = inspect.getsource(obj)
        return source.encode()

    def _open(
        self,
        path: str,
        mode: str = "rb",
        **kwargs: Any,
    ) -> BytesIO:
        """Provide file-like access to source code."""
        if "w" in mode or "a" in mode:
            msg = "Write mode not supported"
            raise NotImplementedError(msg)

        return BytesIO(self.cat(path))

    def info(self, path: str, **kwargs: Any) -> dict[str, Any]:
        """Get info about a path."""
        self._load()  # Make sure module is loaded
        assert self._module is not None

        path = self._strip_protocol(path).strip("/")  # type: ignore

        if not path:
            # Root path - return info about the module itself
            return {
                "name": self._module.__name__,
                "type": "module",
                "size": os.path.getsize(self.source_path),  # noqa: PTH202
                "mtime": os.path.getmtime(self.source_path)  # noqa: PTH204
                if os.path.exists(self.source_path)  # noqa: PTH110
                else None,
                "doc": self._module.__doc__,
            }

        # Get specific member
        obj = getattr(self._module, path, None)
        if obj is None:
            msg = f"Member {path} not found"
            raise FileNotFoundError(msg)

        return {
            "name": path,
            "type": "class" if inspect.isclass(obj) else "function",
            "size": len(inspect.getsource(obj)),  # size of the member's source
            "doc": obj.__doc__,
        }


if __name__ == "__main__":
    fs = fsspec.filesystem("mod", fo="src/upathtools/helpers.py")
    print(fs.info("/"))
    # print(fs.cat("build"))
