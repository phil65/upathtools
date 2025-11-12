"""Filesystem implementation for browsing Python module structure using AST."""

from __future__ import annotations

import ast
from dataclasses import dataclass
import io
import os
from typing import Any, Literal, overload

import fsspec

from upathtools.filesystems.base import BaseAsyncFileSystem, BaseUPath


NodeType = Literal["function", "class", "import", "assign"]


@dataclass
class ModuleMember:
    """A module-level member (function, class, or assignment)."""

    name: str
    type: NodeType
    start_line: int
    end_line: int
    doc: str | None = None


class PythonAstPath(BaseUPath):
    """UPath implementation for browsing Python AST."""

    __slots__ = ()

    def iterdir(self):
        if not self.is_dir():
            raise NotADirectoryError(str(self))
        yield from super().iterdir()


class PythonAstFS(BaseAsyncFileSystem[PythonAstPath]):
    """Browse Python modules statically using AST."""

    protocol = "ast"
    upath_cls = PythonAstPath

    def __init__(
        self,
        python_file: str = "",
        target_protocol: str | None = None,
        target_options: dict[str, Any] | None = None,
        **kwargs: Any,
    ) -> None:
        super().__init__(**kwargs)

        # Handle both direct usage and chaining - fo is used by fsspec for chaining
        fo = kwargs.pop("fo", "")
        path = python_file or fo

        if not path:
            msg = "Python file path required"
            raise ValueError(msg)

        # Store parameters for lazy loading
        self.path = path if path.endswith(".py") else path + ".py"
        self.target_protocol = target_protocol
        self.target_options = target_options or {}

        # Initialize empty state
        self._source: str | None = None
        self._members: dict[str, ModuleMember] = {}

    @staticmethod
    def _get_kwargs_from_urls(path):
        path = path.removeprefix("ast://")
        return {"python_file": path}

    def _load(self) -> None:
        """Load and parse the source file if not already loaded."""
        if self._source is not None:
            return

        with fsspec.open(
            self.path,
            protocol=self.target_protocol,
            **self.target_options,
        ) as f:
            self._source = f.read().decode()  # type: ignore

        self._analyze_source()

    def _analyze_source(self) -> None:
        """Parse source and find all members."""
        if self._source is None:
            msg = "Source not loaded"
            raise RuntimeError(msg)

        tree = ast.parse(self._source)

        for node in ast.iter_child_nodes(tree):
            match node:
                case ast.FunctionDef() | ast.AsyncFunctionDef():
                    self._members[node.name] = ModuleMember(
                        name=node.name,
                        type="function",
                        start_line=node.lineno - 1,
                        end_line=node.end_lineno or node.lineno,
                        doc=ast.get_docstring(node),
                    )
                case ast.ClassDef():
                    self._members[node.name] = ModuleMember(
                        name=node.name,
                        type="class",
                        start_line=node.lineno - 1,
                        end_line=node.end_lineno or node.lineno,
                        doc=ast.get_docstring(node),
                    )

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
        """List module contents."""
        self._load()

        if not detail:
            return list(self._members)

        return [
            {
                "name": member.name,
                "type": member.type,
                "size": member.end_line - member.start_line,
                "doc": member.doc,
            }
            for member in self._members.values()
        ]

    def cat(self, path: str = "") -> bytes:
        """Get source code of whole module or specific member."""
        self._load()
        assert self._source is not None

        path = self._strip_protocol(path).strip("/")  # type: ignore
        if not path:
            return self._source.encode()

        if path not in self._members:
            msg = f"Member {path} not found"
            raise FileNotFoundError(msg)

        member = self._members[path]
        lines = self._source.splitlines()
        return "\n".join(lines[member.start_line : member.end_line]).encode()

    def _open(
        self,
        path: str,
        mode: str = "rb",
        **kwargs: Any,
    ) -> Any:
        """Provide file-like access to module or member source."""
        # Make sure we have the source
        self._load()
        assert self._source is not None

        path = self._strip_protocol(path).strip("/")  # type: ignore
        content: bytes

        if not path:
            content = self._source.encode()
        else:
            if path not in self._members:
                msg = f"Member {path} not found"
                raise FileNotFoundError(msg)

            member = self._members[path]
            lines = self._source.splitlines()
            content = "\n".join(lines[member.start_line : member.end_line]).encode()

        # Return a file-like object
        return io.BytesIO(content)

    def info(self, path: str, **kwargs: Any) -> dict[str, Any]:
        """Get info about the module or a specific member."""
        self._load()
        assert self._source is not None

        path = self._strip_protocol(path).strip("/")  # type: ignore

        if not path:
            # Root path - return info about the module itself
            return {
                "name": os.path.splitext(os.path.basename(self.path))[0],  # noqa: PTH119, PTH122
                "type": "module",
                "size": len(self._source),
                "doc": ast.get_docstring(ast.parse(self._source)),
            }

        # Get specific member info
        if path not in self._members:
            msg = f"Member {path} not found"
            raise FileNotFoundError(msg)

        member = self._members[path]
        size = member.end_line - member.start_line
        return {"name": member.name, "type": member.type, "size": size, "doc": member.doc}


if __name__ == "__main__":
    fs = fsspec.filesystem("ast", python_file="duties.py")
    print(fs.ls("/"))
    print(fs.cat("build"))
