"""The filesystem base classes."""

from __future__ import annotations

import io
from typing import Any, Literal, overload

from fsspec.asyn import AsyncFileSystem
from fsspec.spec import AbstractFileSystem
from upath import UPath

from upathtools.filesystems.base.file_objects import AsyncFile


class BaseAsyncFileSystem[TPath: UPath, TInfoDict = dict[str, Any]](AsyncFileSystem):
    """Filesystem for browsing Pydantic BaseModel schemas and field definitions."""

    upath_cls: type[TPath]

    def get_upath(self, path: str | None = None) -> TPath:
        """Get a UPath object for the given path.

        Args:
            path: The path to the file or directory. If None, the root path is returned.
        """
        path_obj = self.upath_cls(path if path is not None else self.root_marker)
        path_obj._fs_cached = self  # pyright: ignore[reportAttributeAccessIssue]
        return path_obj

    async def open_async(
        self,
        path: str,
        mode: str = "rb",
        **kwargs: Any,
    ) -> io.BytesIO | AsyncFile:
        """Open a file asynchronously.

        Args:
            path: Path to the file
            mode: File mode ('rb' for reading, 'wb' for writing)
            **kwargs: Additional arguments for write operations
                gist_description: Optional description for new gists
                public: Whether the gist should be public (default: False)

        Returns:
            File-like object for reading or async writer for writing

        Raises:
            ValueError: If token is not provided for write operations
            NotImplementedError: If mode is not supported
        """
        if "r" in mode:
            content = await self._cat_file(path, **kwargs)
            return io.BytesIO(content)
        if "w" in mode:
            return AsyncFile(self, path, **kwargs)
        msg = f"Mode {mode} not supported"
        raise NotImplementedError(msg)

    @overload
    async def list_root_async(self, detail: Literal[False]) -> list[str]: ...

    @overload
    async def list_root_async(self, detail: Literal[True]) -> list[TInfoDict]: ...

    async def list_root_async(self, detail: bool = False) -> list[str] | list[TInfoDict]:
        """List the contents of the root directory.

        Args:
            detail: Whether to return detailed information about each item

        Returns:
            List of filenames or detailed information about each item
        """
        if detail:
            return await self._ls(self.root_marker, detail=True)
        return await self._ls(self.root_marker)


class BaseFileSystem[TPath: UPath, TInfoDict = dict[str, Any]](AbstractFileSystem):
    """Filesystem for browsing Pydantic BaseModel schemas and field definitions."""

    upath_cls: type[TPath]

    def get_upath(self, path: str | None = None) -> TPath:
        """Get a UPath object for the given path.

        Args:
            path: The path to the file or directory. If None, the root path is returned.
        """
        path_obj = self.upath_cls(path if path is not None else self.root_marker)
        path_obj._fs_cached = self  # pyright: ignore[reportAttributeAccessIssue]
        return path_obj

    @overload
    def list_root(self, detail: Literal[False]) -> list[str]: ...

    @overload
    def list_root(self, detail: Literal[True]) -> list[TInfoDict]: ...

    def list_root(self, detail: bool = False) -> list[str] | list[TInfoDict]:
        """List the contents of the root directory.

        Args:
            detail: Whether to return detailed information about each item

        Returns:
            List of filenames or detailed information about each item
        """
        if detail:
            return self.ls(self.root_marker, detail=True)
        return self.ls(self.root_marker)
