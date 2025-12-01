"""The filesystem base classes."""

from __future__ import annotations

import io
from typing import TYPE_CHECKING, Any, Literal, overload

from fsspec.asyn import AsyncFileSystem
from fsspec.spec import AbstractFileSystem
from upath import UPath

from upathtools.filesystems.base.file_objects import AsyncFile


if TYPE_CHECKING:
    from re import Pattern

    from upathtools.filetree import SortCriteria


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

    def get_tree(
        self,
        path: str | None = None,
        *,
        show_hidden: bool = False,
        show_size: bool = False,
        show_date: bool = False,
        show_permissions: bool = False,
        show_icons: bool = True,
        max_depth: int | None = None,
        include_pattern: Pattern[str] | None = None,
        exclude_pattern: Pattern[str] | None = None,
        allowed_extensions: set[str] | None = None,
        hide_empty: bool = True,
        sort_criteria: SortCriteria = "name",
        reverse_sort: bool = False,
        date_format: str = "%Y-%m-%d %H:%M:%S",
    ) -> str:
        """Get a visual directory tree representation.

        Args:
            path: Root path to start the tree from (None for filesystem root)
            show_hidden: Whether to show hidden files/directories
            show_size: Whether to show file sizes
            show_date: Whether to show modification dates
            show_permissions: Whether to show file permissions
            show_icons: Whether to show icons for files/directories
            max_depth: Maximum depth to traverse (None for unlimited)
            include_pattern: Regex pattern for files/directories to include
            exclude_pattern: Regex pattern for files/directories to exclude
            allowed_extensions: Set of allowed file extensions
            hide_empty: Whether to hide empty directories
            sort_criteria: Criteria for sorting entries
            reverse_sort: Whether to reverse the sort order
            date_format: Format string for dates
        """
        from upathtools.filetree import get_directory_tree

        upath = self.get_upath(path)
        return get_directory_tree(
            upath,
            show_hidden=show_hidden,
            show_size=show_size,
            show_date=show_date,
            show_permissions=show_permissions,
            show_icons=show_icons,
            max_depth=max_depth,
            include_pattern=include_pattern,
            exclude_pattern=exclude_pattern,
            allowed_extensions=allowed_extensions,
            hide_empty=hide_empty,
            sort_criteria=sort_criteria,
            reverse_sort=reverse_sort,
            date_format=date_format,
        )


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

    def get_tree(
        self,
        path: str | None = None,
        *,
        show_hidden: bool = False,
        show_size: bool = False,
        show_date: bool = False,
        show_permissions: bool = False,
        show_icons: bool = True,
        max_depth: int | None = None,
        include_pattern: Pattern[str] | None = None,
        exclude_pattern: Pattern[str] | None = None,
        allowed_extensions: set[str] | None = None,
        hide_empty: bool = True,
        sort_criteria: SortCriteria = "name",
        reverse_sort: bool = False,
        date_format: str = "%Y-%m-%d %H:%M:%S",
    ) -> str:
        """Get a visual directory tree representation.

        Args:
            path: Root path to start the tree from (None for filesystem root)
            show_hidden: Whether to show hidden files/directories
            show_size: Whether to show file sizes
            show_date: Whether to show modification dates
            show_permissions: Whether to show file permissions
            show_icons: Whether to show icons for files/directories
            max_depth: Maximum depth to traverse (None for unlimited)
            include_pattern: Regex pattern for files/directories to include
            exclude_pattern: Regex pattern for files/directories to exclude
            allowed_extensions: Set of allowed file extensions
            hide_empty: Whether to hide empty directories
            sort_criteria: Criteria for sorting entries
            reverse_sort: Whether to reverse the sort order
            date_format: Format string for dates
        """
        from upathtools.filetree import get_directory_tree

        upath = self.get_upath(path)
        return get_directory_tree(
            upath,
            show_hidden=show_hidden,
            show_size=show_size,
            show_date=show_date,
            show_permissions=show_permissions,
            show_icons=show_icons,
            max_depth=max_depth,
            include_pattern=include_pattern,
            exclude_pattern=exclude_pattern,
            allowed_extensions=allowed_extensions,
            hide_empty=hide_empty,
            sort_criteria=sort_criteria,
            reverse_sort=reverse_sort,
            date_format=date_format,
        )
