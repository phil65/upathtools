"""The filesystem base classes."""

from __future__ import annotations

from typing import TYPE_CHECKING, Any, Literal, get_args, get_origin, overload

import fsspec
from fsspec.asyn import AsyncFileSystem
from fsspec.spec import AbstractFileSystem
from upath import UPath, registry

from upathtools.filesystems.base.file_objects import AsyncBufferedFile


if TYPE_CHECKING:
    from re import Pattern

    from upathtools.async_upath import AsyncUPath
    from upathtools.filetree import SortCriteria


class BaseAsyncFileSystem[TPath: UPath, TInfoDict = dict[str, Any]](AsyncFileSystem):
    """Filesystem for browsing Pydantic BaseModel schemas and field definitions."""

    upath_cls: type[TPath]

    @classmethod
    def get_info_fields(cls) -> list[str]:
        """Get the field names from the TInfoDict type parameter.

        Returns:
            List of field names defined in the InfoDict type, or empty list if not a TypedDict
        """
        # Get the generic arguments from the class
        if hasattr(cls, "__orig_bases__"):
            for base in cls.__orig_bases__:  # pyright: ignore[reportAttributeAccessIssue]
                if get_origin(base) is not None:
                    args = get_args(base)
                    if len(args) >= 2:  # noqa: PLR2004
                        info_dict_type = args[1]
                        # Check if it's a TypedDict by looking for __annotations__
                        if hasattr(info_dict_type, "__annotations__"):
                            return list(info_dict_type.__annotations__.keys())
        return []

    @overload
    async def _glob(
        self,
        path: str,
        maxdepth: int | None = None,
        *,
        detail: Literal[False] = False,
        **kwargs: Any,
    ) -> list[str]: ...

    @overload
    async def _glob(
        self,
        path: str,
        maxdepth: int | None = None,
        *,
        detail: Literal[True],
        **kwargs: Any,
    ) -> dict[str, TInfoDict]: ...

    async def _glob(
        self,
        path: str,
        maxdepth: int | None = None,
        *,
        detail: bool = False,
        **kwargs: Any,
    ) -> list[str] | dict[str, TInfoDict]:
        """Glob for files matching a pattern.

        Args:
            path: Glob pattern to match
            maxdepth: Maximum directory depth to search
            detail: If True, return dict mapping paths to info dicts
            **kwargs: Additional arguments passed to underlying implementation

        Returns:
            List of matching paths, or dict of path -> info if detail=True
        """
        return await super()._glob(path, maxdepth=maxdepth, detail=detail, **kwargs)  # pyright: ignore[reportReturnType]

    @overload
    def get_upath(self, path: str | None = None, *, as_async: Literal[True]) -> AsyncUPath: ...

    @overload
    def get_upath(self, path: str | None = None, *, as_async: Literal[False] = False) -> TPath: ...

    @overload
    def get_upath(
        self, path: str | None = None, *, as_async: bool = False
    ) -> TPath | AsyncUPath: ...

    def get_upath(self, path: str | None = None, *, as_async: bool = False) -> TPath | AsyncUPath:
        """Get a UPath object for the given path.

        Args:
            path: The path to the file or directory. If None, the root path is returned.
            as_async: If True, return an AsyncUPath wrapper
        """
        from upathtools.async_upath import AsyncUPath

        path_obj = self.upath_cls(path if path is not None else self.root_marker)
        path_obj._fs_cached = self  # pyright: ignore[reportAttributeAccessIssue]

        if as_async:
            return AsyncUPath._from_upath(path_obj)
        return path_obj

    async def open_async(
        self,
        path: str,
        mode: str = "rb",
        **kwargs: Any,
    ) -> AsyncBufferedFile:
        """Open a file asynchronously.

        Args:
            path: Path to the file
            mode: File mode ('rb', 'wb', 'r+b', 'ab', etc.)
            **kwargs: Additional arguments passed to _cat_file/_pipe_file

        Returns:
            AsyncBufferedFile instance supporting read/write/seek operations
        """
        return AsyncBufferedFile(self, path, mode=mode, **kwargs)

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

    @classmethod
    def register_fs(cls) -> None:
        """Register the filesystem with fsspec + UPath."""
        assert isinstance(cls.protocol, str)
        fsspec.register_implementation(cls.protocol, cls)
        registry.register_implementation(cls.protocol, cls.upath_cls)


class BaseFileSystem[TPath: UPath, TInfoDict = dict[str, Any]](AbstractFileSystem):
    """Filesystem for browsing Pydantic BaseModel schemas and field definitions."""

    upath_cls: type[TPath]

    @classmethod
    def get_info_fields(cls) -> list[str]:
        """Get the field names from the TInfoDict type parameter.

        Returns:
            List of field names defined in the InfoDict type, or empty list if not a TypedDict
        """
        # Get the generic arguments from the class
        if hasattr(cls, "__orig_bases__"):
            for base in cls.__orig_bases__:  # pyright: ignore[reportAttributeAccessIssue]
                if get_origin(base) is not None:
                    args = get_args(base)
                    if len(args) >= 2:  # noqa: PLR2004
                        info_dict_type = args[1]
                        # Check if it's a TypedDict by looking for __annotations__
                        if hasattr(info_dict_type, "__annotations__"):
                            return list(info_dict_type.__annotations__.keys())
        return []

    @overload
    def glob(
        self,
        path: str,
        maxdepth: int | None = None,
        *,
        detail: Literal[False] = False,
        **kwargs: Any,
    ) -> list[str]: ...

    @overload
    def glob(
        self,
        path: str,
        maxdepth: int | None = None,
        *,
        detail: Literal[True],
        **kwargs: Any,
    ) -> dict[str, TInfoDict]: ...

    def glob(
        self,
        path: str,
        maxdepth: int | None = None,
        *,
        detail: bool = False,
        **kwargs: Any,
    ) -> list[str] | dict[str, TInfoDict]:
        """Glob for files matching a pattern.

        Args:
            path: Glob pattern to match
            maxdepth: Maximum directory depth to search
            detail: If True, return dict mapping paths to info dicts
            **kwargs: Additional arguments passed to underlying implementation

        Returns:
            List of matching paths, or dict of path -> info if detail=True
        """
        return super().glob(path, maxdepth=maxdepth, detail=detail, **kwargs)  # pyright: ignore[reportReturnType]

    @overload
    def get_upath(self, path: str | None = None, *, as_async: Literal[True]) -> AsyncUPath: ...

    @overload
    def get_upath(self, path: str | None = None, *, as_async: Literal[False] = False) -> TPath: ...

    @overload
    def get_upath(
        self, path: str | None = None, *, as_async: bool = False
    ) -> TPath | AsyncUPath: ...

    def get_upath(self, path: str | None = None, *, as_async: bool = False) -> TPath | AsyncUPath:
        """Get a UPath object for the given path.

        Args:
            path: The path to the file or directory. If None, the root path is returned.
            as_async: If True, return an AsyncUPath wrapper
        """
        from upathtools.async_upath import AsyncUPath

        path_obj = self.upath_cls(path if path is not None else self.root_marker)
        path_obj._fs_cached = self  # pyright: ignore[reportAttributeAccessIssue]

        if as_async:
            return AsyncUPath._from_upath(path_obj)
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

    @classmethod
    def register_fs(cls) -> None:
        """Register the filesystem with fsspec + UPath."""
        assert isinstance(cls.protocol, str)
        fsspec.register_implementation(cls.protocol, cls)
        registry.register_implementation(cls.protocol, cls.upath_cls)
