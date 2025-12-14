"""Base UPath class (with async methods)."""

from __future__ import annotations

import asyncio
from typing import TYPE_CHECKING, Any, Self, overload

from upath import UPath
from upath._stat import UPathStatResult


if TYPE_CHECKING:
    from collections.abc import AsyncIterator, Iterator
    from re import Pattern

    from fsspec.asyn import AsyncFileSystem
    from upath.types import JoinablePathLike

    from upathtools.filetree import SortCriteria, TreeOptions


class BaseUPath[TInfoDict = dict[str, Any]](UPath):
    """UPath implementation for browsing Pydantic BaseModel schemas."""

    @property
    def name(self) -> str:
        """Return the final component of the path.

        Workaround for upath's WrappedFileSystemFlavour.splitroot bug which
        incorrectly treats the first character as a root marker for relative paths.
        """
        path = self.path
        if not path or path == "/":
            return ""
        return path.rstrip("/").rsplit("/", 1)[-1]

    @classmethod
    def _fs_factory(
        cls,
        urlpath: str,
        protocol: str,
        storage_options,
    ):
        """Override upath's _fs_factory.

        Fix the bug where _get_kwargs_from_urls result is ignored.
        """
        from fsspec.registry import get_filesystem_class

        fs_cls = get_filesystem_class(protocol)
        so_dct = fs_cls._get_kwargs_from_urls(urlpath)
        so_dct.update(storage_options)
        return fs_cls(**so_dct)  # Use so_dct instead of storage_options

    async def afs(self) -> AsyncFileSystem:
        """Get async filesystem instance when possible, otherwise wrapped sync fs."""
        from upathtools.async_ops import get_async_fs

        return await get_async_fs(self.fs)

    async def aread_bytes(self) -> bytes:
        """Asynchronously read file content as bytes."""
        fs = await self.afs()
        data = await fs._cat_file(self.path)
        if isinstance(data, str):
            return data.encode("utf-8")
        return data

    @overload
    async def aread_text(
        self,
        encoding: str = "utf-8",
        errors: str = "strict",
        newline: str | None = None,
    ) -> str: ...

    @overload
    async def aread_text(
        self,
        encoding: None = None,
        errors: str = "strict",
        newline: str | None = None,
    ) -> str: ...

    async def aread_text(
        self,
        encoding: str | None = "utf-8",
        errors: str = "strict",
        newline: str | None = None,
    ) -> str:
        """Asynchronously read file content as text."""
        fs = await self.afs()
        data = await fs._cat_file(self.path)
        if isinstance(data, bytes):
            return data.decode(encoding or "utf-8", errors)
        return data  # Already a string

    async def awrite_bytes(self, data: bytes) -> int:
        """Asynchronously write bytes to file."""
        fs = await self.afs()
        await fs._pipe_file(self.path, data)
        return len(data)

    @overload
    async def awrite_text(
        self,
        data: str,
        encoding: str = "utf-8",
        errors: str = "strict",
        newline: str | None = None,
    ) -> int: ...

    @overload
    async def awrite_text(
        self,
        data: str,
        encoding: None = None,
        errors: str = "strict",
        newline: str | None = None,
    ) -> int: ...

    async def awrite_text(
        self,
        data: str,
        encoding: str | None = "utf-8",
        errors: str = "strict",
        newline: str | None = None,
    ) -> int:
        """Asynchronously write text to file."""
        fs = await self.afs()
        encoded_data = data.encode(encoding or "utf-8", errors)
        await fs._pipe_file(self.path, encoded_data)
        return len(data)

    async def aexists(self) -> bool:
        """Asynchronously check if path exists."""
        fs = await self.afs()
        return await fs._exists(self.path)

    async def ais_file(self) -> bool:
        """Asynchronously check if path is a file."""
        fs = await self.afs()
        return await fs._isfile(self.path)

    async def ais_dir(self) -> bool:
        """Asynchronously check if path is a directory."""
        fs = await self.afs()
        return await fs._isdir(self.path)

    async def amkdir(
        self, mode: int = 0o777, parents: bool = False, exist_ok: bool = False
    ) -> None:
        """Asynchronously create directory."""
        fs = await self.afs()
        await fs._makedirs(self.path, exist_ok=exist_ok)

    async def atouch(self, exist_ok: bool = True) -> None:
        """Asynchronously create empty file or update timestamp."""
        fs = await self.afs()
        if hasattr(fs, "_touch"):
            await fs._touch(self.path, exist_ok=exist_ok)  # type: ignore
        else:
            await asyncio.to_thread(self.touch, exist_ok=exist_ok)

    async def aunlink(self, missing_ok: bool = False) -> None:
        """Asynchronously remove file."""
        fs = await self.afs()
        await fs._rm(self.path)

    async def armdir(self) -> None:
        """Asynchronously remove directory."""
        fs = await self.afs()
        if hasattr(fs, "_rmdir"):
            await fs._rmdir(self.path)  # pyright: ignore[reportAttributeAccessIssue]
        else:
            await asyncio.to_thread(self.rmdir)

    async def aiterdir(self) -> AsyncIterator[Self]:
        """Asynchronously iterate over directory contents."""
        fs = await self.afs()
        entries: list[TInfoDict | str] = await fs._ls(self.path, detail=False)
        for entry in entries:
            if isinstance(entry, dict):
                entry_path = entry.get("name", entry.get("path", ""))
            else:
                entry_path = str(entry)

            if entry_path and entry_path != self.path:
                yield type(self)(entry_path, protocol=self.protocol, **self.storage_options)

    async def aglob(
        self, pattern: str, *, case_sensitive: bool | None = None
    ) -> AsyncIterator[Self]:
        """Asynchronously glob for paths matching pattern."""
        # TODO: deal with None
        case_sensitive = case_sensitive or False
        fs = await self.afs()
        full_pattern = str(self / pattern) if not pattern.startswith("/") else pattern
        matches = await fs._glob(full_pattern)
        for match_path in matches:
            if isinstance(match_path, dict):
                path_str = match_path.get("name", match_path.get("path", ""))
            else:
                path_str = str(match_path)

            if path_str:
                yield type(self)(path_str, protocol=self.protocol, **self.storage_options)

    async def arglob(
        self,
        pattern: str,
        *,
        case_sensitive: bool | None = None,
    ) -> AsyncIterator[Self]:
        """Asynchronously recursively glob for paths matching pattern."""
        async for i in self.aglob(f"**/{pattern}", case_sensitive=case_sensitive):
            yield i

    async def astat(self, *, follow_symlinks: bool = True):
        """Asynchronously get file stats."""
        fs = await self.afs()
        info = await fs._info(self.path)
        return UPathStatResult.from_info(info)

    async def aopen(
        self,
        mode: str = "r",
        buffering: int = -1,
        encoding: str | None = None,
        errors: str | None = None,
        newline: str | None = None,
        **kwargs: Any,
    ):
        """Asynchronously open file."""
        fs = await self.afs()

        try:
            return await fs.open_async(
                self.path,
                mode=mode,
                buffering=buffering,
                encoding=encoding,
                errors=errors,
                newline=newline,
                **kwargs,
            )
        except (NotImplementedError, ValueError):
            return await asyncio.to_thread(
                self.open,
                mode=mode,
                buffering=buffering,
                encoding=encoding,
                errors=errors,
                newline=newline,
                **kwargs,
            )

    async def acopy(self, target: JoinablePathLike, **kwargs: Any) -> BaseUPath:
        """Asynchronously copy file to target location."""
        path = type(self)(target) if not isinstance(target, BaseUPath) else target
        content = await self.aread_bytes()  # Read source and write to target
        await path.awrite_bytes(content)
        return path  # pyright: ignore[reportReturnType]

    async def amove(self, target: JoinablePathLike) -> BaseUPath:
        """Asynchronously move file to target location."""
        target_path = await self.acopy(target)
        await self.aunlink()
        return target_path

    def get_tree(
        self,
        options: TreeOptions | None = None,
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
        """Get a visual tree representation of this directory.

        Args:
            options: Pre-configured TreeOptions (overrides other kwargs if provided)
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
        from upathtools.filetree import DirectoryTree, TreeOptions as TreeOpts

        if options is None:
            options = TreeOpts(
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
        return DirectoryTree(self, options).get_tree_text()

    def iter_tree(
        self,
        options: TreeOptions | None = None,
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
    ) -> Iterator[str]:
        """Iterate over tree lines for this directory.

        Args:
            options: Pre-configured TreeOptions (overrides other kwargs if provided)
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
        from upathtools.filetree import DirectoryTree, TreeOptions as TreeOpts

        if options is None:
            options = TreeOpts(
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
        yield from DirectoryTree(self, options).iter_tree_lines()

    async def aget_tree(
        self,
        options: TreeOptions | None = None,
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
        """Asynchronously get a visual tree representation of this directory.

        Args:
            options: Pre-configured TreeOptions (overrides other kwargs if provided)
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
        return await asyncio.to_thread(
            self.get_tree,
            options,
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

    def __repr__(self) -> str:
        return f"BaseUPath({self.path!r}, protocol={self.protocol!r})"

    def cli(self, command: str):
        """Execute a CLI-style command on this path.

        Args:
            command: Shell-like command (e.g., "grep pattern *.py -r")

        Returns:
            CLIResult with command output

        Examples:
            >>> path = UPath(".")
            >>> result = path.cli("grep TODO *.py -r")
            >>> result = path.cli("find . -name '*.py'")
            >>> result = path.cli("ls -lah")
        """
        from upathtools.cli_parser import execute_cli

        return execute_cli(command, self)

    async def acli(self, command: str):
        """Execute a CLI-style command on this path asynchronously.

        Args:
            command: Shell-like command (e.g., "grep pattern *.py -r")

        Returns:
            CLIResult with command output

        Examples:
            >>> path = UPath(".")
            >>> result = await path.acli("grep TODO *.py -r")
            >>> result = await path.acli("find . -name '*.py'")
            >>> result = await path.acli("ls -lah")
        """
        from upathtools.cli_parser import execute_cli_async

        return await execute_cli_async(command, self)
