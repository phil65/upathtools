"""The filesystem base classes."""

from __future__ import annotations

from dataclasses import dataclass, field
from glob import has_magic
import logging
import os
import re
from typing import TYPE_CHECKING, Any, Literal, cast, get_args, get_origin, overload

import fsspec
from fsspec.asyn import AsyncFileSystem, _run_coros_in_chunks
from fsspec.spec import AbstractFileSystem
from fsspec.utils import glob_translate
from upath import UPath, registry

from upathtools.filesystems.base.file_objects import AsyncBufferedFile


if TYPE_CHECKING:
    from collections.abc import AsyncIterator
    from re import Pattern

    from upathtools.async_upath import AsyncUPath
    from upathtools.filetree import SortCriteria


CreationMode = Literal["create", "overwrite"]

# Default batch size for parallel operations on remote filesystems.
# Conservative to avoid overwhelming HTTP endpoints, hitting rate limits,
# or exhausting connection pools. Still provides significant speedup over sequential.
# Can be overridden per-instance via the batch_size constructor argument.
_DEFAULT_PARALLEL_BATCH_SIZE = 8

logger = logging.getLogger(__name__)


@dataclass
class GrepMatch:
    """A single grep match result."""

    path: str
    """File path where match was found."""

    line_number: int
    """Line number (1-based) of the match."""

    text: str
    """The matched line text."""

    submatches: list[tuple[int, int]] = field(default_factory=list)
    """List of (start, end) byte offsets for each submatch within the line."""

    absolute_offset: int = 0
    """Byte offset from the start of the file to the matched line."""

    def __str__(self) -> str:
        return f"{self.path}:{self.line_number}:{self.text}"


class BaseAsyncFileSystem[TPath: UPath, TInfoDict = dict[str, Any]](AsyncFileSystem):
    """Async filesystem base with parallelized directory traversal.

    This class extends fsspec's AsyncFileSystem with optimized implementations
    of _walk, _find, and _glob that parallelize directory listing operations.
    This provides significant performance improvements for remote filesystems
    where network latency dominates.

    The parallel operations use a configurable batch size (default: 32) to avoid
    overwhelming remote endpoints. Override via the `batch_size` constructor arg.
    """

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
        """Glob for files matching a pattern with parallel directory traversal.

        This implementation parallelizes directory listing operations for better
        performance on remote filesystems.

        Args:
            path: Glob pattern to match
            maxdepth: Maximum directory depth to search
            detail: If True, return dict mapping paths to info dicts
            **kwargs: Additional arguments passed to underlying implementation

        Returns:
            List of matching paths, or dict of path -> info if detail=True
        """
        if maxdepth is not None and maxdepth < 1:
            raise ValueError("maxdepth must be at least 1")

        seps = (os.path.sep, os.path.altsep) if os.path.altsep else (os.path.sep,)
        ends_with_sep = path.endswith(seps)
        stripped = self._strip_protocol(path)
        # _strip_protocol can return list for multiple paths, but we only pass one
        path = stripped if isinstance(stripped, str) else stripped[0]
        append_slash_to_dirname = ends_with_sep or path.endswith(tuple(sep + "**" for sep in seps))
        idx_star = path.find("*") if path.find("*") >= 0 else len(path)
        idx_qmark = path.find("?") if path.find("?") >= 0 else len(path)
        idx_brace = path.find("[") if path.find("[") >= 0 else len(path)
        min_idx = min(idx_star, idx_qmark, idx_brace)
        withdirs = kwargs.pop("withdirs", True)
        if not has_magic(path):
            if await self._exists(path, **kwargs):
                if not detail:
                    return [path]
                return {path: await self._info(path, **kwargs)}
            if not detail:
                return []
            return {}

        if "/" in path[:min_idx]:
            min_idx = path[:min_idx].rindex("/")
            root = path[: min_idx + 1]
            depth = path[min_idx + 1 :].count("/") + 1
        else:
            root = ""
            depth = path[min_idx + 1 :].count("/") + 1

        if "**" in path:
            if maxdepth is not None:
                idx_double_stars = path.find("**")
                depth_double_stars = path[idx_double_stars:].count("/") + 1
                depth = depth - depth_double_stars + maxdepth
            else:
                depth = None

        # Use parallelized _find
        allpaths = await self._find(root, maxdepth=depth, withdirs=withdirs, detail=True, **kwargs)
        pattern = glob_translate(path + ("/" if ends_with_sep else ""))
        compiled_pattern = re.compile(pattern)
        out: dict[str, TInfoDict] = {}
        for p, info in sorted(allpaths.items()):
            # TInfoDict is typically dict[str, Any] or a TypedDict with "type" key
            # All fsspec info dicts have a "type" field
            is_dir = info.get("type") == "directory"  # type: ignore[attr-defined]
            match_path = p + "/" if append_slash_to_dirname and is_dir else p
            if compiled_pattern.match(match_path):
                out[p] = info

        if detail:
            return out
        return list(out)

    async def _walk(
        self,
        path: str,
        maxdepth: int | None = None,
        on_error: str = "omit",
        **kwargs: Any,
    ) -> AsyncIterator[
        tuple[str, dict[str, Any], dict[str, Any]] | tuple[str, list[str], list[str]]
    ]:
        """Walk directory tree with parallel directory listing at each level.

        Uses BFS (breadth-first search) with parallel listing of all directories
        at each depth level for better performance on remote filesystems.

        Args:
            path: Root path to start walking from
            maxdepth: Maximum depth to traverse (None for unlimited)
            on_error: Error handling mode ('raise', 'omit', or callable)
            **kwargs: Additional arguments passed to _ls

        Yields:
            Tuples of (dirpath, dirs, files) similar to os.walk.
            If detail=True in kwargs, dirs and files are dicts mapping names to info.
            Otherwise, they are lists of names.
        """
        if maxdepth is not None and maxdepth < 1:
            raise ValueError("maxdepth must be at least 1")

        stripped = self._strip_protocol(path)
        # _strip_protocol can return list for multiple paths, but we only pass one
        path = stripped if isinstance(stripped, str) else stripped[0]
        detail = kwargs.pop("detail", False)

        # BFS approach: process all directories at each level in parallel
        # Each item is (full_path, remaining_depth)
        current_level: list[tuple[str, int | None]] = [(path, maxdepth)]
        batch_size = self.batch_size or _DEFAULT_PARALLEL_BATCH_SIZE

        while current_level:
            # List all directories at current level in parallel
            paths_to_list = [p for p, _ in current_level]

            try:
                # _run_coros_in_chunks returns list[T | Exception] when return_exceptions=True
                # fsspec's type hints are incomplete, so we cast the result
                listings = cast(
                    list[list[dict[str, Any]] | BaseException | None],
                    await _run_coros_in_chunks(
                        [self._ls(p, detail=True, **kwargs) for p in paths_to_list],
                        batch_size=batch_size,
                        nofiles=True,
                        return_exceptions=True,
                    ),
                )
            except Exception as e:
                if on_error == "raise":
                    raise
                if callable(on_error):
                    on_error(e)
                return

            next_level: list[tuple[str, int | None]] = []

            for (dir_path, remaining_depth), listing in zip(current_level, listings, strict=True):
                # Handle errors and None results
                if listing is None or isinstance(listing, BaseException):
                    if isinstance(listing, BaseException) and on_error == "raise":
                        raise listing
                    if isinstance(listing, BaseException) and callable(on_error):
                        on_error(listing)
                    if detail:
                        yield dir_path, {}, {}
                    else:
                        yield dir_path, [], []
                    continue

                full_dirs: dict[str, str] = {}
                dirs: dict[str, Any] = {}
                files: dict[str, Any] = {}

                for info in listing:
                    pathname = info["name"].rstrip("/")
                    name = pathname.rsplit("/", 1)[-1]
                    if info["type"] == "directory" and pathname != dir_path:
                        full_dirs[name] = pathname
                        dirs[name] = info
                    elif pathname == dir_path:
                        files[""] = info
                    else:
                        files[name] = info

                if detail:
                    yield dir_path, dirs, files
                else:
                    yield dir_path, list(dirs), list(files)

                # Queue subdirectories for next level if depth allows
                if remaining_depth is None or remaining_depth > 1:
                    new_depth = None if remaining_depth is None else remaining_depth - 1
                    next_level.extend((p, new_depth) for p in full_dirs.values())

            current_level = next_level

    @overload
    async def _find(
        self,
        path: str,
        maxdepth: int | None = None,
        withdirs: bool = False,
        *,
        detail: Literal[False] = False,
        **kwargs: Any,
    ) -> list[str]: ...

    @overload
    async def _find(
        self,
        path: str,
        maxdepth: int | None = None,
        withdirs: bool = False,
        *,
        detail: Literal[True],
        **kwargs: Any,
    ) -> dict[str, TInfoDict]: ...

    async def _find(
        self,
        path: str,
        maxdepth: int | None = None,
        withdirs: bool = False,
        *,
        detail: bool = False,
        **kwargs: Any,
    ) -> list[str] | dict[str, TInfoDict]:
        """Find all files under a path with parallel directory traversal.

        Uses BFS with parallel directory listing at each depth level for
        significantly better performance on remote filesystems compared to
        the sequential base implementation.

        Args:
            path: Root path to search from
            maxdepth: Maximum depth to traverse (None for unlimited)
            withdirs: If True, include directories in results
            detail: If True, return dict mapping paths to info dicts
            **kwargs: Additional arguments passed to _ls

        Returns:
            List of file paths, or dict of path -> info if detail=True
        """
        stripped = self._strip_protocol(path)
        # _strip_protocol can return list for multiple paths, but we only pass one
        path = stripped if isinstance(stripped, str) else stripped[0]
        out: dict[str, Any] = {}
        # Add root directory if withdirs is requested (for posix glob compliance)
        if withdirs and path != "" and await self._isdir(path):
            out[path] = await self._info(path)

        # Use parallel walk - always with detail=True to get info dicts
        async for _, dirs, files in self._walk(path, maxdepth, detail=True, **kwargs):
            # When detail=True, dirs and files are dicts mapping name -> info
            # Type narrowing: we know these are dicts because we passed detail=True
            dirs_dict = dirs if isinstance(dirs, dict) else {}
            files_dict = files if isinstance(files, dict) else {}
            if withdirs:
                files_dict.update(dirs_dict)
            out.update({info["name"]: info for info in files_dict.values()})

        # Handle case where path is a file (walk works on directories)
        if not out and await self._isfile(path):
            out[path] = await self._info(path)

        names = sorted(out)
        if not detail:
            return names
        return {name: out[name] for name in names}

    async def _grep(
        self,
        path: str,
        pattern: str,
        *,
        max_count: int | None = None,
        case_sensitive: bool | None = None,
        hidden: bool = False,
        no_ignore: bool = False,
        globs: list[str] | None = None,
        context_before: int | None = None,
        context_after: int | None = None,
        multiline: bool = False,
    ) -> list[GrepMatch]:
        """Search for a regex pattern in files under a path.

        Default implementation iterates files via _find, reads their content,
        and applies regex matching. Subclasses can override with faster
        implementations (e.g. ripgrep-rs for local filesystems).

        Args:
            path: Directory to search in.
            pattern: Regex pattern to search for.
            max_count: Maximum total number of matches to return.
            case_sensitive: Force case sensitivity (None = case-insensitive).
            hidden: Search hidden files/directories.
            no_ignore: Don't respect .gitignore rules.
            globs: File patterns to include/exclude (e.g., ``['*.py', '!*_test.py']``).
            context_before: Lines of context before match (not used in default impl).
            context_after: Lines of context after match (not used in default impl).
            multiline: Enable multiline matching (not used in default impl).

        Returns:
            List of GrepMatch objects.
        """
        stripped = self._strip_protocol(path)
        base_path = stripped if isinstance(stripped, str) else stripped[0]
        flags = 0 if case_sensitive else re.IGNORECASE
        if multiline:
            flags |= re.MULTILINE | re.DOTALL
        try:
            regex = re.compile(pattern, flags)
        except re.error:
            return []

        # Build glob pattern for _find filtering
        all_files = await self._find(base_path, withdirs=False)
        # Apply glob filtering if specified
        if globs:
            import fnmatch

            filtered: list[str] = []
            for file_path in all_files:
                name = file_path.rsplit("/", 1)[-1]
                include = not any(g.startswith("!") for g in globs)  # default include
                for g in globs:
                    if g.startswith("!"):
                        if fnmatch.fnmatch(name, g[1:]):
                            include = False
                    elif fnmatch.fnmatch(name, g):
                        include = True
                if include:
                    filtered.append(file_path)
            all_files = filtered

        matches: list[GrepMatch] = []
        limit = max_count or 100

        for file_path in all_files:
            if len(matches) >= limit:
                break
            try:
                content = await self._cat_file(file_path)
                if isinstance(content, bytes):
                    # Skip binary files (null bytes in first 8KB)
                    if b"\x00" in content[:8192]:
                        continue
                    text = content.decode("utf-8", errors="replace")
                else:
                    text = content

                # Get relative path
                rel_path = (
                    file_path[len(base_path) :].lstrip("/")
                    if file_path.startswith(base_path)
                    else file_path
                )

                byte_offset = 0
                for line_num, line in enumerate(text.splitlines(), 1):
                    if len(matches) >= limit:
                        break
                    for m in regex.finditer(line):
                        matches.append(
                            GrepMatch(
                                path=rel_path,
                                line_number=line_num,
                                text=line.rstrip("\n"),
                                submatches=[(m.start(), m.end())],
                                absolute_offset=byte_offset,
                            )
                        )
                        if len(matches) >= limit:
                            break
                    byte_offset += len(line.encode("utf-8")) + 1  # +1 for newline
            except (UnicodeDecodeError, PermissionError, OSError) as exc:
                logger.debug("Error reading file %r during grep: %s", file_path, exc)
                continue

        return matches

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

        prefix = f"{self.protocol}://"
        raw_path = path if path is not None else self.root_marker
        full_path = raw_path if raw_path.startswith(prefix) else prefix + raw_path
        path_obj = self.upath_cls(full_path)
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

    def cli(self, command: str):
        """Execute a CLI-style command on this filesystem.

        Args:
            command: Shell-like command (e.g., "grep pattern file.txt -r")

        Returns:
            CLIResult with command output
        """
        from upathtools.cli_parser import execute_cli

        base = self.get_upath()
        return execute_cli(command, base)

    @classmethod
    def register_fs(cls, clobber: bool = False) -> None:
        """Register the filesystem with fsspec + UPath."""
        assert isinstance(cls.protocol, str)
        fsspec.register_implementation(cls.protocol, cls, clobber=clobber)
        registry.register_implementation(cls.protocol, cls.upath_cls, clobber=clobber)


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

        prefix = f"{self.protocol}://"
        raw_path = path if path is not None else self.root_marker
        full_path = raw_path if raw_path.startswith(prefix) else prefix + raw_path
        path_obj = self.upath_cls(full_path)
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

    def cli(self, command: str):
        """Execute a CLI-style command on this filesystem.

        Args:
            command: Shell-like command (e.g., "grep pattern file.txt -r")

        Returns:
            CLIResult with command output
        """
        from upathtools.cli_parser import execute_cli

        base = self.get_upath()
        return execute_cli(command, base)

    async def acli(self, command: str):
        """Execute a CLI-style command on this filesystem asynchronously.

        Args:
            command: Shell-like command (e.g., "grep pattern file.txt -r")

        Returns:
            CLIResult with command output
        """
        from upathtools.cli_parser import execute_cli_async

        base = self.get_upath()
        return await execute_cli_async(command, base)

    @classmethod
    def register_fs(cls, clobber: bool = False) -> None:
        """Register the filesystem with fsspec + UPath."""
        assert isinstance(cls.protocol, str)
        fsspec.register_implementation(cls.protocol, cls, clobber=clobber)
        registry.register_implementation(cls.protocol, cls.upath_cls, clobber=clobber)
