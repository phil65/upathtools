"""Filesystem that auto-delegates to file-based sub-filesystems.

This filesystem wraps another filesystem and automatically delegates to
appropriate file filesystems when accessing files with supported extensions.
For example, accessing a `.md` file will use the MarkdownFileSystem to
expose the markdown's header structure.

The delegation system uses a two-phase detection:
1. Extension check - quick filter by file extension
2. Content probing - verify content is valid for the filesystem

For files where multiple filesystems match the extension (e.g., .json for
both JSON Schema and OpenAPI), content probing determines the best match.
"""

from __future__ import annotations

from pathlib import PurePosixPath
from typing import TYPE_CHECKING, Any, ClassVar, Literal, overload

from fsspec.asyn import AsyncFileSystem
from fsspec.implementations.asyn_wrapper import AsyncFileSystemWrapper

from upathtools.async_helpers import sync_wrapper
from upathtools.filesystems.base.filefilesystem import (
    BaseAsyncFileFileSystem,
    BaseFileFileSystem,
    ProbeResult,
)


if TYPE_CHECKING:
    from collections.abc import Sequence

    from fsspec.spec import AbstractFileSystem

    from upathtools.filesystems.base.filefilesystem import FileFileSystemMixin

# Type alias for file filesystem instances
FileFS = BaseFileFileSystem[Any, Any] | BaseAsyncFileFileSystem[Any, Any]
FileFSClass = type[BaseFileFileSystem[Any, Any]] | type[BaseAsyncFileFileSystem[Any, Any]]


def _to_async(filesystem: AbstractFileSystem | None) -> AsyncFileSystem:
    """Convert a sync filesystem to async if needed."""
    if filesystem is None:
        msg = "filesystem cannot be None"
        raise ValueError(msg)
    if isinstance(filesystem, AsyncFileSystem):
        return filesystem
    return AsyncFileSystemWrapper(filesystem)


# Registry of file filesystem classes
_FILE_FS_REGISTRY: list[type[FileFileSystemMixin]] = []


def register_file_filesystem(fs_class: type[FileFileSystemMixin]) -> type[FileFileSystemMixin]:
    """Register a file filesystem class for auto-delegation.

    Args:
        fs_class: A filesystem class that implements FileFileSystemMixin.

    Returns:
        The same class (for use as decorator).
    """
    if fs_class not in _FILE_FS_REGISTRY:
        _FILE_FS_REGISTRY.append(fs_class)
    return fs_class


def get_registered_filesystems() -> list[type[FileFileSystemMixin]]:
    """Get all registered file filesystem classes."""
    return list(_FILE_FS_REGISTRY)


def find_filesystems_for_extension(
    extension: str,
) -> list[type[FileFileSystemMixin]]:
    """Find all registered filesystems that support the given extension.

    Args:
        extension: File extension (with or without leading dot).

    Returns:
        List of filesystem classes that support this extension, sorted by priority.
    """
    ext = extension.lower().lstrip(".")
    matches = [fs_class for fs_class in _FILE_FS_REGISTRY if fs_class.supports_extension(ext)]
    # Sort by priority (lower = higher priority)
    return sorted(matches, key=lambda cls: cls.priority)


def find_filesystem_for_extension(
    extension: str,
) -> type[FileFileSystemMixin] | None:
    """Find the highest-priority filesystem that supports the given extension.

    Args:
        extension: File extension (with or without leading dot).

    Returns:
        Filesystem class that supports this extension, or None.
    """
    matches = find_filesystems_for_extension(extension)
    return matches[0] if matches else None


def _discover_file_filesystems() -> None:
    """Discover and register file filesystems from the file_filesystems package."""
    from upathtools.filesystems.file_filesystems.jsonschema_fs import JsonSchemaFileSystem
    from upathtools.filesystems.file_filesystems.markdown_fs import MarkdownFileSystem
    from upathtools.filesystems.file_filesystems.openapi_fs import OpenAPIFileSystem
    from upathtools.filesystems.file_filesystems.sqlite_fs import SqliteFileSystem
    from upathtools.filesystems.file_filesystems.treesitter_fs import TreeSitterFileSystem

    for fs_class in [
        MarkdownFileSystem,
        SqliteFileSystem,
        JsonSchemaFileSystem,
        OpenAPIFileSystem,
        TreeSitterFileSystem,
    ]:
        register_file_filesystem(fs_class)


class DelegatingFileSystem(AsyncFileSystem):
    """Filesystem that auto-delegates to file-based sub-filesystems.

    When accessing a file with a supported extension (e.g., `.md`, `.db`, `.py`),
    this filesystem will delegate to the appropriate file filesystem to expose
    the internal structure of that file.

    Path format for delegated access:
        {file_path}::{internal_path}

    Examples:
        - `readme.md::Introduction` - Access "Introduction" section in markdown
        - `data.db::users` - Access "users" table in SQLite database
        - `module.py::MyClass` - Access "MyClass" in Python module

    If no `::` separator is present, the filesystem behaves like the wrapped
    filesystem normally.
    """

    protocol = "delegating"
    SEPARATOR: ClassVar[str] = "::"

    def __init__(
        self,
        fs: AbstractFileSystem | None = None,
        target_protocol: str | None = None,
        target_options: dict[str, Any] | None = None,
        auto_discover: bool = True,
        **storage_options: Any,
    ) -> None:
        """Initialize the delegating filesystem.

        Args:
            fs: An instantiated filesystem to wrap.
            target_protocol: Protocol to use if fs is not provided.
            target_options: Options for target filesystem if fs is not provided.
            auto_discover: Whether to auto-discover file filesystems on init.
            **storage_options: Additional storage options.
        """
        super().__init__(**storage_options)

        if fs is None:
            from fsspec import filesystem

            fs = filesystem(target_protocol or "file", **(target_options or {}))

        self.fs = _to_async(fs)
        self.target_protocol = target_protocol
        self.target_options = target_options or {}

        # Cache for file filesystem instances
        self._fs_cache: dict[str, FileFS] = {}

        if auto_discover and not _FILE_FS_REGISTRY:
            _discover_file_filesystems()

    def _parse_path(self, path: str) -> tuple[str, str | None]:
        """Parse a path into file path and internal path.

        Args:
            path: Full path, possibly with :: separator.

        Returns:
            Tuple of (file_path, internal_path or None).
        """
        if self.SEPARATOR in path:
            file_path, internal_path = path.split(self.SEPARATOR, 1)
            return file_path, internal_path
        return path, None

    def _get_extension(self, path: str) -> str:
        """Get file extension from path."""
        return PurePosixPath(path).suffix.lstrip(".").lower()

    async def _get_file_fs(self, file_path: str) -> FileFS | None:
        """Get or create a file filesystem for the given file.

        Uses a two-phase detection:
        1. Filter by extension to get candidate filesystems
        2. Probe content to find the best match

        Args:
            file_path: Path to the file on the wrapped filesystem.

        Returns:
            File filesystem instance, or None if not supported.
        """
        if file_path in self._fs_cache:
            return self._fs_cache[file_path]

        ext = self._get_extension(file_path)
        candidates: list[FileFSClass] = [
            c  # type: ignore[misc]
            for c in find_filesystems_for_extension(ext)
            if issubclass(c, (BaseFileFileSystem, BaseAsyncFileFileSystem))
        ]

        if not candidates:
            return None

        # If only one candidate, try from_filesystem_async first (for read-write access)
        if len(candidates) == 1:
            fs_class = candidates[0]
            try:
                file_fs = await fs_class.from_filesystem_async(file_path, self.fs)
            except AttributeError:
                content = await self.fs._cat_file(file_path)
                file_fs = fs_class.from_content(content)
            self._fs_cache[file_path] = file_fs
            return file_fs

        # Multiple candidates - need to probe content to find the best match
        content = await self.fs._cat_file(file_path)

        best_match: FileFSClass | None = None
        maybe_match: FileFSClass | None = None

        for fs_class in candidates:  # Already sorted by priority
            result = fs_class.probe_content(content, ext)
            if result == ProbeResult.SUPPORTED:
                best_match = fs_class
                break
            if result == ProbeResult.MAYBE and maybe_match is None:
                maybe_match = fs_class

        matched_class = best_match or maybe_match
        if matched_class is None:
            return None

        # Use from_filesystem_async if available (for read-write access)
        try:
            file_fs = await matched_class.from_filesystem_async(file_path, self.fs)
        except AttributeError:
            file_fs = matched_class.from_content(content)
        self._fs_cache[file_path] = file_fs
        return file_fs

    def _should_delegate(self, path: str) -> bool:
        """Check if path should be delegated to a file filesystem."""
        file_path, internal_path = self._parse_path(path)
        if internal_path is not None:
            return True
        ext = self._get_extension(file_path)
        return find_filesystem_for_extension(ext) is not None

    def _call_fs_method(self, file_fs: FileFS, method: str, *args: Any, **kwargs: Any) -> Any:
        """Call a method on a file filesystem, handling sync/async differences."""
        return getattr(file_fs, method)(*args, **kwargs)

    async def _info(self, path: str, **kwargs: Any) -> dict[str, Any]:
        file_path, internal_path = self._parse_path(path)

        if internal_path is not None:
            file_fs = await self._get_file_fs(file_path)
            if file_fs is None:
                msg = f"No filesystem available for: {file_path}"
                raise FileNotFoundError(msg)

            return self._call_fs_method(file_fs, "info", internal_path, **kwargs)

        # Check if this is a file with supported extension
        info = await self.fs._info(file_path, **kwargs)

        if info.get("type") == "file":
            ext = self._get_extension(file_path)
            if find_filesystem_for_extension(ext):
                # Mark as directory since it can be "entered"
                info["type"] = "directory"
                info["delegated"] = True
                info["original_type"] = "file"

        return info

    @overload
    async def _ls(
        self, path: str, detail: Literal[True] = ..., **kwargs: Any
    ) -> list[dict[str, Any]]: ...

    @overload
    async def _ls(self, path: str, detail: Literal[False], **kwargs: Any) -> list[str]: ...

    async def _ls(
        self, path: str, detail: bool = True, **kwargs: Any
    ) -> Sequence[str | dict[str, Any]]:
        file_path, internal_path = self._parse_path(path)

        if internal_path is not None:
            # Delegate to file filesystem
            file_fs = await self._get_file_fs(file_path)
            if file_fs is None:
                msg = f"No filesystem available for: {file_path}"
                raise FileNotFoundError(msg)

            result = self._call_fs_method(file_fs, "ls", internal_path, detail=detail, **kwargs)

            # Prefix paths with file_path::
            if detail:
                for item in result:
                    if isinstance(item, dict) and "name" in item:
                        item["name"] = f"{file_path}{self.SEPARATOR}{item['name']}"
            else:
                result = [f"{file_path}{self.SEPARATOR}{p}" for p in result]

            return result

        # Check if path is a file with supported extension - list its contents
        try:
            info = await self.fs._info(file_path, **kwargs)
            if info.get("type") == "file":
                ext = self._get_extension(file_path)
                if find_filesystem_for_extension(ext):
                    file_fs = await self._get_file_fs(file_path)
                    if file_fs:
                        # List root of file filesystem
                        result = self._call_fs_method(file_fs, "ls", "/", detail=detail, **kwargs)

                        if detail:
                            for item in result:
                                if isinstance(item, dict) and "name" in item:
                                    item["name"] = f"{file_path}{self.SEPARATOR}{item['name']}"
                        else:
                            result = [f"{file_path}{self.SEPARATOR}{p}" for p in result]

                        return result
        except FileNotFoundError:
            pass

        # Normal directory listing
        result = await self.fs._ls(file_path, detail=detail, **kwargs)

        if detail:
            # Enhance entries for files with supported extensions
            for item in result:
                if isinstance(item, dict) and item.get("type") == "file":
                    name = item.get("name", "")
                    ext = self._get_extension(name)
                    if find_filesystem_for_extension(ext):
                        item["type"] = "directory"
                        item["delegated"] = True
                        item["original_type"] = "file"

        return result

    async def _cat_file(
        self, path: str, start: int | None = None, end: int | None = None, **kwargs: Any
    ) -> bytes:
        file_path, internal_path = self._parse_path(path)

        if internal_path is not None:
            file_fs = await self._get_file_fs(file_path)
            if file_fs is None:
                msg = f"No filesystem available for: {file_path}"
                raise FileNotFoundError(msg)

            # Try cat_file first (standard fsspec), fall back to cat
            if hasattr(file_fs, "cat_file"):
                content = self._call_fs_method(file_fs, "cat_file", internal_path, **kwargs)
            else:
                content = self._call_fs_method(file_fs, "cat", internal_path)
            if isinstance(content, str):
                content = content.encode()

            if start is not None or end is not None:
                content = content[start:end]
            return content

        return await self.fs._cat_file(file_path, start=start, end=end, **kwargs)

    async def _exists(self, path: str, **kwargs: Any) -> bool:
        file_path, internal_path = self._parse_path(path)

        if internal_path is not None:
            file_fs = await self._get_file_fs(file_path)
            if file_fs is None:
                return False

            try:
                return self._call_fs_method(file_fs, "exists", internal_path)
            except (FileNotFoundError, OSError):
                return False

        return await self.fs._exists(file_path, **kwargs)

    async def _isdir(self, path: str, **kwargs: Any) -> bool:
        file_path, internal_path = self._parse_path(path)

        if internal_path is not None:
            file_fs = await self._get_file_fs(file_path)
            if file_fs is None:
                return False

            return self._call_fs_method(file_fs, "isdir", internal_path)

        # Check if it's a file with supported extension (treated as directory)
        try:
            info = await self.fs._info(file_path, **kwargs)
            if info.get("type") == "file":
                ext = self._get_extension(file_path)
                if find_filesystem_for_extension(ext):
                    return True
        except FileNotFoundError:
            pass

        return await self.fs._isdir(file_path, **kwargs)

    async def _isfile(self, path: str, **kwargs: Any) -> bool:
        file_path, internal_path = self._parse_path(path)

        if internal_path is not None:
            file_fs = await self._get_file_fs(file_path)
            if file_fs is None:
                return False

            return self._call_fs_method(file_fs, "isfile", internal_path)

        # Files with supported extensions are treated as directories
        try:
            info = await self.fs._info(file_path, **kwargs)
            if info.get("type") == "file":
                ext = self._get_extension(file_path)
                if find_filesystem_for_extension(ext):
                    return False  # Treated as directory
        except FileNotFoundError:
            pass

        return await self.fs._isfile(file_path, **kwargs)

    # Write operations - only work on regular files, not delegated paths
    async def _pipe_file(self, path: str, value: bytes, **kwargs: Any) -> None:
        file_path, internal_path = self._parse_path(path)
        if internal_path is not None:
            msg = "Cannot write to internal paths in delegated filesystems"
            raise NotImplementedError(msg)
        await self.fs._pipe_file(file_path, value, **kwargs)

    async def _rm_file(self, path: str, **kwargs: Any) -> None:
        file_path, internal_path = self._parse_path(path)
        if internal_path is not None:
            msg = "Cannot remove internal paths in delegated filesystems"
            raise NotImplementedError(msg)
        # Clear cache if removing a file we have cached
        self._fs_cache.pop(file_path, None)
        await self.fs._rm_file(file_path, **kwargs)

    async def _rm(
        self, path: str, recursive: bool = False, maxdepth: int | None = None, **kwargs: Any
    ) -> None:
        file_path, internal_path = self._parse_path(path)
        if internal_path is not None:
            msg = "Cannot remove internal paths in delegated filesystems"
            raise NotImplementedError(msg)
        self._fs_cache.pop(file_path, None)
        await self.fs._rm(file_path, recursive=recursive, maxdepth=maxdepth, **kwargs)

    async def _makedirs(self, path: str, exist_ok: bool = False, **kwargs: Any) -> None:
        file_path, internal_path = self._parse_path(path)
        if internal_path is not None:
            msg = "Cannot create directories inside delegated filesystems"
            raise NotImplementedError(msg)
        await self.fs._makedirs(file_path, exist_ok=exist_ok, **kwargs)

    async def _cp_file(self, path1: str, path2: str, overwrite: bool = True, **kwargs: Any) -> None:
        # Only support copying regular files
        _, internal1 = self._parse_path(path1)
        _, internal2 = self._parse_path(path2)
        if internal1 is not None or internal2 is not None:
            msg = "Cannot copy internal paths in delegated filesystems"
            raise NotImplementedError(msg)
        await self.fs._cp_file(path1, path2, overwrite=overwrite, **kwargs)

    def clear_cache(self, path: str | None = None) -> None:
        """Clear cached file filesystem instances.

        Args:
            path: Specific file path to clear, or None to clear all.
        """
        if path is None:
            self._fs_cache.clear()
        else:
            self._fs_cache.pop(path, None)

    # Sync wrappers
    info = sync_wrapper(_info)
    ls = sync_wrapper(_ls)  # pyright: ignore[reportAssignmentType]
    cat_file = sync_wrapper(_cat_file)  # pyright: ignore[reportAssignmentType]
    exists = sync_wrapper(_exists)  # pyright: ignore[reportAssignmentType]
    isdir = sync_wrapper(_isdir)
    isfile = sync_wrapper(_isfile)
    pipe_file = sync_wrapper(_pipe_file)
    rm_file = sync_wrapper(_rm_file)
    rm = sync_wrapper(_rm)
    makedirs = sync_wrapper(_makedirs)
    cp_file = sync_wrapper(_cp_file)

    def __repr__(self) -> str:
        return f"{self.__class__.__qualname__}(fs={self.fs}, registered={len(_FILE_FS_REGISTRY)})"
