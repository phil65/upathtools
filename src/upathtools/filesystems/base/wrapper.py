"""Wrapper filesystem base class."""

from __future__ import annotations

import asyncio
from dataclasses import dataclass
import inspect
from typing import TYPE_CHECKING, Any, Literal, overload

from fsspec import filesystem
from fsspec.asyn import AsyncFileSystem
from fsspec.implementations.dirfs import DirFileSystem
from fsspec.implementations.local import LocalFileSystem
from upath.registry import get_upath_class

from upathtools.async_helpers import sync_wrapper
from upathtools.async_ops import to_async_fs
from upathtools.async_upath import AsyncUPath


if TYPE_CHECKING:
    from collections.abc import Awaitable, Callable

    from fsspec.spec import AbstractFileSystem
    from upath import UPath

    # Callback for single info dict: receives info dict + filesystem, returns enriched dict
    InfoCallback = Callable[
        [dict[str, Any], "WrapperFileSystem"],
        dict[str, Any] | Awaitable[dict[str, Any]],
    ]
    # Callback for batch info dicts: receives list of info dicts + filesystem, returns enriched list
    LsInfoCallback = Callable[
        [list[dict[str, Any]], "WrapperFileSystem"],
        list[dict[str, Any]] | Awaitable[list[dict[str, Any]]],
    ]
    # Callback for lazy initialization: called once on first access
    OnFirstAccessCallback = Callable[["WrapperFileSystem"], None]


@dataclass(frozen=True)
class ContentMount:
    """A mount point with static content."""

    path: str
    content: bytes


@dataclass(frozen=True)
class FilesystemMount:
    """A mount point backed by another filesystem."""

    path: str
    fs: AsyncFileSystem


class WrapperFileSystem(AsyncFileSystem):
    """Base class for filesystems that wrap another filesystem.

    This class delegates most operations to the wrapped filesystem using __getattr__.
    Only methods that need custom behavior (like applying info callbacks) are overridden.

    The info_callback and ls_info_callback can be used to enrich file info dicts:
    - info_callback: Applied to single info dicts (from _info/info)
    - ls_info_callback: Applied to lists of info dicts (from _ls/ls)
      If not provided, falls back to applying info_callback to each item.
    """

    protocol = "wrapper"

    def __init__(
        self,
        fs: AbstractFileSystem | None = None,
        target_protocol: str | None = None,
        target_options: dict[str, Any] | None = None,
        info_callback: InfoCallback | None = None,
        ls_info_callback: LsInfoCallback | None = None,
        on_first_access: OnFirstAccessCallback | None = None,
        asynchronous: bool = True,
        loop: Any = None,
        batch_size: int | None = None,
        **storage_options: Any,
    ) -> None:
        """Initialize wrapper filesystem.

        Args:
            fs: An instantiated filesystem to wrap.
            target_protocol: Protocol to use if fs is not provided.
            target_options: Options for target filesystem if fs is not provided.
            info_callback: Optional callback to enrich single info dict. Receives (info, fs)
                          and returns enriched info dict. Can be sync or async.
            ls_info_callback: Optional callback to enrich batch of info dicts. Receives
                             (infos, fs) and returns enriched list. Can be sync or async.
                             If not provided, falls back to applying info_callback individually.
            on_first_access: Optional callback for lazy initialization. Called once on first
                            filesystem access. Receives the wrapper filesystem instance.
            asynchronous: Whether filesystem operations should be async.
            loop: Event loop to use for async operations.
            batch_size: Number of operations to batch together for concurrent execution.
            **storage_options: Additional storage options (skip_instance_cache, etc.).
        """
        super().__init__(
            asynchronous=asynchronous, loop=loop, batch_size=batch_size, **storage_options
        )

        if fs is None:
            fs = filesystem(protocol=target_protocol, **(target_options or {}))
        self.fs = to_async_fs(fs)
        self._info_callback = info_callback
        self._ls_info_callback = ls_info_callback
        self._on_first_access = on_first_access
        self._initialized = on_first_access is None
        # Mount storage: path -> mount info
        self._content_mounts: dict[str, ContentMount] = {}
        self._fs_mounts: dict[str, FilesystemMount] = {}
        self.protocol = self.fs.protocol

    def __getattr__(self, name: str) -> Any:
        """Delegate attribute access to wrapped filesystem."""
        # Avoid infinite recursion for attributes accessed during __init__
        if name in (
            "fs",
            "_info_callback",
            "_ls_info_callback",
            "_on_first_access",
            "_initialized",
            "_content_mounts",
            "_fs_mounts",
        ):
            raise AttributeError(name)
        return getattr(self.fs, name)

    def _ensure_initialized(self) -> None:
        """Run lazy initialization callback if not yet initialized."""
        if self._initialized:
            return
        self._initialized = True
        if self._on_first_access is not None:
            self._on_first_access(self)

    # Mount helpers

    def _normalize_mount_path(self, path: str) -> str:
        """Normalize a path for mount lookup."""
        # Strip protocol if present, normalize slashes
        if "://" in path:
            path = path.split("://", 1)[1]
        return "/" + path.strip("/")

    def _resolve_mount(
        self, path: str
    ) -> tuple[Literal["content", "fs", "none"], ContentMount | FilesystemMount | None, str]:
        """Resolve a path to its mount and relative path.

        Returns:
            Tuple of (mount_type, mount, relative_path)
            - mount_type: 'content', 'fs', or 'none'
            - mount: The mount object or None
            - relative_path: Path relative to mount (or original path if no mount)
        """
        normalized = self._normalize_mount_path(path)

        # Check exact content mount match first
        if normalized in self._content_mounts:
            return "content", self._content_mounts[normalized], ""

        # Check filesystem mounts (longest prefix match)
        for mount_path in sorted(self._fs_mounts, key=len, reverse=True):
            if normalized == mount_path or normalized.startswith(mount_path + "/"):
                relative = normalized[len(mount_path) :].lstrip("/") or "/"
                return "fs", self._fs_mounts[mount_path], relative

        return "none", None, path

    def _get_virtual_entries_for_path(
        self, path: str, detail: bool
    ) -> list[str] | list[dict[str, Any]]:
        """Get virtual directory entries for mounts under a path."""
        normalized = self._normalize_mount_path(path)
        if not normalized.endswith("/"):
            normalized += "/"

        seen_names: set[str] = set()
        str_entries: list[str] = []
        dict_entries: list[dict[str, Any]] = []

        # Check content mounts
        for mount_path, mount in self._content_mounts.items():
            if mount_path.startswith(normalized):
                # Get the immediate child name
                relative = mount_path[len(normalized) :]
                name = relative.split("/")[0]
                if name and name not in seen_names:
                    seen_names.add(name)
                    full_path = normalized + name
                    # Check if it's a direct file or a parent directory
                    is_file = mount_path == full_path.rstrip("/")
                    if detail:
                        dict_entries.append({
                            "name": full_path.rstrip("/"),
                            "type": "file" if is_file else "directory",
                            "size": len(mount.content) if is_file else 0,
                        })
                    else:
                        str_entries.append(full_path.rstrip("/"))

        # Check filesystem mounts
        for mount_path in self._fs_mounts:
            if mount_path.startswith(normalized):
                relative = mount_path[len(normalized) :]
                name = relative.split("/")[0]
                if name and name not in seen_names:
                    seen_names.add(name)
                    full_path = normalized + name
                    if detail:
                        dict_entries.append({
                            "name": full_path.rstrip("/"),
                            "type": "directory",
                            "size": 0,
                        })
                    else:
                        str_entries.append(full_path.rstrip("/"))

        return dict_entries if detail else str_entries

    @overload
    def mount(
        self,
        path: str,
        *,
        content: bytes | str,
    ) -> None: ...

    @overload
    def mount(
        self,
        path: str,
        *,
        fs: AbstractFileSystem,
        root: str | None = None,
    ) -> None: ...

    def mount(
        self,
        path: str,
        *,
        content: bytes | str | None = None,
        fs: AbstractFileSystem | None = None,
        root: str | None = None,
    ) -> None:
        """Mount content or a filesystem at the given path.

        Args:
            path: Virtual path where the mount appears
            content: Static content for a virtual file
            fs: Filesystem to mount at this path
            root: Root path within the mounted filesystem (wraps in DirFileSystem)

        Raises:
            ValueError: If neither or both content/fs are provided

        Examples:
            >>> fs.mount("/config.json", content=b'{"key": "value"}')
            >>> fs.mount("/data", fs=S3FileSystem(bucket="my-bucket"))
            >>> fs.mount("/subdir", fs=other_fs, root="/some/path")
        """
        if content is not None and fs is not None:
            msg = "Cannot specify both 'content' and 'fs'"
            raise ValueError(msg)
        if content is None and fs is None:
            msg = "Must specify either 'content' or 'fs'"
            raise ValueError(msg)

        normalized = self._normalize_mount_path(path)

        if content is not None:
            if isinstance(content, str):
                content = content.encode("utf-8")
            self._content_mounts[normalized] = ContentMount(path=normalized, content=content)
        else:
            assert fs is not None
            if root is not None:
                # DirFileSystem requires matching sync/async mode with inner fs
                # Both wrapper and DirFileSystem need asynchronous=True
                async_fs = to_async_fs(fs, asynchronous=True)
                async_fs = DirFileSystem(path=root, fs=async_fs, asynchronous=True)
            else:
                async_fs = to_async_fs(fs)
            self._fs_mounts[normalized] = FilesystemMount(path=normalized, fs=async_fs)

    def unmount(self, path: str) -> None:
        """Remove a mount at the given path.

        Args:
            path: Path of the mount to remove

        Raises:
            KeyError: If no mount exists at the path
        """
        normalized = self._normalize_mount_path(path)
        if normalized in self._content_mounts:
            del self._content_mounts[normalized]
        elif normalized in self._fs_mounts:
            del self._fs_mounts[normalized]
        else:
            msg = f"No mount at path: {path}"
            raise KeyError(msg)

    def mounts(self) -> dict[str, ContentMount | FilesystemMount]:
        """Return all current mounts.

        Returns:
            Dictionary mapping paths to mount info
        """
        result: dict[str, ContentMount | FilesystemMount] = {}
        result.update(self._content_mounts)
        result.update(self._fs_mounts)
        return result

    # Callback helpers

    async def _apply_info_callback(self, info: dict[str, Any]) -> dict[str, Any]:
        """Apply the info callback if set."""
        if self._info_callback is None:
            return info
        result = self._info_callback(info, self)
        if inspect.isawaitable(result):
            return await result
        return result

    async def _apply_ls_info_callback(self, infos: list[dict[str, Any]]) -> list[dict[str, Any]]:
        """Apply the ls_info_callback or fall back to info_callback individually."""
        if self._ls_info_callback is not None:
            result = self._ls_info_callback(infos, self)
            if inspect.isawaitable(result):
                return await result
            return result
        if self._info_callback is not None:
            return list(await asyncio.gather(*[self._apply_info_callback(i) for i in infos]))
        return infos

    # Core async methods with lazy init

    # UPath helper

    @overload
    def get_upath(self, path: str | None = None, *, as_async: Literal[True]) -> AsyncUPath: ...

    @overload
    def get_upath(self, path: str | None = None, *, as_async: Literal[False] = False) -> UPath: ...

    @overload
    def get_upath(
        self, path: str | None = None, *, as_async: bool = False
    ) -> UPath | AsyncUPath: ...

    def get_upath(self, path: str | None = None, *, as_async: bool = False) -> UPath | AsyncUPath:
        """Get a UPath object for the given path.

        Args:
            path: The path to the file or directory. If None, the root path is returned.
            as_async: If True, return an AsyncUPath wrapper
        """
        upath_cls = get_upath_class(self.protocol)
        assert upath_cls
        path_obj = upath_cls(path if path is not None else self.root_marker)
        path_obj._fs_cached = self  # pyright: ignore[reportAttributeAccessIssue]

        if as_async:
            return AsyncUPath._from_upath(path_obj)
        return path_obj

    def is_local(self) -> bool:
        """Check if the wrapped filesystem is local."""
        return isinstance(self.fs, LocalFileSystem)

    # Async methods that need callback application

    async def _info(self, path: str, **kwargs: Any) -> dict[str, Any]:
        self._ensure_initialized()
        mount_type, mount, relative = self._resolve_mount(path)

        if mount_type == "content":
            assert isinstance(mount, ContentMount)
            return {"name": mount.path, "type": "file", "size": len(mount.content)}
        if mount_type == "fs":
            assert isinstance(mount, FilesystemMount)
            info = await mount.fs._info(relative, **kwargs)
            # Rewrite the name to use our mount path
            info["name"] = self._normalize_mount_path(path)
            return await self._apply_info_callback(info)

        info = await self.fs._info(path, **kwargs)
        return await self._apply_info_callback(info)

    @overload
    async def _ls(
        self, path: str, detail: Literal[True] = ..., **kwargs: Any
    ) -> list[dict[str, Any]]: ...

    @overload
    async def _ls(self, path: str, detail: Literal[False], **kwargs: Any) -> list[str]: ...

    async def _ls(
        self, path: str, detail: bool = True, **kwargs: Any
    ) -> list[str] | list[dict[str, Any]]:
        self._ensure_initialized()
        mount_type, mount, relative = self._resolve_mount(path)

        if mount_type == "content":
            # Content mounts are files, not directories
            msg = f"Not a directory: {path}"
            raise NotADirectoryError(msg)

        if mount_type == "fs":
            assert isinstance(mount, FilesystemMount)
            result = await mount.fs._ls(relative, detail=detail, **kwargs)
            # Rewrite paths to use our mount namespace
            if detail:
                for item in result:  # type: ignore[union-attr]
                    rel_name = item["name"].lstrip("/")
                    item["name"] = f"{mount.path}/{rel_name}".rstrip("/")
            else:
                result = [
                    f"{mount.path}/{name.lstrip('/')}".rstrip("/")
                    for name in result  # type: ignore[union-attr]
                ]
        else:
            result = await self.fs._ls(path, detail=detail, **kwargs)

        # Merge in virtual entries from mounts
        virtual = self._get_virtual_entries_for_path(path, detail=detail)
        if virtual:
            if detail:
                existing = {item["name"] for item in result}  # type: ignore[union-attr]
                for v in virtual:  # type: ignore[union-attr]
                    if v["name"] not in existing:  # type: ignore[index]
                        result.append(v)  # type: ignore[union-attr]
            else:
                existing = set(result)  # type: ignore[arg-type]
                for v in virtual:  # type: ignore[union-attr]
                    if v not in existing:
                        result.append(v)  # type: ignore[union-attr]

        if detail and (self._ls_info_callback is not None or self._info_callback is not None):
            return await self._apply_ls_info_callback(result)  # type: ignore[arg-type]
        return result

    async def _cat_file(
        self, path: str, start: int | None = None, end: int | None = None, **kwargs: Any
    ) -> bytes:
        self._ensure_initialized()
        mount_type, mount, relative = self._resolve_mount(path)

        if mount_type == "content":
            assert isinstance(mount, ContentMount)
            content = mount.content
            if start is not None or end is not None:
                content = content[start:end]
            return content

        if mount_type == "fs":
            assert isinstance(mount, FilesystemMount)
            return await mount.fs._cat_file(relative, start=start, end=end, **kwargs)

        return await self.fs._cat_file(path, start=start, end=end, **kwargs)

    async def _pipe_file(
        self, path: str, value: bytes, overwrite: bool = True, **kwargs: Any
    ) -> None:
        self._ensure_initialized()
        await self.fs._pipe_file(path, value, overwrite=overwrite, **kwargs)

    async def _rm_file(self, path: str, **kwargs: Any) -> None:
        self._ensure_initialized()
        await self.fs._rm_file(path, **kwargs)

    async def _rm(
        self,
        path: str,
        recursive: bool = False,
        maxdepth: int | None = None,
        **kwargs: Any,
    ) -> None:
        self._ensure_initialized()
        await self.fs._rm(path, recursive=recursive, maxdepth=maxdepth, **kwargs)

    async def _cp_file(self, path1: str, path2: str, overwrite: bool = True, **kwargs: Any) -> None:
        self._ensure_initialized()
        await self.fs._cp_file(path1, path2, overwrite=overwrite, **kwargs)

    async def _makedirs(self, path: str, exist_ok: bool = False, **kwargs: Any) -> None:
        self._ensure_initialized()
        await self.fs._makedirs(path, exist_ok=exist_ok, **kwargs)

    async def _isdir(self, path: str, **kwargs: Any) -> bool:
        self._ensure_initialized()
        mount_type, mount, relative = self._resolve_mount(path)

        if mount_type == "content":
            return False  # Content mounts are files

        if mount_type == "fs":
            assert isinstance(mount, FilesystemMount)
            return await mount.fs._isdir(relative, **kwargs)

        # Also check if path is a virtual directory (has mounts underneath)
        normalized = self._normalize_mount_path(path)
        for mount_path in self._content_mounts:
            if mount_path.startswith(normalized + "/"):
                return True
        for mount_path in self._fs_mounts:
            if mount_path.startswith(normalized + "/"):
                return True

        return await self.fs._isdir(path, **kwargs)

    async def _exists(self, path: str, **kwargs: Any) -> bool:
        self._ensure_initialized()
        mount_type, mount, relative = self._resolve_mount(path)

        if mount_type == "content":
            return True

        if mount_type == "fs":
            assert isinstance(mount, FilesystemMount)
            return await mount.fs._exists(relative, **kwargs)

        # Also check if path is a virtual directory (has mounts underneath)
        normalized = self._normalize_mount_path(path)
        for mount_path in self._content_mounts:
            if mount_path.startswith(normalized + "/"):
                return True
        for mount_path in self._fs_mounts:
            if mount_path.startswith(normalized + "/"):
                return True

        return await self.fs._exists(path, **kwargs)

    # Additional filesystem operations with proper signatures

    async def _mkdir(self, path: str, create_parents: bool = True, **kwargs: Any) -> None:
        self._ensure_initialized()
        await self.fs._mkdir(path, create_parents=create_parents, **kwargs)

    async def _rmdir(self, path: str, **kwargs: Any) -> None:
        self._ensure_initialized()
        await self.fs._rmdir(path, **kwargs)

    async def _mv(self, path1: str, path2: str, recursive: bool = False, **kwargs: Any) -> None:
        self._ensure_initialized()
        await self.fs._mv(path1, path2, recursive=recursive, **kwargs)

    async def _copy(
        self,
        path1: str,
        path2: str,
        recursive: bool = False,
        overwrite: bool = True,
        **kwargs: Any,
    ) -> None:
        self._ensure_initialized()
        await self.fs._copy(path1, path2, recursive=recursive, overwrite=overwrite, **kwargs)

    async def _put_file(
        self,
        lpath: str,
        rpath: str,
        callback: Any = None,
        overwrite: bool = True,
        **kwargs: Any,
    ) -> None:
        self._ensure_initialized()
        await self.fs._put_file(lpath, rpath, callback=callback, overwrite=overwrite, **kwargs)

    async def _get_file(
        self,
        rpath: str,
        lpath: str,
        callback: Any = None,
        overwrite: bool = True,
        **kwargs: Any,
    ) -> None:
        self._ensure_initialized()
        await self.fs._get_file(rpath, lpath, callback=callback, overwrite=overwrite, **kwargs)

    async def _find(
        self,
        path: str,
        maxdepth: int | None = None,
        withdirs: bool = False,
        detail: bool = False,
        **kwargs: Any,
    ) -> list[str] | list[dict[str, Any]]:
        self._ensure_initialized()
        result = await self.fs._find(
            path, maxdepth=maxdepth, withdirs=withdirs, detail=detail, **kwargs
        )
        if detail and (self._ls_info_callback is not None or self._info_callback is not None):
            return await self._apply_ls_info_callback(result)  # type: ignore[arg-type]
        return result

    async def _glob(
        self, path: str, detail: bool = False, **kwargs: Any
    ) -> list[str] | list[dict[str, Any]]:
        self._ensure_initialized()
        result = await self.fs._glob(path, detail=detail, **kwargs)
        if detail and (self._ls_info_callback is not None or self._info_callback is not None):
            return await self._apply_ls_info_callback(result)  # type: ignore[arg-type]
        return result

    async def _du(
        self,
        path: str,
        total: bool = True,
        maxdepth: int | None = None,
        withdirs: bool = False,
        **kwargs: Any,
    ) -> int | dict[str, int]:
        self._ensure_initialized()
        return await self.fs._du(path, total=total, maxdepth=maxdepth, withdirs=withdirs, **kwargs)

    async def _size(self, path: str, **kwargs: Any) -> int:
        self._ensure_initialized()
        mount_type, mount, relative = self._resolve_mount(path)

        if mount_type == "content":
            assert isinstance(mount, ContentMount)
            return len(mount.content)

        if mount_type == "fs":
            assert isinstance(mount, FilesystemMount)
            return await mount.fs._size(relative, **kwargs)

        return await self.fs._size(path, **kwargs)

    async def _isfile(self, path: str, **kwargs: Any) -> bool:
        self._ensure_initialized()
        mount_type, mount, relative = self._resolve_mount(path)

        if mount_type == "content":
            return True  # Content mounts are always files

        if mount_type == "fs":
            assert isinstance(mount, FilesystemMount)
            return await mount.fs._isfile(relative, **kwargs)

        return await self.fs._isfile(path, **kwargs)

    async def _checksum(self, path: str, **kwargs: Any) -> str:
        self._ensure_initialized()
        return await self.fs._checksum(path, **kwargs)

    async def _modified(self, path: str, **kwargs: Any) -> Any:
        self._ensure_initialized()
        return await self.fs._modified(path, **kwargs)

    # Explicit sync wrappers for commonly used methods
    info = sync_wrapper(_info)
    ls = sync_wrapper(_ls)  # pyright: ignore[reportAssignmentType]
    mkdir = sync_wrapper(_mkdir)
    rmdir = sync_wrapper(_rmdir)
    mv = sync_wrapper(_mv)  # pyright: ignore[reportAssignmentType]
    copy = sync_wrapper(_copy)  # pyright: ignore[reportAssignmentType]
    put_file = sync_wrapper(_put_file)  # pyright: ignore[reportAssignmentType]
    get_file = sync_wrapper(_get_file)  # pyright: ignore[reportAssignmentType]
    find = sync_wrapper(_find)  # pyright: ignore[reportAssignmentType]
    glob = sync_wrapper(_glob)  # pyright: ignore[reportAssignmentType]
    du = sync_wrapper(_du)  # pyright: ignore[reportAssignmentType]
    size = sync_wrapper(_size)
    isfile = sync_wrapper(_isfile)
    checksum = sync_wrapper(_checksum)  # pyright: ignore[reportAssignmentType]
    modified = sync_wrapper(_modified)

    def __repr__(self) -> str:
        return f"{self.__class__.__qualname__}(fs={self.fs})"
