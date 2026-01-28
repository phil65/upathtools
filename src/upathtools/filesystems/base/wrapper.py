"""Wrapper filesystem base class."""

from __future__ import annotations

import asyncio
import inspect
from typing import TYPE_CHECKING, Any, Literal, overload

from fsspec.asyn import AsyncFileSystem
from fsspec.implementations.asyn_wrapper import AsyncFileSystemWrapper
from fsspec.implementations.local import LocalFileSystem
from upath.registry import get_upath_class

from upathtools.async_helpers import sync_wrapper
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


def _to_async(filesystem: AbstractFileSystem) -> AsyncFileSystem:
    """Convert a sync filesystem to async if needed."""
    if isinstance(filesystem, AsyncFileSystem):
        return filesystem
    return AsyncFileSystemWrapper(filesystem)


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
            from fsspec import filesystem

            fs = filesystem(protocol=target_protocol, **(target_options or {}))
        self.fs = _to_async(fs)
        self._info_callback = info_callback
        self._ls_info_callback = ls_info_callback
        self._on_first_access = on_first_access
        self._initialized = on_first_access is None

    def __getattr__(self, name: str) -> Any:
        """Delegate attribute access to wrapped filesystem."""
        # Avoid infinite recursion for attributes accessed during __init__
        if name in (
            "fs",
            "_info_callback",
            "_ls_info_callback",
            "_on_first_access",
            "_initialized",
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
        result = await self.fs._ls(path, detail=detail, **kwargs)
        if detail and (self._ls_info_callback is not None or self._info_callback is not None):
            return await self._apply_ls_info_callback(result)  # type: ignore[arg-type]
        return result

    async def _cat_file(
        self, path: str, start: int | None = None, end: int | None = None, **kwargs: Any
    ) -> bytes:
        self._ensure_initialized()
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
        return await self.fs._isdir(path, **kwargs)

    async def _exists(self, path: str, **kwargs: Any) -> bool:
        self._ensure_initialized()
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
        return await self.fs._size(path, **kwargs)

    async def _isfile(self, path: str, **kwargs: Any) -> bool:
        self._ensure_initialized()
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
    mv = sync_wrapper(_mv)
    copy = sync_wrapper(_copy)
    put_file = sync_wrapper(_put_file)
    get_file = sync_wrapper(_get_file)
    find = sync_wrapper(_find)  # pyright: ignore[reportAssignmentType]
    glob = sync_wrapper(_glob)  # pyright: ignore[reportAssignmentType]
    du = sync_wrapper(_du)  # pyright: ignore[reportAssignmentType]
    size = sync_wrapper(_size)
    isfile = sync_wrapper(_isfile)
    checksum = sync_wrapper(_checksum)
    modified = sync_wrapper(_modified)

    def __repr__(self) -> str:
        return f"{self.__class__.__qualname__}(fs={self.fs})"
