"""Wrapper filesystem base class."""

from __future__ import annotations

from typing import TYPE_CHECKING, Any, Literal, overload

from fsspec.asyn import AsyncFileSystem
from fsspec.implementations.asyn_wrapper import AsyncFileSystemWrapper
from fsspec.implementations.local import LocalFileSystem


if TYPE_CHECKING:
    from collections.abc import AsyncIterator, Iterator

    from fsspec.spec import AbstractFileSystem


def to_async(filesystem: AbstractFileSystem) -> AsyncFileSystem:
    if not isinstance(filesystem, AsyncFileSystem):
        return AsyncFileSystemWrapper(filesystem)
    return filesystem


class WrapperFileSystem(AsyncFileSystem):
    """Base class for filesystems that wrap another filesystem.

    This class delegates all operations to the wrapped filesystem without
    any path transformation. Subclasses can override specific methods to
    add custom behavior.
    """

    protocol = "wrapper"

    def __init__(
        self,
        fs: AbstractFileSystem | None = None,
        target_protocol: str | None = None,
        target_options: dict[str, Any] | None = None,
        **storage_options: Any,
    ) -> None:
        """Initialize wrapper filesystem.

        Args:
            fs: An instantiated filesystem to wrap.
            target_protocol: Protocol to use if fs is not provided.
            target_options: Options for target filesystem if fs is not provided.
            **storage_options: Additional storage options passed to parent.
        """
        super().__init__(**storage_options)

        if fs is None:
            from fsspec import filesystem

            fs = filesystem(protocol=target_protocol, **(target_options or {}))
        self.fs = to_async(fs)

    def is_local(self) -> bool:
        """Did we read this from the local filesystem?"""
        # see also fsspec.utils.can_be_local for more flexibility with caching.
        return isinstance(self.fs, LocalFileSystem)

    @property
    def sep(self) -> str:
        """Path separator."""
        return self.fs.sep

    # Session management

    async def set_session(self, *args: Any, **kwargs: Any) -> Any:
        """Set session on wrapped filesystem."""
        return await self.fs.set_session(*args, **kwargs)

    # File operations

    async def _rm_file(self, path: str, **kwargs: Any) -> None:
        return await self.fs._rm_file(path, **kwargs)

    def rm_file(self, path: str, **kwargs: Any) -> None:
        return self.fs.rm_file(path, **kwargs)

    async def _rm(self, path: str, *args: Any, **kwargs: Any) -> None:
        return await self.fs._rm(path, *args, **kwargs)

    def rm(self, path: str, *args: Any, **kwargs: Any) -> None:
        return self.fs.rm(path, *args, **kwargs)

    async def _cp_file(self, path1: str, path2: str, **kwargs: Any) -> None:
        return await self.fs._cp_file(path1, path2, **kwargs)

    def cp_file(self, path1: str, path2: str, **kwargs: Any) -> None:
        return self.fs.cp_file(path1, path2, **kwargs)

    async def _copy(self, path1: str, path2: str, *args: Any, **kwargs: Any) -> None:
        return await self.fs._copy(path1, path2, *args, **kwargs)

    def copy(self, path1: str, path2: str, *args: Any, **kwargs: Any) -> None:
        return self.fs.copy(path1, path2, *args, **kwargs)

    async def _pipe(self, path: str, *args: Any, **kwargs: Any) -> None:
        return await self.fs._pipe(path, *args, **kwargs)

    def pipe(self, path: str, *args: Any, **kwargs: Any) -> None:
        return self.fs.pipe(path, *args, **kwargs)

    async def _pipe_file(self, path: str, value: bytes, **kwargs: Any) -> None:
        return await self.fs._pipe_file(path, value, **kwargs)

    def pipe_file(self, path: str, value: bytes, **kwargs: Any) -> None:
        return self.fs.pipe_file(path, value, **kwargs)

    async def _cat_file(self, path: str, *args: Any, **kwargs: Any) -> bytes:
        return await self.fs._cat_file(path, *args, **kwargs)

    def cat_file(self, path: str, *args: Any, **kwargs: Any) -> bytes:
        return self.fs.cat_file(path, *args, **kwargs)

    async def _cat(
        self, path: str | list[str], *args: Any, **kwargs: Any
    ) -> bytes | dict[str, bytes]:
        return await self.fs._cat(path, *args, **kwargs)

    def cat(self, path: str | list[str], *args: Any, **kwargs: Any) -> bytes | dict[str, bytes]:
        return self.fs.cat(path, *args, **kwargs)

    async def _put_file(self, lpath: str, rpath: str, **kwargs: Any) -> None:
        return await self.fs._put_file(lpath, rpath, **kwargs)

    def put_file(self, lpath: str, rpath: str, **kwargs: Any) -> None:
        return self.fs.put_file(lpath, rpath, **kwargs)

    async def _put(self, lpath: str, rpath: str, *args: Any, **kwargs: Any) -> None:
        return await self.fs._put(lpath, rpath, *args, **kwargs)

    def put(self, lpath: str, rpath: str, *args: Any, **kwargs: Any) -> None:
        return self.fs.put(lpath, rpath, *args, **kwargs)

    async def _get_file(self, rpath: str, lpath: str, **kwargs: Any) -> None:
        return await self.fs._get_file(rpath, lpath, **kwargs)

    def get_file(self, rpath: str, lpath: str, **kwargs: Any) -> None:
        return self.fs.get_file(rpath, lpath, **kwargs)

    async def _get(self, rpath: str, *args: Any, **kwargs: Any) -> None:
        return await self.fs._get(rpath, *args, **kwargs)

    def get(self, rpath: str, *args: Any, **kwargs: Any) -> None:
        return self.fs.get(rpath, *args, **kwargs)

    # File info operations

    async def _isfile(self, path: str) -> bool:
        return await self.fs._isfile(path)

    def isfile(self, path: str) -> bool:
        return self.fs.isfile(path)

    async def _isdir(self, path: str) -> bool:
        return await self.fs._isdir(path)

    def isdir(self, path: str) -> bool:
        return self.fs.isdir(path)

    async def _size(self, path: str) -> int:
        return await self.fs._size(path)

    def size(self, path: str) -> int:
        return self.fs.size(path)

    async def _exists(self, path: str) -> bool:
        return await self.fs._exists(path)

    def exists(self, path: str) -> bool:
        return self.fs.exists(path)

    async def _info(self, path: str, **kwargs: Any) -> dict[str, Any]:
        return await self.fs._info(path, **kwargs)

    def info(self, path: str, **kwargs: Any) -> dict[str, Any]:
        return self.fs.info(path, **kwargs)

    # Directory listing operations

    @overload
    async def _ls(
        self, path: str, detail: Literal[True] = ..., **kwargs: Any
    ) -> list[dict[str, Any]]: ...

    @overload
    async def _ls(self, path: str, detail: Literal[False], **kwargs: Any) -> list[str]: ...

    async def _ls(
        self, path: str, detail: bool = True, **kwargs: Any
    ) -> list[str] | list[dict[str, Any]]:
        return await self.fs._ls(path, detail=detail, **kwargs)

    @overload
    def ls(self, path: str, detail: Literal[True] = ..., **kwargs: Any) -> list[dict[str, Any]]: ...

    @overload
    def ls(self, path: str, detail: Literal[False], **kwargs: Any) -> list[str]: ...

    def ls(self, path: str, detail: bool = True, **kwargs: Any) -> list[str] | list[dict[str, Any]]:
        return self.fs.ls(path, detail=detail, **kwargs)

    async def _walk(
        self, path: str, *args: Any, **kwargs: Any
    ) -> AsyncIterator[tuple[str, list[str], list[str]]]:
        async for root, dirs, files in self.fs._walk(path, *args, **kwargs):
            yield root, dirs, files

    def walk(
        self, path: str, *args: Any, **kwargs: Any
    ) -> Iterator[tuple[str, list[str], list[str]]]:
        yield from self.fs.walk(path, *args, **kwargs)

    async def _glob(self, path: str, **kwargs: Any) -> list[str] | dict[str, dict[str, Any]]:
        return await self.fs._glob(path, **kwargs)

    def glob(self, path: str, **kwargs: Any) -> list[str] | dict[str, dict[str, Any]]:
        return self.fs.glob(path, **kwargs)

    async def _du(self, path: str, *args: Any, **kwargs: Any) -> int | dict[str, int]:
        return await self.fs._du(path, *args, **kwargs)

    def du(self, path: str, *args: Any, **kwargs: Any) -> int | dict[str, int]:
        return self.fs.du(path, *args, **kwargs)

    async def _find(
        self, path: str, *args: Any, **kwargs: Any
    ) -> list[str] | dict[str, dict[str, Any]]:
        return await self.fs._find(path, *args, **kwargs)

    def find(self, path: str, *args: Any, **kwargs: Any) -> list[str] | dict[str, dict[str, Any]]:
        return self.fs.find(path, *args, **kwargs)

    async def _expand_path(self, path: str, *args: Any, **kwargs: Any) -> list[str]:
        return await self.fs._expand_path(path, *args, **kwargs)

    def expand_path(self, path: str, *args: Any, **kwargs: Any) -> list[str]:
        return self.fs.expand_path(path, *args, **kwargs)

    # Directory operations

    async def _mkdir(self, path: str, *args: Any, **kwargs: Any) -> None:
        return await self.fs._mkdir(path, *args, **kwargs)

    def mkdir(self, path: str, *args: Any, **kwargs: Any) -> None:
        return self.fs.mkdir(path, *args, **kwargs)

    async def _makedirs(self, path: str, *args: Any, **kwargs: Any) -> None:
        return await self.fs._makedirs(path, *args, **kwargs)

    def makedirs(self, path: str, *args: Any, **kwargs: Any) -> None:
        return self.fs.makedirs(path, *args, **kwargs)

    def rmdir(self, path: str) -> None:
        return self.fs.rmdir(path)

    def mv(self, path1: str, path2: str, **kwargs: Any) -> None:
        return self.fs.mv(path1, path2, **kwargs)

    # Misc operations

    def touch(self, path: str, **kwargs: Any) -> None:
        return self.fs.touch(path, **kwargs)

    def created(self, path: str) -> Any:
        return self.fs.created(path)

    def modified(self, path: str) -> Any:
        return self.fs.modified(path)

    def sign(self, path: str, *args: Any, **kwargs: Any) -> str:
        return self.fs.sign(path, *args, **kwargs)

    # File opening

    def open(self, path: str, *args: Any, **kwargs: Any) -> Any:
        return self.fs.open(path, *args, **kwargs)

    async def open_async(self, path: str, *args: Any, **kwargs: Any) -> Any:
        return await self.fs.open_async(path, *args, **kwargs)

    def __repr__(self) -> str:
        return f"{self.__class__.__qualname__}(fs={self.fs})"
