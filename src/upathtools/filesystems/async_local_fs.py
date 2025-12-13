from __future__ import annotations

from asyncio import get_running_loop, iscoroutinefunction
from functools import partial, wraps
import os
import shutil
from typing import TYPE_CHECKING, Any, Literal, Required, overload

from fsspec.implementations.local import LocalFileSystem

from upathtools.filesystems.base import BaseAsyncFileSystem, BaseUPath
from upathtools.filesystems.base.file_objects import FileInfo


if TYPE_CHECKING:
    from collections.abc import Awaitable, Callable


class LocalFileInfo(FileInfo, total=False):
    """Info dict for local filesystem paths."""

    size: int
    created: float
    islink: bool
    mode: int
    uid: int
    gid: int
    mtime: float
    ino: int
    nlink: int
    destination: Required[bool]


class LocalPath(BaseUPath[LocalFileInfo]):
    """UPath implementation for local filesystem."""

    __slots__ = ()


def wrap[**P, R](func: Callable[P, R]) -> Callable[P, Awaitable[R]]:
    @wraps(func)
    async def run(*args: P.args, **kwargs: P.kwargs) -> R:
        loop = get_running_loop()
        pfunc = partial(func, *args, **kwargs)
        return await loop.run_in_executor(None, pfunc)

    return run


class AsyncLocalFileSystem(BaseAsyncFileSystem[LocalPath, LocalFileInfo], LocalFileSystem):
    """Async implementation of LocalFileSystem.

    This filesystem provides both async and sync methods. The sync methods are not
    overridden and use LocalFileSystem's implementation.

    The async methods run the respective sync methods in a threadpool executor.
    It also provides open_async() method that supports asynchronous file operations,
    using `aiofile`_.

    Note that some async methods like _find may call these wrapped async methods
    many times, and might have high overhead.
    In that case, it might be faster to run the whole operation in a threadpool,
    which is available as `_*_async()` versions of the API.
    eg: _find_async()/_get_file_async, etc.

    .. aiofile:
        https://github.com/mosquito/aiofile
    """

    mirror_sync_methods = False
    upath_cls = LocalPath

    _cat_file = wrap(LocalFileSystem.cat_file)  # type: ignore[assignment]
    _chmod = wrap(LocalFileSystem.chmod)
    _cp_file = wrap(LocalFileSystem.cp_file)  # type: ignore[assignment]
    _created = wrap(LocalFileSystem.created)
    _find_async = wrap(LocalFileSystem.find)
    _get_file_async = wrap(LocalFileSystem.get_file)
    _islink = wrap(LocalFileSystem.islink)
    _lexists = wrap(LocalFileSystem.lexists)
    _link = wrap(LocalFileSystem.link)
    _makedirs = wrap(LocalFileSystem.makedirs)  # type: ignore[assignment]
    _mkdir = wrap(LocalFileSystem.mkdir)  # type: ignore[assignment]
    _modified = wrap(LocalFileSystem.modified)
    # `mv_file` was renamed to `mv` in fsspec==2024.5.0
    # https://github.com/fsspec/filesystem_spec/pull/1585
    _mv = wrap(getattr(LocalFileSystem, "mv", None) or LocalFileSystem.mv_file)  # type: ignore[arg-type,assignment]
    _mv_file = _mv  # type: ignore[assignment]
    _pipe_file = wrap(LocalFileSystem.pipe_file)  # type: ignore[assignment]
    _put_file = wrap(LocalFileSystem.put_file)  # type: ignore[assignment]
    _read_bytes = wrap(LocalFileSystem.read_bytes)
    _read_text = wrap(LocalFileSystem.read_text)
    _rm = wrap(LocalFileSystem.rm)  # type: ignore[assignment]
    _rm_file = wrap(LocalFileSystem.rm_file)  # type: ignore[assignment]
    _rmdir = wrap(LocalFileSystem.rmdir)
    _touch = wrap(LocalFileSystem.touch)
    _symlink = wrap(LocalFileSystem.symlink)
    _write_bytes = wrap(LocalFileSystem.write_bytes)
    _write_text = wrap(LocalFileSystem.write_text)
    sign = LocalFileSystem.sign

    async def _info(self, path: str, **kwargs: Any) -> LocalFileInfo:
        """Get info for a single path."""
        loop = get_running_loop()
        return await loop.run_in_executor(None, partial(LocalFileSystem.info, self, path, **kwargs))  # type: ignore[return-value]

    @overload
    async def _ls(
        self,
        path: str,
        detail: Literal[True] = ...,
        **kwargs: Any,
    ) -> list[LocalFileInfo]: ...

    @overload
    async def _ls(
        self,
        path: str,
        detail: Literal[False],
        **kwargs: Any,
    ) -> list[str]: ...

    async def _ls(
        self,
        path: str,
        detail: bool = True,
        **kwargs: Any,
    ) -> list[LocalFileInfo] | list[str]:
        """List directory contents."""
        loop = get_running_loop()
        return await loop.run_in_executor(  # type: ignore[return-value]
            None, partial(LocalFileSystem.ls, self, path, detail=detail, **kwargs)
        )

    async def _get_file(
        self,
        src: str,
        dst: Any,
        **kwargs: Any,
    ) -> None:
        if not iscoroutinefunction(getattr(dst, "write", None)):
            src = self._strip_protocol(src)
            return await self._get_file_async(src, dst)

        fsrc = await self.open_async(src, "rb")
        async with fsrc:
            while True:
                buf = await fsrc.read(length=shutil.COPY_BUFSIZE)  # type: ignore[attr-defined]
                if not buf:
                    break
                await dst.write(buf)

    async def open_async(
        self,
        path: str,
        mode: str = "rb",
        **kwargs: Any,
    ) -> Any:
        import aiofile

        path = self._strip_protocol(path)
        if self.auto_mkdir and "w" in mode:
            await self._makedirs(self._parent(path), exist_ok=True)
        return await aiofile.async_open(path, mode, **kwargs)


def register_flavour() -> bool:
    """Register UPath flavour for AsyncLocalFileSystem."""
    try:
        from upath._flavour_sources import AbstractFileSystemFlavour
    except ImportError:
        return False

    from fsspec.implementations.local import make_path_posix
    from fsspec.utils import stringify_path

    class AsyncLocalFileSystemFlavour(AbstractFileSystemFlavour):
        __orig_class__ = "upathtools.filesystems.async_local.AsyncLocalFileSystem"
        protocol = ()
        root_marker = "/"
        sep = "/"
        local_file = True

        @classmethod
        def _strip_protocol(cls, path):  # type: ignore[override]
            path = stringify_path(path)
            if path.startswith("file://"):
                path = path[7:]
            elif path.startswith("file:"):
                path = path[5:]
            elif path.startswith("local://"):
                path = path[8:]
            elif path.startswith("local:"):
                path = path[6:]

            path = str(make_path_posix(path))
            if os.sep != "/":
                if path[1:2] == ":":
                    drive, path = path[:2], path[2:]
                elif path[:2] == "//":
                    if (index1 := path.find("/", 2)) == -1 or (
                        index2 := path.find("/", index1 + 1)
                    ) == -1:
                        drive, path = path, ""
                    else:
                        drive, path = path[:index2], path[index2:]
                else:
                    drive = ""

                return drive + (path.rstrip("/") or cls.root_marker)

            return path.rstrip("/") or cls.root_marker

        @classmethod
        def _parent(cls, path):  # type: ignore[override]
            path = cls._strip_protocol(path)
            if os.sep == "/":
                return path.rsplit("/", 1)[0] or "/"
            path_ = path.rsplit("/", 1)[0]
            if len(path_) <= 3 and path_[1:2] == ":":  # noqa: PLR2004
                return path_[0] + ":/"
            return path_

    return True


if __name__ == "__main__":
    import asyncio

    async def main():
        fs = AsyncLocalFileSystem()
        await fs._mkdir("test")
        ls = await fs._ls("")
        print(ls)

    asyncio.run(main())
