"""Filesystem implementation for browsing Pydantic BaseModel schemas."""

from __future__ import annotations

import asyncio
import io
from typing import TYPE_CHECKING, Any, Literal, Self, overload

from fsspec.asyn import AsyncFileSystem
from fsspec.spec import AbstractFileSystem
from upath import UPath


if TYPE_CHECKING:
    from collections.abc import AsyncIterator, Buffer

    from upath.types import JoinablePathLike


class BaseUPath(UPath):
    """UPath implementation for browsing Pydantic BaseModel schemas."""

    @classmethod
    def _from_upath(cls, upath: UPath, /) -> Self:
        if isinstance(upath, cls):
            return upath
        return object.__new__(cls)

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
        try:
            fs = await self.afs()
            if hasattr(fs, "_cat_file"):
                return await fs._cat_file(self.path)
            if hasattr(fs, "cat_file"):
                return await asyncio.to_thread(fs.cat_file, self.path)
            # Final fallback to sync method
            return await asyncio.to_thread(self.read_bytes)
        except Exception:  # noqa: BLE001
            # If all else fails, use sync method in thread
            return await asyncio.to_thread(self.read_bytes)

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
        try:
            fs = await self.afs()

            # Try async open if available
            if hasattr(fs, "_open") or hasattr(fs, "open_async"):
                open_method = getattr(fs, "open_async", None) or fs._open

                async_file = await open_method(
                    self.path, "rt", encoding=encoding, errors=errors, newline=newline
                )
                async with async_file:
                    return await async_file.read()
            else:
                # Fallback to sync method in thread
                return await asyncio.to_thread(
                    self.read_text, encoding=encoding, errors=errors, newline=newline
                )

        except Exception:  # noqa: BLE001
            # Final fallback
            return await asyncio.to_thread(
                self.read_text, encoding=encoding, errors=errors, newline=newline
            )

    async def awrite_bytes(self, data: bytes) -> int:
        """Asynchronously write bytes to file."""
        try:
            fs = await self.afs()
            if hasattr(fs, "_pipe_file"):
                await fs._pipe_file(self.path, data)
                return len(data)
            if hasattr(fs, "pipe_file"):
                await asyncio.to_thread(fs.pipe_file, self.path, data)
                return len(data)
            return await asyncio.to_thread(self.write_bytes, data)
        except Exception:  # noqa: BLE001
            return await asyncio.to_thread(self.write_bytes, data)

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
        try:
            fs = await self.afs()

            if hasattr(fs, "_open") or hasattr(fs, "open_async"):
                open_method = getattr(fs, "open_async", None) or fs._open

                async_file = await open_method(
                    self.path, "wt", encoding=encoding, errors=errors, newline=newline
                )
                async with async_file:
                    return await async_file.write(data)
            else:
                return await asyncio.to_thread(
                    self.write_text,
                    data,
                    encoding=encoding,
                    errors=errors,
                    newline=newline,
                )

        except Exception:  # noqa: BLE001
            return await asyncio.to_thread(
                self.write_text, data, encoding=encoding, errors=errors, newline=newline
            )

    async def aexists(self) -> bool:
        """Asynchronously check if path exists."""
        try:
            fs = await self.afs()
            if hasattr(fs, "_exists"):
                return await fs._exists(self.path)
            return await asyncio.to_thread(fs.exists, self.path)
        except Exception:  # noqa: BLE001
            return await asyncio.to_thread(self.exists)

    async def ais_file(self) -> bool:
        """Asynchronously check if path is a file."""
        try:
            fs = await self.afs()
            if hasattr(fs, "_isfile"):
                return await fs._isfile(self.path)
            return await asyncio.to_thread(fs.isfile, self.path)
        except Exception:  # noqa: BLE001
            return await asyncio.to_thread(self.is_file)

    async def ais_dir(self) -> bool:
        """Asynchronously check if path is a directory."""
        try:
            fs = await self.afs()
            if hasattr(fs, "_isdir"):
                return await fs._isdir(self.path)
            return await asyncio.to_thread(fs.isdir, self.path)
        except Exception:  # noqa: BLE001
            return await asyncio.to_thread(self.is_dir)

    async def amkdir(
        self, mode: int = 0o777, parents: bool = False, exist_ok: bool = False
    ) -> None:
        """Asynchronously create directory."""
        try:
            fs = await self.afs()
            if hasattr(fs, "_makedirs"):
                await fs._makedirs(self.path, exist_ok=exist_ok)
            else:
                await asyncio.to_thread(self.mkdir, mode, parents, exist_ok)
        except Exception:  # noqa: BLE001
            await asyncio.to_thread(self.mkdir, mode, parents, exist_ok)

    async def atouch(self, exist_ok: bool = True) -> None:
        """Asynchronously create empty file or update timestamp."""
        try:
            fs = await self.afs()
            if hasattr(fs, "_touch"):
                await fs._touch(self.path, exist_ok=exist_ok)  # type: ignore
            else:
                await asyncio.to_thread(self.touch, exist_ok=exist_ok)
        except Exception:  # noqa: BLE001
            await asyncio.to_thread(self.touch, exist_ok=exist_ok)

    async def aunlink(self, missing_ok: bool = False) -> None:
        """Asynchronously remove file."""
        try:
            fs = await self.afs()
            if hasattr(fs, "_rm_file"):
                await fs._rm_file(self.path)
            elif hasattr(fs, "_rm"):
                await fs._rm(self.path)
            else:
                await asyncio.to_thread(self.unlink, missing_ok=missing_ok)
        except Exception:  # noqa: BLE001
            await asyncio.to_thread(self.unlink, missing_ok=missing_ok)

    async def armdir(self) -> None:
        """Asynchronously remove directory."""
        try:
            fs = await self.afs()
            if hasattr(fs, "_rmdir"):
                await fs._rmdir(self.path)
            else:
                await asyncio.to_thread(self.rmdir)
        except Exception:  # noqa: BLE001
            await asyncio.to_thread(self.rmdir)

    def aiterdir(self) -> AsyncIterator[Self]:
        """Asynchronously iterate over directory contents."""
        return self._aiterdir_impl()

    async def _aiterdir_impl(self) -> AsyncIterator[Self]:
        """Implementation of async directory iteration."""
        try:
            fs = await self.afs()
            if hasattr(fs, "_ls"):
                entries = await fs._ls(self.path, detail=False)
            else:
                entries = await asyncio.to_thread(fs.ls, self.path, detail=False)

            for entry in entries:
                if isinstance(entry, dict):
                    entry_path = entry.get("name", entry.get("path", ""))
                else:
                    entry_path = str(entry)

                if entry_path and entry_path != self.path:
                    yield self._from_upath(
                        type(self)(entry_path, protocol=self.protocol, **self.storage_options)
                    )

        except Exception:  # noqa: BLE001
            # Fallback to sync iteration in thread
            sync_iter = await asyncio.to_thread(lambda: list(self.iterdir()))
            for item in sync_iter:
                yield item

    def aglob(self, pattern: str, *, case_sensitive: bool | None = None) -> AsyncIterator[Self]:
        """Asynchronously glob for paths matching pattern."""
        return self._aglob_impl(pattern, case_sensitive=case_sensitive)

    async def _aglob_impl(
        self, pattern: str, *, case_sensitive: bool | None = None
    ) -> AsyncIterator[Self]:
        """Implementation of async glob."""
        # TODO: deal with None
        case_sensitive = case_sensitive or False
        try:
            fs = await self.afs()
            if hasattr(fs, "_glob"):
                full_pattern = str(self / pattern) if not pattern.startswith("/") else pattern
                matches = await fs._glob(full_pattern)
            else:
                # Fallback to sync glob in thread
                sync_matches = await asyncio.to_thread(
                    lambda: list(self.glob(pattern, case_sensitive=case_sensitive))
                )
                for match in sync_matches:
                    yield match
                return

            for match_path in matches:
                if isinstance(match_path, dict):
                    match_path = match_path.get("name", match_path.get("path", ""))
                yield self._from_upath(
                    type(self)(match_path, protocol=self.protocol, **self.storage_options)
                )

        except Exception:  # noqa: BLE001
            # Final fallback to sync glob in thread
            sync_matches = await asyncio.to_thread(
                lambda: list(self.glob(pattern, case_sensitive=case_sensitive))
            )
            for match in sync_matches:
                yield match

    def arglob(self, pattern: str, *, case_sensitive: bool | None = None) -> AsyncIterator[Self]:
        """Asynchronously recursively glob for paths matching pattern."""
        return self.aglob(f"**/{pattern}", case_sensitive=case_sensitive)

    async def astat(self, *, follow_symlinks: bool = True):
        """Asynchronously get file stats."""
        try:
            fs = await self.afs()
            if hasattr(fs, "_info"):
                info = await fs._info(self.path)
                from upath._stat import UPathStatResult

                return UPathStatResult.from_info(info)
            return await asyncio.to_thread(self.stat, follow_symlinks=follow_symlinks)
        except Exception:  # noqa: BLE001
            return await asyncio.to_thread(self.stat, follow_symlinks=follow_symlinks)

    @overload
    async def aopen(
        self,
        mode: Literal["r", "rt"] = "r",
        buffering: int = -1,
        encoding: str | None = None,
        errors: str | None = None,
        newline: str | None = None,
    ) -> None: ...

    @overload
    async def aopen(
        self,
        mode: Literal["rb"],
        buffering: int = -1,
        encoding: str | None = None,
        errors: str | None = None,
        newline: str | None = None,
    ) -> None: ...

    @overload
    async def aopen(
        self,
        mode: Literal["w", "wt", "x", "xt", "a", "at"],
        buffering: int = -1,
        encoding: str | None = None,
        errors: str | None = None,
        newline: str | None = None,
    ) -> None: ...

    @overload
    async def aopen(
        self,
        mode: Literal["wb", "xb", "ab"],
        buffering: int = -1,
        encoding: str | None = None,
        errors: str | None = None,
        newline: str | None = None,
    ) -> None: ...

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
        try:
            fs = await self.afs()

            if hasattr(fs, "_open") or hasattr(fs, "open_async"):
                open_method = getattr(fs, "open_async", None) or fs._open
                return await open_method(
                    self.path,
                    mode=mode,
                    buffering=buffering,
                    encoding=encoding,
                    errors=errors,
                    newline=newline,
                    **kwargs,
                )
            # Note: This returns a sync file object wrapped to work in async context
            return await asyncio.to_thread(
                self.open,
                mode=mode,
                buffering=buffering,
                encoding=encoding,
                errors=errors,
                newline=newline,
                **kwargs,
            )
        except Exception:  # noqa: BLE001
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
        target_path = (
            self._from_upath(type(self)(target)) if not isinstance(target, BaseUPath) else target
        )

        # Read source and write to target
        content = await self.aread_bytes()
        await target_path.awrite_bytes(content)

        return target_path

    async def amove(self, target: JoinablePathLike) -> BaseUPath:
        """Asynchronously move file to target location."""
        target_path = await self.acopy(target)
        await self.aunlink()
        return target_path

    def __repr__(self) -> str:
        return f"BaseUPath({self.path!r}, protocol={self.protocol!r})"


class BaseAsyncFileSystem[TPath: UPath](AsyncFileSystem):
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


class BaseFileSystem[TPath: UPath](AbstractFileSystem):
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


class AsyncFile:
    """Asynchronous writer for gist files."""

    def __init__(self, fs: AsyncFileSystem, path: str, **kwargs: Any) -> None:
        """Initialize the writer.

        Args:
            fs: GistFileSystem instance
            path: Path to write to
            **kwargs: Additional arguments to pass to _pipe_file
        """
        self.fs = fs
        self.path = path
        self.buffer = io.BytesIO()
        self.kwargs = kwargs
        self.closed = False

    async def write(self, data: bytes) -> int:
        """Write data to the buffer.

        Args:
            data: Data to write

        Returns:
            Number of bytes written
        """
        return self.buffer.write(data)

    async def close(self) -> None:
        """Close the writer and write content to the file."""
        if not self.closed:
            self.closed = True
            content = self.buffer.getvalue()
            await self.fs._pipe_file(self.path, content, **self.kwargs)
            self.buffer.close()

    def __aenter__(self) -> AsyncFile:
        """Enter the context manager."""
        return self

    async def __aexit__(self, exc_type: object, exc_val: object, exc_tb: object) -> None:
        """Exit the context manager and close the writer."""
        await self.close()


class BufferedWriter(io.BufferedIOBase):
    """Buffered writer for filesystems that writes when closed.

    Generic implementation that can be used by any filesystem implementing
    a pipe_file method for writing content.
    """

    def __init__(
        self,
        buffer: io.BytesIO,
        fs: AbstractFileSystem,
        path: str,
        **kwargs: Any,
    ) -> None:
        """Initialize the writer.

        Args:
            buffer: Buffer to store content
            fs: Filesystem instance with pipe_file method
            path: Path to write to
            **kwargs: Additional arguments to pass to pipe_file
        """
        super().__init__()
        self.buffer = buffer
        self.fs = fs
        self.path = path
        self.kwargs = kwargs

    def write(self, data: Buffer) -> int:
        """Write data to the buffer.

        Args:
            data: Data to write

        Returns:
            Number of bytes written
        """
        return self.buffer.write(data)

    def close(self) -> None:
        """Close the writer and write content to the filesystem."""
        if not self.closed:
            # Get the buffer contents and write to the filesystem
            content = self.buffer.getvalue()
            self.fs.pipe_file(self.path, content, **self.kwargs)
            self.buffer.close()
            super().close()

    def readable(self) -> bool:
        """Whether the writer is readable."""
        return False

    def writable(self) -> bool:
        """Whether the writer is writable."""
        return True
