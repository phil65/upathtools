"""Async UPath implementation using ProxyUPath with fallback to thread pools."""

from __future__ import annotations

import asyncio
from typing import TYPE_CHECKING, Any, Literal, Self, overload

from upath.extensions import ProxyUPath


if TYPE_CHECKING:
    from collections.abc import AsyncIterator

    from fsspec.asyn import AsyncFileSystem
    from upath.types import JoinablePathLike


class AsyncUPath(ProxyUPath):
    """UPath with async I/O capabilities.

    Provides async versions of I/O methods using the 'a' prefix (e.g., aread_bytes).
    For filesystems that don't support async operations, falls back to running
    sync methods in thread pools.

    This class extends ProxyUPath to properly maintain UPath compatibility
    while adding async capabilities.
    """

    async def afs(self) -> AsyncFileSystem:
        """Get async filesystem instance when possible, otherwise wrapped sync fs."""
        from upathtools.async_ops import get_async_fs

        return await get_async_fs(self.fs)

    async def aread_bytes(self) -> bytes:
        """Asynchronously read file content as bytes."""
        fs = await self.afs()
        return await fs._cat_file(self.path)

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
        fs = await self.afs()
        entries = await fs._ls(self.path, detail=False)
        for entry in entries:
            if isinstance(entry, dict):
                entry_path = entry.get("name", entry.get("path", ""))
            else:
                entry_path = str(entry)

            if entry_path and entry_path != self.path:
                yield self._from_upath(
                    type(self.__wrapped__)(
                        entry_path, protocol=self.protocol, **self.storage_options
                    )
                )

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
            full_pattern = str(self / pattern) if not pattern.startswith("/") else pattern
            matches = await fs._glob(full_pattern)
            for match_path in matches:
                if isinstance(match_path, dict):
                    match_path = match_path.get("name", match_path.get("path", ""))
                yield self._from_upath(
                    type(self.__wrapped__)(
                        match_path, protocol=self.protocol, **self.storage_options
                    )
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
        fs = await self.afs()
        info = await fs._info(self.path)
        from upath._stat import UPathStatResult

        return UPathStatResult.from_info(info)

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

    async def acopy(self, target: JoinablePathLike, **kwargs: Any) -> AsyncUPath:
        """Asynchronously copy file to target location."""
        target_path = (
            self._from_upath(type(self.__wrapped__)(target))
            if not isinstance(target, AsyncUPath)
            else target
        )

        # Read source and write to target
        content = await self.aread_bytes()
        await target_path.awrite_bytes(content)

        return target_path

    async def amove(self, target: JoinablePathLike) -> AsyncUPath:
        """Asynchronously move file to target location."""
        target_path = await self.acopy(target)
        await self.aunlink()
        return target_path

    def __repr__(self) -> str:
        return f"AsyncUPath({self.path!r}, protocol={self.protocol!r})"


if __name__ == "__main__":

    async def main() -> None:
        path = AsyncUPath("https://www.google.de")
        result = await path.aread_bytes()
        print(result)

    asyncio.run(main())
