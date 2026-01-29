"""Overlay filesystem with copy-on-write semantics.

Provides a layered filesystem where reads search through layers top-to-bottom,
but writes always go to the uppermost (first) filesystem layer.
"""

from __future__ import annotations

import asyncio
import errno
import os
from typing import TYPE_CHECKING, Any, Literal, overload

from fsspec.asyn import AsyncFileSystem
from fsspec.implementations.asyn_wrapper import AsyncFileSystemWrapper

from upathtools.filesystems.base import BaseAsyncFileSystem, BaseUPath
from upathtools.filesystems.base.file_objects import FileInfo


if TYPE_CHECKING:
    from collections.abc import Awaitable, Callable, Sequence

    from fsspec.spec import AbstractFileSystem

    from upathtools.filesystems.base import CreationMode


class OverlayInfo(FileInfo, total=False):
    """Info dict for overlay filesystem paths."""

    size: int
    created: float
    modified: float
    layer: int  # Which layer the file was found in (0 = upper)


class OverlayPath(BaseUPath[OverlayInfo]):
    """UPath implementation for overlay filesystem."""

    __slots__ = ()


def to_async(filesystem: AbstractFileSystem) -> AsyncFileSystem:
    """Convert a sync filesystem to async if needed."""
    if isinstance(filesystem, AsyncFileSystem):
        return filesystem
    return AsyncFileSystemWrapper(filesystem)


class OverlayFileSystem(BaseAsyncFileSystem[OverlayPath, OverlayInfo]):
    """Async overlay filesystem with copy-on-write semantics.

    This filesystem layers multiple filesystems on top of each other:
    - Reads search through layers from top (index 0) to bottom
    - Writes always go to the uppermost layer (index 0)

    This is similar to Docker's overlay filesystem or union mounts.

    Example:
        ```python
        from fsspec.implementations.local import LocalFileSystem
        from fsspec.implementations.memory import MemoryFileSystem

        # Writable memory layer on top of read-only local filesystem
        overlay = OverlayFileSystem(
            filesystems=[MemoryFileSystem(), LocalFileSystem()]
        )

        # Reads from local if not in memory, writes go to memory
        content = await overlay._cat_file("/some/file")
        await overlay._pipe_file("/some/file", b"modified")  # Goes to memory
        ```
    """

    protocol = "overlay"
    root_marker = "/"
    upath_cls = OverlayPath
    cachable = False

    def __init__(
        self,
        filesystems: Sequence[AbstractFileSystem] | None = None,
        **kwargs: Any,
    ) -> None:
        """Initialize overlay filesystem.

        Args:
            filesystems: List of filesystems, first is the writable upper layer
            **kwargs: Additional arguments, can include protocol names with options
                      to create filesystems (e.g., `memory={}`, `file={"auto_mkdir": True}`)
        """
        super().__init__(**kwargs)

        self.layers: list[AsyncFileSystem] = []

        if filesystems:
            self.layers.extend(to_async(fs) for fs in filesystems)

        # Support creating filesystems from protocol kwargs
        for key, options in kwargs.items():
            if key.startswith("_") or key in ("asynchronous", "loop"):
                continue
            if options is None:
                options = {}
            import fsspec

            self.layers.append(to_async(fsspec.filesystem(key, **options)))

        if not self.layers:
            msg = "Must provide at least one filesystem layer"
            raise ValueError(msg)

    @property
    def upper_fs(self) -> AsyncFileSystem:
        """The writable upper layer (first filesystem)."""
        return self.layers[0]

    @property
    def fsid(self) -> str:
        """Unique identifier for this filesystem configuration."""
        layer_ids = []
        for fs in self.layers:
            if hasattr(fs, "fsid"):
                layer_ids.append(fs.fsid)
            else:
                layer_ids.append(type(fs).__name__)
        return "overlay_" + "+".join(layer_ids)

    async def _find_in_layers(self, path: str) -> tuple[AsyncFileSystem, int] | None:
        """Find which layer contains the path.

        Returns:
            Tuple of (filesystem, layer_index) or None if not found
        """
        for i, fs in enumerate(self.layers):
            try:
                if await fs._exists(path):
                    return fs, i
            except (NotImplementedError, AttributeError):
                continue
        return None

    async def _cat_file(
        self,
        path: str,
        start: int | None = None,
        end: int | None = None,
        **kwargs: Any,
    ) -> bytes:
        """Read file contents from the first layer that has it."""
        for fs in self.layers:
            try:
                return await fs._cat_file(path, start=start, end=end, **kwargs)
            except (FileNotFoundError, NotImplementedError, AttributeError):
                continue
        raise FileNotFoundError(errno.ENOENT, os.strerror(errno.ENOENT), path)

    async def _pipe_file(
        self,
        path: str,
        value: bytes,
        mode: CreationMode = "overwrite",
        **kwargs: Any,
    ) -> None:
        """Write file contents to the upper layer."""
        await self.upper_fs._pipe_file(path, value, **kwargs)

    async def _info(self, path: str, **kwargs: Any) -> OverlayInfo:
        """Get info from the first layer that has the path."""
        for i, fs in enumerate(self.layers):
            try:
                info = await fs._info(path, **kwargs)
                return OverlayInfo(
                    name=info.get("name", path),
                    type=info.get("type", "file"),
                    size=info.get("size", 0),
                    layer=i,
                )
            except (FileNotFoundError, NotImplementedError, AttributeError):
                continue
        raise FileNotFoundError(errno.ENOENT, os.strerror(errno.ENOENT), path)

    @overload
    async def _ls(
        self,
        path: str,
        detail: Literal[True] = ...,
        **kwargs: Any,
    ) -> list[OverlayInfo]: ...

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
    ) -> list[OverlayInfo] | list[str]:
        """List directory contents, merging from all layers.

        Files in upper layers shadow those in lower layers.
        """

        async def _ls_layer(
            fs: AsyncFileSystem, layer_index: int
        ) -> tuple[int, list[dict[str, Any]]]:
            # Translate root marker for filesystems with different conventions
            layer_path = fs.root_marker if path == self.root_marker else path
            try:
                items = await fs._ls(layer_path, detail=True, **kwargs)
            except (FileNotFoundError, NotImplementedError):
                return layer_index, []
            else:
                return layer_index, items

        results = await asyncio.gather(*(_ls_layer(fs, i) for i, fs in enumerate(self.layers)))

        merged: dict[str, OverlayInfo] = {}
        for layer_index, items in sorted(results, key=lambda r: r[0]):
            for item in items:
                name = item.get("name", "").strip("/")
                if name not in merged:  # Upper layers take precedence
                    merged[name] = OverlayInfo(
                        name=name,
                        type=item.get("type", "file"),
                        size=item.get("size", 0),
                        layer=layer_index,
                    )

        if not merged:
            exists_results = await asyncio.gather(
                *(self._safe_exists(fs, path) for fs in self.layers)
            )
            if not any(exists_results):
                raise FileNotFoundError(errno.ENOENT, os.strerror(errno.ENOENT), path)

        result = [v for _, v in sorted(merged.items())]
        if detail:
            return result
        return [item["name"] for item in result]

    async def _safe_exists(self, fs: AsyncFileSystem, path: str) -> bool:
        """Check if path exists, returning False on errors."""
        try:
            return await fs._exists(path)
        except (NotImplementedError, AttributeError):
            return False

    async def _any_layer(
        self,
        check: Callable[[AsyncFileSystem], Awaitable[bool]],
    ) -> bool:
        """Run a boolean check against all layers concurrently.

        Returns True as soon as the first layer returns True,
        cancelling remaining tasks.
        """

        async def _safe_check(fs: AsyncFileSystem) -> bool:
            try:
                return await check(fs)
            except (NotImplementedError, AttributeError):
                return False

        tasks = [asyncio.ensure_future(_safe_check(fs)) for fs in self.layers]
        try:
            for coro in asyncio.as_completed(tasks):
                if await coro:
                    return True
            return False
        finally:
            for task in tasks:
                task.cancel()

    async def _exists(self, path: str, **kwargs: Any) -> bool:
        """Check if path exists in any layer."""
        return await self._any_layer(lambda fs: fs._exists(path, **kwargs))

    async def _isdir(self, path: str) -> bool:
        """Check if path is a directory in any layer."""
        return await self._any_layer(lambda fs: fs._isdir(path))

    async def _isfile(self, path: str) -> bool:
        """Check if path is a file in any layer."""
        return await self._any_layer(lambda fs: fs._isfile(path))

    async def _mkdir(self, path: str, create_parents: bool = True, **kwargs: Any) -> None:
        """Create directory in the upper layer."""
        if await self._exists(path):
            raise FileExistsError(errno.EEXIST, os.strerror(errno.EEXIST), path)
        parent = self._parent(path)
        if not create_parents and not await self._isdir(parent):
            raise FileNotFoundError(errno.ENOENT, os.strerror(errno.ENOENT), path)
        await self.upper_fs._mkdir(path, create_parents=True, **kwargs)

    async def _makedirs(self, path: str, exist_ok: bool = False) -> None:
        """Create directory and parents in the upper layer."""
        await self.upper_fs._makedirs(path, exist_ok=exist_ok)

    async def _rmdir(self, path: str, **kwargs: Any) -> None:
        """Remove directory from the upper layer."""
        await self.upper_fs._rmdir(path, **kwargs)  # type: ignore[attr-defined]

    async def _rm_file(self, path: str, **kwargs: Any) -> None:
        """Remove file from the upper layer."""
        await self.upper_fs._rm_file(path, **kwargs)

    async def _rm(
        self,
        path: str,
        recursive: bool = False,
        batch_size: int | None = None,
        **kwargs: Any,
    ) -> None:
        """Remove path from the upper layer."""
        await self.upper_fs._rm(path, recursive=recursive, **kwargs)

    async def _cp_file(self, path1: str, path2: str, **kwargs: Any) -> None:
        """Copy file, reading from any layer, writing to upper layer."""
        result = await self._find_in_layers(path1)
        if result is None:
            raise FileNotFoundError(errno.ENOENT, os.strerror(errno.ENOENT), path1)

        src_fs, _ = result

        if src_fs is self.upper_fs:
            # Source is in upper layer, use its cp_file
            await src_fs._cp_file(path1, path2, **kwargs)
        else:
            # Cross-layer copy: read from source layer, write to upper
            content = await src_fs._cat_file(path1)
            await self.upper_fs._pipe_file(path2, content)

    async def _created(self, path: str) -> float:
        """Get creation time from the first layer that has the path."""
        for fs in self.layers:
            try:
                return await fs._created(path)  # type: ignore[attr-defined]
            except (FileNotFoundError, NotImplementedError, AttributeError):
                continue
        raise FileNotFoundError(errno.ENOENT, os.strerror(errno.ENOENT), path)

    async def _modified(self, path: str) -> float:
        """Get modification time from the first layer that has the path."""
        for fs in self.layers:
            try:
                return await fs._modified(path)  # type: ignore[attr-defined]
            except (FileNotFoundError, NotImplementedError, AttributeError):
                continue
        raise FileNotFoundError(errno.ENOENT, os.strerror(errno.ENOENT), path)

    def sign(self, path: str, expiration: int = 100, **kwargs: Any) -> str:
        """Sign a path using the upper layer."""
        return self.upper_fs.sign(path, expiration, **kwargs)
