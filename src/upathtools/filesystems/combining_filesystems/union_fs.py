"""Filesystem implementation that combines multiple filesystems under named folders."""

from __future__ import annotations

from collections.abc import Mapping
import logging
from typing import TYPE_CHECKING, Any, Literal, overload

from fsspec.asyn import AsyncFileSystem
from fsspec.implementations.asyn_wrapper import AsyncFileSystemWrapper
from fsspec.spec import AbstractFileSystem

from upathtools.filesystems.base import BaseAsyncFileSystem, BaseUPath, FileInfo


if TYPE_CHECKING:
    from collections.abc import Sequence

    from upath.types import JoinablePathLike


def to_async(filesystem: AbstractFileSystem) -> AsyncFileSystem:
    if not isinstance(filesystem, AsyncFileSystem):
        return AsyncFileSystemWrapper(filesystem)
    return filesystem


class UnionInfo(FileInfo, total=False):
    """Info dict for union filesystem paths."""

    size: int
    mount_points: list[str]


logger = logging.getLogger(__name__)


class UnionPath(BaseUPath[UnionInfo]):
    """UPath implementation for browsing UnionFS."""

    __slots__ = ()

    def iterdir(self):
        if not self.is_dir():
            raise NotADirectoryError(str(self))
        yield from super().iterdir()

    @property
    def path(self) -> str:
        """Return the path part without protocol."""
        path = super().path
        if path in ("", ".", "/"):
            return "/"
        return path.lstrip("/")

    def __str__(self) -> str:
        """Return string representation."""
        return super().__str__().replace(":///", "://")


class UnionFileSystem(BaseAsyncFileSystem[UnionPath, UnionInfo]):
    """Filesystem that combines multiple filesystems under named mount points.

    Accepts either:
    - A list of filesystems: mount points will be the filesystem's protocol
    - A dict mapping names to filesystems: mount points will be the dict keys

    The first folder level is always the mount point name.
    """

    protocol = "union"
    root_marker = "/"
    upath_cls = UnionPath
    cachable = False

    def __init__(
        self,
        filesystems: (
            Sequence[AbstractFileSystem | JoinablePathLike]
            | Mapping[str, AbstractFileSystem | JoinablePathLike]
            | None
        ) = None,
        **kwargs: Any,
    ) -> None:
        """Initialize the filesystem.

        Args:
            filesystems: Either a sequence (uses protocol as key), mapping (uses dict keys),
            or None for empty
            kwargs: Additional keyword arguments for AsyncFileSystem
        """
        from upathtools.helpers import upath_to_fs

        super().__init__(**kwargs)
        resolved: dict[str, AsyncFileSystem] = {}
        filesystems = filesystems or {}
        if isinstance(filesystems, Mapping):
            for name, fs_or_path in filesystems.items():
                if isinstance(fs_or_path, AbstractFileSystem):
                    resolved[name] = to_async(fs_or_path)
                else:
                    resolved[name] = upath_to_fs(fs_or_path, asynchronous=True)
        else:
            for fs_or_path in filesystems:
                if isinstance(fs_or_path, AbstractFileSystem):
                    fs = to_async(fs_or_path)
                else:
                    fs = upath_to_fs(fs_or_path, asynchronous=True)
                # Use protocol as key, handle tuple protocols
                proto = fs.protocol
                key = proto[0] if isinstance(proto, tuple) else proto
                if key in resolved:
                    msg = f"Duplicate protocol '{key}' in filesystem list"
                    raise ValueError(msg)
                resolved[key] = fs

        self.filesystems = resolved
        logger.debug("Created UnionFileSystem with mount points: %s", list(resolved))

    @staticmethod
    def _get_kwargs_from_urls(path: str) -> dict[str, Any]:
        """Parse union URL and return constructor kwargs.

        Supports URL formats:
        - union://name1=path1,name2=path2  (name=path pairs)
        - union://?cache=memory://&data=/tmp/dir  (query parameters)
        """
        import urllib.parse

        path_without_protocol = path.removeprefix("union://")
        filesystem_paths: dict[str, str] = {}

        if "?" in path_without_protocol:
            _, query_part = path_without_protocol.split("?", 1)
            query_params = urllib.parse.parse_qs(query_part)
            for name, path_list in query_params.items():
                if path_list:
                    filesystem_paths[name] = path_list[0]
        elif path_without_protocol:
            pairs = [p.strip() for p in path_without_protocol.split(",") if p.strip()]
            for pair in pairs:
                if "=" in pair:
                    name, path_value = pair.split("=", 1)
                    filesystem_paths[name.strip()] = path_value.strip()

        return {"filesystems": filesystem_paths} if filesystem_paths else {}

    def _get_fs_and_path(self, path: str) -> tuple[AsyncFileSystem, str]:
        """Get filesystem and normalized path from a union path.

        Path format: /mount_point/rest/of/path or mount_point/rest/of/path
        Also handles protocol URLs like union:// or union:///
        """
        if not path or path == self.root_marker:
            return self, self.root_marker

        # Handle protocol prefixes
        if path.startswith("union://"):
            path = path[8:]  # Remove "union://"
            if not path:  # Just "union://" -> root
                return self, self.root_marker

        # Normalize: strip leading slash and split
        norm_path = path.lstrip("/")
        if not norm_path:
            return self, self.root_marker

        parts = norm_path.split("/", 1)
        mount_point = parts[0]
        rest = parts[1] if len(parts) > 1 else ""

        if mount_point == "union":
            return self, self.root_marker

        if mount_point not in self.filesystems:
            msg = f"Unknown mount point: {mount_point}"
            raise ValueError(msg)

        fs = self.filesystems[mount_point]
        return fs, rest or fs.root_marker

    def _get_mount_point(self, fs: AsyncFileSystem) -> str:
        """Get mount point name for a filesystem."""
        return next(name for name, f in self.filesystems.items() if f is fs)

    def _make_path(self, mount_point: str, path: str) -> str:
        """Create a union path from mount point and inner path."""
        path = path.lstrip("/")
        if path:
            return f"{mount_point}/{path}"
        return mount_point

    async def _cat_file(self, path: str, start=None, end=None, **kwargs: Any):
        """Get file contents."""
        logger.debug("Reading from path: %s", path)
        fs, inner_path = self._get_fs_and_path(path)
        return await fs._cat_file(inner_path, start=start, end=end, **kwargs)

    async def _pipe_file(
        self,
        path: str,
        value,
        mode: Literal["create", "overwrite"] = "overwrite",
        **kwargs: Any,
    ) -> None:
        """Write file contents."""
        logger.debug("Writing to path: %s", path)
        fs, inner_path = self._get_fs_and_path(path)
        await fs._pipe_file(inner_path, value, **kwargs)

    async def _info(self, path: str, **kwargs: Any) -> UnionInfo:
        """Get info about a path."""
        logger.debug("Getting info for path: %s", path)
        fs, inner_path = self._get_fs_and_path(path)

        if fs is self:
            return UnionInfo(
                name="/",
                size=0,
                type="directory",
                mount_points=list(self.filesystems),
            )

        out = await fs._info(inner_path, **kwargs)
        mount_point = self._get_mount_point(fs)

        # Normalize the name from underlying fs
        name = out.get("name", "")
        if "://" in name:
            name = name.split("://", 1)[1]
        name = name.lstrip("/")

        return UnionInfo(
            name=self._make_path(mount_point, name),
            type=out.get("type", "file"),
            size=out.get("size", 0),
        )

    @overload
    async def _ls(
        self, path: str, detail: Literal[True] = ..., **kwargs: Any
    ) -> list[UnionInfo]: ...

    @overload
    async def _ls(self, path: str, detail: Literal[False], **kwargs: Any) -> list[str]: ...

    async def _ls(
        self,
        path: str,
        detail: bool = True,
        **kwargs: Any,
    ) -> list[str] | list[UnionInfo]:
        """List contents of a path."""
        logger.debug("Listing path: %s", path)
        fs, inner_path = self._get_fs_and_path(path)

        # Root listing: show mount points
        if fs is self:
            if detail:
                return [UnionInfo(name=name, type="directory", size=0) for name in self.filesystems]
            return list(self.filesystems.keys())

        # Delegate to underlying filesystem
        logger.debug("Using filesystem %s for path %s", fs, inner_path)
        out = await fs._ls(inner_path, detail=True, **kwargs)
        logger.debug("Raw listing: %s", out)

        mount_point = self._get_mount_point(fs)
        results = []

        for item in out:
            item_copy = item.copy()
            name = item_copy.get("name", "")
            # Strip any protocol prefix from underlying fs
            if "://" in name:
                name = name.split("://", 1)[1]
            name = name.lstrip("/")
            item_copy["name"] = self._make_path(mount_point, name)
            results.append(item_copy)

        logger.debug("Final listing: %s", results)
        return results if detail else [o["name"] for o in results]

    async def _exists(self, path: str, **kwargs: Any) -> bool:
        """Check if path exists."""
        try:
            fs, inner_path = self._get_fs_and_path(path)
        except ValueError:
            return False
        if fs is self:
            return True
        return await fs._exists(inner_path, **kwargs)

    async def _isdir(self, path: str) -> bool:
        """Check if path is a directory."""
        logger.debug("Checking if directory: %s", path)
        try:
            fs, inner_path = self._get_fs_and_path(path)
        except ValueError:
            return False
        if fs is self:
            return True
        return await fs._isdir(inner_path)

    async def _isfile(self, path: str) -> bool:
        """Check if path is a file."""
        try:
            fs, inner_path = self._get_fs_and_path(path)
        except ValueError:
            return False
        if fs is self:
            return False
        return await fs._isfile(inner_path)

    async def _makedirs(self, path: str, exist_ok: bool = False) -> None:
        """Create a directory and parents."""
        logger.debug("Making directories: %s", path)
        fs, inner_path = self._get_fs_and_path(path)
        if fs is self:
            return
        await fs._makedirs(inner_path, exist_ok=exist_ok)

    async def _rm_file(self, path: str, **kwargs: Any) -> None:
        """Remove a file."""
        logger.debug("Removing file: %s", path)
        fs, inner_path = self._get_fs_and_path(path)
        await fs._rm_file(inner_path, **kwargs)

    async def _rm(
        self,
        path: str,
        recursive: bool = False,
        batch_size: int | None = None,
        **kwargs: Any,
    ) -> None:
        """Remove a file or directory."""
        logger.debug("Removing path: %s (recursive=%s)", path, recursive)
        fs, inner_path = self._get_fs_and_path(path)
        await fs._rm(inner_path, recursive=recursive, **kwargs)

    async def _cp_file(self, path1: str, path2: str, **kwargs: Any) -> None:
        """Copy a file, possibly between filesystems."""
        logger.debug("Copying %s to %s", path1, path2)
        fs1, inner_path1 = self._get_fs_and_path(path1)
        fs2, inner_path2 = self._get_fs_and_path(path2)

        if fs1 is fs2:
            await fs1._cp_file(inner_path1, inner_path2, **kwargs)
            return

        # Cross-filesystem copy via streaming
        content = await fs1._cat_file(inner_path1)
        await fs2._pipe_file(inner_path2, content)

    def register(
        self, name: str, filesystem: AbstractFileSystem | JoinablePathLike, *, replace: bool = False
    ) -> None:
        """Register a new filesystem under a mount point.

        Args:
            name: Mount point name
            filesystem: Filesystem or path to mount
            replace: Whether to replace existing mount point

        Raises:
            ValueError: If mount point already exists and replace=False
        """
        from upathtools import upath_to_fs

        if name in self.filesystems and not replace:
            msg = f"Mount point already exists: {name}"
            raise ValueError(msg)

        if isinstance(filesystem, AbstractFileSystem):
            fs = to_async(filesystem)
        else:
            fs = upath_to_fs(filesystem, asynchronous=True)

        self.filesystems[name] = fs
        logger.debug("Registered filesystem at mount point: %s", name)

    def unregister(self, name: str) -> None:
        """Remove a filesystem from a mount point.

        Args:
            name: Mount point name to remove

        Raises:
            ValueError: If mount point doesn't exist
        """
        if name not in self.filesystems:
            msg = f"Mount point not found: {name}"
            raise ValueError(msg)

        del self.filesystems[name]
        logger.debug("Unregistered filesystem from mount point: %s", name)

    def list_mount_points(self) -> list[str]:
        """List all registered mount points."""
        return list(self.filesystems.keys())


if __name__ == "__main__":
    # Example usage
    from fsspec.implementations.memory import MemoryFileSystem

    from upathtools.filesystems import UnionFileSystem

    fs = UnionFileSystem({})
    fs.register("fs1", MemoryFileSystem())
    fs.register("fs2", MemoryFileSystem())
    print(fs.ls(""))
