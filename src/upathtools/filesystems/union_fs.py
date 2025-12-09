from __future__ import annotations

import logging
from typing import TYPE_CHECKING, Any, Literal, overload

from fsspec.asyn import AsyncFileSystem
from fsspec.implementations.asyn_wrapper import AsyncFileSystemWrapper
from fsspec.spec import AbstractFileSystem

from upathtools.filesystems.base import BaseAsyncFileSystem, BaseUPath, FileInfo
from upathtools.helpers import upath_to_fs


if TYPE_CHECKING:
    from collections.abc import Mapping

    from upath.types import JoinablePathLike


def to_async(filesystem: AbstractFileSystem) -> AsyncFileSystem:
    if not isinstance(filesystem, AsyncFileSystem):
        return AsyncFileSystemWrapper(filesystem)
    return filesystem


class UnionInfo(FileInfo, total=False):
    """Info dict for union filesystem paths."""

    size: int
    protocols: list[str]


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
            return "/"  # Root paths should return "/"
        return path.lstrip("/")  # Other paths strip leading slash

    def __str__(self) -> str:
        """Return string representation."""
        # TODO: this is all fishy
        # if self.path == "/":
        #     return f"{self.protocol}://"  # Special case for root
        return super().__str__().replace(":///", "://")


class UnionFileSystem(BaseAsyncFileSystem[UnionPath, UnionInfo]):
    """Filesystem that combines multiple filesystems by protocol."""

    protocol = "union"
    root_marker = "/"
    upath_cls = UnionPath
    cachable = False  # Underlying filesystems handle their own caching

    def __init__(
        self,
        filesystems: Mapping[str, AbstractFileSystem | JoinablePathLike] | None = None,
    ):
        super().__init__()

        # Convert paths to filesystems
        resolved_filesystems: dict[str, AsyncFileSystem] = {}

        # Handle filesystems dict
        if filesystems:
            for protocol, fs_or_path in filesystems.items():
                if isinstance(fs_or_path, AbstractFileSystem):
                    # It's already a filesystem
                    resolved_filesystems[protocol] = to_async(fs_or_path)
                else:
                    # It's a path - convert to filesystem
                    resolved_filesystems[protocol] = upath_to_fs(fs_or_path, asynchronous=True)

        if not resolved_filesystems:
            msg = "Must provide filesystems dict"
            raise ValueError(msg)

        self.filesystems = resolved_filesystems
        logger.debug("Created UnionFileSystem with protocols: %s", list(resolved_filesystems))

    @staticmethod
    def _get_kwargs_from_urls(path: str) -> dict[str, Any]:
        """Parse union URL and return constructor kwargs.

        Supports URL formats:
        - union://protocol1=path1,protocol2=path2  (protocol=path pairs)
        - union://?s3=s3://bucket&file=/tmp/dir  (query parameters)
        """
        # Remove protocol prefix first
        path_without_protocol = path.removeprefix("union://")
        filesystem_paths = {}
        if "?" in path_without_protocol:  # Check if using query parameter format
            import urllib.parse

            _, query_part = path_without_protocol.split("?", 1)
            query_params = urllib.parse.parse_qs(query_part)
            for protocol, path_list in query_params.items():
                if path_list:
                    filesystem_paths[protocol] = path_list[0]
        elif path_without_protocol:  # Default: protocol=path pairs separated by commas
            pairs = [p.strip() for p in path_without_protocol.split(",") if p.strip()]
            for pair in pairs:
                if "=" in pair:
                    protocol, path_value = pair.split("=", 1)
                    filesystem_paths[protocol.strip()] = path_value.strip()

        return filesystem_paths if filesystem_paths else {}

    def _get_fs_and_path(self, path: str) -> tuple[AsyncFileSystem, str]:
        """Get filesystem and normalized path."""
        if not path or path == self.root_marker:
            return self, self.root_marker

        try:
            if "://" in path:
                protocol, path = path.split("://", 1)
            else:
                protocol = path
                path = ""

            if protocol == "union":
                return self, self.root_marker

            fs = self.filesystems[protocol]
            logger.debug("Protocol %s -> filesystem %s, path: %s", protocol, fs, path)
        except (KeyError, IndexError) as e:
            msg = f"Invalid or unknown protocol in path: {path}"
            raise ValueError(msg) from e
        else:
            return fs, path or fs.root_marker  # Use the target filesystem's root_marker

    async def _cat_file(self, path: str, start=None, end=None, **kwargs: Any):
        """Get file contents."""
        logger.debug("Reading from path: %s", path)
        fs, path = self._get_fs_and_path(path)
        return await fs._cat_file(path, start=start, end=end, **kwargs)

    async def _pipe_file(
        self,
        path: str,
        value,
        mode: Literal["create", "overwrite"] = "overwrite",
        **kwargs: Any,
    ) -> None:
        """Write file contents."""
        logger.debug("Writing to path: %s", path)
        fs, path = self._get_fs_and_path(path)
        await fs._pipe_file(path, value, **kwargs)

    async def _info(self, path: str, **kwargs: Any) -> UnionInfo:
        """Get info about a path."""
        logger.debug("Getting info for path: %s", path)
        fs, norm_path = self._get_fs_and_path(path)
        if fs is self:
            protos = list(self.filesystems)
            return UnionInfo(name="", size=0, type="directory", protocols=protos)

        out = await fs._info(norm_path, **kwargs)
        # Only try to get protocol if we're not dealing with self
        protocol = next(p for p, f in self.filesystems.items() if f is fs)
        name = out.get("name", "")
        if "://" in name:
            name = name.split("://", 1)[1]
        name = self._normalize_path(protocol, name)
        return UnionInfo(name=name, type=out.get("type", "file"), size=out.get("size", 0))

    def _normalize_path(self, protocol: str, path: str) -> str:
        """Normalize path to handle protocol and slashes correctly."""
        if "://" in path:  # Strip protocol if present
            path = path.split("://", 1)[1]
        path = path.lstrip("/")  # Strip leading slashes
        return f"{protocol}://{path}"  # Add protocol back

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
        fs, norm_path = self._get_fs_and_path(path)
        if fs is self:
            if detail:
                return [
                    UnionInfo(name=f"{protocol}://", type="directory", size=0)
                    for protocol in self.filesystems
                ]
            return [f"{protocol}://" for protocol in self.filesystems]

        logger.debug("Using filesystem %s for path %s", fs, norm_path)
        out = await fs._ls(norm_path, detail=True, **kwargs)
        logger.debug("Raw listing: %s", out)
        # Add protocol back to paths
        protocol = next(p for p, f in self.filesystems.items() if f is fs)
        out = [o.copy() for o in out]
        for o in out:
            o["name"] = self._normalize_path(protocol, o["name"])

        logger.debug("Final listing: %s", out)
        return out if detail else [o["name"] for o in out]

    async def _makedirs(self, path: str, exist_ok=False) -> None:
        """Create a directory and parents."""
        logger.debug("Making directories: %s", path)
        fs, path = self._get_fs_and_path(path)
        await fs._makedirs(path, exist_ok=exist_ok)

    async def _rm_file(self, path: str, **kwargs: Any) -> None:
        """Remove a file."""
        logger.debug("Removing file: %s", path)
        fs, path = self._get_fs_and_path(path)
        await fs._rm_file(path, **kwargs)

    async def _rm(
        self,
        path: str,
        recursive: bool = False,
        batch_size: int | None = None,
        **kwargs: Any,
    ) -> None:
        """Remove a file or directory."""
        logger.debug("Removing path: %s (recursive=%s)", path, recursive)
        fs, path = self._get_fs_and_path(path)
        await fs._rm(path, recursive=recursive, **kwargs)

    async def _isdir(self, path: str) -> bool:
        """Check if path is a directory."""
        logger.debug("Checking if directory: %s", path)
        fs, norm_path = self._get_fs_and_path(path)
        if fs is self:
            return True
        return await fs._isdir(norm_path)

    async def _cp_file(self, path1: str, path2: str, **kwargs: Any) -> None:
        """Copy a file, possibly between filesystems."""
        logger.debug("Copying %s to %s", path1, path2)
        fs1, path1 = self._get_fs_and_path(path1)
        fs2, path2 = self._get_fs_and_path(path2)

        if fs1 is fs2:
            await fs1._cp_file(path1, path2, **kwargs)
            return

        # Cross-filesystem copy via streaming
        content = await fs1._cat_file(path1)
        await fs2._pipe_file(path2, content)
