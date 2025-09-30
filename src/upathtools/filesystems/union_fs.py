from __future__ import annotations

import logging
from typing import TYPE_CHECKING, Any, Literal, overload

from fsspec.asyn import AsyncFileSystem
from upath import UPath


if TYPE_CHECKING:
    from fsspec.spec import AbstractFileSystem


logger = logging.getLogger(__name__)


class UnionPath(UPath):
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


class UnionFileSystem(AsyncFileSystem):
    """Filesystem that combines multiple filesystems by protocol."""

    protocol = "union"
    root_marker = "/"

    def __init__(self, filesystems: dict[str, AbstractFileSystem]):
        super().__init__()
        self.filesystems = filesystems
        logger.debug("Created UnionFileSystem with protocols: %s", list(filesystems))

    def _make_path(self, path: str) -> UPath:
        """Create a path object from string."""
        return UnionPath(path)

    def _get_fs_and_path(self, path: str) -> tuple[AbstractFileSystem, str]:
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

    async def _cat_file(self, path: str, start=None, end=None, **kwargs):
        """Get file contents."""
        logger.debug("Reading from path: %s", path)
        fs, path = self._get_fs_and_path(path)
        if isinstance(fs, AsyncFileSystem):
            return await fs._cat_file(path, start=start, end=end, **kwargs)
        return fs.cat_file(path, start=start, end=end, **kwargs)

    async def _pipe_file(self, path: str, value, **kwargs):
        """Write file contents."""
        logger.debug("Writing to path: %s", path)
        fs, path = self._get_fs_and_path(path)
        if isinstance(fs, AsyncFileSystem):
            await fs._pipe_file(path, value, **kwargs)
        else:
            fs.pipe_file(path, value, **kwargs)

    async def _info(self, path: str, **kwargs):
        """Get info about a path."""
        logger.debug("Getting info for path: %s", path)

        fs, norm_path = self._get_fs_and_path(path)

        if fs is self:
            return {
                "name": "",
                "size": 0,
                "type": "directory",
                "protocols": list(self.filesystems),
            }

        if isinstance(fs, AsyncFileSystem):
            out = await fs._info(norm_path, **kwargs)
        else:
            out = fs.info(norm_path, **kwargs)

        # Only try to get protocol if we're not dealing with self
        protocol = next(p for p, f in self.filesystems.items() if f is fs)
        if "name" in out:
            name = out["name"]
            if "://" in name:
                name = name.split("://", 1)[1]
            out["name"] = self._normalize_path(protocol, name)
        return out

    def _normalize_path(self, protocol: str, path: str) -> str:
        """Normalize path to handle protocol and slashes correctly."""
        # Strip protocol if present
        if "://" in path:
            path = path.split("://", 1)[1]
        # Strip leading slashes
        path = path.lstrip("/")
        # Add protocol back
        return f"{protocol}://{path}"

    @overload
    async def _ls(
        self,
        path: str,
        detail: Literal[True] = True,
        **kwargs: Any,
    ) -> list[dict[str, Any]]: ...

    @overload
    async def _ls(
        self,
        path: str,
        detail: Literal[False],
        **kwargs: Any,
    ) -> list[str]: ...

    async def _ls(self, path: str, detail=True, **kwargs):
        """List contents of a path."""
        logger.debug("Listing path: %s", path)

        fs, norm_path = self._get_fs_and_path(path)

        if fs is self:
            # Root shows available protocols
            out = [
                {"name": f"{protocol}://", "type": "directory", "size": 0}
                for protocol in self.filesystems
            ]
            return out if detail else [o["name"] for o in out]

        logger.debug("Using filesystem %s for path %s", fs, norm_path)

        if isinstance(fs, AsyncFileSystem):
            out = await fs._ls(norm_path, detail=True, **kwargs)
        else:
            out = fs.ls(norm_path, detail=True, **kwargs)

        logger.debug("Raw listing: %s", out)

        # Add protocol back to paths
        protocol = next(p for p, f in self.filesystems.items() if f is fs)
        out = [o.copy() for o in out]
        for o in out:
            o["name"] = self._normalize_path(protocol, o["name"])  # type: ignore

        logger.debug("Final listing: %s", out)
        return out if detail else [o["name"] for o in out]

    async def _makedirs(self, path: str, exist_ok=False):
        """Create a directory and parents."""
        logger.debug("Making directories: %s", path)
        fs, path = self._get_fs_and_path(path)
        if isinstance(fs, AsyncFileSystem):
            await fs._makedirs(path, exist_ok=exist_ok)
        else:
            fs.makedirs(path, exist_ok=exist_ok)

    async def _rm_file(self, path: str, **kwargs):
        """Remove a file."""
        logger.debug("Removing file: %s", path)
        fs, path = self._get_fs_and_path(path)
        if isinstance(fs, AsyncFileSystem):
            await fs._rm_file(path, **kwargs)
        else:
            fs.rm_file(path, **kwargs)

    async def _rm(self, path: str, recursive=False, **kwargs):
        """Remove a file or directory."""
        logger.debug("Removing path: %s (recursive=%s)", path, recursive)
        fs, path = self._get_fs_and_path(path)
        if isinstance(fs, AsyncFileSystem):
            await fs._rm(path, recursive=recursive, **kwargs)
        else:
            fs.rm(path, recursive=recursive, **kwargs)

    async def _cp_file(self, path1: str, path2: str, **kwargs):
        """Copy a file, possibly between filesystems."""
        logger.debug("Copying %s to %s", path1, path2)
        fs1, path1 = self._get_fs_and_path(path1)
        fs2, path2 = self._get_fs_and_path(path2)

        if fs1 is fs2:
            if isinstance(fs1, AsyncFileSystem):
                await fs1._cp_file(path1, path2, **kwargs)
            else:
                fs1.cp_file(path1, path2, **kwargs)
            return

        # Cross-filesystem copy via streaming
        if isinstance(fs1, AsyncFileSystem):
            content = await fs1._cat_file(path1)
        else:
            content = fs1.cat_file(path1)

        if isinstance(fs2, AsyncFileSystem):
            await fs2._pipe_file(path2, content)
        else:
            fs2.pipe_file(path2, content)
