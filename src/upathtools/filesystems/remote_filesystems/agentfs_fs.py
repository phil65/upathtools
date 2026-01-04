"""AgentFS filesystem implementation with async support.

Provides a POSIX-like filesystem backed by SQLite/TursoDB for AI agent storage.
"""

from __future__ import annotations

import io
import logging
from typing import TYPE_CHECKING, Any, Literal, overload

from fsspec.asyn import sync_wrapper
from fsspec.utils import infer_storage_options

from upathtools.filesystems.base import BaseAsyncFileSystem, BaseUPath, BufferedWriter, FileInfo


if TYPE_CHECKING:
    from agentfs_sdk import AgentFS, Filesystem


class AgentFSInfo(FileInfo, total=False):
    """Info dict for AgentFS filesystem paths."""

    size: int
    mode: int
    atime: int
    mtime: int
    ctime: int
    ino: int
    nlink: int
    uid: int
    gid: int


logger = logging.getLogger(__name__)


class AgentFSPath(BaseUPath[AgentFSInfo]):
    """UPath implementation for AgentFS filesystem."""

    __slots__ = ()


class AgentFSFileSystem(BaseAsyncFileSystem[AgentFSPath, AgentFSInfo]):
    """Filesystem for AI agent storage backed by SQLite/TursoDB.

    Provides a POSIX-like filesystem interface with support for files and directories.
    Uses the agentfs-sdk for storage operations.

    Examples:
        >>> # Using agent ID (creates .agentfs/my-agent.db)
        >>> fs = AgentFSFileSystem(agent_id="my-agent")
        >>> fs.pipe_file("config.json", b'{"key": "value"}')
        >>> content = fs.cat_file("config.json")

        >>> # Using explicit database path
        >>> fs = AgentFSFileSystem(db_path="/path/to/storage.db")

        >>> # With UPath interface
        >>> path = fs.get_upath("data/file.txt")
        >>> path.write_text("hello")
        >>> print(path.read_text())
    """

    protocol = "agentfs"
    upath_cls = AgentFSPath

    def __init__(
        self,
        agent_id: str | None = None,
        db_path: str | None = None,
        asynchronous: bool = False,
        loop: Any = None,
        **kwargs: Any,
    ) -> None:
        """Initialize the filesystem.

        Args:
            agent_id: Unique identifier for the agent. Creates storage at `.agentfs/{id}.db`
            db_path: Explicit path to the database file. Takes precedence over agent_id.
            asynchronous: Whether to use async operations
            loop: Event loop for async operations
            **kwargs: Additional filesystem options

        Raises:
            ValueError: If neither agent_id nor db_path is provided
        """
        super().__init__(asynchronous=asynchronous, loop=loop, **kwargs)

        if not agent_id and not db_path:
            msg = "Either agent_id or db_path must be provided"
            raise ValueError(msg)

        self.agent_id = agent_id
        self.db_path = db_path
        self._agentfs: AgentFS | None = None
        self._fs: Filesystem | None = None

    @property
    def fsid(self) -> str:
        """Filesystem ID."""
        return f"agentfs-{self.agent_id or self.db_path}"

    async def _get_fs(self) -> Filesystem:
        """Get or create the AgentFS filesystem instance."""
        if self._fs is None:
            from agentfs_sdk import AgentFS, AgentFSOptions

            options = AgentFSOptions(id=self.agent_id, path=self.db_path)
            self._agentfs = await AgentFS.open(options)
            self._fs = self._agentfs.fs
        return self._fs

    async def _close(self) -> None:
        """Close the AgentFS connection."""
        if self._agentfs is not None:
            await self._agentfs.close()
            self._agentfs = None
            self._fs = None

    @classmethod
    def _strip_protocol(cls, path: str) -> str:
        """Strip protocol prefix from path."""
        path = infer_storage_options(path).get("path", path)
        # Normalize to absolute path
        stripped = path.lstrip("/")
        return "/" + stripped if stripped else "/"

    @classmethod
    def _get_kwargs_from_urls(cls, path: str) -> dict[str, Any]:
        """Parse URL into constructor kwargs."""
        so = infer_storage_options(path)
        out = {}

        if so.get("host"):
            out["agent_id"] = so["host"]
        if so.get("username"):
            out["agent_id"] = so["username"]

        return out

    @overload
    async def _ls(
        self, path: str, detail: Literal[True] = ..., **kwargs: Any
    ) -> list[AgentFSInfo]: ...

    @overload
    async def _ls(self, path: str, detail: Literal[False], **kwargs: Any) -> list[str]: ...

    async def _ls(
        self,
        path: str,
        detail: bool = True,
        **kwargs: Any,
    ) -> list[AgentFSInfo] | list[str]:
        """List contents of path.

        Args:
            path: Path to list
            detail: Whether to include detailed information
            **kwargs: Additional arguments

        Returns:
            List of file/directory information or names

        Raises:
            FileNotFoundError: If path doesn't exist
        """
        fs = await self._get_fs()
        path = self._strip_protocol(path)
        logger.debug("Listing path: %s", path)

        try:
            entries = await fs.readdir(path)
        except FileNotFoundError:
            raise
        except Exception as e:
            msg = f"Failed to list directory: {path}"
            raise FileNotFoundError(msg) from e

        if not detail:
            return entries

        results: list[AgentFSInfo] = []
        for entry in entries:
            entry_path = f"{path.rstrip('/')}/{entry}"
            try:
                stats = await fs.stat(entry_path)
                results.append(
                    AgentFSInfo(
                        name=entry,
                        type="directory" if stats.is_directory() else "file",
                        size=stats.size,
                        mode=stats.mode,
                        atime=stats.atime,
                        mtime=stats.mtime,
                        ctime=stats.ctime,
                        ino=stats.ino,
                        nlink=stats.nlink,
                        uid=stats.uid,
                        gid=stats.gid,
                    )
                )
            except FileNotFoundError:
                # Entry may have been deleted between readdir and stat
                continue

        return results

    ls = sync_wrapper(_ls)

    async def _cat_file(
        self,
        path: str,
        start: int | None = None,
        end: int | None = None,
        **kwargs: Any,
    ) -> bytes:
        """Get contents of a file."""
        fs = await self._get_fs()
        path = self._strip_protocol(path)
        logger.debug("Reading file: %s", path)

        # Read as bytes (encoding=None)
        content = await fs.read_file(path, encoding=None)
        assert isinstance(content, bytes)

        if start is not None or end is not None:
            start = start or 0
            end = min(end or len(content), len(content))
            content = content[start:end]

        return content

    cat_file = sync_wrapper(_cat_file)  # type: ignore

    async def _pipe_file(self, path: str, value: bytes, **kwargs: Any) -> None:
        """Write bytes to a file.

        Args:
            path: Path to the file
            value: Content to write
            **kwargs: Additional keyword arguments
        """
        fs = await self._get_fs()
        path = self._strip_protocol(path)
        logger.debug("Writing file: %s (%d bytes)", path, len(value))

        await fs.write_file(path, value)

    pipe_file = sync_wrapper(_pipe_file)

    async def _rm_file(self, path: str, **kwargs: Any) -> None:
        """Delete a file.

        Args:
            path: Path to the file
            **kwargs: Additional keyword arguments
        """
        fs = await self._get_fs()
        path = self._strip_protocol(path)
        logger.debug("Deleting file: %s", path)

        await fs.delete_file(path)

    rm_file = sync_wrapper(_rm_file)

    async def _info(self, path: str, **kwargs: Any) -> AgentFSInfo:
        """Get info about a path."""
        fs = await self._get_fs()
        path = self._strip_protocol(path)
        logger.debug("Getting info: %s", path)

        if path == "/":
            return AgentFSInfo(
                name="/",
                type="directory",
                size=0,
            )

        try:
            stats = await fs.stat(path)
            name = path.rsplit("/", 1)[-1] if "/" in path else path
            return AgentFSInfo(
                name=name,
                type="directory" if stats.is_directory() else "file",
                size=stats.size,
                mode=stats.mode,
                atime=stats.atime,
                mtime=stats.mtime,
                ctime=stats.ctime,
                ino=stats.ino,
                nlink=stats.nlink,
                uid=stats.uid,
                gid=stats.gid,
            )
        except FileNotFoundError:
            msg = f"Path not found: {path}"
            raise FileNotFoundError(msg) from None

    info = sync_wrapper(_info)

    async def _exists(self, path: str, **kwargs: Any) -> bool:
        """Check if a path exists."""
        try:
            await self._info(path, **kwargs)
        except FileNotFoundError:
            return False
        else:
            return True

    exists = sync_wrapper(_exists)  # pyright: ignore

    async def _isdir(self, path: str, **kwargs: Any) -> bool:
        """Check if path is a directory."""
        if path == "/" or self._strip_protocol(path) == "/":
            return True

        try:
            info = await self._info(path, **kwargs)
            return info["type"] == "directory"
        except FileNotFoundError:
            return False

    isdir = sync_wrapper(_isdir)

    async def _isfile(self, path: str, **kwargs: Any) -> bool:
        """Check if path is a file."""
        try:
            info = await self._info(path, **kwargs)
            return info["type"] == "file"
        except FileNotFoundError:
            return False

    isfile = sync_wrapper(_isfile)

    async def _mkdir(self, path: str, create_parents: bool = True, **kwargs: Any) -> None:
        """Create a directory.

        AgentFS auto-creates parent directories when writing files,
        so we create a placeholder to ensure the directory exists.
        """
        fs = await self._get_fs()
        path = self._strip_protocol(path)
        logger.debug("Creating directory: %s", path)

        # AgentFS doesn't have explicit mkdir, but directories are created
        # automatically when files are written. We can create an empty
        # .keep file to ensure the directory exists.
        # However, this is a bit of a hack. For now, we'll just note that
        # directories are created implicitly.

        # Check if path already exists
        try:
            stats = await fs.stat(path)
            if stats.is_directory():
                return  # Already exists as directory
            msg = f"Path exists and is not a directory: {path}"
            raise FileExistsError(msg)
        except FileNotFoundError:
            pass  # Path doesn't exist, we can proceed

        # Create a .agentfs_dir marker file to ensure directory exists
        marker_path = f"{path.rstrip('/')}/.agentfs_dir"
        await fs.write_file(marker_path, b"")

    mkdir = sync_wrapper(_mkdir)

    async def _makedirs(self, path: str, exist_ok: bool = False, **kwargs: Any) -> None:
        """Create directory and parents."""
        await self._mkdir(path, create_parents=True, **kwargs)

    makedirs = sync_wrapper(_makedirs)

    def _open(self, path: str, mode: str = "rb", **kwargs: Any) -> io.BytesIO | BufferedWriter:
        """Open a file.

        Args:
            path: Path to the file
            mode: File mode ('rb' for reading, 'wb' for writing)
            **kwargs: Additional arguments

        Returns:
            File-like object for reading or writing

        Raises:
            NotImplementedError: If mode is not supported
        """
        if "r" in mode:
            content = self.cat_file(path)
            assert isinstance(content, bytes)
            return io.BytesIO(content)
        if "w" in mode:
            buffer = io.BytesIO()
            return BufferedWriter(buffer, self, path, **kwargs)
        msg = f"Mode {mode} not supported"
        raise NotImplementedError(msg)

    def invalidate_cache(self, path: str | None = None) -> None:
        """Clear the cache."""
        # AgentFS doesn't use caching in this implementation


if __name__ == "__main__":
    import asyncio

    logging.basicConfig(level=logging.INFO)

    async def main() -> None:
        fs = AgentFSFileSystem(agent_id="test-agent")
        try:
            # Write a file
            await fs._pipe_file("/test/hello.txt", b"Hello, AgentFS!")

            # Read it back
            content = await fs._cat_file("/test/hello.txt")
            print(f"Content: {content!r}")

            # List directory
            entries = await fs._ls("/test", detail=True)
            print(f"Entries: {entries}")

            # Get info
            info = await fs._info("/test/hello.txt")
            print(f"Info: {info}")

            # Clean up
            await fs._rm_file("/test/hello.txt")
        finally:
            await fs._close()

    asyncio.run(main())
