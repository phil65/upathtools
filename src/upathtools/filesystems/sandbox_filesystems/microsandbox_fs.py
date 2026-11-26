"""Microsandbox async filesystem implementation for upathtools.

Uses microsandbox's native guest filesystem API (`Sandbox.fs`), so binary content,
special characters and metadata are handled without shelling out.
"""

from contextlib import contextmanager
import errno
import logging
import re
from typing import TYPE_CHECKING, Any, Literal, Required, overload
import uuid

from upathtools.async_helpers import sync_wrapper
from upathtools.filesystems.base import BaseAsyncFileSystem, BaseUPath, FileInfo


if TYPE_CHECKING:
    from collections.abc import Generator

    from microsandbox import FsEntry, FsMetadata, Sandbox

    from upathtools.filesystems.base import CreationMode


logger = logging.getLogger(__name__)

_OS_ERROR = re.compile(r"\(os error (\d+)\)")
_ERRNO_EXCEPTIONS: dict[int, type[OSError]] = {
    errno.ENOENT: FileNotFoundError,
    errno.EISDIR: IsADirectoryError,
    errno.ENOTDIR: NotADirectoryError,
    errno.EEXIST: FileExistsError,
    errno.EACCES: PermissionError,
    errno.EPERM: PermissionError,
}


class MicrosandboxInfo(FileInfo, total=False):
    """Info dict for Microsandbox filesystem paths."""

    size: Required[int]
    mtime: float | None
    mode: int


def _file_type(kind: str) -> Literal["file", "directory", "link", "other"]:
    match kind:
        case "file":
            return "file"
        case "directory":
            return "directory"
        case "symlink":
            return "link"
        case _:
            return "other"


def _mtime(milliseconds: float | None) -> float | None:
    """Convert microsandbox timestamps (milliseconds) to fsspec seconds."""
    return None if milliseconds is None else milliseconds / 1000


def _entry_info(entry: FsEntry) -> MicrosandboxInfo:
    return MicrosandboxInfo(
        name=entry.path,
        type=_file_type(str(entry.kind)),
        size=entry.size,
        mtime=_mtime(entry.modified),
        mode=entry.mode,
    )


@contextmanager
def _translate_errors(path: str, operation: str) -> Generator[None]:
    """Map microsandbox filesystem errors (which carry the guest errno) to OSError types."""
    from microsandbox import MicrosandboxError

    try:
        yield
    except MicrosandboxError as exc:
        message = str(exc)
        if (match := _OS_ERROR.search(message)) is not None:
            code = int(match.group(1))
            exc_type = _ERRNO_EXCEPTIONS.get(code, OSError)
            raise exc_type(code, f"Failed to {operation}: {message}", path) from exc
        raise OSError(f"Failed to {operation} {path}: {message}") from exc


class MicrosandboxPath(BaseUPath[MicrosandboxInfo]):
    """Microsandbox-specific UPath implementation."""

    __slots__ = ()


class MicrosandboxFS(BaseAsyncFileSystem[MicrosandboxPath, MicrosandboxInfo]):
    """Async filesystem for Microsandbox microVMs.

    Operates on an existing sandbox (e.g. one owned by an execution environment) or
    creates its own on first use, which is then stopped by `close_session()`.
    """

    protocol = "microsandbox"
    upath_cls = MicrosandboxPath
    root_marker = "/"
    cachable = False  # Disable fsspec caching to prevent instance sharing

    def __init__(
        self,
        sandbox: Sandbox | None = None,
        name: str | None = None,
        image: str = "python:3.13-slim",
        memory: int = 512,
        cpus: int = 1,
        **kwargs: Any,
    ) -> None:
        """Initialize Microsandbox filesystem.

        Args:
            sandbox: Existing sandbox to operate on (not stopped by `close_session()`)
            name: Name for a sandbox created by this filesystem (default: unique name)
            image: OCI image for a sandbox created by this filesystem
            memory: Memory limit in MiB for a sandbox created by this filesystem
            cpus: Number of virtual CPUs for a sandbox created by this filesystem
            **kwargs: Additional filesystem arguments
        """
        super().__init__(**kwargs)
        self._sandbox = sandbox
        self._owns_sandbox = False
        self.name = name
        self.image = image
        self.memory = memory
        self.cpus = cpus

    async def _get_sandbox(self) -> Sandbox:
        """Return the sandbox, creating an owned one on first use if none was given."""
        if self._sandbox is None:
            from microsandbox import Sandbox

            self._sandbox = await Sandbox.create(
                self.name or f"upathtools-{uuid.uuid4().hex[:12]}",
                image=self.image,
                memory=self.memory,
                cpus=self.cpus,
                ephemeral=True,
            )
            self._owns_sandbox = True
            logger.info("Started Microsandbox: %s", self._sandbox.name)
        return self._sandbox

    async def set_session(self) -> None:
        """Ensure the sandbox exists."""
        await self._get_sandbox()

    async def close_session(self) -> None:
        """Stop the sandbox if this filesystem created it."""
        if self._sandbox is not None and self._owns_sandbox:
            name = self._sandbox.name
            await self._sandbox.stop()
            self._sandbox = None
            self._owns_sandbox = False
            logger.info("Stopped Microsandbox: %s", name)

    async def _stat(self, path: str) -> FsMetadata:
        sandbox = await self._get_sandbox()
        with _translate_errors(path, "stat"):
            return await sandbox.fs.stat(path)

    @overload
    async def _ls(
        self, path: str, detail: Literal[True] = ..., **kwargs: Any
    ) -> list[MicrosandboxInfo]: ...

    @overload
    async def _ls(self, path: str, detail: Literal[False], **kwargs: Any) -> list[str]: ...

    async def _ls(
        self, path: str, detail: bool = True, **kwargs: Any
    ) -> list[MicrosandboxInfo] | list[str]:
        """List directory contents."""
        sandbox = await self._get_sandbox()
        with _translate_errors(path, "list directory"):
            entries = await sandbox.fs.list(path)
        if not detail:
            return [entry.path for entry in entries]
        return [_entry_info(entry) for entry in entries]

    async def _cat_file(
        self, path: str, start: int | None = None, end: int | None = None, **kwargs: Any
    ) -> bytes:
        """Read file contents."""
        sandbox = await self._get_sandbox()
        with _translate_errors(path, "read file"):
            content = await sandbox.fs.read(path)
        if start is not None or end is not None:
            content = content[start:end]
        return content

    async def _pipe_file(
        self, path: str, value: bytes, mode: CreationMode = "overwrite", **kwargs: Any
    ) -> None:
        """Write data to a file, creating parent directories."""
        if mode == "create" and await self._exists(path):
            raise FileExistsError(errno.EEXIST, "File exists", path)
        sandbox = await self._get_sandbox()
        parent = path.rsplit("/", 1)[0]
        with _translate_errors(path, "write file"):
            if parent:
                await sandbox.fs.mkdir(parent)
            await sandbox.fs.write(path, value)

    async def _mkdir(self, path: str, create_parents: bool = True, **kwargs: Any) -> None:
        """Create a directory."""
        if not create_parents:
            parent = path.rstrip("/").rsplit("/", 1)[0] or "/"
            if not await self._isdir(parent):
                raise FileNotFoundError(errno.ENOENT, "Parent directory does not exist", parent)
        sandbox = await self._get_sandbox()
        with _translate_errors(path, "create directory"):
            await sandbox.fs.mkdir(path)

    async def _rm_file(self, path: str, **kwargs: Any) -> None:
        """Remove a file."""
        sandbox = await self._get_sandbox()
        with _translate_errors(path, "remove file"):
            await sandbox.fs.remove(path)

    async def _rmdir(self, path: str, **kwargs: Any) -> None:
        """Remove an empty directory."""
        # The guest API removes directories recursively; keep fsspec's rmdir semantics.
        if await self._ls(path, detail=False):
            raise OSError(errno.ENOTEMPTY, "Directory not empty", path)
        sandbox = await self._get_sandbox()
        with _translate_errors(path, "remove directory"):
            await sandbox.fs.remove_dir(path)

    async def _mv(self, path1: str, path2: str, **kwargs: Any) -> None:
        """Move or rename a file or directory."""
        sandbox = await self._get_sandbox()
        with _translate_errors(path1, "move"):
            await sandbox.fs.rename(path1, path2)

    async def _cp_file(self, path1: str, path2: str, **kwargs: Any) -> None:
        """Copy a file within the sandbox."""
        sandbox = await self._get_sandbox()
        with _translate_errors(path1, "copy"):
            await sandbox.fs.copy(path1, path2)

    async def _get_file(self, rpath: str, lpath: str, **kwargs: Any) -> None:
        """Download a file from the sandbox to the host."""
        sandbox = await self._get_sandbox()
        with _translate_errors(rpath, "download"):
            await sandbox.fs.copy_to_host(rpath, lpath)

    async def _put_file(self, lpath: str, rpath: str, **kwargs: Any) -> None:
        """Upload a file from the host to the sandbox, creating parent directories."""
        sandbox = await self._get_sandbox()
        parent = rpath.rsplit("/", 1)[0]
        with _translate_errors(rpath, "upload"):
            if parent:
                await sandbox.fs.mkdir(parent)
            await sandbox.fs.copy_from_host(lpath, rpath)

    async def _exists(self, path: str, **kwargs: Any) -> bool:
        """Check if a path exists."""
        sandbox = await self._get_sandbox()
        with _translate_errors(path, "check existence"):
            return await sandbox.fs.exists(path)

    async def _isfile(self, path: str, **kwargs: Any) -> bool:
        """Check if a path is a file."""
        try:
            return str((await self._stat(path)).kind) == "file"
        except FileNotFoundError:
            return False

    async def _isdir(self, path: str, **kwargs: Any) -> bool:
        """Check if a path is a directory."""
        try:
            return str((await self._stat(path)).kind) == "directory"
        except FileNotFoundError:
            return False

    async def _size(self, path: str, **kwargs: Any) -> int:
        """Get file size in bytes."""
        return (await self._stat(path)).size

    async def _modified(self, path: str, **kwargs: Any) -> float:
        """Get modification time (seconds since the epoch)."""
        modified = _mtime((await self._stat(path)).modified)
        if modified is None:
            raise OSError(errno.ENODATA, "Modification time not available", path)
        return modified

    async def _info(self, path: str, **kwargs: Any) -> MicrosandboxInfo:
        """Get info about a file or directory."""
        metadata = await self._stat(path)
        return MicrosandboxInfo(
            name=path,
            type=_file_type(str(metadata.kind)),
            size=metadata.size,
            mtime=_mtime(metadata.modified),
            mode=metadata.mode,
        )

    # Sync wrappers for async methods
    ls = sync_wrapper(_ls)  # pyright: ignore[reportAssignmentType]
    cat_file = sync_wrapper(_cat_file)  # pyright: ignore[reportAssignmentType]
    pipe_file = sync_wrapper(_pipe_file)  # pyright: ignore[reportAssignmentType]
    mkdir = sync_wrapper(_mkdir)
    rm_file = sync_wrapper(_rm_file)
    rmdir = sync_wrapper(_rmdir)
    mv = sync_wrapper(_mv)
    cp_file = sync_wrapper(_cp_file)
    get_file = sync_wrapper(_get_file)
    put_file = sync_wrapper(_put_file)
    exists = sync_wrapper(_exists)  # pyright: ignore[reportAssignmentType]
    isfile = sync_wrapper(_isfile)
    isdir = sync_wrapper(_isdir)
    size = sync_wrapper(_size)
    modified = sync_wrapper(_modified)
    info = sync_wrapper(_info)
