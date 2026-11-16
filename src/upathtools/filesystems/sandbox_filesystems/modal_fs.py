"""Modal async filesystem implementation for upathtools."""

from __future__ import annotations

from contextlib import contextmanager
import logging
from typing import TYPE_CHECKING, Any, Literal, Required, overload

from upathtools.async_helpers import sync_wrapper
from upathtools.filesystems.base import BaseAsyncFileSystem, BaseUPath, FileInfo


if TYPE_CHECKING:
    from collections.abc import Collection, Iterator
    import os

    import modal
    from modal import Image
    import modal.types

    from upathtools.filesystems.base import CreationMode


logger = logging.getLogger(__name__)


class ModalInfo(FileInfo, total=False):
    """Info dict for Modal filesystem paths."""

    size: Required[int]
    mtime: Required[float]


def _to_info(entry: modal.types.FileInfo) -> ModalInfo:
    """Convert Modal file metadata to a ModalInfo dict."""
    file_type: Literal["file", "directory", "link"]
    if entry.is_dir():
        file_type = "directory"
    elif entry.symlink_target is not None:
        file_type = "link"
    else:
        file_type = "file"
    return ModalInfo(name=entry.path, size=entry.size, type=file_type, mtime=entry.modified_time)


@contextmanager
def _translate_errors(path: str, operation: str) -> Iterator[None]:
    """Map Modal sandbox filesystem errors to standard Python exceptions."""
    from modal.exception import (
        SandboxFilesystemError,
        SandboxFilesystemIsADirectoryError,
        SandboxFilesystemNotADirectoryError,
        SandboxFilesystemNotFoundError,
        SandboxFilesystemPathAlreadyExistsError,
        SandboxFilesystemPermissionError,
    )

    try:
        yield
    except SandboxFilesystemNotFoundError as exc:
        raise FileNotFoundError(path) from exc
    except SandboxFilesystemIsADirectoryError as exc:
        raise IsADirectoryError(path) from exc
    except SandboxFilesystemNotADirectoryError as exc:
        raise NotADirectoryError(path) from exc
    except SandboxFilesystemPermissionError as exc:
        raise PermissionError(path) from exc
    except SandboxFilesystemPathAlreadyExistsError as exc:
        raise FileExistsError(path) from exc
    except SandboxFilesystemError as exc:
        raise OSError(f"Failed to {operation} {path}: {exc}") from exc


class ModalPath(BaseUPath[ModalInfo]):
    """Modal-specific UPath implementation."""

    __slots__ = ()


class ModalFS(BaseAsyncFileSystem[ModalPath, ModalInfo]):
    """Async filesystem for Modal sandbox environments.

    This filesystem provides access to files within a Modal sandbox environment,
    allowing you to read, write, and manipulate files remotely through the
    Modal native filesystem interface.
    """

    protocol = "modal"
    upath_cls = ModalPath
    root_marker = "/"
    cachable = False  # Disable fsspec caching to prevent instance sharing

    def __init__(
        self,
        app_name: str | None = None,
        sandbox_id: str | None = None,
        sandbox_name: str | None = None,
        image: Image | None = None,
        cpu: float | None = None,
        memory: int | None = None,
        gpu: str | None = None,
        timeout: int = 300,
        idle_timeout: int | None = None,
        workdir: str | None = None,
        volumes: dict[
            str | os.PathLike, modal.volume.Volume | modal.cloud_bucket_mount.CloudBucketMount
        ]
        | None = None,
        secrets: Collection[modal.secret.Secret] | None = None,
        **kwargs: Any,
    ) -> None:
        """Initialize Modal filesystem.

        Args:
            app_name: Modal app name (will lookup or create)
            sandbox_id: Existing sandbox ID to connect to
            sandbox_name: Named sandbox to connect to
            image: Modal Image for sandboxes
            cpu: CPU allocation for new sandboxes
            memory: Memory allocation for new sandboxes
            gpu: GPU type for new sandboxes
            timeout: Maximum sandbox lifetime in seconds (default 300)
            idle_timeout: Idle timeout in seconds
            workdir: Working directory in sandbox
            volumes: Volume mounts
            secrets: Secrets to inject
            **kwargs: Additional filesystem options
        """
        super().__init__(**kwargs)
        self._app_name = app_name or "upathtools-modal-fs"
        self._sandbox_id = sandbox_id
        self._sandbox_name = sandbox_name
        self._image = image
        self._cpu = cpu
        self._memory = memory
        self._gpu = gpu
        self._timeout = timeout
        self._idle_timeout = idle_timeout
        self._workdir = workdir
        self._volumes = volumes or {}
        self._secrets = secrets or []
        self._app: modal.App | None = None
        self._sandbox: modal.Sandbox | None = None
        self._session_started = False

    @staticmethod
    def _get_kwargs_from_urls(path: str) -> dict[str, Any]:
        path = path.removeprefix("modal://")
        app_name, sandbox_id = path.split(":")
        return {"sandbox_id": sandbox_id, "app_name": app_name}

    async def _get_sandbox(self) -> modal.Sandbox:
        """Get or create Modal sandbox instance."""
        import modal

        if self._sandbox is not None:
            return self._sandbox
        if self._app is None:
            self._app = await modal.App.lookup.aio(self._app_name, create_if_missing=True)
        if self._sandbox_id:  # Connect to existing sandbox by ID
            self._sandbox = await modal.Sandbox.from_id.aio(self._sandbox_id)
        elif self._sandbox_name:  # Connect to named sandbox
            self._sandbox = await modal.Sandbox.from_name.aio(self._app_name, self._sandbox_name)
        else:
            self._sandbox = await modal.Sandbox.create.aio(
                app=self._app,
                image=self._image,
                timeout=self._timeout,
                workdir=self._workdir,
                volumes=self._volumes,
                secrets=self._secrets,
                cpu=self._cpu,
                memory=self._memory,
                gpu=self._gpu,
                idle_timeout=self._idle_timeout,
            )
            self._sandbox_id = self._sandbox.object_id

        return self._sandbox

    async def set_session(self) -> None:
        """Initialize the Modal session."""
        if not self._session_started:
            await self._get_sandbox()
            self._session_started = True

    async def close_session(self) -> None:
        """Close the Modal session."""
        if self._sandbox and self._session_started:
            self._sandbox.terminate()
            self._sandbox = None
            self._session_started = False

    @overload
    async def _ls(
        self, path: str, detail: Literal[True] = True, **kwargs: Any
    ) -> list[ModalInfo]: ...

    @overload
    async def _ls(self, path: str, detail: bool = False, **kwargs: Any) -> list[str]: ...

    async def _ls(
        self, path: str, detail: bool = True, **kwargs: Any
    ) -> list[ModalInfo] | list[str]:
        """List directory contents."""
        await self.set_session()
        sandbox = await self._get_sandbox()
        with _translate_errors(path, "list directory"):
            entries = await sandbox.filesystem.list_files.aio(path)
        if not detail:
            return [entry.path for entry in entries]
        return [_to_info(entry) for entry in entries]

    async def _cat_file(
        self, path: str, start: int | None = None, end: int | None = None, **kwargs: Any
    ) -> bytes:
        """Read file contents."""
        await self.set_session()
        sandbox = await self._get_sandbox()
        with _translate_errors(path, "read file"):
            content = await sandbox.filesystem.read_bytes.aio(path)
        if start is not None or end is not None:
            content = content[start or 0 : end if end is not None else len(content)]
        return content

    async def _pipe_file(
        self, path: str, value: bytes, mode: CreationMode = "overwrite", **kwargs: Any
    ) -> None:
        """Write data to a file in the sandbox."""
        await self.set_session()
        sandbox = await self._get_sandbox()
        with _translate_errors(path, "write file"):
            await sandbox.filesystem.write_bytes.aio(value, path)

    async def _mkdir(self, path: str, create_parents: bool = True, **kwargs: Any) -> None:
        """Create a directory."""
        await self.set_session()
        sandbox = await self._get_sandbox()
        with _translate_errors(path, "create directory"):
            await sandbox.filesystem.make_directory.aio(path, create_parents=create_parents)

    async def _rm_file(self, path: str, **kwargs: Any) -> None:
        """Remove a file."""
        await self.set_session()
        sandbox = await self._get_sandbox()
        with _translate_errors(path, "remove file"):
            await sandbox.filesystem.remove.aio(path, recursive=False)

    async def _rmdir(self, path: str, **kwargs: Any) -> None:
        """Remove a directory."""
        await self.set_session()
        sandbox = await self._get_sandbox()
        with _translate_errors(path, "remove directory"):
            await sandbox.filesystem.remove.aio(path, recursive=True)

    async def _stat(self, path: str) -> modal.types.FileInfo:
        """Get Modal's metadata for a path."""
        await self.set_session()
        sandbox = await self._get_sandbox()
        with _translate_errors(path, "stat"):
            return await sandbox.filesystem.stat.aio(path)

    async def _exists(self, path: str, **kwargs: Any) -> bool:
        """Check if path exists."""
        try:
            await self._stat(path)
        except FileNotFoundError:
            return False
        return True

    async def _isfile(self, path: str, **kwargs: Any) -> bool:
        """Check if path is a file."""
        try:
            return (await self._stat(path)).is_file()
        except FileNotFoundError:
            return False

    async def _isdir(self, path: str, **kwargs: Any) -> bool:
        """Check if path is a directory."""
        try:
            return (await self._stat(path)).is_dir()
        except FileNotFoundError:
            return False

    async def _size(self, path: str, **kwargs: Any) -> int:
        """Get file size."""
        return (await self._stat(path)).size

    async def _modified(self, path: str, **kwargs: Any) -> float:
        """Get file modification time."""
        return (await self._stat(path)).modified_time

    async def _info(self, path: str, **kwargs: Any) -> ModalInfo:
        """Get info about a file or directory."""
        return _to_info(await self._stat(path))

    # Sync wrappers for async methods
    ls = sync_wrapper(_ls)
    cat_file = sync_wrapper(_cat_file)  # pyright: ignore[reportAssignmentType]
    pipe_file = sync_wrapper(_pipe_file)  # pyright: ignore[reportAssignmentType]
    mkdir = sync_wrapper(_mkdir)
    rm_file = sync_wrapper(_rm_file)
    rmdir = sync_wrapper(_rmdir)
    exists = sync_wrapper(_exists)  # pyright: ignore[reportAssignmentType]
    isfile = sync_wrapper(_isfile)
    isdir = sync_wrapper(_isdir)
    size = sync_wrapper(_size)
    modified = sync_wrapper(_modified)
    info = sync_wrapper(_info)
