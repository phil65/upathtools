"""Modal async filesystem implementation for upathtools."""

from __future__ import annotations

import io
import logging
from typing import TYPE_CHECKING, Any, Literal, Required, Self, overload

from upathtools.async_helpers import sync_wrapper
from upathtools.filesystems.base import BaseAsyncFileSystem, BaseUPath, FileInfo


if TYPE_CHECKING:
    from collections.abc import Collection
    import os

    from _typeshed import OpenTextMode
    import modal
    from modal import Image
    from modal.file_io import FileIO

    from upathtools.filesystems.base import CreationMode


logger = logging.getLogger(__name__)


class ModalInfo(FileInfo, total=False):
    """Info dict for Modal filesystem paths."""

    size: Required[int]
    mtime: Required[float]


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
    async def _ls(self, path: str, detail: Literal[False] = False, **kwargs: Any) -> list[str]: ...

    async def _ls(
        self, path: str, detail: bool = True, **kwargs: Any
    ) -> list[ModalInfo] | list[str]:
        """List directory contents with caching."""
        await self.set_session()
        sandbox = await self._get_sandbox()

        try:
            items = await sandbox.ls.aio(path)
        except Exception as exc:
            if "not found" in str(exc).lower() or "no such file" in str(exc).lower():
                raise FileNotFoundError(path) from exc
            if "not a directory" in str(exc).lower():
                raise NotADirectoryError(path) from exc
            msg = f"Failed to list directory {path}: {exc}"
            raise OSError(msg) from exc

        if not detail:
            return items

        # TODO: Enhance with actual file metadata when Modal provides it
        # For now, return minimal info since Modal's ls() only returns paths
        result = []
        for item in items:
            # Try to determine if it's a directory by attempting to list it
            is_dir = False
            try:
                await sandbox.ls.aio(item)
                is_dir = True
            except Exception:  # noqa: BLE001
                pass  # If ls fails, assume it's a file
            # TODO: Get actual size/mtime when Modal provides metadata API
            info = ModalInfo(name=item, size=0, type="directory" if is_dir else "file", mtime=0)
            result.append(info)

        return result

    async def _cat_file(
        self, path: str, start: int | None = None, end: int | None = None, **kwargs: Any
    ) -> bytes:
        """Read file contents."""
        await self.set_session()
        sandbox = await self._get_sandbox()
        try:
            f = await sandbox.open.aio(path, "rb")  # Use Modal's file API to read the file
            try:
                content = await f.read.aio()
            finally:
                await f.close.aio()

        except Exception as exc:
            # Map Modal exceptions to standard Python exceptions
            if "not found" in str(exc).lower() or "no such file" in str(exc).lower():
                raise FileNotFoundError(path) from exc
            if "is a directory" in str(exc).lower():
                raise IsADirectoryError(path) from exc
            msg = f"Failed to read file {path}: {exc}"
            raise OSError(msg) from exc

        # Handle byte ranges if specified
        if start is not None or end is not None:
            start = start or 0
            end = end or len(content)
            content = content[start:end]

        return content

    async def _pipe_file(
        self, path: str, value: bytes, mode: CreationMode = "overwrite", **kwargs: Any
    ) -> None:
        """Write data to a file in the sandbox."""
        await self.set_session()
        sandbox = await self._get_sandbox()
        try:
            f = await sandbox.open.aio(path, "wb")  # Use Modal's file API to write the file
            try:
                await f.write.aio(value)
                await f.flush.aio()
            finally:
                await f.close.aio()

        except Exception as exc:
            msg = f"Failed to write file {path}: {exc}"
            raise OSError(msg) from exc

    async def _mkdir(self, path: str, create_parents: bool = True, **kwargs: Any) -> None:
        """Create a directory."""
        await self.set_session()
        sandbox = await self._get_sandbox()
        try:
            await sandbox.mkdir.aio(path, parents=create_parents)
        except Exception as exc:
            msg = f"Failed to create directory {path}: {exc}"
            raise OSError(msg) from exc

    async def _rm_file(self, path: str, **kwargs: Any) -> None:
        """Remove a file."""
        await self.set_session()
        sandbox = await self._get_sandbox()
        try:
            await sandbox.rm.aio(path, recursive=False)
        except Exception as exc:
            if "not found" in str(exc).lower() or "no such file" in str(exc).lower():
                raise FileNotFoundError(path) from exc
            if "is a directory" in str(exc).lower():
                raise IsADirectoryError(path) from exc
            msg = f"Failed to remove file {path}: {exc}"
            raise OSError(msg) from exc

    async def _rmdir(self, path: str, **kwargs: Any) -> None:
        """Remove a directory."""
        await self.set_session()
        sandbox = await self._get_sandbox()
        try:
            await sandbox.rm.aio(path, recursive=True)
        except Exception as exc:
            if "not found" in str(exc).lower() or "no such file" in str(exc).lower():
                raise FileNotFoundError(path) from exc
            if "not a directory" in str(exc).lower():
                raise NotADirectoryError(path) from exc
            msg = f"Failed to remove directory {path}: {exc}"
            raise OSError(msg) from exc

    async def _exists(self, path: str, **kwargs: Any) -> bool:
        """Check if path exists."""
        await self.set_session()
        sandbox = await self._get_sandbox()

        try:
            # Try to open the file/directory to check existence
            f = await sandbox.open.aio(path, "r")
            await f.close.aio()
        except Exception:  # noqa: BLE001
            # If open fails, try ls to see if path exists as directory
            try:
                await sandbox.ls.aio(path)
            except Exception:  # noqa: BLE001
                return False
            else:
                return True
        else:
            return True

    async def _isfile(self, path: str, **kwargs: Any) -> bool:
        """Check if path is a file."""
        await self.set_session()
        sandbox = await self._get_sandbox()

        try:
            f = await sandbox.open.aio(path, "r")  # Try to open as file
            await f.close.aio()
        except Exception:  # noqa: BLE001
            return False
        else:
            return True

    async def _isdir(self, path: str, **kwargs: Any) -> bool:
        """Check if path is a directory."""
        await self.set_session()
        sandbox = await self._get_sandbox()

        try:
            # Try to list the path - if it works, it's a directory
            await sandbox.ls.aio(path)
        except Exception:  # noqa: BLE001
            return False
        else:
            return True

    async def _size(self, path: str, **kwargs: Any) -> int:
        """Get file size."""
        await self.set_session()
        sandbox = await self._get_sandbox()

        try:
            # TODO: This is inefficient - reading entire file to get size
            # Modal should provide a stat() or size() method in the future
            f = await sandbox.open.aio(path, "rb")
            try:
                content = await f.read.aio()
                return len(content)
            finally:
                await f.close.aio()
        except Exception as exc:
            if "not found" in str(exc).lower() or "no such file" in str(exc).lower():
                raise FileNotFoundError(path) from exc
            msg = f"Failed to get file size for {path}: {exc}"
            raise OSError(msg) from exc

    async def _modified(self, path: str, **kwargs: Any) -> float:
        """Get file modification time."""
        # TODO: Modal doesn't provide modification time in current API
        # Return 0.0 as placeholder until Modal provides metadata API
        await self.set_session()
        if not await self._exists(path):
            raise FileNotFoundError(path)
        return 0.0  # TODO: Get actual mtime when Modal provides metadata API

    async def _info(self, path: str, **kwargs: Any) -> ModalInfo:
        """Get info about a file or directory."""
        await self.set_session()
        is_dir = await self._isdir(path)
        size = 0 if is_dir else await self._size(path)
        # TODO: Get actual mtime when Modal provides metadata API
        return ModalInfo(name=path, size=size, type="directory" if is_dir else "file", mtime=0.0)

    # TODO: Add _find and _grep using sandbox.exec() with Linux CLI tools (find, grep)
    # This would be faster than fsspec's default _walk which does multiple _ls round trips.

    # Sync wrappers for async methods
    ls = sync_wrapper(_ls)
    cat_file = sync_wrapper(_cat_file)  # pyright: ignore[reportAssignmentType]
    pipe_file = sync_wrapper(_pipe_file)
    mkdir = sync_wrapper(_mkdir)
    rm_file = sync_wrapper(_rm_file)
    rmdir = sync_wrapper(_rmdir)
    exists = sync_wrapper(_exists)  # pyright: ignore[reportAssignmentType]
    isfile = sync_wrapper(_isfile)
    isdir = sync_wrapper(_isdir)
    size = sync_wrapper(_size)
    modified = sync_wrapper(_modified)
    info = sync_wrapper(_info)


class ModalFile:
    """File-like object wrapping Modal's native FileIO.

    Unlike other sandbox filesystems, Modal provides a native async file API
    (`sandbox.open.aio`) that may support true partial reads and seeks on the
    remote end. This class wraps that native API rather than buffering the
    entire file in memory like `AsyncBufferedFile`.
    """

    def __init__(
        self,
        fs: ModalFS,
        path: str,
        mode: OpenTextMode = "rt",
        **kwargs: Any,
    ) -> None:
        """Initialize Modal file object.

        Args:
            fs: Modal filesystem instance
            path: File path
            mode: File open mode
            **kwargs: Additional options
        """
        self.fs = fs
        self.path = path
        self.mode: OpenTextMode = mode
        self._modal_file: FileIO | None = None
        self._closed = False

    async def _ensure_opened(self) -> FileIO:
        """Ensure Modal file is opened."""
        if self._modal_file is None:
            await self.fs.set_session()
            sandbox = await self.fs._get_sandbox()
            self._modal_file = await sandbox.open.aio(self.path, self.mode)
        return self._modal_file

    def readable(self) -> bool:
        """Check if file is readable."""
        return "r" in self.mode

    def writable(self) -> bool:
        """Check if file is writable."""
        return "w" in self.mode or "a" in self.mode

    def seekable(self) -> bool:
        """Check if file is seekable."""
        return True

    @property
    def closed(self) -> bool:
        """Check if file is closed."""
        return self._closed

    async def read(self, size: int = -1) -> bytes:
        """Read data from file."""
        if self._closed:
            msg = "I/O operation on closed file"
            raise ValueError(msg)
        if not self.readable():
            msg = "not readable"
            raise io.UnsupportedOperation(msg)

        file = await self._ensure_opened()
        if size == -1:
            return await file.read()
        return await file.read(size)

    async def write(self, data: bytes) -> int:
        """Write data to file."""
        if self._closed:
            msg = "I/O operation on closed file"
            raise ValueError(msg)
        if not self.writable():
            msg = "not writable"
            raise io.UnsupportedOperation(msg)

        file = await self._ensure_opened()
        await file.write.aio(data)
        return len(data)

    async def flush(self) -> None:
        """Flush buffer to remote file."""
        if self._closed:
            return
        if self._modal_file and self.writable():
            await self._modal_file.flush.aio()

    async def seek(self, offset: int, whence: int = 0) -> int:
        """Seek to position."""
        if self._closed:
            msg = "I/O operation on closed file"
            raise ValueError(msg)

        file = await self._ensure_opened()
        await file.seek.aio(offset, whence)
        return offset  # TODO: Modal should return actual position

    async def close(self) -> None:
        """Close file."""
        if not self._closed:
            if self._modal_file:
                await self._modal_file.close.aio()
            self._closed = True

    async def __aenter__(self) -> Self:
        """Async context manager entry."""
        return self

    async def __aexit__(self, *args: object) -> None:
        """Async context manager exit."""
        await self.close()
