"""Modal async filesystem implementation for upathtools."""

from __future__ import annotations

import io
import logging
from typing import TYPE_CHECKING, Any

from fsspec.asyn import sync_wrapper

from upathtools.filesystems.base import BaseAsyncFileSystem, BaseUPath


if TYPE_CHECKING:
    import modal


logger = logging.getLogger(__name__)


class ModalPath(BaseUPath):
    """Modal-specific UPath implementation."""


class ModalFS(BaseAsyncFileSystem[ModalPath]):
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
        image: Any | None = None,
        cpu: float | None = None,
        memory: int | None = None,
        gpu: str | None = None,
        timeout: int = 300,
        idle_timeout: int | None = None,
        workdir: str | None = None,
        volumes: dict[str, Any] | None = None,
        secrets: list[Any] | None = None,
        **kwargs: Any,
    ):
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

    async def _get_app(self):
        """Get or create Modal app."""
        if self._app is not None:
            return self._app

        try:
            # Import here to avoid requiring modal as a hard dependency
            import modal
        except ImportError as exc:
            msg = "modal package is required for ModalFS"
            raise ImportError(msg) from exc

        self._app = modal.App.lookup(self._app_name, create_if_missing=True)
        return self._app

    async def _get_sandbox(self):
        """Get or create Modal sandbox instance."""
        if self._sandbox is not None:
            return self._sandbox

        try:
            import modal
        except ImportError as exc:
            msg = "modal package is required for ModalFS"
            raise ImportError(msg) from exc

        app = await self._get_app()

        if self._sandbox_id:
            # Connect to existing sandbox by ID
            self._sandbox = modal.Sandbox.from_id(self._sandbox_id)
        elif self._sandbox_name:
            # Connect to named sandbox
            self._sandbox = modal.Sandbox.from_name(self._app_name, self._sandbox_name)
        else:
            # Create new sandbox
            create_kwargs = {"app": app, "timeout": self._timeout}

            if self._image is not None:
                create_kwargs["image"] = self._image
            if self._cpu is not None:
                create_kwargs["cpu"] = self._cpu
            if self._memory is not None:
                create_kwargs["memory"] = self._memory
            if self._gpu is not None:
                create_kwargs["gpu"] = self._gpu
            if self._idle_timeout is not None:
                create_kwargs["idle_timeout"] = self._idle_timeout
            if self._workdir is not None:
                create_kwargs["workdir"] = self._workdir
            if self._volumes:
                create_kwargs["volumes"] = self._volumes
            if self._secrets:
                create_kwargs["secrets"] = self._secrets

            self._sandbox = modal.Sandbox.create(**create_kwargs)
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

    async def _ls_real(
        self, path: str, detail: bool = True, **kwargs: Any
    ) -> list[dict[str, Any]] | list[str]:
        """List directory contents."""
        await self.set_session()
        sandbox = await self._get_sandbox()

        try:
            from modal.file_io import FileIO

            # Get client and task_id from sandbox
            # NOTE: This is accessing internal attributes - may need adjustment
            # based on actual Modal API structure
            client = sandbox._client  # TODO: Verify correct way to get client
            task_id = sandbox._task_id  # TODO: Verify correct way to get task_id

            items = await FileIO.ls(path, client, task_id)
        except Exception as exc:
            # Map Modal exceptions to standard Python exceptions
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
            # This is a heuristic and could be improved with better Modal API support
            is_dir = False
            try:
                await FileIO.ls(item, client, task_id)
                is_dir = True
            except Exception:  # noqa: BLE001
                pass  # If ls fails, assume it's a file

            result.append({
                "name": item,
                "size": 0,  # TODO: Get actual size when Modal provides metadata API
                "type": "directory" if is_dir else "file",
                "mtime": 0,  # TODO: Get actual mtime when Modal provides metadata API
            })

        return result

    async def _ls(
        self, path: str, detail: bool = True, **kwargs: Any
    ) -> list[dict[str, Any]] | list[str]:
        """List directory contents with caching."""
        return await self._ls_real(path, detail, **kwargs)

    async def _cat_file(
        self, path: str, start: int | None = None, end: int | None = None, **kwargs: Any
    ) -> bytes:
        """Read file contents."""
        await self.set_session()
        sandbox = await self._get_sandbox()

        try:
            # Use Modal's file API to read the file
            f = sandbox.open(path, "rb")
            try:
                content = await f.read()
            finally:
                await f.close()

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
        self, path: str, value: bytes, mode: str = "overwrite", **kwargs: Any
    ) -> None:
        """Write data to a file in the sandbox."""
        await self.set_session()
        sandbox = await self._get_sandbox()

        try:
            # Use Modal's file API to write the file
            f = sandbox.open(path, "wb")
            try:
                await f.write(value)
                await f.flush()
            finally:
                await f.close()

        except Exception as exc:
            msg = f"Failed to write file {path}: {exc}"
            raise OSError(msg) from exc

    async def _mkdir(self, path: str, create_parents: bool = True, **kwargs: Any) -> None:
        """Create a directory."""
        await self.set_session()
        sandbox = await self._get_sandbox()

        try:
            from modal.file_io import FileIO

            client = sandbox._client  # TODO: Verify correct way to get client
            task_id = sandbox._task_id  # TODO: Verify correct way to get task_id

            await FileIO.mkdir(path, client, task_id, parents=create_parents)
        except Exception as exc:
            msg = f"Failed to create directory {path}: {exc}"
            raise OSError(msg) from exc

    async def _rm_file(self, path: str, **kwargs: Any) -> None:
        """Remove a file."""
        await self.set_session()
        sandbox = await self._get_sandbox()

        try:
            from modal.file_io import FileIO

            client = sandbox._client  # TODO: Verify correct way to get client
            task_id = sandbox._task_id  # TODO: Verify correct way to get task_id

            await FileIO.rm(path, client, task_id, recursive=False)
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
            from modal.file_io import FileIO

            client = sandbox._client  # TODO: Verify correct way to get client
            task_id = sandbox._task_id  # TODO: Verify correct way to get task_id

            await FileIO.rm(path, client, task_id, recursive=True)
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
            # TODO: This could be optimized with a dedicated exists() API
            # if Modal provides one
            f = sandbox.open(path, "r")
            await f.close()
        except Exception:  # noqa: BLE001
            # If open fails, try ls on parent to see if path exists as directory
            try:
                from modal.file_io import FileIO

                client = sandbox._client  # TODO: Verify correct way to get client
                task_id = sandbox._task_id  # TODO: Verify correct way to get task_id

                await FileIO.ls(path, client, task_id)
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
            # Try to open as file
            f = sandbox.open(path, "r")
            await f.close()
        except Exception:  # noqa: BLE001
            return False
        else:
            return True

    async def _isdir(self, path: str, **kwargs: Any) -> bool:
        """Check if path is a directory."""
        await self.set_session()
        sandbox = await self._get_sandbox()

        try:
            from modal.file_io import FileIO

            client = sandbox._client  # TODO: Verify correct way to get client
            task_id = sandbox._task_id  # TODO: Verify correct way to get task_id

            # Try to list the path - if it works, it's a directory
            await FileIO.ls(path, client, task_id)
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
            f = sandbox.open(path, "rb")
            try:
                content = await f.read()
                return len(content)
            finally:
                await f.close()
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

        # Check if file exists first
        if not await self._exists(path):
            raise FileNotFoundError(path)

        return 0.0  # TODO: Get actual mtime when Modal provides metadata API

    # Sync wrappers for async methods
    ls = sync_wrapper(_ls)
    cat_file = sync_wrapper(_cat_file)
    pipe_file = sync_wrapper(_pipe_file)
    mkdir = sync_wrapper(_mkdir)
    rm_file = sync_wrapper(_rm_file)
    rmdir = sync_wrapper(_rmdir)
    exists = sync_wrapper(_exists)
    isfile = sync_wrapper(_isfile)
    isdir = sync_wrapper(_isdir)
    size = sync_wrapper(_size)
    modified = sync_wrapper(_modified)


class ModalFile:
    """File-like object for Modal files."""

    def __init__(
        self,
        fs: ModalFS,
        path: str,
        mode: str = "rb",
        **kwargs: Any,
    ):
        """Initialize Modal file object.

        Args:
            fs: Modal filesystem instance
            path: File path
            mode: File open mode
            **kwargs: Additional options
        """
        self.fs = fs
        self.path = path
        self.mode = mode
        self._modal_file: Any = None
        self._closed = False

    async def _ensure_opened(self) -> None:
        """Ensure Modal file is opened."""
        if self._modal_file is None:
            await self.fs.set_session()
            sandbox = await self.fs._get_sandbox()
            self._modal_file = sandbox.open(self.path, self.mode)

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

        await self._ensure_opened()
        if size == -1:
            return await self._modal_file.read()
        return await self._modal_file.read(size)

    async def write(self, data: bytes) -> int:
        """Write data to file."""
        if self._closed:
            msg = "I/O operation on closed file"
            raise ValueError(msg)
        if not self.writable():
            msg = "not writable"
            raise io.UnsupportedOperation(msg)

        await self._ensure_opened()
        await self._modal_file.write(data)
        return len(data)

    async def flush(self) -> None:
        """Flush buffer to remote file."""
        if self._closed:
            return
        if self._modal_file and self.writable():
            await self._modal_file.flush()

    async def seek(self, offset: int, whence: int = 0) -> int:
        """Seek to position."""
        if self._closed:
            msg = "I/O operation on closed file"
            raise ValueError(msg)

        await self._ensure_opened()
        await self._modal_file.seek(offset, whence)
        return offset  # TODO: Modal should return actual position

    async def close(self) -> None:
        """Close file."""
        if not self._closed:
            if self._modal_file:
                await self._modal_file.close()
            self._closed = True

    async def __aenter__(self):
        """Async context manager entry."""
        return self

    async def __aexit__(self, exc_type, exc_val, exc_tb):
        """Async context manager exit."""
        await self.close()
