"""Beam async filesystem implementation for upathtools."""

from __future__ import annotations

import io
import logging
import tempfile
from typing import TYPE_CHECKING, Any, overload

from fsspec.asyn import sync_wrapper

from upathtools.filesystems.base import BaseAsyncFileSystem, BaseUPath


if TYPE_CHECKING:
    from beta9.type import GpuTypeAlias


logger = logging.getLogger(__name__)


class BeamPath(BaseUPath):
    """Beam-specific UPath implementation."""


class BeamFS(BaseAsyncFileSystem[BeamPath]):
    """Async filesystem for Beam sandbox environments.

    This filesystem provides access to files within a Beam sandbox environment,
    allowing you to read, write, and manipulate files remotely through the
    Beam native filesystem interface.
    """

    upath_cls = BeamPath
    protocol = "beam"
    root_marker = "/"
    cachable = False  # Disable fsspec caching to prevent instance sharing

    def __init__(
        self,
        sandbox_id: str | None = None,
        cpu: float | str = 1.0,
        memory: int | str = 128,
        gpu: GpuTypeAlias | list[GpuTypeAlias] | None = None,
        gpu_count: int = 0,
        image: Any | None = None,
        keep_warm_seconds: int = 600,
        timeout: float = 300,
        env_variables: dict[str, str] | None = None,
        **kwargs: Any,
    ):
        """Initialize Beam filesystem.

        Args:
            sandbox_id: Existing sandbox ID to connect to
            cpu: CPU allocation for new sandboxes
            memory: Memory allocation for new sandboxes
            gpu: GPU type for new sandboxes
            gpu_count: Number of GPUs for new sandboxes
            image: Beam Image for new sandboxes
            keep_warm_seconds: How long to keep sandbox alive
            timeout: Default timeout for operations
            env_variables: Environment variables for new sandboxes
            **kwargs: Additional filesystem options
        """
        super().__init__(**kwargs)
        self._sandbox_id = sandbox_id
        self._cpu = cpu
        self._memory = memory
        self._gpu = gpu
        self._gpu_count = gpu_count
        self._image = image
        self._keep_warm_seconds = keep_warm_seconds
        self._timeout = timeout
        self.env_variables = env_variables
        self._sandbox_instance = None
        self._session_started = False

    async def _get_sandbox(self):
        """Get or create Beam sandbox instance."""
        if self._sandbox_instance is not None:
            return self._sandbox_instance

        try:
            # Import here to avoid requiring beta9 as a hard dependency
            from beta9 import Image, PythonVersion, Sandbox
        except ImportError as exc:
            msg = "beta9 package is required for BeamFS"
            raise ImportError(msg) from exc

        # Set default image if none provided
        if self._image is None:
            self._image = Image(python_version=PythonVersion.Python311)

        if self._sandbox_id:
            # Connect to existing sandbox
            sandbox = Sandbox()
            self._sandbox_instance = sandbox.connect(self._sandbox_id)
        else:
            # Create new sandbox
            sandbox = Sandbox(
                cpu=self._cpu,
                memory=self._memory,
                gpu=self._gpu or "NoGPU",  # pyright: ignore[reportArgumentType]
                gpu_count=self._gpu_count,
                image=self._image,
                keep_warm_seconds=self._keep_warm_seconds,
                env=self.env_variables,
            )
            self._sandbox_instance = sandbox.create()
            assert self._sandbox_instance
            self._sandbox_id = self._sandbox_instance.sandbox_id()

        return self._sandbox_instance

    async def set_session(self) -> None:
        """Initialize the Beam session."""
        if not self._session_started:
            await self._get_sandbox()
            self._session_started = True

    async def close_session(self) -> None:
        """Close the Beam session."""
        if self._sandbox_instance and self._session_started:
            self._sandbox_instance.terminate()
            self._sandbox_instance = None
            self._session_started = False

    async def _ls_real(
        self, path: str, detail: bool = True, **kwargs: Any
    ) -> list[dict[str, Any]] | list[str]:
        """List directory contents."""
        await self.set_session()
        sandbox = await self._get_sandbox()

        try:
            items = sandbox.fs.list_files(path)
        except Exception as exc:
            from beta9.exceptions import SandboxFileSystemError

            if isinstance(exc, SandboxFileSystemError) and (
                "not found" in str(exc).lower() or "no such file" in str(exc).lower()
            ):
                raise FileNotFoundError(path) from exc
            msg = f"Failed to list directory {path}: {exc}"
            raise OSError(msg) from exc

        if not detail:
            return [item.name for item in items]

        return [
            {
                "name": item.name,
                "size": item.size,
                "type": "directory" if item.is_dir else "file",
                "mtime": item.mod_time if hasattr(item, "mod_time") else 0,
            }
            for item in items
        ]

    @overload
    async def _ls(
        self, path: str, detail: bool = True, **kwargs: Any
    ) -> list[dict[str, Any]]: ...

    @overload
    async def _ls(self, path: str, detail: bool = False, **kwargs: Any) -> list[str]: ...

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
            # Create temporary file for download
            with tempfile.NamedTemporaryFile() as tmp_file:
                sandbox.fs.download_file(path, tmp_file.name)

                # Read the downloaded content
                with open(tmp_file.name, "rb") as f:  # noqa: PTH123
                    content = f.read()

        except Exception as exc:
            from beta9.exceptions import SandboxFileSystemError

            if isinstance(exc, SandboxFileSystemError):
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

    async def _put_file(
        self,
        lpath: str,
        rpath: str,
        callback=None,
        **kwargs: Any,
    ) -> None:
        """Upload a local file to the sandbox."""
        await self.set_session()
        sandbox = await self._get_sandbox()

        try:
            sandbox.fs.upload_file(lpath, rpath)
        except Exception as exc:
            msg = f"Failed to upload file {lpath} to {rpath}: {exc}"
            raise OSError(msg) from exc

    async def _pipe_file(
        self, path: str, value: bytes, mode: str = "overwrite", **kwargs: Any
    ) -> None:
        """Write data to a file in the sandbox."""
        await self.set_session()
        sandbox = await self._get_sandbox()

        try:
            # Create temporary file with the data
            with tempfile.NamedTemporaryFile(delete=False) as tmp_file:
                tmp_file.write(value)
                tmp_file.flush()

                # Upload the temporary file
                sandbox.fs.upload_file(tmp_file.name, path)

        except Exception as exc:
            msg = f"Failed to write file {path}: {exc}"
            raise OSError(msg) from exc
        finally:
            # Clean up temporary file
            try:
                import os

                os.unlink(tmp_file.name)  # noqa: PTH108
            except (OSError, UnboundLocalError):
                pass

    async def _mkdir(self, path: str, create_parents: bool = True, **kwargs: Any) -> None:
        """Create a directory."""
        await self.set_session()
        sandbox = await self._get_sandbox()

        try:
            sandbox.fs.create_directory(path)
        except Exception as exc:
            from beta9.exceptions import SandboxFileSystemError

            # Try to create parent directories if needed
            if (
                isinstance(exc, SandboxFileSystemError)
                and create_parents
                and "parent" in str(exc).lower()
            ):
                import os

                parent = os.path.dirname(path)  # noqa: PTH120
                if parent and parent not in (path, "/"):
                    await self._mkdir(parent, create_parents=True)
                    sandbox.fs.create_directory(path)
            else:
                msg = f"Failed to create directory {path}: {exc}"
                raise OSError(msg) from exc

    async def _rm_file(self, path: str, **kwargs: Any) -> None:
        """Remove a file."""
        await self.set_session()
        sandbox = await self._get_sandbox()

        try:
            sandbox.fs.delete_file(path)
        except Exception as exc:
            from beta9.exceptions import SandboxFileSystemError

            if isinstance(exc, SandboxFileSystemError):
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
            sandbox.fs.delete_directory(path)
        except Exception as exc:
            from beta9.exceptions import SandboxFileSystemError

            if isinstance(exc, SandboxFileSystemError):
                if "not found" in str(exc).lower() or "no such file" in str(exc).lower():
                    raise FileNotFoundError(path) from exc
                if "not a directory" in str(exc).lower():
                    raise NotADirectoryError(path) from exc
                if "not empty" in str(exc).lower():
                    msg = f"Directory not empty: {path}"
                    raise OSError(msg) from exc
            msg = f"Failed to remove directory {path}: {exc}"
            raise OSError(msg) from exc

    async def _exists(self, path: str, **kwargs: Any) -> bool:
        """Check if path exists."""
        await self.set_session()
        sandbox = await self._get_sandbox()

        try:
            # Try to stat the file/directory
            sandbox.fs.stat_file(path)
        except Exception:  # noqa: BLE001
            return False
        else:
            return True

    async def _isfile(self, path: str, **kwargs: Any) -> bool:
        """Check if path is a file."""
        await self.set_session()
        sandbox = await self._get_sandbox()

        try:
            info = sandbox.fs.stat_file(path)
        except Exception:  # noqa: BLE001
            return False
        else:
            return not info.is_dir

    async def _isdir(self, path: str, **kwargs: Any) -> bool:
        """Check if path is a directory."""
        await self.set_session()
        sandbox = await self._get_sandbox()

        try:
            info = sandbox.fs.stat_file(path)
        except Exception:  # noqa: BLE001
            return False
        else:
            return info.is_dir

    async def _size(self, path: str, **kwargs: Any) -> int:
        """Get file size."""
        await self.set_session()
        sandbox = await self._get_sandbox()

        try:
            info = sandbox.fs.stat_file(path)
        except Exception as exc:
            from beta9.exceptions import SandboxFileSystemError

            if isinstance(exc, SandboxFileSystemError) and (
                "not found" in str(exc).lower() or "no such file" in str(exc).lower()
            ):
                raise FileNotFoundError(path) from exc
            msg = f"Failed to get file size for {path}: {exc}"
            raise OSError(msg) from exc
        else:
            return info.size

    async def _modified(self, path: str, **kwargs: Any) -> float:
        """Get file modification time."""
        await self.set_session()
        sandbox = await self._get_sandbox()

        try:
            info = sandbox.fs.stat_file(path)
            return (
                float(info.mod_time)
                if hasattr(info, "mod_time") and info.mod_time
                else 0.0
            )
        except Exception as exc:
            from beta9.exceptions import SandboxFileSystemError

            if isinstance(exc, SandboxFileSystemError) and (
                "not found" in str(exc).lower() or "no such file" in str(exc).lower()
            ):
                raise FileNotFoundError(path) from exc
            msg = f"Failed to get modification time for {path}: {exc}"
            raise OSError(msg) from exc

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


class BeamFile:
    """File-like object for Beam files."""

    def __init__(
        self,
        fs: BeamFS,
        path: str,
        mode: str = "rb",
        **kwargs: Any,
    ):
        """Initialize Beam file object.

        Args:
            fs: Beam filesystem instance
            path: File path
            mode: File open mode
            **kwargs: Additional options
        """
        self.fs = fs
        self.path = path
        self.mode = mode
        self._buffer = io.BytesIO()
        self._position = 0
        self._closed = False
        self._loaded = False

    async def _ensure_loaded(self) -> None:
        """Ensure file content is loaded."""
        if not self._loaded and "r" in self.mode:
            try:
                content = await self.fs._cat_file(self.path)
                self._buffer = io.BytesIO(content)
                self._loaded = True
            except FileNotFoundError:
                if "w" not in self.mode and "a" not in self.mode:
                    raise

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

    def tell(self) -> int:
        """Get current position."""
        if self._closed:
            msg = "I/O operation on closed file"
            raise ValueError(msg)
        return self._buffer.tell()

    def seek(self, offset: int, whence: int = 0) -> int:
        """Seek to position."""
        if self._closed:
            msg = "I/O operation on closed file"
            raise ValueError(msg)
        return self._buffer.seek(offset, whence)

    async def read(self, size: int = -1) -> bytes:
        """Read data from file."""
        if self._closed:
            msg = "I/O operation on closed file"
            raise ValueError(msg)
        if not self.readable():
            msg = "not readable"
            raise io.UnsupportedOperation(msg)

        await self._ensure_loaded()
        return self._buffer.read(size)

    async def write(self, data: bytes) -> int:
        """Write data to file."""
        if self._closed:
            msg = "I/O operation on closed file"
            raise ValueError(msg)
        if not self.writable():
            msg = "not writable"
            raise io.UnsupportedOperation(msg)

        return self._buffer.write(data)

    async def flush(self) -> None:
        """Flush buffer to remote file."""
        if self._closed:
            return
        if self.writable():
            self._buffer.seek(0)
            content = self._buffer.read()
            await self.fs._pipe_file(self.path, content)

    async def close(self) -> None:
        """Close file."""
        if not self._closed:
            if self.writable():
                await self.flush()
            self._buffer.close()
            self._closed = True

    async def __aenter__(self):
        """Async context manager entry."""
        return self

    async def __aexit__(self, exc_type, exc_val, exc_tb):
        """Async context manager exit."""
        await self.close()
