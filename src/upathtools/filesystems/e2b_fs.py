"""E2B async filesystem implementation for upathtools."""

from __future__ import annotations

import io
import logging
from typing import Any, overload

from fsspec.asyn import sync_wrapper

from upathtools.filesystems.base import BaseAsyncFileSystem, BaseUPath


logger = logging.getLogger(__name__)


class E2BPath(BaseUPath):
    """E2B-specific UPath implementation."""


class E2BFS(BaseAsyncFileSystem[E2BPath]):
    """Async filesystem for E2B sandbox environments.

    This filesystem provides access to files within an E2B sandbox environment,
    allowing you to read, write, and manipulate files remotely through the
    E2B native filesystem interface.
    """

    protocol = "e2b"
    upath_cls = E2BPath
    root_marker = "/"
    cachable = False  # Disable fsspec caching to prevent instance sharing

    def __init__(
        self,
        sandbox_id: str | None = None,
        api_key: str | None = None,
        template: str = "code-interpreter-v1",
        timeout: float = 300,
        **kwargs: Any,
    ):
        """Initialize E2B filesystem.

        Args:
            sandbox_id: Existing sandbox ID to connect to
            api_key: E2B API key
            template: E2B template to use for new sandboxes
            timeout: Default timeout for operations
            **kwargs: Additional filesystem options
        """
        super().__init__(**kwargs)
        self._sandbox_id = sandbox_id
        self._api_key = api_key
        self._template = template
        self._timeout = timeout
        self._sandbox = None
        self._session_started = False

    @staticmethod
    def _get_kwargs_from_urls(path):
        path = path.removeprefix("e2b://")
        return {"sandbox_id": path}

    async def _get_sandbox(self):
        """Get or create E2B sandbox instance."""
        if self._sandbox is not None:
            return self._sandbox

        try:
            # Import here to avoid requiring e2b as a hard dependency
            from e2b_code_interpreter import AsyncSandbox
        except ImportError as exc:
            msg = "e2b_code_interpreter package is required for E2BFS"
            raise ImportError(msg) from exc

        if self._sandbox_id:
            # Connect to existing sandbox
            self._sandbox = await AsyncSandbox.connect(
                sandbox_id=self._sandbox_id,
                api_key=self._api_key,
            )
        else:
            # Create new sandbox
            self._sandbox = await AsyncSandbox.create(
                template=self._template,
                api_key=self._api_key,
            )
            assert self._sandbox
            self._sandbox_id = self._sandbox.sandbox_id

        return self._sandbox

    async def set_session(self) -> None:
        """Initialize the E2B session."""
        if not self._session_started:
            await self._get_sandbox()
            self._session_started = True

    async def close_session(self) -> None:
        """Close the E2B session."""
        if self._sandbox and self._session_started:
            await self._sandbox.kill()
            self._sandbox = None
            self._session_started = False

    async def _ls_real(
        self, path: str, detail: bool = True, **kwargs: Any
    ) -> list[dict[str, Any]] | list[str]:
        """List directory contents."""
        await self.set_session()
        sandbox = await self._get_sandbox()

        try:
            items = await sandbox.files.list(path)
        except Exception as exc:
            if "not found" in str(exc).lower() or "no such file" in str(exc).lower():
                raise FileNotFoundError(path) from exc
            msg = f"Failed to list directory {path}: {exc}"
            raise OSError(msg) from exc

        if not detail:
            return [item.path for item in items]

        result = []
        for item in items:
            try:
                info = await sandbox.files.get_info(item.path)
                from e2b.sandbox.filesystem.filesystem import FileType

                result.append({
                    "name": item.path,
                    "size": info.size,
                    "type": "directory" if info.type == FileType.DIR else "file",
                    "mtime": info.modified_time.timestamp() if info.modified_time else 0,
                })
            except Exception:  # noqa: BLE001
                # Fallback if get_info fails - use item.is_dir
                result.append({
                    "name": item.path,
                    "size": 0,
                    "type": "directory" if item.type == FileType.DIR else "file",
                    "mtime": 0,
                })

        return result

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
            content = await sandbox.files.read(path)
        except Exception as exc:
            if "not found" in str(exc).lower() or "no such file" in str(exc).lower():
                raise FileNotFoundError(path) from exc
            if "is a directory" in str(exc).lower():
                raise IsADirectoryError(path) from exc
            msg = f"Failed to read file {path}: {exc}"
            raise OSError(msg) from exc

        # Handle byte ranges if specified
        if isinstance(content, str):
            content = content.encode("utf-8")

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

        # Read local file
        with open(lpath, "rb") as f:  # noqa: PTH123
            data = f.read()

        await self._pipe_file(rpath, data, **kwargs)

    async def _pipe_file(
        self, path: str, value: bytes, mode: str = "overwrite", **kwargs: Any
    ) -> None:
        """Write data to a file in the sandbox."""
        await self.set_session()
        sandbox = await self._get_sandbox()

        try:
            # Convert bytes to string for E2B - they handle encoding
            if isinstance(value, bytes):
                try:
                    content = value.decode("utf-8")
                except UnicodeDecodeError:
                    # For binary files, encode as base64 and write a script to decode
                    import base64

                    encoded = base64.b64encode(value).decode("ascii")
                    decode_script = f"""
import base64
with open({path!r}, 'wb') as f:
    f.write(base64.b64decode({encoded!r}))
"""
                    # Execute the decode script
                    execution = await sandbox.run_code(decode_script)
                    if execution.error:
                        msg = f"Failed to write binary file: {execution.error.value}"
                        raise OSError(msg)  # noqa: B904
                    return
            else:
                content = value

            await sandbox.files.write(path, content)
        except Exception as exc:
            msg = f"Failed to write file {path}: {exc}"
            raise OSError(msg) from exc

    async def _mkdir(self, path: str, create_parents: bool = True, **kwargs: Any) -> None:
        """Create a directory."""
        await self.set_session()
        sandbox = await self._get_sandbox()

        try:
            await sandbox.files.make_dir(path)
        except Exception as exc:
            # E2B make_dir might not have create_parents option
            if create_parents and "parent" in str(exc).lower():
                # Try to create parent directories first
                import os

                parent = os.path.dirname(path)  # noqa: PTH120
                if parent and parent not in (path, "/"):
                    await self._mkdir(parent, create_parents=True)
                    await sandbox.files.make_dir(path)
            else:
                msg = f"Failed to create directory {path}: {exc}"
                raise OSError(msg) from exc

    async def _rm_file(self, path: str, **kwargs: Any) -> None:
        """Remove a file."""
        await self.set_session()
        sandbox = await self._get_sandbox()

        try:
            await sandbox.files.remove(path)
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
            await sandbox.files.remove(path)
        except Exception as exc:
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
            return await sandbox.files.exists(path)
        except Exception:  # noqa: BLE001
            return False

    async def _isfile(self, path: str, **kwargs: Any) -> bool:
        """Check if path is a file."""
        await self.set_session()
        sandbox = await self._get_sandbox()

        try:
            if not await sandbox.files.exists(path):
                return False

            info = await sandbox.files.get_info(path)
            from e2b.sandbox.filesystem.filesystem import FileType

            return info.type == FileType.FILE  # noqa: TRY300
        except Exception:  # noqa: BLE001
            return False

    async def _isdir(self, path: str, **kwargs: Any) -> bool:
        """Check if path is a directory."""
        await self.set_session()
        sandbox = await self._get_sandbox()

        try:
            if not await sandbox.files.exists(path):
                return False

            info = await sandbox.files.get_info(path)
            from e2b.sandbox.filesystem.filesystem import FileType

            return info.type == FileType.DIR  # noqa: TRY300
        except Exception:  # noqa: BLE001
            return False

    async def _size(self, path: str, **kwargs: Any) -> int:
        """Get file size."""
        await self.set_session()
        sandbox = await self._get_sandbox()

        try:
            info = await sandbox.files.get_info(path)
        except Exception as exc:
            if "not found" in str(exc).lower() or "no such file" in str(exc).lower():
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
            info = await sandbox.files.get_info(path)
            return info.modified_time.timestamp() if info.modified_time else 0.0
        except Exception as exc:
            if "not found" in str(exc).lower() or "no such file" in str(exc).lower():
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


class E2BFile:
    """File-like object for E2B files."""

    def __init__(
        self,
        fs: E2BFS,
        path: str,
        mode: str = "rb",
        **kwargs: Any,
    ):
        """Initialize E2B file object.

        Args:
            fs: E2B filesystem instance
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
