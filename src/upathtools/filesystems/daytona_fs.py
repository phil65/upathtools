"""Daytona async filesystem implementation for upathtools."""

from __future__ import annotations

import asyncio
import io
import logging
from typing import Any, overload

from fsspec.asyn import sync_wrapper

from upathtools.filesystems.base import BaseAsyncFileSystem, BaseUPath


logger = logging.getLogger(__name__)


class DaytonaPath(BaseUPath):
    """Daytona-specific UPath implementation."""


class DaytonaFS(BaseAsyncFileSystem[DaytonaPath]):
    """Async filesystem for Daytona sandbox environments.

    This filesystem provides access to files within a Daytona sandbox environment,
    allowing you to read, write, and manipulate files remotely through the
    Daytona native filesystem interface.
    """

    protocol = "daytona"
    upath_cls = DaytonaPath
    root_marker = "/"
    cachable = False  # Disable fsspec caching to prevent instance sharing

    def __init__(
        self,
        sandbox_id: str | None = None,
        api_key: str | None = None,
        **kwargs: Any,
    ):
        """Initialize Daytona filesystem.

        Args:
            sandbox_id: Existing sandbox ID to connect to
            api_key: Daytona API key
            **kwargs: Additional filesystem options
        """
        super().__init__(**kwargs)
        self._sandbox_id = sandbox_id
        self._api_key = api_key
        self._sandbox = None
        self._daytona = None
        self._session_started = False

    @staticmethod
    def _get_kwargs_from_urls(path):
        path = path.removeprefix("daytona://")
        return {"sandbox_id": path}

    async def _get_sandbox(self):
        """Get or create Daytona sandbox instance."""
        if self._sandbox is not None:
            return self._sandbox

        from daytona import Daytona, DaytonaConfig

        config = DaytonaConfig(api_key=self._api_key)
        self._daytona = Daytona(config)
        assert self._daytona
        if self._sandbox_id:
            self._sandbox = await asyncio.to_thread(
                self._daytona.get, self._sandbox_id
            )  # Connect to existing
        else:
            self._sandbox = await asyncio.to_thread(self._daytona.create)
            assert self._sandbox
            self._sandbox_id = self._sandbox.id

        return self._sandbox

    async def set_session(self) -> None:
        """Initialize the Daytona session."""
        if not self._session_started:
            await self._get_sandbox()
            self._session_started = True

    async def close_session(self) -> None:
        """Close the Daytona session."""
        if self._sandbox and self._session_started:
            await asyncio.to_thread(self._sandbox.delete)
            self._sandbox = None
            self._session_started = False

    async def _ls_real(
        self, path: str, detail: bool = True, **kwargs: Any
    ) -> list[dict[str, Any]] | list[str]:
        """List directory contents."""
        await self.set_session()
        sandbox = await self._get_sandbox()

        try:
            file_infos = await asyncio.to_thread(sandbox.fs.list_files, path)
        except Exception as exc:
            if "not found" in str(exc).lower() or "no such file" in str(exc).lower():
                raise FileNotFoundError(path) from exc
            msg = f"Failed to list directory {path}: {exc}"
            raise OSError(msg) from exc

        if not detail:
            return [info.name for info in file_infos]

        return [
            {
                "name": f"{path.rstrip('/')}/{info.name}",
                "size": info.size,
                "type": "directory" if info.is_dir else "file",
                "mtime": info.mod_time if info.mod_time else 0,
                "mode": info.mode if hasattr(info, "mode") else 0,
                "permissions": info.permissions if hasattr(info, "permissions") else "",
                "owner": info.owner if hasattr(info, "owner") else "",
                "group": info.group if hasattr(info, "group") else "",
            }
            for info in file_infos
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
            content = await asyncio.to_thread(sandbox.fs.download_file, path)
        except Exception as exc:
            if "not found" in str(exc).lower() or "no such file" in str(exc).lower():
                raise FileNotFoundError(path) from exc
            if "is a directory" in str(exc).lower():
                raise IsADirectoryError(path) from exc
            msg = f"Failed to read file {path}: {exc}"
            raise OSError(msg) from exc

        # Ensure we have bytes
        if isinstance(content, str):
            content = content.encode("utf-8")

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
            await asyncio.to_thread(sandbox.fs.upload_file, lpath, rpath)
        except Exception as exc:
            msg = f"Failed to upload file from {lpath} to {rpath}: {exc}"
            raise OSError(msg) from exc

    async def _pipe_file(
        self, path: str, value: bytes, mode: str = "overwrite", **kwargs: Any
    ) -> None:
        """Write data to a file in the sandbox."""
        await self.set_session()
        sandbox = await self._get_sandbox()

        try:
            await asyncio.to_thread(sandbox.fs.upload_file, value, path)
        except Exception as exc:
            msg = f"Failed to write file {path}: {exc}"
            raise OSError(msg) from exc

    async def _mkdir(self, path: str, create_parents: bool = True, **kwargs: Any) -> None:
        """Create a directory."""
        await self.set_session()
        sandbox = await self._get_sandbox()

        try:
            # Daytona's create_folder uses octal permissions as string
            await asyncio.to_thread(sandbox.fs.create_folder, path, "755")
        except Exception as exc:
            if create_parents and "parent" in str(exc).lower():
                # Try to create parent directories first
                import os

                parent = os.path.dirname(path)  # noqa: PTH120
                if parent and parent not in (path, "/"):
                    await self._mkdir(parent, create_parents=True)
                    await asyncio.to_thread(sandbox.fs.create_folder, path, "755")
            else:
                msg = f"Failed to create directory {path}: {exc}"
                raise OSError(msg) from exc

    async def _rm_file(self, path: str, **kwargs: Any) -> None:
        """Remove a file."""
        await self.set_session()
        sandbox = await self._get_sandbox()

        try:
            await asyncio.to_thread(sandbox.fs.delete_file, path)
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
            # doesnt have delete-dir, so workaround.
            await asyncio.to_thread(sandbox.fs.move_files, path, "tmp/upathools")
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
            await asyncio.to_thread(sandbox.fs.get_file_info, path)
        except Exception:  # noqa: BLE001
            return False
        else:
            return True

    async def _isfile(self, path: str, **kwargs: Any) -> bool:
        """Check if path is a file."""
        await self.set_session()
        sandbox = await self._get_sandbox()

        try:
            info = await asyncio.to_thread(sandbox.fs.get_file_info, path)
        except Exception:  # noqa: BLE001
            return False
        else:
            return not info.is_dir

    async def _isdir(self, path: str, **kwargs: Any) -> bool:
        """Check if path is a directory."""
        await self.set_session()
        sandbox = await self._get_sandbox()

        try:
            info = await asyncio.to_thread(sandbox.fs.get_file_info, path)
        except Exception:  # noqa: BLE001
            return False
        else:
            return info.is_dir

    async def _size(self, path: str, **kwargs: Any) -> int:
        """Get file size."""
        await self.set_session()
        sandbox = await self._get_sandbox()

        try:
            info = await asyncio.to_thread(sandbox.fs.get_file_info, path)
        except Exception as exc:
            if "not found" in str(exc).lower() or "no such file" in str(exc).lower():
                raise FileNotFoundError(path) from exc
            msg = f"Failed to get file size for {path}: {exc}"
            raise OSError(msg) from exc
        else:
            return int(info.size)

    async def _modified(self, path: str, **kwargs: Any) -> float:
        """Get file modification time."""
        await self.set_session()
        sandbox = await self._get_sandbox()

        try:
            info = await asyncio.to_thread(sandbox.fs.get_file_info, path)
            return float(info.mod_time) if info.mod_time else 0.0
        except Exception as exc:
            if "not found" in str(exc).lower() or "no such file" in str(exc).lower():
                raise FileNotFoundError(path) from exc
            msg = f"Failed to get modification time for {path}: {exc}"
            raise OSError(msg) from exc

    async def _mv_file(self, path1: str, path2: str, **kwargs: Any) -> None:
        """Move/rename a file."""
        await self.set_session()
        sandbox = await self._get_sandbox()

        try:
            await asyncio.to_thread(sandbox.fs.move_files, path1, path2)
        except Exception as exc:
            if "not found" in str(exc).lower() or "no such file" in str(exc).lower():
                raise FileNotFoundError(path1) from exc
            msg = f"Failed to move {path1} to {path2}: {exc}"
            raise OSError(msg) from exc

    async def _find(
        self,
        path: str,
        maxdepth: int | None = None,
        withdirs: bool = False,
        **kwargs: Any,
    ) -> list[str]:
        """Find files matching a pattern."""
        await self.set_session()
        sandbox = await self._get_sandbox()

        try:
            # Use Daytona's search functionality
            pattern = kwargs.get("pattern", "*")
            result = await asyncio.to_thread(sandbox.fs.search_files, path, pattern)
        except Exception as exc:
            msg = f"Failed to find files in {path}: {exc}"
            raise OSError(msg) from exc
        else:
            return result.files

    async def _grep(self, path: str, pattern: str, **kwargs: Any) -> list[dict[str, Any]]:
        """Search for pattern in files."""
        await self.set_session()
        sandbox = await self._get_sandbox()

        try:
            matches = await asyncio.to_thread(sandbox.fs.find_files, path, pattern)
            result = [
                {
                    "file": match.file,
                    "line": match.line,
                    "content": match.content,
                }
                for match in matches
            ]

        except Exception as exc:
            msg = f"Failed to grep pattern {pattern!r} in {path}: {exc}"
            raise OSError(msg) from exc
        else:
            return result

    async def _chmod(self, path: str, mode: int, **kwargs: Any) -> None:
        """Change file permissions."""
        await self.set_session()
        sandbox = await self._get_sandbox()

        try:
            # Convert integer mode to octal string
            mode_str = oct(mode)[2:]  # Remove '0o' prefix
            await asyncio.to_thread(sandbox.fs.set_file_permissions, path, mode=mode_str)
        except Exception as exc:
            if "not found" in str(exc).lower() or "no such file" in str(exc).lower():
                raise FileNotFoundError(path) from exc
            msg = f"Failed to change permissions for {path}: {exc}"
            raise OSError(msg) from exc

    # Sync wrappers for async methods
    ls = sync_wrapper(_ls)
    cat_file = sync_wrapper(_cat_file)  # pyright: ignore[reportAssignmentType]
    put_file = sync_wrapper(_put_file)
    pipe_file = sync_wrapper(_pipe_file)
    mkdir = sync_wrapper(_mkdir)
    rm_file = sync_wrapper(_rm_file)
    rmdir = sync_wrapper(_rmdir)
    exists = sync_wrapper(_exists)  # pyright: ignore[reportAssignmentType]
    isfile = sync_wrapper(_isfile)
    isdir = sync_wrapper(_isdir)
    size = sync_wrapper(_size)
    modified = sync_wrapper(_modified)
    mv_file = sync_wrapper(_mv_file)
    find = sync_wrapper(_find)  # pyright: ignore[reportAssignmentType]
    grep = sync_wrapper(_grep)
    chmod = sync_wrapper(_chmod)


class DaytonaFile:
    """File-like object for Daytona files."""

    def __init__(
        self,
        fs: DaytonaFS,
        path: str,
        mode: str = "rb",
        **kwargs: Any,
    ):
        """Initialize Daytona file object.

        Args:
            fs: Daytona filesystem instance
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
