"""Beam async filesystem implementation for upathtools."""

from __future__ import annotations

import asyncio
import contextlib
import logging
import os
import tempfile
from typing import TYPE_CHECKING, Any, Literal, Required, overload

from upathtools.async_helpers import sync_wrapper
from upathtools.filesystems.base import BaseAsyncFileSystem, BaseUPath, FileInfo, GrepMatch


if TYPE_CHECKING:
    from beta9.type import GpuTypeAlias

    from upathtools.filesystems.base import CreationMode


logger = logging.getLogger(__name__)


class BeamInfo(FileInfo, total=False):
    """Info dict for Beam filesystem paths."""

    size: Required[int]
    mtime: float


class BeamPath(BaseUPath[BeamInfo]):
    """Beam-specific UPath implementation."""

    __slots__ = ()


class BeamFS(BaseAsyncFileSystem[BeamPath, BeamInfo]):
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
    ) -> None:
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
        self._gpu: GpuTypeAlias | list[GpuTypeAlias] | None = gpu
        self._gpu_count = gpu_count
        self._image = image
        self._keep_warm_seconds = keep_warm_seconds
        self._timeout = timeout
        self.env_variables = env_variables
        self._sandbox_instance = None
        self._session_started = False

    @staticmethod
    def _get_kwargs_from_urls(path: str) -> dict[str, Any]:
        path = path.removeprefix("beam://")
        return {"sandbox_id": path}

    async def _get_sandbox(self):
        """Get or create Beam sandbox instance."""
        from beta9 import GpuType, Image, PythonVersion, Sandbox

        if self._sandbox_instance is not None:
            return self._sandbox_instance

        if self._image is None:  # Set default image if none provided
            self._image = Image(python_version=PythonVersion.Python311)
        if self._sandbox_id:  # Connect to existing sandbox
            sandbox = Sandbox()
            self._sandbox_instance = sandbox.connect(self._sandbox_id)
        else:  # Create new sandbox
            sandbox = Sandbox(
                cpu=self._cpu,
                memory=self._memory,
                gpu=self._gpu or GpuType.NoGPU,
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

    @overload
    async def _ls(
        self, path: str, detail: Literal[True] = True, **kwargs: Any
    ) -> list[BeamInfo]: ...

    @overload
    async def _ls(self, path: str, detail: Literal[False] = False, **kwargs: Any) -> list[str]: ...

    async def _ls(
        self, path: str, detail: bool = True, **kwargs: Any
    ) -> list[BeamInfo] | list[str]:
        """List directory contents with caching."""
        await self.set_session()
        sandbox = await self._get_sandbox()

        try:
            items = await asyncio.to_thread(sandbox.fs.list_files, path)
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
            BeamInfo(
                name=item.name,
                size=item.size,
                type="directory" if item.is_dir else "file",
                mtime=item.mod_time if hasattr(item, "mod_time") else 0,
            )
            for item in items
        ]

    async def _cat_file(
        self, path: str, start: int | None = None, end: int | None = None, **kwargs: Any
    ) -> bytes:
        """Read file contents."""
        await self.set_session()
        sandbox = await self._get_sandbox()

        try:
            # Create temporary file for download
            with tempfile.NamedTemporaryFile() as tmp_file:
                await asyncio.to_thread(sandbox.fs.download_file, path, tmp_file.name)

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

    async def _put_file(self, lpath: str, rpath: str, callback=None, **kwargs: Any) -> None:
        """Upload a local file to the sandbox."""
        await self.set_session()
        sandbox = await self._get_sandbox()

        try:
            await asyncio.to_thread(sandbox.fs.upload_file, lpath, rpath)
        except Exception as exc:
            msg = f"Failed to upload file {lpath} to {rpath}: {exc}"
            raise OSError(msg) from exc

    async def _pipe_file(
        self, path: str, value: bytes, mode: CreationMode = "overwrite", **kwargs: Any
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
                await asyncio.to_thread(sandbox.fs.upload_file, tmp_file.name, path)

        except Exception as exc:
            msg = f"Failed to write file {path}: {exc}"
            raise OSError(msg) from exc
        finally:
            # Clean up temporary file
            with contextlib.suppress(OSError, UnboundLocalError):
                os.unlink(tmp_file.name)  # noqa: PTH108

    async def _mkdir(self, path: str, create_parents: bool = True, **kwargs: Any) -> None:
        """Create a directory."""
        await self.set_session()
        sandbox = await self._get_sandbox()

        try:
            await asyncio.to_thread(sandbox.fs.create_directory, path)
        except Exception as exc:
            from beta9.exceptions import SandboxFileSystemError

            # Try to create parent directories if needed
            if (
                isinstance(exc, SandboxFileSystemError)
                and create_parents
                and "parent" in str(exc).lower()
            ):
                parent = os.path.dirname(path)  # noqa: PTH120
                if parent and parent not in (path, "/"):
                    await self._mkdir(parent, create_parents=True)
                    await asyncio.to_thread(sandbox.fs.create_directory, path)
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
            await asyncio.to_thread(sandbox.fs.delete_directory, path)
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
            await asyncio.to_thread(sandbox.fs.stat_file, path)
        except Exception:  # noqa: BLE001
            return False
        else:
            return True

    async def _isfile(self, path: str, **kwargs: Any) -> bool:
        """Check if path is a file."""
        await self.set_session()
        sandbox = await self._get_sandbox()

        try:
            info = await asyncio.to_thread(sandbox.fs.stat_file, path)
        except Exception:  # noqa: BLE001
            return False
        else:
            return not info.is_dir

    async def _isdir(self, path: str, **kwargs: Any) -> bool:
        """Check if path is a directory."""
        await self.set_session()
        sandbox = await self._get_sandbox()

        try:
            info = await asyncio.to_thread(sandbox.fs.stat_file, path)
        except Exception:  # noqa: BLE001
            return False
        else:
            return info.is_dir

    async def _size(self, path: str, **kwargs: Any) -> int:
        """Get file size."""
        await self.set_session()
        sandbox = await self._get_sandbox()

        try:
            info = await asyncio.to_thread(sandbox.fs.stat_file, path)
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
            info = await asyncio.to_thread(sandbox.fs.stat_file, path)
            return float(info.mod_time) if hasattr(info, "mod_time") and info.mod_time else 0.0
        except Exception as exc:
            from beta9.exceptions import SandboxFileSystemError

            if isinstance(exc, SandboxFileSystemError) and (
                "not found" in str(exc).lower() or "no such file" in str(exc).lower()
            ):
                raise FileNotFoundError(path) from exc
            msg = f"Failed to get modification time for {path}: {exc}"
            raise OSError(msg) from exc

    async def _info(self, path: str, **kwargs: Any) -> BeamInfo:
        """Get info about a file or directory."""
        await self.set_session()
        sandbox = await self._get_sandbox()

        try:
            stat = await asyncio.to_thread(sandbox.fs.stat_file, path)
            return BeamInfo(
                name=path,
                size=stat.size,
                type="directory" if stat.is_dir else "file",
                mtime=float(stat.mod_time) if hasattr(stat, "mod_time") and stat.mod_time else 0.0,
            )
        except Exception as exc:
            from beta9.exceptions import SandboxFileSystemError

            if isinstance(exc, SandboxFileSystemError) and (
                "not found" in str(exc).lower() or "no such file" in str(exc).lower()
            ):
                raise FileNotFoundError(path) from exc
            msg = f"Failed to get info for {path}: {exc}"
            raise OSError(msg) from exc

    async def _grep(
        self,
        path: str,
        pattern: str,
        *,
        max_count: int | None = None,
        case_sensitive: bool | None = None,
        hidden: bool = False,
        no_ignore: bool = False,
        globs: list[str] | None = None,
        context_before: int | None = None,
        context_after: int | None = None,
        multiline: bool = False,
    ) -> list[GrepMatch]:
        """Search for pattern in files using Beam's find_in_files."""
        await self.set_session()
        sandbox = await self._get_sandbox()

        try:
            results = await asyncio.to_thread(sandbox.fs.find_in_files, path, pattern)
        except Exception as exc:
            msg = f"Failed to grep pattern {pattern!r} in {path}: {exc}"
            raise OSError(msg) from exc
        else:
            matches = [
                GrepMatch(
                    path=result.path,
                    line_number=match.range.start.line,
                    text=match.content,
                )
                for result in results
                for match in result.matches
            ]
            if max_count is not None:
                return matches[:max_count]
            return matches

    # TODO: Add _find and _glob using sandbox.process.exec() with Linux CLI tools (find)
    # This would be faster than fsspec's default _walk which does multiple _ls round trips.

    # Sync wrappers for async methods
    ls = sync_wrapper(_ls)  # pyright: ignore[reportAssignmentType]
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
    grep = sync_wrapper(_grep)
