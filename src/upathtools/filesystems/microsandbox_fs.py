"""Microsandbox async filesystem implementation for upathtools."""

from __future__ import annotations

import io
import logging
from typing import TYPE_CHECKING, Any

from fsspec.asyn import sync_wrapper

from upathtools.filesystems.base import BaseAsyncFileSystem, BaseUPath


if TYPE_CHECKING:
    from microsandbox import BaseSandbox


logger = logging.getLogger(__name__)


class MicrosandboxPath(BaseUPath):
    """Microsandbox-specific UPath implementation."""


class MicrosandboxFS(BaseAsyncFileSystem[MicrosandboxPath]):
    """Async filesystem for Microsandbox environments.

    This filesystem provides access to files within a Microsandbox environment,
    allowing you to read, write, and manipulate files remotely through the
    Microsandbox CLI interface.
    """

    protocol = "microsandbox"
    upath_cls = MicrosandboxPath
    root_marker = "/"
    cachable = False  # Disable fsspec caching to prevent instance sharing

    def __init__(
        self,
        sandbox: BaseSandbox | None = None,
        server_url: str | None = None,
        namespace: str = "default",
        name: str | None = None,
        api_key: str | None = None,
        image: str | None = None,
        memory: int = 512,
        cpus: float = 1.0,
        **kwargs: Any,
    ):
        """Initialize Microsandbox filesystem.

        Args:
            sandbox: Existing sandbox instance to use
            server_url: Microsandbox server URL
            namespace: Sandbox namespace
            name: Sandbox name
            api_key: API key for authentication
            image: Docker image to use
            memory: Memory limit in MB
            cpus: CPU limit
            **kwargs: Additional filesystem arguments
        """
        super().__init__(**kwargs)
        self._sandbox = sandbox
        self.server_url = server_url
        self.namespace = namespace
        self.name = name
        self.api_key = api_key
        self.image = image
        self.memory = memory
        self.cpus = cpus

    async def _get_sandbox(self) -> BaseSandbox:
        """Get sandbox instance."""
        if self._sandbox is None:
            msg = "No sandbox provided and cannot create one without sandbox class"
            raise RuntimeError(msg)

        if not self._sandbox._is_started:
            await self._sandbox.start(
                image=self.image,
                memory=self.memory,
                cpus=self.cpus,
            )
            logger.info("Started Microsandbox: %s", self._sandbox._name)

        return self._sandbox

    async def close_session(self) -> None:
        """Close sandbox session."""
        if self._sandbox is not None and self._sandbox._is_started:
            await self._sandbox.stop()
            logger.info("Stopped Microsandbox: %s", self._sandbox._name)

    async def _ls_real(
        self, path: str = "/", detail: bool = True
    ) -> list[dict[str, Any]]:
        """List directory contents using ls command."""
        sandbox = await self._get_sandbox()

        # Use ls -la to get detailed directory listing
        result = await sandbox.command.run("ls", ["-la", path])

        output = result.output_data.get("stdout", "")
        stderr = result.output_data.get("stderr", "")
        exit_code = result.output_data.get("exit_code", 0)

        if exit_code != 0:
            if "No such file or directory" in stderr:
                msg = f"Path not found: {path}"
                raise FileNotFoundError(msg)
            msg = f"Failed to list directory {path}: {stderr}"
            raise OSError(msg)

        files = []
        for line in output.strip().split("\n"):
            if not line or line.startswith("total"):
                continue

            parts = line.split()
            min_parts = 9
            if len(parts) < min_parts:
                continue

            permissions = parts[0]
            name = parts[-1]

            # Skip . and .. entries
            if name in (".", ".."):
                continue

            is_dir = permissions.startswith("d")
            full_path = f"{path.rstrip('/')}/{name}" if path != "/" else f"/{name}"

            files.append({
                "name": full_path,
                "size": 0 if is_dir else int(parts[4]) if parts[4].isdigit() else 0,
                "type": "directory" if is_dir else "file",
                "isdir": is_dir,
            })

        return files

    async def _ls(
        self, path: str = "/", detail: bool = True, **kwargs
    ) -> list[str] | list[dict[str, Any]]:
        """List directory contents."""
        files = await self._ls_real(path, detail=detail)
        return files if detail else [f["name"] for f in files]

    async def _cat_file(
        self, path: str, start: int | None = None, end: int | None = None, **kwargs
    ) -> bytes:
        """Read file contents using cat command."""
        sandbox = await self._get_sandbox()

        result = await sandbox.command.run("cat", [path])

        stdout = result.output_data.get("stdout", "")
        stderr = result.output_data.get("stderr", "")
        exit_code = result.output_data.get("exit_code", 0)

        if exit_code != 0:
            if "No such file or directory" in stderr:
                msg = f"File not found: {path}"
                raise FileNotFoundError(msg)
            msg = f"Failed to read file {path}: {stderr}"
            raise OSError(msg)

        content = stdout.encode("utf-8")

        if start is not None or end is not None:
            return content[start:end]
        return content

    async def _pipe_file(self, path: str, value: bytes, **kwargs) -> None:
        """Write file contents using shell redirection."""
        sandbox = await self._get_sandbox()

        # Create parent directories if needed
        parent = path.rsplit("/", 1)[0]
        if parent and parent != path:
            await self._mkdir(parent, create_parents=True)

        # Use echo with redirection to write content
        content_str = value.decode("utf-8")
        # Use printf to handle special characters and newlines properly
        result = await sandbox.command.run(
            "sh", ["-c", f"printf '%s' '{content_str}' > '{path}'"]
        )

        exit_code = result.output_data.get("exit_code", 0)
        stderr = result.output_data.get("stderr", "")

        if exit_code != 0:
            msg = f"Failed to write file {path}: {stderr}"
            raise OSError(msg)

    async def _mkdir(self, path: str, create_parents: bool = True, **kwargs) -> None:
        """Create directory using mkdir command."""
        sandbox = await self._get_sandbox()

        args = [path]
        if create_parents:
            args = ["-p", *args]

        result = await sandbox.command.run("mkdir", args)

        exit_code = result.output_data.get("exit_code", 0)
        stderr = result.output_data.get("stderr", "")

        if exit_code != 0:
            msg = f"Failed to create directory {path}: {stderr}"
            raise OSError(msg)

    async def _rm_file(self, path: str, **kwargs) -> None:
        """Remove file using rm command."""
        sandbox = await self._get_sandbox()

        result = await sandbox.command.run("rm", ["-f", path])

        exit_code = result.output_data.get("exit_code", 0)
        stderr = result.output_data.get("stderr", "")

        if exit_code != 0:
            msg = f"Failed to remove file {path}: {stderr}"
            raise OSError(msg)

    async def _rmdir(self, path: str, **kwargs) -> None:
        """Remove directory using rmdir command."""
        sandbox = await self._get_sandbox()

        result = await sandbox.command.run("rmdir", [path])

        exit_code = result.output_data.get("exit_code", 0)
        stderr = result.output_data.get("stderr", "")

        if exit_code != 0:
            msg = f"Failed to remove directory {path}: {stderr}"
            raise OSError(msg)

    async def _exists(self, path: str, **kwargs) -> bool:
        """Check if path exists using test command."""
        sandbox = await self._get_sandbox()

        result = await sandbox.command.run("test", ["-e", path])
        return result.output_data.get("exit_code", 1) == 0

    async def _isfile(self, path: str, **kwargs) -> bool:
        """Check if path is a file using test command."""
        sandbox = await self._get_sandbox()

        result = await sandbox.command.run("test", ["-f", path])
        return result.output_data.get("exit_code", 1) == 0

    async def _isdir(self, path: str, **kwargs) -> bool:
        """Check if path is a directory using test command."""
        sandbox = await self._get_sandbox()

        result = await sandbox.command.run("test", ["-d", path])
        return result.output_data.get("exit_code", 1) == 0

    async def _size(self, path: str, **kwargs) -> int:
        """Get file size using stat command."""
        sandbox = await self._get_sandbox()

        result = await sandbox.command.run("stat", ["-c", "%s", path])

        stdout = result.output_data.get("stdout", "0")
        stderr = result.output_data.get("stderr", "")
        exit_code = result.output_data.get("exit_code", 0)

        if exit_code != 0:
            if "No such file or directory" in stderr:
                msg = f"File not found: {path}"
                raise FileNotFoundError(msg)
            msg = f"Failed to get size for {path}: {stderr}"
            raise OSError(msg)

        return int(stdout.strip())

    async def _modified(self, path: str, **kwargs) -> float:
        """Get file modification time using stat command."""
        sandbox = await self._get_sandbox()

        result = await sandbox.command.run("stat", ["-c", "%Y", path])

        stdout = result.output_data.get("stdout", "0")
        stderr = result.output_data.get("stderr", "")
        exit_code = result.output_data.get("exit_code", 0)

        if exit_code != 0:
            if "No such file or directory" in stderr:
                msg = f"File not found: {path}"
                raise FileNotFoundError(msg)
            msg = f"Failed to get modification time for {path}: {stderr}"
            raise OSError(msg)

        return float(stdout.strip())

    # Sync wrapper methods
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

    def cat(self, path: str, **kwargs) -> bytes:
        """Read file contents (sync wrapper)."""
        result = self.cat_file(path, **kwargs)
        if isinstance(result, str):
            return result.encode("utf-8")
        return result

    def info(self, path: str, **kwargs) -> dict[str, Any]:
        """Get file info (sync wrapper)."""
        return {
            "name": path,
            "size": self.size(path) if self.isfile(path) else 0,
            "type": "directory" if self.isdir(path) else "file",
        }


class MicrosandboxFile:
    """File-like object for Microsandbox filesystem operations."""

    def __init__(
        self,
        fs: MicrosandboxFS,
        path: str,
        mode: str = "rb",
        **kwargs,
    ):
        """Initialize Microsandbox file.

        Args:
            fs: Microsandbox filesystem instance
            path: File path
            mode: File open mode
            **kwargs: Additional file arguments
        """
        self.fs = fs
        self.path = path
        self.mode = mode
        self._buffer = io.BytesIO()
        self._pos = 0
        self._closed = False
        self._loaded = False

    async def _ensure_loaded(self) -> None:
        """Load file content if needed."""
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
        return self._buffer.tell()

    def seek(self, pos: int, whence: int = 0) -> int:
        """Seek to position."""
        return self._buffer.seek(pos, whence)

    async def read(self, size: int = -1) -> bytes:
        """Read from file."""
        if self._closed:
            msg = "I/O operation on closed file"
            raise ValueError(msg)

        await self._ensure_loaded()
        return self._buffer.read(size)

    async def write(self, data: bytes) -> int:
        """Write to file."""
        if self._closed:
            msg = "I/O operation on closed file"
            raise ValueError(msg)

        if not self.writable():
            msg = "File not open for writing"
            raise OSError(msg)

        return self._buffer.write(data)

    async def flush(self) -> None:
        """Flush buffer to remote file."""
        if self.writable() and not self._closed:
            content = self._buffer.getvalue()
            await self.fs._pipe_file(self.path, content)

    async def close(self) -> None:
        """Close file and flush if needed."""
        if not self._closed:
            await self.flush()
            self._buffer.close()
            self._closed = True

    async def __aenter__(self):
        """Enter async context."""
        return self

    async def __aexit__(self, exc_type, exc_val, exc_tb) -> None:
        """Exit async context."""
        await self.close()
