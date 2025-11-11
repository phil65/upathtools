"""Vercel async filesystem implementation for upathtools."""

from __future__ import annotations

import io
import logging
from typing import TYPE_CHECKING, Any

from fsspec.asyn import AsyncFileSystem, sync_wrapper
from upath import UPath


if TYPE_CHECKING:
    from vercel.sandbox import AsyncSandbox


logger = logging.getLogger(__name__)


class VercelPath(UPath):
    """Vercel-specific UPath implementation."""


class VercelFS(AsyncFileSystem):
    """Async filesystem for Vercel sandbox environments.

    This filesystem provides access to files within a Vercel sandbox environment,
    allowing you to read, write, and manipulate files remotely through the
    Vercel native filesystem interface.
    """

    protocol = "vercel"
    root_marker = "/"
    cachable = False  # Disable fsspec caching to prevent instance sharing

    def __init__(
        self,
        sandbox_id: str | None = None,
        source: dict[str, Any] | None = None,
        ports: list[int] | None = None,
        timeout: int | None = None,
        resources: dict[str, Any] | None = None,
        runtime: str | None = None,
        token: str | None = None,
        project_id: str | None = None,
        team_id: str | None = None,
        **kwargs: Any,
    ):
        """Initialize Vercel filesystem.

        Args:
            sandbox_id: Existing sandbox ID to connect to
            source: Source configuration for new sandbox
            ports: List of ports to expose
            timeout: Sandbox timeout in seconds
            resources: Resource allocation configuration
            runtime: Runtime environment
            token: Vercel API token
            project_id: Vercel project ID
            team_id: Vercel team ID
            **kwargs: Additional filesystem arguments
        """
        super().__init__(**kwargs)
        self.sandbox_id = sandbox_id
        self.source = source
        self.ports = ports
        self.timeout = timeout
        self.resources = resources
        self.runtime = runtime
        self.token = token
        self.project_id = project_id
        self.team_id = team_id
        self._sandbox: AsyncSandbox | None = None

    def _make_path(self, path: str) -> VercelPath:
        return VercelPath(path, protocol="vercel")

    async def _get_sandbox(self) -> AsyncSandbox:
        """Get or create sandbox instance."""
        if self._sandbox is not None:
            return self._sandbox

        # Import here to avoid circular imports
        from vercel.sandbox import AsyncSandbox

        if self.sandbox_id:
            # Connect to existing sandbox
            self._sandbox = await AsyncSandbox.get(
                sandbox_id=self.sandbox_id,
                token=self.token,
                project_id=self.project_id,
                team_id=self.team_id,
            )
        else:
            # Create new sandbox
            self._sandbox = await AsyncSandbox.create(
                source=self.source,
                ports=self.ports,
                timeout=self.timeout,
                resources=self.resources,
                runtime=self.runtime,
                token=self.token,
                project_id=self.project_id,
                team_id=self.team_id,
            )

        logger.info("Connected to Vercel sandbox: %s", self._sandbox.sandbox_id)
        return self._sandbox

    async def close_session(self) -> None:
        """Close sandbox session."""
        if self._sandbox is not None:
            await self._sandbox.stop()
            self._sandbox = None

    async def _ls_real(
        self, path: str = "/", detail: bool = True
    ) -> list[dict[str, Any]]:
        """List directory contents."""
        sandbox = await self._get_sandbox()

        # Use ls command to get directory listing
        result = await sandbox.run_command("ls", ["-la", path], cwd="/")

        if result.exit_code != 0:
            stderr_str = await result.stderr() or ""
            if "No such file or directory" in stderr_str:
                msg = f"Path not found: {path}"
                raise FileNotFoundError(msg)
            msg = f"Failed to list directory {path}: {stderr_str}"
            raise OSError(msg)

        files = []
        stdout_str = await result.stdout() or ""
        for line in stdout_str.strip().split("\n"):
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
        """Read file contents."""
        sandbox = await self._get_sandbox()

        content = await sandbox.read_file(path)
        if content is None:
            msg = f"File not found: {path}"
            raise FileNotFoundError(msg)

        if start is not None or end is not None:
            return content[start:end]
        return content

    async def _pipe_file(self, path: str, value: bytes, **kwargs) -> None:
        """Write file contents."""
        sandbox = await self._get_sandbox()

        # Import WriteFile from the models module based on your provided code
        from vercel.sandbox.models import WriteFile

        # Create parent directories if needed
        parent = path.rsplit("/", 1)[0]
        if parent and parent != path:
            await sandbox.mk_dir(parent)

        # Write file
        files = [WriteFile(path=path, content=value)]
        await sandbox.write_files(files)

    async def _mkdir(self, path: str, create_parents: bool = True, **kwargs) -> None:
        """Create directory."""
        sandbox = await self._get_sandbox()
        await sandbox.mk_dir(path)

    async def _rm_file(self, path: str, **kwargs) -> None:
        """Remove file."""
        sandbox = await self._get_sandbox()
        result = await sandbox.run_command("rm", ["-f", path])
        if result.exit_code != 0:
            msg = f"Failed to remove file {path}: {result.stderr}"
            raise OSError(msg)

    async def _rmdir(self, path: str, **kwargs) -> None:
        """Remove directory."""
        sandbox = await self._get_sandbox()
        result = await sandbox.run_command("rmdir", [path])
        if result.exit_code != 0:
            msg = f"Failed to remove directory {path}: {result.stderr}"
            raise OSError(msg)

    async def _exists(self, path: str, **kwargs) -> bool:
        """Check if path exists."""
        sandbox = await self._get_sandbox()
        result = await sandbox.run_command("test", ["-e", path])
        return result.exit_code == 0

    async def _isfile(self, path: str, **kwargs) -> bool:
        """Check if path is a file."""
        sandbox = await self._get_sandbox()
        result = await sandbox.run_command("test", ["-f", path])
        return result.exit_code == 0

    async def _isdir(self, path: str, **kwargs) -> bool:
        """Check if path is a directory."""
        sandbox = await self._get_sandbox()
        result = await sandbox.run_command("test", ["-d", path])
        return result.exit_code == 0

    async def _size(self, path: str, **kwargs) -> int:
        """Get file size."""
        sandbox = await self._get_sandbox()
        result = await sandbox.run_command("stat", ["-c", "%s", path])
        if result.exit_code != 0:
            msg = f"File not found: {path}"
            raise FileNotFoundError(msg)
        stdout_str = await result.stdout() or "0"
        return int(stdout_str.strip())

    async def _modified(self, path: str, **kwargs) -> float:
        """Get file modification time."""
        sandbox = await self._get_sandbox()
        result = await sandbox.run_command("stat", ["-c", "%Y", path])
        if result.exit_code != 0:
            msg = f"File not found: {path}"
            raise FileNotFoundError(msg)
        stdout_str = await result.stdout() or "0"
        return float(stdout_str.strip())

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


class VercelFile:
    """File-like object for Vercel filesystem operations."""

    def __init__(
        self,
        fs: VercelFS,
        path: str,
        mode: str = "rb",
        **kwargs,
    ):
        """Initialize Vercel file.

        Args:
            fs: Vercel filesystem instance
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
