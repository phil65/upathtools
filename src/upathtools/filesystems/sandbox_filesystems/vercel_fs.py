"""Vercel async filesystem implementation for upathtools."""

from __future__ import annotations

import logging
from typing import TYPE_CHECKING, Any, Literal, Required, overload

from upathtools.async_helpers import sync_wrapper
from upathtools.filesystems.base import BaseAsyncFileSystem, BaseUPath, FileInfo


if TYPE_CHECKING:
    from vercel.sandbox import AsyncSandbox, Source


class VercelInfo(FileInfo, total=False):
    """Info dict for Vercel filesystem paths."""

    size: Required[int]


logger = logging.getLogger(__name__)


class VercelPath(BaseUPath[VercelInfo]):
    """Vercel-specific UPath implementation."""

    __slots__ = ()


class VercelFS(BaseAsyncFileSystem[VercelPath, VercelInfo]):
    """Async filesystem for Vercel sandbox environments.

    This filesystem provides access to files within a Vercel sandbox environment,
    allowing you to read, write, and manipulate files remotely through the
    Vercel native filesystem interface.
    """

    upath_cls = VercelPath
    protocol = "vercel"
    root_marker = "/"
    cachable = False  # Disable fsspec caching to prevent instance sharing

    def __init__(
        self,
        sandbox_id: str | None = None,
        source: Source | None = None,
        ports: list[int] | None = None,
        timeout: int | None = None,
        resources: dict[str, Any] | None = None,
        runtime: str | None = None,
        token: str | None = None,
        project_id: str | None = None,
        team_id: str | None = None,
        **kwargs: Any,
    ) -> None:
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

    @staticmethod
    def _get_kwargs_from_urls(path: str) -> dict[str, Any]:
        path = path.removeprefix("vercel://")
        return {"sandbox_id": path}

    async def _get_sandbox(self) -> AsyncSandbox:
        """Get or create sandbox instance."""
        from vercel.sandbox import AsyncSandbox

        if self._sandbox is not None:
            return self._sandbox
        if self.sandbox_id:  # Connect to existing sandbox
            self._sandbox = await AsyncSandbox.get(
                sandbox_id=self.sandbox_id,
                token=self.token,
                project_id=self.project_id,
                team_id=self.team_id,
            )
        else:  # Create new sandbox
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

    @overload
    async def _ls(
        self, path: str, detail: Literal[True] = ..., **kwargs: Any
    ) -> list[VercelInfo]: ...

    @overload
    async def _ls(self, path: str, detail: Literal[False], **kwargs: Any) -> list[str]: ...

    async def _ls(
        self,
        path: str,
        detail: bool = True,
        **kwargs: Any,
    ) -> list[str] | list[VercelInfo]:
        """List directory contents."""
        sandbox = await self._get_sandbox()
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
            if name in (".", ".."):  # Skip . and .. entries
                continue
            is_dir = permissions.startswith("d")
            full_path = f"{path.rstrip('/')}/{name}" if path != "/" else f"/{name}"
            size = 0 if is_dir else int(parts[4]) if parts[4].isdigit() else 0
            info = VercelInfo(name=full_path, size=size, type="directory" if is_dir else "file")
            files.append(info)
        return files if detail else [f["name"] for f in files]  # type: ignore[misc]

    async def _cat_file(
        self,
        path: str,
        start: int | None = None,
        end: int | None = None,
        **kwargs: Any,
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

    async def _pipe_file(self, path: str, value: bytes, **kwargs: Any) -> None:
        """Write file contents."""
        from vercel.sandbox.models import WriteFile

        sandbox = await self._get_sandbox()
        parent = path.rsplit("/", 1)[0]  # Create parent directories if needed
        if parent and parent != path:
            await sandbox.mk_dir(parent)
        await sandbox.write_files([WriteFile(path=path, content=value)])

    async def _mkdir(self, path: str, create_parents: bool = True, **kwargs: Any) -> None:
        """Create directory."""
        sandbox = await self._get_sandbox()
        await sandbox.mk_dir(path)

    async def _rm_file(self, path: str, **kwargs: Any) -> None:
        """Remove file."""
        sandbox = await self._get_sandbox()
        result = await sandbox.run_command("rm", ["-f", path])
        if result.exit_code != 0:
            msg = f"Failed to remove file {path}: {result.stderr}"
            raise OSError(msg)

    async def _rmdir(self, path: str, **kwargs: Any) -> None:
        """Remove directory."""
        sandbox = await self._get_sandbox()
        result = await sandbox.run_command("rmdir", [path])
        if result.exit_code != 0:
            msg = f"Failed to remove directory {path}: {result.stderr}"
            raise OSError(msg)

    async def _exists(self, path: str, **kwargs: Any) -> bool:
        """Check if path exists."""
        sandbox = await self._get_sandbox()
        result = await sandbox.run_command("test", ["-e", path])
        return result.exit_code == 0

    async def _isfile(self, path: str, **kwargs: Any) -> bool:
        """Check if path is a file."""
        sandbox = await self._get_sandbox()
        result = await sandbox.run_command("test", ["-f", path])
        return result.exit_code == 0

    async def _isdir(self, path: str, **kwargs: Any) -> bool:
        """Check if path is a directory."""
        sandbox = await self._get_sandbox()
        result = await sandbox.run_command("test", ["-d", path])
        return result.exit_code == 0

    async def _size(self, path: str, **kwargs: Any) -> int:
        """Get file size."""
        sandbox = await self._get_sandbox()
        result = await sandbox.run_command("stat", ["-c", "%s", path])
        if result.exit_code != 0:
            msg = f"File not found: {path}"
            raise FileNotFoundError(msg)
        stdout_str = await result.stdout() or "0"
        return int(stdout_str.strip())

    async def _modified(self, path: str, **kwargs: Any) -> float:
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

    def info(self, path: str, **kwargs: Any) -> VercelInfo:
        """Get file info (sync wrapper)."""
        return VercelInfo(
            name=path,
            size=self.size(path) if self.isfile(path) else 0,  # pyright: ignore[reportArgumentType]
            type="directory" if self.isdir(path) else "file",
        )
