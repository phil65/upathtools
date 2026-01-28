"""Microsandbox async filesystem implementation for upathtools."""

from __future__ import annotations

import logging
from typing import TYPE_CHECKING, Any, Literal, Required, overload

from upathtools.async_helpers import sync_wrapper
from upathtools.filesystems.base import BaseAsyncFileSystem, BaseUPath, FileInfo


if TYPE_CHECKING:
    from microsandbox import BaseSandbox


class MicrosandboxInfo(FileInfo, total=False):
    """Info dict for Microsandbox filesystem paths."""

    size: Required[int]


logger = logging.getLogger(__name__)


class MicrosandboxPath(BaseUPath[MicrosandboxInfo]):
    """Microsandbox-specific UPath implementation."""

    __slots__ = ()


class MicrosandboxFS(BaseAsyncFileSystem[MicrosandboxPath, MicrosandboxInfo]):
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
    ) -> None:
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

    @overload
    async def _ls(
        self,
        path: str,
        detail: Literal[True] = ...,
        **kwargs: Any,
    ) -> list[MicrosandboxInfo]: ...

    @overload
    async def _ls(self, path: str, detail: Literal[False], **kwargs: Any) -> list[str]: ...

    async def _ls(
        self,
        path: str,
        detail: bool = True,
        **kwargs: Any,
    ) -> list[str] | list[MicrosandboxInfo]:
        """List directory contents."""
        sandbox = await self._get_sandbox()
        # Use ls -la to get detailed directory listing
        result = await sandbox.command.run("ls", ["-la", path])
        output = await result.output()
        stderr = await result.error()
        if result.exit_code != 0:
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
            if name in (".", ".."):
                continue
            is_dir = permissions.startswith("d")
            full_path = f"{path.rstrip('/')}/{name}" if path != "/" else f"/{name}"

            files.append(
                MicrosandboxInfo(
                    name=full_path,
                    size=0 if is_dir else int(parts[4]) if parts[4].isdigit() else 0,
                    type="directory" if is_dir else "file",
                )
            )
        return files if detail else [f["name"] for f in files]  # type: ignore[misc]

    async def _cat_file(
        self,
        path: str,
        start: int | None = None,
        end: int | None = None,
        **kwargs: Any,
    ) -> bytes:
        """Read file contents using cat command."""
        sandbox = await self._get_sandbox()
        result = await sandbox.command.run("cat", [path])
        stdout = await result.output()
        stderr = await result.error()
        if result.exit_code != 0:
            if "No such file or directory" in stderr:
                msg = f"File not found: {path}"
                raise FileNotFoundError(msg)
            msg = f"Failed to read file {path}: {stderr}"
            raise OSError(msg)

        content = stdout.encode("utf-8")
        if start is not None or end is not None:
            return content[start:end]
        return content

    async def _pipe_file(self, path: str, value: bytes, **kwargs: Any) -> None:
        """Write file contents using shell redirection."""
        sandbox = await self._get_sandbox()
        parent = path.rsplit("/", 1)[0]
        if parent and parent != path:
            await self._mkdir(parent, create_parents=True)
        # Use echo with redirection to write content
        # Use printf to handle special characters and newlines properly
        result = await sandbox.command.run(
            "sh", ["-c", f"printf '%s' '{value.decode('utf-8')}' > '{path}'"]
        )
        stderr = await result.error()
        if result.exit_code != 0:
            msg = f"Failed to write file {path}: {stderr}"
            raise OSError(msg)

    async def _mkdir(self, path: str, create_parents: bool = True, **kwargs: Any) -> None:
        """Create directory using mkdir command."""
        sandbox = await self._get_sandbox()

        args = [path]
        if create_parents:
            args = ["-p", *args]

        result = await sandbox.command.run("mkdir", args)
        stderr = await result.error()
        if result.exit_code != 0:
            msg = f"Failed to create directory {path}: {stderr}"
            raise OSError(msg)

    async def _rm_file(self, path: str, **kwargs: Any) -> None:
        """Remove file using rm command."""
        sandbox = await self._get_sandbox()
        result = await sandbox.command.run("rm", ["-f", path])
        stderr = await result.error()
        if result.exit_code != 0:
            msg = f"Failed to remove file {path}: {stderr}"
            raise OSError(msg)

    async def _rmdir(self, path: str, **kwargs: Any) -> None:
        """Remove directory using rmdir command."""
        sandbox = await self._get_sandbox()
        result = await sandbox.command.run("rmdir", [path])
        stderr = await result.error()
        if result.exit_code != 0:
            msg = f"Failed to remove directory {path}: {stderr}"
            raise OSError(msg)

    async def _exists(self, path: str, **kwargs: Any) -> bool:
        """Check if path exists using test command."""
        sandbox = await self._get_sandbox()
        result = await sandbox.command.run("test", ["-e", path])
        return result.exit_code == 0

    async def _isfile(self, path: str, **kwargs: Any) -> bool:
        """Check if path is a file using test command."""
        sandbox = await self._get_sandbox()
        result = await sandbox.command.run("test", ["-f", path])
        return result.exit_code == 0

    async def _isdir(self, path: str, **kwargs: Any) -> bool:
        """Check if path is a directory using test command."""
        sandbox = await self._get_sandbox()
        result = await sandbox.command.run("test", ["-d", path])
        return result.exit_code == 0

    async def _size(self, path: str, **kwargs: Any) -> int:
        """Get file size using stat command."""
        sandbox = await self._get_sandbox()
        result = await sandbox.command.run("stat", ["-c", "%s", path])
        stdout = await result.output()
        stderr = await result.error()
        if result.exit_code != 0:
            if "No such file or directory" in stderr:
                msg = f"File not found: {path}"
                raise FileNotFoundError(msg)
            msg = f"Failed to get size for {path}: {stderr}"
            raise OSError(msg)

        return int(stdout.strip())

    async def _modified(self, path: str, **kwargs: Any) -> float:
        """Get file modification time using stat command."""
        sandbox = await self._get_sandbox()
        result = await sandbox.command.run("stat", ["-c", "%Y", path])
        stdout = await result.output()
        stderr = await result.error()
        if result.exit_code != 0:
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

    def cat(self, path: str, **kwargs: Any) -> bytes:
        """Read file contents (sync wrapper)."""
        result = self.cat_file(path, **kwargs)
        if isinstance(result, str):
            return result.encode("utf-8")
        return result

    def info(self, path: str, **kwargs: Any) -> MicrosandboxInfo:
        """Get file info (sync wrapper)."""
        return MicrosandboxInfo(
            name=path,
            size=self.size(path) if self.isfile(path) else 0,  # pyright: ignore[reportArgumentType]
            type="directory" if self.isdir(path) else "file",
        )
