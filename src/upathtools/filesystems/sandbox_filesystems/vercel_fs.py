"""Vercel async filesystem implementation for upathtools."""

from __future__ import annotations

from contextlib import contextmanager
import logging
from typing import TYPE_CHECKING, Any, Literal, Required, overload

from upathtools.async_helpers import sync_wrapper
from upathtools.filesystems.base import BaseAsyncFileSystem, BaseUPath, FileInfo


if TYPE_CHECKING:
    from collections.abc import Generator

    from vercel.sandbox import (
        GitSource,
        Sandbox,
        SandboxClient,
        SandboxCredentials,
        SnapshotSource,
        TarballSource,
    )

    from upathtools.filesystems.base import CreationMode


logger = logging.getLogger(__name__)

# `find -printf` / `stat -c` field separator (cannot appear in the numeric fields)
_SEP = "\t"
_FIND_FORMAT = f"%p{_SEP}%s{_SEP}%T@{_SEP}%y\\n"
_STAT_FORMAT = f"%s{_SEP}%Y{_SEP}%F"


class VercelInfo(FileInfo, total=False):
    """Info dict for Vercel filesystem paths."""

    size: Required[int]
    mtime: Required[float]


class VercelPath(BaseUPath[VercelInfo]):
    """Vercel-specific UPath implementation."""

    __slots__ = ()


def _find_type(type_char: str) -> Literal["file", "directory", "link", "other"]:
    """Map a `find -printf %y` type character to a FileInfo type."""
    match type_char:
        case "f":
            return "file"
        case "d":
            return "directory"
        case "l":
            return "link"
        case _:
            return "other"


def _stat_type(description: str) -> Literal["file", "directory", "link", "other"]:
    """Map a `stat -c %F` file type description to a FileInfo type."""
    match description:
        case "regular file" | "regular empty file":
            return "file"
        case "directory":
            return "directory"
        case "symbolic link":
            return "link"
        case _:
            return "other"


@contextmanager
def _translate_errors(path: str, operation: str) -> Generator[None]:
    """Map Vercel sandbox filesystem errors to standard Python exceptions."""
    from vercel.sandbox import SandboxFilesystemError, SandboxPathNotFoundError

    try:
        yield
    except SandboxPathNotFoundError as exc:
        raise FileNotFoundError(path) from exc
    except SandboxFilesystemError as exc:
        raise OSError(f"Failed to {operation} {path}: {exc}") from exc


class VercelFS(BaseAsyncFileSystem[VercelPath, VercelInfo]):
    """Async filesystem for Vercel sandbox environments.

    This filesystem provides access to files within a Vercel sandbox environment,
    allowing you to read, write, and manipulate files remotely through the
    Vercel native filesystem interface.

    Authentication uses `token`, `project_id` and `team_id` when all three are given.
    Otherwise the Vercel SDK resolves credentials itself (OIDC token, or the
    `VERCEL_TOKEN`, `VERCEL_PROJECT_ID` and `VERCEL_TEAM_ID` environment variables).
    """

    upath_cls = VercelPath
    protocol = "vercel"
    root_marker = "/"
    cachable = False  # Disable fsspec caching to prevent instance sharing

    def __init__(
        self,
        sandbox_name: str | None = None,
        image: str | None = None,
        source: GitSource | TarballSource | SnapshotSource | None = None,
        ports: list[int] | None = None,
        timeout: float | None = None,
        vcpus: int | None = None,
        memory: int | None = None,
        env: dict[str, str] | None = None,
        token: str | None = None,
        project_id: str | None = None,
        team_id: str | None = None,
        **kwargs: Any,
    ) -> None:
        """Initialize Vercel filesystem.

        Args:
            sandbox_name: Name of the sandbox to connect to (created if it doesn't exist).
                          A new, unnamed sandbox is created when not given.
            image: Base image for new sandboxes
            source: Source (git repository, tarball or snapshot) for new sandboxes
            ports: Ports to expose on new sandboxes
            timeout: Maximum sandbox lifetime in seconds
            vcpus: Number of virtual CPUs for new sandboxes
            memory: Memory in MB for new sandboxes
            env: Environment variables for new sandboxes
            token: Vercel API token
            project_id: Vercel project ID
            team_id: Vercel team ID
            **kwargs: Additional filesystem arguments
        """
        super().__init__(**kwargs)
        explicit_credentials = (token, project_id, team_id)
        if any(explicit_credentials) and not all(explicit_credentials):
            msg = "token, project_id and team_id must be provided together"
            raise ValueError(msg)
        self._sandbox_name = sandbox_name
        self._image = image
        self._source = source
        self._ports = ports
        self._timeout = timeout
        self._vcpus = vcpus
        self._memory = memory
        self._env = env
        self._token = token
        self._project_id = project_id
        self._team_id = team_id
        self._client: SandboxClient | None = None
        self._sandbox: Sandbox | None = None
        self._session_started = False

    @staticmethod
    def _get_kwargs_from_urls(path: str) -> dict[str, Any]:
        path = path.removeprefix("vercel://")
        return {"sandbox_name": path}

    def _get_client(self) -> SandboxClient:
        """Get or create the Vercel sandbox client."""
        from vercel.sandbox import SandboxClient, SandboxServiceOptions

        if self._client is not None:
            return self._client
        if self._token and self._project_id and self._team_id:
            token, project_id, team_id = self._token, self._project_id, self._team_id

            async def credentials_factory() -> SandboxCredentials:
                from vercel.sandbox import SandboxCredentials

                return SandboxCredentials(token=token, project_id=project_id, team_id=team_id)

            options = SandboxServiceOptions(credentials_factory=credentials_factory)
            self._client = SandboxClient.create(options=options)
        else:
            self._client = SandboxClient.create()
        return self._client

    async def _get_sandbox(self) -> Sandbox:
        """Get or create the Vercel sandbox instance."""
        from vercel.sandbox import SandboxResources

        if self._sandbox is not None:
            return self._sandbox
        client = self._get_client()
        resources = (
            SandboxResources(vcpus=self._vcpus, memory=self._memory)
            if self._vcpus is not None or self._memory is not None
            else None
        )
        if self._sandbox_name:
            self._sandbox, created = await client.get_or_create_sandbox(
                name=self._sandbox_name,
                image=self._image,
                source=self._source,
                ports=self._ports,
                execution_time_limit=self._timeout,
                resources=resources,
                env=self._env,
            )
            action = "Created" if created else "Connected to"
        else:
            self._sandbox = await client.create_sandbox(
                image=self._image,
                source=self._source,
                ports=self._ports,
                execution_time_limit=self._timeout,
                resources=resources,
                env=self._env,
            )
            self._sandbox_name = self._sandbox.name
            action = "Created"
        logger.info("%s Vercel sandbox: %s", action, self._sandbox.name)
        return self._sandbox

    async def set_session(self) -> None:
        """Initialize the Vercel session."""
        if not self._session_started:
            await self._get_sandbox()
            self._session_started = True

    async def close_session(self) -> None:
        """Stop the sandbox and close the client."""
        if self._sandbox is not None and self._session_started:
            await self._sandbox.stop()
            self._sandbox = None
            self._session_started = False
        if self._client is not None:
            await self._client.aclose()
            self._client = None

    async def _run(self, command: str, args: list[str]) -> tuple[int, str, str]:
        """Run a command in the sandbox and return (returncode, stdout, stderr)."""
        sandbox = await self._get_sandbox()
        result = await sandbox.run_command(command, args, cwd="/", capture_output=True)
        stdout = (result.stdout or b"").decode()
        stderr = (result.stderr or b"").decode()
        return result.returncode, stdout, stderr

    @overload
    async def _ls(
        self, path: str, detail: Literal[True] = ..., **kwargs: Any
    ) -> list[VercelInfo]: ...

    @overload
    async def _ls(self, path: str, detail: bool, **kwargs: Any) -> list[str]: ...

    async def _ls(
        self,
        path: str,
        detail: bool = True,
        **kwargs: Any,
    ) -> list[str] | list[VercelInfo]:
        """List directory contents."""
        await self.set_session()
        sandbox = await self._get_sandbox()
        if not detail:
            with _translate_errors(path, "list directory"):
                entries = await sandbox.fs.listdir(path)
            return [str(entry.path) for entry in entries]

        args = [path, "-mindepth", "1", "-maxdepth", "1", "-printf", _FIND_FORMAT]
        returncode, stdout, stderr = await self._run("find", args)
        if returncode != 0:
            if "No such file or directory" in stderr:
                raise FileNotFoundError(path)
            raise OSError(f"Failed to list directory {path}: {stderr}")
        result: list[VercelInfo] = []
        for line in stdout.splitlines():
            entry_path, size, mtime, type_char = line.rsplit(_SEP, 3)
            info = VercelInfo(
                name=entry_path,
                size=int(size),
                mtime=float(mtime),
                type=_find_type(type_char),
            )
            result.append(info)
        return result

    async def _cat_file(
        self,
        path: str,
        start: int | None = None,
        end: int | None = None,
        **kwargs: Any,
    ) -> bytes:
        """Read file contents."""
        await self.set_session()
        sandbox = await self._get_sandbox()
        with _translate_errors(path, "read file"):
            content = await sandbox.fs.read_bytes(path)
        if start is not None or end is not None:
            return content[start:end]
        return content

    async def _pipe_file(
        self, path: str, value: bytes, mode: CreationMode = "overwrite", **kwargs: Any
    ) -> None:
        """Write file contents, creating parent directories if needed."""
        await self.set_session()
        sandbox = await self._get_sandbox()
        parent = path.rsplit("/", 1)[0]
        with _translate_errors(path, "write file"):
            if parent and parent != path:
                await sandbox.fs.mkdir(parent, recursive=True)
            await sandbox.fs.write_bytes(path, value)

    async def _mkdir(self, path: str, create_parents: bool = True, **kwargs: Any) -> None:
        """Create directory."""
        await self.set_session()
        sandbox = await self._get_sandbox()
        with _translate_errors(path, "create directory"):
            await sandbox.fs.mkdir(path, recursive=create_parents)

    async def _rm_file(self, path: str, **kwargs: Any) -> None:
        """Remove file."""
        await self.set_session()
        sandbox = await self._get_sandbox()
        if await sandbox.fs.is_dir(path):
            raise IsADirectoryError(path)
        with _translate_errors(path, "remove file"):
            await sandbox.fs.remove(path)

    async def _rmdir(self, path: str, **kwargs: Any) -> None:
        """Remove directory."""
        await self.set_session()
        sandbox = await self._get_sandbox()
        if not await sandbox.fs.exists(path):
            raise FileNotFoundError(path)
        if not await sandbox.fs.is_dir(path):
            raise NotADirectoryError(path)
        with _translate_errors(path, "remove directory"):
            await sandbox.fs.remove(path, recursive=True)

    async def _exists(self, path: str, **kwargs: Any) -> bool:
        """Check if path exists."""
        await self.set_session()
        sandbox = await self._get_sandbox()
        return await sandbox.fs.exists(path)

    async def _isfile(self, path: str, **kwargs: Any) -> bool:
        """Check if path is a file."""
        await self.set_session()
        sandbox = await self._get_sandbox()
        return await sandbox.fs.is_file(path)

    async def _isdir(self, path: str, **kwargs: Any) -> bool:
        """Check if path is a directory."""
        await self.set_session()
        sandbox = await self._get_sandbox()
        return await sandbox.fs.is_dir(path)

    async def _info(self, path: str, **kwargs: Any) -> VercelInfo:
        """Get info about a file or directory."""
        await self.set_session()
        returncode, stdout, stderr = await self._run("stat", ["-c", _STAT_FORMAT, path])
        if returncode != 0:
            if "No such file or directory" in stderr:
                raise FileNotFoundError(path)
            raise OSError(f"Failed to get info for {path}: {stderr}")
        size, mtime, type_description = stdout.strip().split(_SEP, 2)
        return VercelInfo(
            name=path,
            size=int(size),
            mtime=float(mtime),
            type=_stat_type(type_description),
        )

    async def _size(self, path: str, **kwargs: Any) -> int:
        """Get file size."""
        return (await self._info(path))["size"]

    async def _modified(self, path: str, **kwargs: Any) -> float:
        """Get file modification time."""
        return (await self._info(path))["mtime"]

    # Sync wrapper methods
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
