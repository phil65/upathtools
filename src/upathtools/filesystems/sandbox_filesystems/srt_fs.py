"""SRT (Sandbox Runtime) async filesystem implementation for upathtools.

Uses Anthropic's sandbox-runtime to provide sandboxed filesystem access
with configurable network and filesystem restrictions.
"""

from __future__ import annotations

import base64
import json
from pathlib import Path
import shlex
import tempfile
from typing import TYPE_CHECKING, Any, Literal, Required, overload

from upathtools.async_helpers import sync_wrapper
from upathtools.filesystems.base import BaseAsyncFileSystem, BaseUPath, FileInfo


if TYPE_CHECKING:
    from anyenv.os_commands.providers import OSCommandProvider


class SRTInfo(FileInfo, total=False):
    """Info dict for SRT filesystem paths."""

    size: Required[int]
    mtime: float
    permissions: str | None


class SRTPath(BaseUPath["SRTInfo"]):
    """SRT-specific UPath implementation."""

    __slots__ = ()


class SRTFS(BaseAsyncFileSystem[SRTPath, SRTInfo]):
    """Async filesystem for SRT (Sandbox Runtime) environments.

    This filesystem provides sandboxed access to files using Anthropic's
    sandbox-runtime, enforcing network and filesystem restrictions.
    """

    protocol = "srt"
    upath_cls = SRTPath
    root_marker = "/"
    cachable = False
    local_file = True

    def __init__(
        self,
        allowed_domains: list[str] | None = None,
        denied_domains: list[str] | None = None,
        allow_unix_sockets: list[str] | None = None,
        allow_all_unix_sockets: bool = False,
        allow_local_binding: bool = False,
        deny_read: list[str] | None = None,
        allow_write: list[str] | None = None,
        deny_write: list[str] | None = None,
        timeout: float = 30.0,
        **kwargs: Any,
    ) -> None:
        """Initialize SRT filesystem.

        Args:
            allowed_domains: Domains that can be accessed (empty = no network)
            denied_domains: Domains explicitly blocked
            allow_unix_sockets: Specific Unix socket paths to allow
            allow_all_unix_sockets: Allow all Unix sockets
            allow_local_binding: Allow binding to localhost ports
            deny_read: Paths blocked from reading
            allow_write: Paths where writes are permitted
            deny_write: Paths denied within allowed write paths
            timeout: Command execution timeout in seconds
            **kwargs: Additional filesystem options
        """
        super().__init__(**kwargs)
        self._allowed_domains = allowed_domains or []
        self._denied_domains = denied_domains or []
        self._allow_unix_sockets = allow_unix_sockets or []
        self._allow_all_unix_sockets = allow_all_unix_sockets
        self._allow_local_binding = allow_local_binding
        self._deny_read = deny_read or []
        self._allow_write = allow_write or ["."]
        self._deny_write = deny_write or []
        self._timeout = timeout
        self._settings_file: Path | None = None
        self._command_provider: OSCommandProvider | None = None

    def _get_command_provider(self) -> OSCommandProvider:
        """Get the OS command provider (Unix commands for srt)."""
        if self._command_provider is None:
            from anyenv.os_commands.providers import UnixCommandProvider

            self._command_provider = UnixCommandProvider()
        return self._command_provider

    def _get_settings_file(self) -> Path:
        """Get or create the srt settings file."""
        if self._settings_file is None:
            settings = {
                "network": {
                    "allowedDomains": self._allowed_domains,
                    "deniedDomains": self._denied_domains,
                    "allowUnixSockets": self._allow_unix_sockets,
                    "allowAllUnixSockets": self._allow_all_unix_sockets,
                    "allowLocalBinding": self._allow_local_binding,
                },
                "filesystem": {
                    "denyRead": self._deny_read,
                    "allowWrite": self._allow_write,
                    "denyWrite": self._deny_write,
                },
            }
            _fd, path = tempfile.mkstemp(suffix=".json", prefix="srt-fs-settings-")
            self._settings_file = Path(path)
            self._settings_file.write_text(json.dumps(settings, indent=2))
        return self._settings_file

    def _wrap_command(self, command: str) -> str:
        """Wrap a command with srt sandbox."""
        settings_file = self._get_settings_file()
        return shlex.join(["srt", "--settings", str(settings_file), command])

    async def _run_command(self, command: str) -> tuple[str, str, int]:
        """Run a command in the sandbox and return (stdout, stderr, exit_code)."""
        import asyncio

        wrapped = self._wrap_command(command)
        proc = await asyncio.create_subprocess_shell(
            wrapped,
            stdout=asyncio.subprocess.PIPE,
            stderr=asyncio.subprocess.PIPE,
        )
        stdout, stderr = await asyncio.wait_for(proc.communicate(), timeout=self._timeout)
        return (
            stdout.decode() if stdout else "",
            stderr.decode() if stderr else "",
            proc.returncode or 0,
        )

    @staticmethod
    def _get_kwargs_from_urls(path: str) -> dict[str, Any]:
        # srt://path -> just use the path
        return {}

    @overload
    async def _ls(self, path: str, detail: Literal[True] = ..., **kwargs: Any) -> list[SRTInfo]: ...

    @overload
    async def _ls(self, path: str, detail: Literal[False], **kwargs: Any) -> list[str]: ...

    async def _ls(self, path: str, detail: bool = True, **kwargs: Any) -> list[SRTInfo] | list[str]:
        """List directory contents."""
        provider = self._get_command_provider()
        ls_cmd = provider.get_command("list_directory")

        command = ls_cmd.create_command(path)
        stdout, stderr, exit_code = await self._run_command(command)

        if exit_code != 0:
            if "No such file or directory" in stderr or "No such file" in stderr:
                raise FileNotFoundError(path)
            msg = f"Failed to list directory {path}: {stderr}"
            raise OSError(msg)

        entries = ls_cmd.parse_command(stdout, path)

        if not detail:
            return [entry.path for entry in entries]

        return [
            SRTInfo(name=e.path, size=e.size or 0, type=e.type, permissions=e.permissions)
            for e in entries
        ]

    async def _cat_file(
        self, path: str, start: int | None = None, end: int | None = None, **kwargs: Any
    ) -> bytes:
        """Read file contents."""
        provider = self._get_command_provider()
        b64_cmd = provider.get_command("base64_encode")

        command = b64_cmd.create_command(path)
        stdout, stderr, exit_code = await self._run_command(command)

        if exit_code != 0:
            if "No such file or directory" in stderr:
                raise FileNotFoundError(path)
            if "Is a directory" in stderr:
                raise IsADirectoryError(path)
            msg = f"Failed to read file {path}: {stderr}"
            raise OSError(msg)

        content = b64_cmd.parse_command(stdout)

        if start is not None or end is not None:
            start = start or 0
            end = end or len(content)
            content = content[start:end]

        return content

    async def _pipe_file(self, path: str, value: bytes, **kwargs: Any) -> None:
        """Write bytes to a file."""
        # Base64 encode the content and decode on the remote side
        encoded = base64.b64encode(value).decode("ascii")
        command = f'echo "{encoded}" | base64 -d > {shlex.quote(path)}'
        _stdout, stderr, exit_code = await self._run_command(command)

        if exit_code != 0:
            msg = f"Failed to write file {path}: {stderr}"
            raise OSError(msg)

    async def _mkdir(self, path: str, create_parents: bool = True, **kwargs: Any) -> None:
        """Create a directory."""
        provider = self._get_command_provider()
        mkdir_cmd = provider.get_command("create_directory")

        command = mkdir_cmd.create_command(path, parents=create_parents)
        _stdout, stderr, exit_code = await self._run_command(command)

        if exit_code != 0 and "File exists" not in stderr:
            msg = f"Failed to create directory {path}: {stderr}"
            raise OSError(msg)

    async def _rm_file(self, path: str, **kwargs: Any) -> None:
        """Remove a file."""
        provider = self._get_command_provider()
        rm_cmd = provider.get_command("remove_path")

        command = rm_cmd.create_command(path, recursive=False)
        _stdout, stderr, exit_code = await self._run_command(command)

        if exit_code != 0:
            if "No such file or directory" in stderr:
                raise FileNotFoundError(path)
            msg = f"Failed to remove file {path}: {stderr}"
            raise OSError(msg)

    async def _rmdir(self, path: str, **kwargs: Any) -> None:
        """Remove a directory."""
        provider = self._get_command_provider()
        rm_cmd = provider.get_command("remove_path")

        command = rm_cmd.create_command(path, recursive=True)
        _stdout, stderr, exit_code = await self._run_command(command)

        if exit_code != 0:
            if "No such file or directory" in stderr:
                raise FileNotFoundError(path)
            msg = f"Failed to remove directory {path}: {stderr}"
            raise OSError(msg)

    async def _exists(self, path: str, **kwargs: Any) -> bool:
        """Check if a path exists."""
        provider = self._get_command_provider()
        exists_cmd = provider.get_command("exists")

        command = exists_cmd.create_command(path)
        stdout, _stderr, exit_code = await self._run_command(command)

        return exists_cmd.parse_command(stdout, exit_code)

    async def _isfile(self, path: str, **kwargs: Any) -> bool:
        """Check if path is a file."""
        provider = self._get_command_provider()
        isfile_cmd = provider.get_command("is_file")

        command = isfile_cmd.create_command(path)
        stdout, _stderr, exit_code = await self._run_command(command)

        return isfile_cmd.parse_command(stdout, exit_code)

    async def _isdir(self, path: str, **kwargs: Any) -> bool:
        """Check if path is a directory."""
        provider = self._get_command_provider()
        isdir_cmd = provider.get_command("is_directory")
        command = isdir_cmd.create_command(path)
        stdout, _stderr, exit_code = await self._run_command(command)
        return isdir_cmd.parse_command(stdout, exit_code)

    async def _info(self, path: str, **kwargs: Any) -> SRTInfo:
        """Get file/directory info."""
        provider = self._get_command_provider()
        info_cmd = provider.get_command("file_info")

        command = info_cmd.create_command(path)
        stdout, stderr, exit_code = await self._run_command(command)

        if exit_code != 0:
            if "No such file or directory" in stderr:
                raise FileNotFoundError(path)
            msg = f"Failed to get info for {path}: {stderr}"
            raise OSError(msg)

        file_info = info_cmd.parse_command(stdout, path)
        return SRTInfo(
            name=path,
            size=file_info.size,
            type=file_info.type,
            mtime=file_info.mtime if file_info.mtime else 0,
            permissions=file_info.permissions,
        )

    async def _size(self, path: str) -> int:
        """Get file size."""
        info = await self._info(path)
        return info["size"]

    def __del__(self) -> None:
        """Cleanup settings file."""
        if self._settings_file and self._settings_file.exists():
            self._settings_file.unlink(missing_ok=True)

    # Sync wrappers
    ls = sync_wrapper(_ls)
    cat_file = sync_wrapper(_cat_file)  # pyright: ignore[reportAssignmentType]
    pipe_file = sync_wrapper(_pipe_file)
    mkdir = sync_wrapper(_mkdir)
    rm_file = sync_wrapper(_rm_file)
    rmdir = sync_wrapper(_rmdir)
    exists = sync_wrapper(_exists)  # pyright: ignore[reportAssignmentType]
    isfile = sync_wrapper(_isfile)
    isdir = sync_wrapper(_isdir)
    info = sync_wrapper(_info)
    size = sync_wrapper(_size)
