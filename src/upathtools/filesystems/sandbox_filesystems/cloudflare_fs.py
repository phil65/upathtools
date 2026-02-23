"""Cloudflare Sandbox async filesystem implementation for upathtools.

Uses the Cloudflare Sandbox HTTP API directly (no official Python SDK).
File operations use the /api/file/* endpoints where available, falling back
to shell commands via /api/execute for metadata operations.
"""

from __future__ import annotations

import base64
import logging
import shlex
from typing import Any, Literal, Required, overload

import httpx

from upathtools.async_helpers import sync_wrapper
from upathtools.filesystems.base import BaseAsyncFileSystem, BaseUPath, FileInfo


logger = logging.getLogger(__name__)


class CloudflareInfo(FileInfo, total=False):
    """Info dict for Cloudflare Sandbox filesystem paths."""

    size: Required[int]
    mtime: float
    permissions: str


class CloudflarePath(BaseUPath[CloudflareInfo]):
    """Cloudflare Sandbox-specific UPath implementation."""

    __slots__ = ()


class CloudflareFS(BaseAsyncFileSystem[CloudflarePath, CloudflareInfo]):
    """Async filesystem for Cloudflare Sandbox environments.

    This filesystem provides access to files within a Cloudflare Sandbox,
    communicating via the Sandbox Worker HTTP API. It uses the file API
    endpoints for read/write and falls back to shell commands executed
    via /api/execute for metadata operations like ls, stat, and test.
    """

    protocol = "cloudflare"
    upath_cls = CloudflarePath
    root_marker = "/"
    cachable = False

    def __init__(
        self,
        base_url: str,
        session_id: str | None = None,
        api_token: str | None = None,
        account_id: str | None = None,
        working_dir: str = "/workspace",
        env_vars: dict[str, str] | None = None,
        timeout: float = 30.0,
        **kwargs: Any,
    ) -> None:
        """Initialize Cloudflare Sandbox filesystem.

        Args:
            base_url: Base URL of the Cloudflare Sandbox Worker deployment
            session_id: Existing session ID to connect to (created if not provided)
            api_token: Cloudflare API token for authentication
            account_id: Cloudflare account ID
            working_dir: Working directory for the session
            env_vars: Environment variables for new sessions
            timeout: HTTP request timeout in seconds
            **kwargs: Additional filesystem options
        """
        super().__init__(**kwargs)
        self._base_url = base_url.rstrip("/")
        self._session_id = session_id
        self._api_token = api_token
        self._account_id = account_id
        self._working_dir = working_dir
        self._env_vars = env_vars or {}
        self._timeout = timeout
        self._session_started = False

    @staticmethod
    def _get_kwargs_from_urls(path: str) -> dict[str, Any]:
        path = path.removeprefix("cloudflare://")
        return {"session_id": path}

    def _build_headers(self) -> dict[str, str]:
        """Build HTTP headers for API requests."""
        headers: dict[str, str] = {
            "Content-Type": "application/json",
            "User-Agent": "upathtools-cloudflare-fs/1.0",
        }
        if self._api_token:
            headers["Authorization"] = f"Bearer {self._api_token}"
        if self._account_id:
            headers["CF-Account-ID"] = self._account_id
        return headers

    async def _request(
        self,
        method: str,
        path: str,
        *,
        json: dict[str, Any] | None = None,
        params: dict[str, Any] | None = None,
    ) -> Any:
        """Make an HTTP request to the Cloudflare Sandbox API."""
        url = f"{self._base_url}{path}"
        async with httpx.AsyncClient(
            timeout=httpx.Timeout(self._timeout),
        ) as client:
            response = await client.request(
                method,
                url,
                json=json,
                params=params,
                headers=self._build_headers(),
            )

        if response.status_code == 404:
            raise FileNotFoundError(f"Cloudflare resource not found: {path}")
        if response.status_code >= 400:
            try:
                payload = response.json()
                message = payload.get("error") or payload.get("message") or response.text
            except Exception:  # noqa: BLE001
                message = response.text
            msg = f"Cloudflare API error ({response.status_code}): {message}"
            raise OSError(msg)

        if response.headers.get("content-type", "").startswith("application/json"):
            return response.json()
        return None

    async def _exec(self, command: str) -> tuple[str, str, int]:
        """Execute a shell command in the sandbox and return (stdout, stderr, exit_code)."""
        await self.set_session()
        data = await self._request(
            "POST",
            "/api/execute",
            json={"id": self._session_id, "command": command},
        )
        stdout = data.get("stdout", "") if data else ""
        stderr = data.get("stderr", "") if data else ""
        exit_code = data.get("exitCode", data.get("exit_code", 0)) if data else 1
        return stdout, stderr, exit_code

    async def set_session(self) -> None:
        """Ensure the sandbox session is created."""
        if self._session_started:
            return

        if not self._session_id:
            import uuid

            self._session_id = f"cf-sbx-{uuid.uuid4().hex[:12]}"

        await self._request(
            "POST",
            "/api/session/create",
            json={
                "id": self._session_id,
                "env": self._env_vars,
                "cwd": self._working_dir,
                "isolation": True,
            },
        )
        self._session_started = True

    async def close_session(self) -> None:
        """Close the sandbox session by killing all processes."""
        if self._session_id and self._session_started:
            try:
                await self._request(
                    "DELETE",
                    "/api/process/kill-all",
                    params={"session": self._session_id},
                )
            except (OSError, FileNotFoundError):
                pass
            self._session_started = False

    @overload
    async def _ls(
        self, path: str, detail: Literal[True] = ..., **kwargs: Any
    ) -> list[CloudflareInfo]: ...

    @overload
    async def _ls(self, path: str, detail: bool, **kwargs: Any) -> list[str]: ...

    async def _ls(
        self, path: str, detail: bool = True, **kwargs: Any
    ) -> list[CloudflareInfo] | list[str]:
        """List directory contents using ls -la."""
        stdout, stderr, exit_code = await self._exec(f"ls -la {shlex.quote(path)}")
        if exit_code != 0:
            if "No such file or directory" in stderr:
                raise FileNotFoundError(f"Path not found: {path}")
            msg = f"Failed to list directory {path}: {stderr}"
            raise OSError(msg)

        files: list[CloudflareInfo] = []
        for line in stdout.strip().split("\n"):
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
            size = 0 if is_dir else int(parts[4]) if parts[4].isdigit() else 0
            info = CloudflareInfo(
                name=full_path,
                size=size,
                type="directory" if is_dir else "file",
                permissions=permissions,
            )
            files.append(info)

        if not detail:
            return [f["name"] for f in files]
        return files

    async def _cat_file(
        self, path: str, start: int | None = None, end: int | None = None, **kwargs: Any
    ) -> bytes:
        """Read file contents via the file read API."""
        await self.set_session()
        try:
            data = await self._request(
                "POST",
                "/api/file/read",
                json={"id": self._session_id, "path": path},
            )
            content_str = data.get("content", "") if data else ""
            content = content_str.encode("utf-8") if isinstance(content_str, str) else content_str
        except FileNotFoundError:
            raise
        except OSError:
            # Fallback: read via base64-encoded cat
            stdout, stderr, exit_code = await self._exec(f"base64 {shlex.quote(path)}")
            if exit_code != 0:
                if "No such file or directory" in stderr:
                    raise FileNotFoundError(path)
                if "Is a directory" in stderr:
                    raise IsADirectoryError(path)
                msg = f"Failed to read file {path}: {stderr}"
                raise OSError(msg)
            content = base64.b64decode(stdout.strip())

        if start is not None or end is not None:
            content = content[start:end]
        return content

    async def _put_file(self, lpath: str, rpath: str, callback=None, **kwargs: Any) -> None:
        """Upload a local file to the sandbox."""
        await self.set_session()
        with open(lpath, "rb") as f:  # noqa: PTH123
            data = f.read()
        await self._pipe_file(rpath, data, **kwargs)

    async def _pipe_file(self, path: str, value: bytes, **kwargs: Any) -> None:
        """Write data to a file in the sandbox."""
        await self.set_session()

        try:
            if isinstance(value, bytes):
                try:
                    content = value.decode("utf-8")
                except UnicodeDecodeError:
                    content = None
            else:
                content = value

            if content is not None:
                await self._request(
                    "POST",
                    "/api/file/write",
                    json={"id": self._session_id, "path": path, "content": content},
                )
            else:
                # Binary content: base64 encode and decode on remote
                encoded = base64.b64encode(value).decode("ascii")
                await self._request(
                    "POST",
                    "/api/file/write",
                    json={
                        "id": self._session_id,
                        "path": path,
                        "content": encoded,
                        "encoding": "base64",
                    },
                )
        except (FileNotFoundError, OSError):
            # Fallback: use shell command to write via base64
            encoded = base64.b64encode(value).decode("ascii")
            dir_path = path.rsplit("/", 1)[0]
            if dir_path and dir_path != path:
                await self._exec(f"mkdir -p {shlex.quote(dir_path)}")
            stdout, stderr, exit_code = await self._exec(
                f"echo {shlex.quote(encoded)} | base64 -d > {shlex.quote(path)}"
            )
            if exit_code != 0:
                msg = f"Failed to write file {path}: {stderr}"
                raise OSError(msg)

    async def _mkdir(self, path: str, create_parents: bool = True, **kwargs: Any) -> None:
        """Create a directory."""
        flag = "-p " if create_parents else ""
        stdout, stderr, exit_code = await self._exec(f"mkdir {flag}{shlex.quote(path)}")
        if exit_code != 0 and "File exists" not in stderr:
            msg = f"Failed to create directory {path}: {stderr}"
            raise OSError(msg)

    async def _rm_file(self, path: str, **kwargs: Any) -> None:
        """Remove a file."""
        stdout, stderr, exit_code = await self._exec(f"rm -f {shlex.quote(path)}")
        if exit_code != 0:
            if "No such file or directory" in stderr:
                raise FileNotFoundError(path)
            if "Is a directory" in stderr:
                raise IsADirectoryError(path)
            msg = f"Failed to remove file {path}: {stderr}"
            raise OSError(msg)

    async def _rmdir(self, path: str, **kwargs: Any) -> None:
        """Remove a directory."""
        stdout, stderr, exit_code = await self._exec(f"rmdir {shlex.quote(path)}")
        if exit_code != 0:
            if "No such file or directory" in stderr:
                raise FileNotFoundError(path)
            if "Not a directory" in stderr:
                raise NotADirectoryError(path)
            if "not empty" in stderr.lower():
                msg = f"Directory not empty: {path}"
                raise OSError(msg)
            msg = f"Failed to remove directory {path}: {stderr}"
            raise OSError(msg)

    async def _exists(self, path: str, **kwargs: Any) -> bool:
        """Check if path exists."""
        _stdout, _stderr, exit_code = await self._exec(f"test -e {shlex.quote(path)}")
        return exit_code == 0

    async def _isfile(self, path: str, **kwargs: Any) -> bool:
        """Check if path is a file."""
        _stdout, _stderr, exit_code = await self._exec(f"test -f {shlex.quote(path)}")
        return exit_code == 0

    async def _isdir(self, path: str, **kwargs: Any) -> bool:
        """Check if path is a directory."""
        _stdout, _stderr, exit_code = await self._exec(f"test -d {shlex.quote(path)}")
        return exit_code == 0

    async def _size(self, path: str, **kwargs: Any) -> int:
        """Get file size."""
        stdout, stderr, exit_code = await self._exec(f"stat -c %s {shlex.quote(path)}")
        if exit_code != 0:
            if "No such file" in stderr:
                raise FileNotFoundError(path)
            msg = f"Failed to get file size for {path}: {stderr}"
            raise OSError(msg)
        return int(stdout.strip())

    async def _modified(self, path: str, **kwargs: Any) -> float:
        """Get file modification time as epoch seconds."""
        stdout, stderr, exit_code = await self._exec(f"stat -c %Y {shlex.quote(path)}")
        if exit_code != 0:
            if "No such file" in stderr:
                raise FileNotFoundError(path)
            msg = f"Failed to get modification time for {path}: {stderr}"
            raise OSError(msg)
        return float(stdout.strip())

    async def _info(self, path: str, **kwargs: Any) -> CloudflareInfo:
        """Get info about a file or directory."""
        # stat -c '%s %Y %A' gives size, mtime epoch, human-readable permissions
        stdout, stderr, exit_code = await self._exec(f"stat -c '%s %Y %A %F' {shlex.quote(path)}")
        if exit_code != 0:
            if "No such file" in stderr:
                raise FileNotFoundError(path)
            msg = f"Failed to get info for {path}: {stderr}"
            raise OSError(msg)

        parts = stdout.strip().split(None, 3)
        size = int(parts[0]) if len(parts) > 0 else 0
        mtime = float(parts[1]) if len(parts) > 1 else 0.0
        permissions = parts[2] if len(parts) > 2 else ""
        file_type_str = parts[3] if len(parts) > 3 else ""
        is_dir = "directory" in file_type_str.lower()

        return CloudflareInfo(
            name=path,
            size=0 if is_dir else size,
            type="directory" if is_dir else "file",
            mtime=mtime,
            permissions=permissions,
        )

    async def _mv_file(self, path1: str, path2: str, **kwargs: Any) -> None:
        """Move/rename a file."""
        stdout, stderr, exit_code = await self._exec(
            f"mv {shlex.quote(path1)} {shlex.quote(path2)}"
        )
        if exit_code != 0:
            if "No such file" in stderr:
                raise FileNotFoundError(path1)
            msg = f"Failed to move {path1} to {path2}: {stderr}"
            raise OSError(msg)

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


if __name__ == "__main__":
    import asyncio

    async def main():
        fs = CloudflareFS(base_url="http://localhost:8787")
        await fs.set_session()
        result = await fs._ls("/workspace")
        print(result)
        await fs.close_session()

    asyncio.run(main())
