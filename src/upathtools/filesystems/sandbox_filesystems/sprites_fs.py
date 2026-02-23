"""Sprites (Fly.io) async filesystem implementation for upathtools.

Uses the Sprites filesystem REST API directly via httpx.
All file operations use dedicated /fs/* endpoints.
"""

from __future__ import annotations

from datetime import datetime
import logging
from typing import Any, Literal, Required, overload

import httpx

from upathtools.async_helpers import sync_wrapper
from upathtools.filesystems.base import BaseAsyncFileSystem, BaseUPath, FileInfo


logger = logging.getLogger(__name__)


class SpritesInfo(FileInfo, total=False):
    """Info dict for Sprites filesystem paths."""

    size: Required[int]
    mtime: Required[float]
    mode: str
    is_dir: bool


class SpritesPath(BaseUPath[SpritesInfo]):
    """Sprites-specific UPath implementation."""

    __slots__ = ()


class SpritesFS(BaseAsyncFileSystem[SpritesPath, SpritesInfo]):
    """Async filesystem for Sprites (Fly.io) sandbox environments.

    This filesystem provides access to files within a Sprites sandbox,
    communicating via the dedicated Sprites filesystem REST API.
    All operations use the /v1/sprites/{name}/fs/* endpoints.
    """

    protocol = "sprites"
    upath_cls = SpritesPath
    root_marker = "/"
    cachable = False

    def __init__(
        self,
        sprite_name: str,
        token: str,
        base_url: str = "https://api.sprites.dev",
        working_dir: str = "/",
        timeout: float = 30.0,
        **kwargs: Any,
    ) -> None:
        """Initialize Sprites filesystem.

        Args:
            sprite_name: Name of the sprite to access
            token: Sprites API authentication token
            base_url: Sprites API base URL
            working_dir: Working directory for path resolution
            timeout: HTTP request timeout in seconds
            **kwargs: Additional filesystem options
        """
        super().__init__(**kwargs)
        self._sprite_name = sprite_name
        self._token = token
        self._base_url = base_url.rstrip("/")
        self._working_dir = working_dir.rstrip("/") or "/"
        self._timeout = timeout

    @staticmethod
    def _get_kwargs_from_urls(path: str) -> dict[str, Any]:
        path = path.removeprefix("sprites://")
        return {"sprite_name": path}

    def _fs_url(self, endpoint: str) -> str:
        """Build a filesystem API URL."""
        return f"{self._base_url}/v1/sprites/{self._sprite_name}/fs{endpoint}"

    def _headers(self, *, content_type: str = "application/json") -> dict[str, str]:
        """Build request headers."""
        headers: dict[str, str] = {"Authorization": f"Bearer {self._token}"}
        if content_type:
            headers["Content-Type"] = content_type
        return headers

    async def _request(
        self,
        method: str,
        endpoint: str,
        *,
        params: dict[str, Any] | None = None,
        json: dict[str, Any] | None = None,
        content: bytes | None = None,
        content_type: str = "application/json",
    ) -> httpx.Response:
        """Make an HTTP request to the Sprites filesystem API."""
        url = self._fs_url(endpoint)
        headers = self._headers(content_type=content_type)

        async with httpx.AsyncClient(timeout=httpx.Timeout(self._timeout)) as client:
            response = await client.request(
                method,
                url,
                params=params,
                json=json,
                content=content,
                headers=headers,
            )

        if response.status_code == 404:
            path = (params or {}).get("path", "")
            raise FileNotFoundError(path)
        if not response.is_success:
            self._handle_error(response, params)
        return response

    @staticmethod
    def _handle_error(response: httpx.Response, params: dict[str, Any] | None = None) -> None:
        """Handle HTTP error responses, raising appropriate Python exceptions."""
        path = (params or {}).get("path", "")
        try:
            data = response.json()
            error_msg = data.get("error", response.text)
            error_code = data.get("code", "")
        except Exception:  # noqa: BLE001
            error_msg = response.text
            error_code = ""

        if error_code == "EISDIR" or "is a directory" in error_msg.lower():
            raise IsADirectoryError(path)
        if error_code == "ENOTDIR" or "not a directory" in error_msg.lower():
            raise NotADirectoryError(path)
        if error_code == "ENOTEMPTY" or "not empty" in error_msg.lower():
            msg = f"Directory not empty: {path}"
            raise OSError(msg)
        msg = f"Sprites API error ({response.status_code}): {error_msg}"
        raise OSError(msg)

    def _path_params(self, path: str) -> dict[str, str]:
        """Build common query params for path-based endpoints."""
        return {"path": path, "workingDir": self._working_dir}

    @staticmethod
    def _parse_mtime(mod_time_str: str | None) -> float:
        """Parse an ISO 8601 mod_time string to epoch float."""
        if not mod_time_str:
            return 0.0
        try:
            dt = datetime.fromisoformat(mod_time_str.replace("Z", "+00:00"))
            return dt.timestamp()
        except (ValueError, AttributeError):
            return 0.0

    @overload
    async def _ls(
        self, path: str, detail: Literal[True] = ..., **kwargs: Any
    ) -> list[SpritesInfo]: ...

    @overload
    async def _ls(self, path: str, detail: bool, **kwargs: Any) -> list[str]: ...

    async def _ls(
        self, path: str, detail: bool = True, **kwargs: Any
    ) -> list[SpritesInfo] | list[str]:
        """List directory contents via /fs/list."""
        response = await self._request("GET", "/list", params=self._path_params(path))
        data = response.json()
        entries = data.get("entries", [])

        if not entries:
            # /fs/list returns empty entries for non-existent paths or empty dirs.
            # An empty dir is still valid, but we got here so the path was accepted.
            pass

        if not detail:
            return [
                entry.get("path") or f"{path.rstrip('/')}/{entry.get('name', '')}"
                for entry in entries
                if entry.get("name")
            ]

        result: list[SpritesInfo] = []
        for entry in entries:
            name = entry.get("name", "")
            if not name:
                continue
            entry_path = entry.get("path") or f"{path.rstrip('/')}/{name}"
            is_dir = entry.get("isDir", False)
            result.append(
                SpritesInfo(
                    name=entry_path,
                    size=0 if is_dir else entry.get("size", 0),
                    type="directory" if is_dir else "file",
                    mtime=self._parse_mtime(entry.get("modTime")),
                    mode=entry.get("mode", "0644"),
                    is_dir=is_dir,
                )
            )
        return result

    async def _cat_file(
        self, path: str, start: int | None = None, end: int | None = None, **kwargs: Any
    ) -> bytes:
        """Read file contents via /fs/read (returns raw bytes)."""
        response = await self._request(
            "GET", "/read", params=self._path_params(path), content_type=""
        )
        content = response.content

        if start is not None or end is not None:
            content = content[start:end]
        return content

    async def _put_file(self, lpath: str, rpath: str, callback=None, **kwargs: Any) -> None:
        """Upload a local file to the sprite."""
        with open(lpath, "rb") as f:  # noqa: PTH123
            data = f.read()
        await self._pipe_file(rpath, data, **kwargs)

    async def _pipe_file(self, path: str, value: bytes, **kwargs: Any) -> None:
        """Write data to a file via /fs/write (PUT with octet-stream body)."""
        params = self._path_params(path)
        params["mode"] = "0644"
        params["mkdirParents"] = "true"

        await self._request(
            "PUT",
            "/write",
            params=params,
            content=value,
            content_type="application/octet-stream",
        )

    async def _mkdir(self, path: str, create_parents: bool = True, **kwargs: Any) -> None:
        """Create a directory.

        Sprites API doesn't have a dedicated mkdir endpoint, so we create
        a .keep file with mkdirParents=true and then remove it.
        """
        keep_path = f"{path.rstrip('/')}/.keep"
        params = self._path_params(keep_path)
        params["mode"] = "0644"
        params["mkdirParents"] = str(create_parents).lower()

        await self._request(
            "PUT",
            "/write",
            params=params,
            content=b"",
            content_type="application/octet-stream",
        )

        # Clean up the .keep file
        try:
            delete_params = self._path_params(keep_path)
            delete_params["recursive"] = "false"
            await self._request("DELETE", "/delete", params=delete_params)
        except FileNotFoundError:
            pass

    async def _rm_file(self, path: str, **kwargs: Any) -> None:
        """Remove a file via /fs/delete."""
        params = self._path_params(path)
        params["recursive"] = "false"
        await self._request("DELETE", "/delete", params=params)

    async def _rmdir(self, path: str, **kwargs: Any) -> None:
        """Remove a directory via /fs/delete (non-recursive)."""
        params = self._path_params(path)
        params["recursive"] = "false"
        await self._request("DELETE", "/delete", params=params)

    async def _rm(
        self, path: str, recursive: bool = False, maxdepth: int | None = None, **kwargs: Any
    ) -> None:
        """Remove a file or directory, optionally recursively."""
        params = self._path_params(path)
        params["recursive"] = str(recursive).lower()
        await self._request("DELETE", "/delete", params=params)

    async def _exists(self, path: str, **kwargs: Any) -> bool:
        """Check if path exists by trying to stat it."""
        try:
            await self._stat(path)
        except FileNotFoundError:
            return False
        else:
            return True

    async def _stat(self, path: str) -> SpritesInfo:
        """Stat a path using /fs/list (returns the entry for the path itself)."""
        response = await self._request("GET", "/list", params=self._path_params(path))
        data = response.json()
        entries = data.get("entries", [])

        if not entries:
            raise FileNotFoundError(path)

        # For a file, /fs/list returns a single entry for the file itself.
        # For a directory, it returns directory contents.
        # The response also includes a "path" field indicating what was listed.
        response_path = data.get("path", "")

        # If the response path matches and entries contain items with their own names,
        # we're looking at a directory listing. The directory itself exists.
        entry = entries[0]
        entry_name = entry.get("name", "")
        entry_path = entry.get("path", "")

        # If the first entry's path matches our queried path, it's the item itself (file stat)
        if entry_path == path or (not entry_path and entry_name and path.endswith(entry_name)):
            is_dir = entry.get("isDir", False)
            return SpritesInfo(
                name=path,
                size=0 if is_dir else entry.get("size", 0),
                type="directory" if is_dir else "file",
                mtime=self._parse_mtime(entry.get("modTime")),
                mode=entry.get("mode", "0644"),
                is_dir=is_dir,
            )

        # Otherwise this is a directory listing; the directory itself exists
        return SpritesInfo(
            name=path,
            size=0,
            type="directory",
            mtime=0.0,
            mode="0755",
            is_dir=True,
        )

    async def _isfile(self, path: str, **kwargs: Any) -> bool:
        """Check if path is a file."""
        try:
            info = await self._stat(path)
        except FileNotFoundError:
            return False
        return info["type"] == "file"

    async def _isdir(self, path: str, **kwargs: Any) -> bool:
        """Check if path is a directory."""
        try:
            info = await self._stat(path)
        except FileNotFoundError:
            return False
        return info["type"] == "directory"

    async def _size(self, path: str, **kwargs: Any) -> int:
        """Get file size."""
        info = await self._stat(path)
        return info["size"]

    async def _modified(self, path: str, **kwargs: Any) -> float:
        """Get file modification time."""
        info = await self._stat(path)
        return info["mtime"]

    async def _info(self, path: str, **kwargs: Any) -> SpritesInfo:
        """Get info about a file or directory."""
        return await self._stat(path)

    async def _mv_file(self, path1: str, path2: str, **kwargs: Any) -> None:
        """Rename/move a file via /fs/rename."""
        await self._request(
            "POST",
            "/rename",
            json={
                "source": path1,
                "dest": path2,
                "workingDir": self._working_dir,
            },
        )

    async def _cp_file(self, path1: str, path2: str, **kwargs: Any) -> None:
        """Copy a file via /fs/copy."""
        await self._request(
            "POST",
            "/copy",
            json={
                "source": path1,
                "dest": path2,
                "workingDir": self._working_dir,
                "recursive": True,
            },
        )

    async def _chmod(self, path: str, mode: int, **kwargs: Any) -> None:
        """Change file permissions via /fs/chmod."""
        await self._request(
            "POST",
            "/chmod",
            json={
                "path": path,
                "workingDir": self._working_dir,
                "mode": f"{mode:04o}",
                "recursive": False,
            },
        )

    # Sync wrappers for async methods
    ls = sync_wrapper(_ls)  # pyright: ignore[reportAssignmentType]
    cat_file = sync_wrapper(_cat_file)  # pyright: ignore[reportAssignmentType]
    pipe_file = sync_wrapper(_pipe_file)  # pyright: ignore[reportAssignmentType]
    mkdir = sync_wrapper(_mkdir)
    rm_file = sync_wrapper(_rm_file)
    rmdir = sync_wrapper(_rmdir)
    rm = sync_wrapper(_rm)
    exists = sync_wrapper(_exists)  # pyright: ignore[reportAssignmentType]
    isfile = sync_wrapper(_isfile)
    isdir = sync_wrapper(_isdir)
    size = sync_wrapper(_size)
    modified = sync_wrapper(_modified)
    info = sync_wrapper(_info)


if __name__ == "__main__":
    import asyncio

    async def main():
        fs = SpritesFS(sprite_name="my-sprite", token="my-token")
        result = await fs._ls("/")
        print(result)

    asyncio.run(main())
