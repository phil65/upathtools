"""Pyodide fsspec filesystem implementation."""

from __future__ import annotations

import base64
from collections.abc import Awaitable, Callable
from pathlib import Path
from typing import TYPE_CHECKING, Any, Literal, TypedDict, overload

from upathtools.async_helpers import sync_wrapper
from upathtools.filesystems.base import BaseAsyncFileFileSystem, BaseUPath


if TYPE_CHECKING:
    from upathtools.filesystems.base.basefilesystem import CreationMode


PyodideMethod = Literal[
    "fs_ls",
    "fs_cat",
    "fs_write",
    "fs_mkdir",
    "fs_rm",
    "fs_rmdir",
    "fs_stat",
    "fs_exists",
]

# Type alias for filesystem request callback
PyodideFilesystemCallback = Callable[[PyodideMethod, dict[str, Any]], Awaitable[Any]]
SERVER_SCRIPT = Path(__file__).parent / "pyodide_server.ts"


def build_command(
    deno_executable: str,
    allow_net: bool | list[str],
    allow_read: bool | list[str],
    allow_write: bool | list[str],
    allow_env: bool | list[str],
    allow_run: bool | list[str],
    allow_ffi: bool | list[str],
) -> list[str]:
    """Build the Deno command with permissions."""
    cmd = [deno_executable, "run"]
    # Build permission flags
    permission_defs = [
        ("--allow-net", allow_net),
        ("--allow-read", allow_read),
        ("--allow-write", allow_write),
        ("--allow-env", allow_env),
        ("--allow-run", allow_run),
        ("--allow-ffi", allow_ffi),
    ]

    for flag, value in permission_defs:
        match value:
            case True:
                cmd.append(flag)
            case list() if value:
                perm = f"{flag}={','.join(value)}"
                cmd.append(perm)
            case _:
                pass
    # Always need read for node_modules (Pyodide downloads)
    if not allow_read:
        cmd.append("--allow-read=node_modules")
    if not allow_write:
        cmd.append("--allow-write=node_modules")

    cmd.append("--node-modules-dir=auto")
    cmd.append(str(SERVER_SCRIPT))
    return cmd


class PyodideFileInfo(TypedDict):
    """Info dict for Pyodide filesystem paths."""

    name: str
    size: int
    type: Literal["file", "directory"]
    mtime: float | None


def _to_file_info(data: dict[str, Any]) -> PyodideFileInfo:
    """Convert a dict to PyodideFileInfo."""
    return PyodideFileInfo(
        name=data["name"],
        size=data["size"],
        type=data["type"],
        mtime=data.get("mtime"),
    )


class PyodidePath(BaseUPath[PyodideFileInfo]):
    """UPath implementation for browsing Pydantic BaseModel schemas."""

    __slots__ = ()


class PyodideFS(BaseAsyncFileFileSystem[PyodidePath, PyodideFileInfo]):  # type: ignore[misc]
    """Async filesystem for Pyodide environments.

    This filesystem provides access to files within the Pyodide WASM environment's
    in-memory filesystem (MEMFS). Files written here persist for the duration of
    the session but are lost when the environment is closed.

    Note: This filesystem operates on Pyodide's virtual filesystem, not the host.
    """

    protocol = "pyodide"
    root_marker = "/"
    cachable = False
    upath_cls = PyodidePath

    def __init__(self, request_callback: PyodideFilesystemCallback, **kwargs: Any) -> None:
        """Initialize Pyodide filesystem.

        Args:
            request_callback: Async callback for sending filesystem requests
            **kwargs: Additional filesystem arguments
        """
        super().__init__(**kwargs)
        self._request_callback = request_callback

    async def _fs_request(self, method: PyodideMethod, params: dict[str, Any]) -> Any:
        """Send a filesystem request via the injected callback."""
        return await self._request_callback(method, params)

    @overload
    async def _ls(
        self,
        path: str,
        detail: Literal[True] = ...,
        **kwargs: Any,
    ) -> list[PyodideFileInfo]: ...

    @overload
    async def _ls(self, path: str, detail: bool, **kwargs: Any) -> list[str]: ...

    async def _ls(
        self,
        path: str,
        detail: bool = False,
        **kwargs: Any,
    ) -> list[str] | list[PyodideFileInfo]:
        """List directory contents."""
        try:
            entries = await self._fs_request("fs_ls", {"path": path})
            if detail:
                return [_to_file_info(e) for e in entries]
            return [e["name"] for e in entries]
        except RuntimeError as e:
            if "No such file or directory" in str(e) or "FileNotFoundError" in str(e):
                msg = f"Path not found: {path}"
                raise FileNotFoundError(msg) from e
            raise

    async def _cat_file(
        self,
        path: str,
        start: int | None = None,
        end: int | None = None,
        **kwargs: Any,
    ) -> bytes:
        """Read file contents."""
        try:
            result = await self._fs_request("fs_cat", {"path": path})
            content = base64.b64decode(result["content"])
            if start is not None or end is not None:
                return content[start:end]
        except RuntimeError as e:
            if "No such file or directory" in str(e) or "FileNotFoundError" in str(e):
                msg = f"File not found: {path}"
                raise FileNotFoundError(msg) from e
            raise
        else:
            return content

    async def _pipe_file(
        self,
        path: str,
        value: bytes,
        mode: CreationMode = "overwrite",
        **kwargs: Any,
    ) -> None:
        """Write file contents."""
        content_b64 = base64.b64encode(value).decode("ascii")
        await self._fs_request("fs_write", {"path": path, "content": content_b64})

    async def _mkdir(self, path: str, create_parents: bool = True, **kwargs: Any) -> None:
        """Create directory."""
        await self._fs_request("fs_mkdir", {"path": path, "recursive": create_parents})

    async def _rm_file(self, path: str, **kwargs: Any) -> None:
        """Remove file."""
        await self._fs_request("fs_rm", {"path": path})

    async def _rmdir(self, path: str, **kwargs: Any) -> None:
        """Remove directory."""
        await self._fs_request("fs_rmdir", {"path": path, "recursive": False})

    async def _rm(
        self,
        path: str,
        recursive: bool = False,
        batch_size: int | None = None,
        **kwargs: Any,
    ) -> None:
        """Remove file or directory."""
        if recursive:
            await self._fs_request("fs_rmdir", {"path": path, "recursive": True})
        else:
            # Check if it's a file or directory
            try:
                info = await self._info(path)
                if info["type"] == "directory":
                    await self._rmdir(path)
                else:
                    await self._rm_file(path)
            except FileNotFoundError:
                pass  # Already gone

    async def _exists(self, path: str, **kwargs: Any) -> bool:
        """Check if path exists."""
        result = await self._fs_request("fs_exists", {"path": path})
        return bool(result)

    async def _info(self, path: str, **kwargs: Any) -> PyodideFileInfo:
        """Get file/directory info."""
        try:
            result = await self._fs_request("fs_stat", {"path": path})
            return _to_file_info(result)
        except RuntimeError as e:
            if "No such file or directory" in str(e) or "FileNotFoundError" in str(e):
                msg = f"Path not found: {path}"
                raise FileNotFoundError(msg) from e
            raise

    async def _isfile(self, path: str, **kwargs: Any) -> bool:
        """Check if path is a file."""
        try:
            info = await self._info(path)
            return info["type"] == "file"
        except FileNotFoundError:
            return False

    async def _isdir(self, path: str, **kwargs: Any) -> bool:
        """Check if path is a directory."""
        try:
            info = await self._info(path)
            return info["type"] == "directory"
        except FileNotFoundError:
            return False

    async def _size(self, path: str, **kwargs: Any) -> int:
        """Get file size."""
        info = await self._info(path)
        return info["size"]

    async def _modified(self, path: str, **kwargs: Any) -> float:
        """Get file modification time."""
        info = await self._info(path)
        return info.get("mtime") or 0.0

    # Sync wrapper methods
    ls = sync_wrapper(_ls)  # pyright: ignore[reportAssignmentType]
    cat_file = sync_wrapper(_cat_file)  # pyright: ignore[reportAssignmentType]
    pipe_file = sync_wrapper(_pipe_file)  # pyright: ignore[reportAssignmentType]
    mkdir = sync_wrapper(_mkdir)
    rm_file = sync_wrapper(_rm_file)
    rmdir = sync_wrapper(_rmdir)
    rm = sync_wrapper(_rm)  # pyright: ignore[reportAssignmentType]
    exists = sync_wrapper(_exists)  # pyright: ignore[reportAssignmentType]
    info = sync_wrapper(_info)
    isfile = sync_wrapper(_isfile)
    isdir = sync_wrapper(_isdir)
    size = sync_wrapper(_size)
    modified = sync_wrapper(_modified)
