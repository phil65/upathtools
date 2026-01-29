"""Daytona async filesystem implementation for upathtools."""

from __future__ import annotations

from datetime import datetime
import logging
import os
from typing import TYPE_CHECKING, Any, Literal, overload

from upathtools.async_helpers import sync_wrapper
from upathtools.filesystems.base import BaseAsyncFileSystem, BaseUPath, FileInfo, GrepMatch


if TYPE_CHECKING:
    from daytona import AsyncDaytona, AsyncSandbox

    from upathtools.filesystems.base import CreationMode


logger = logging.getLogger(__name__)


def _parse_daytona_time(time_str: str | None) -> float:
    """Parse Daytona timestamp format to Unix timestamp.

    Args:
        time_str: Timestamp like '2025-11-30 18:14:39.486327786 +0000 UTC'
    """
    if not time_str:
        return 0.0
    try:
        # Format: '2025-11-30 18:14:39.486327786 +0000 UTC'
        # Truncate nanoseconds to microseconds and remove 'UTC' suffix
        parts = time_str.rsplit(" ", 1)  # Split off 'UTC'
        dt_str = parts[0]  # '2025-11-30 18:14:39.486327786 +0000'
        # Truncate nanoseconds (9 digits) to microseconds (6 digits)
        if "." in dt_str:
            base, rest = dt_str.split(".")
            frac_and_tz = rest.split(" ", 1)
            frac = frac_and_tz[0][:6]  # Take only first 6 digits
            tz = frac_and_tz[1] if len(frac_and_tz) > 1 else "+0000"
            dt_str = f"{base}.{frac} {tz}"
        dt = datetime.strptime(dt_str, "%Y-%m-%d %H:%M:%S.%f %z")
        return dt.timestamp()
    except (ValueError, IndexError):
        return 0.0


class DaytonaInfo(FileInfo, total=False):
    """Info dict for Daytona filesystem paths."""

    size: int
    mtime: float
    mode: int | None
    permissions: str | None
    owner: str | None
    group: str | None


class DaytonaPath(BaseUPath[DaytonaInfo]):
    """Daytona-specific UPath implementation."""

    __slots__ = ()


class DaytonaFS(BaseAsyncFileSystem[DaytonaPath, DaytonaInfo]):
    """Async filesystem for Daytona sandbox environments.

    This filesystem provides access to files within a Daytona sandbox environment,
    allowing you to read, write, and manipulate files remotely through the
    Daytona native filesystem interface.
    """

    protocol = "daytona"
    upath_cls = DaytonaPath
    root_marker = "/"
    cachable = False  # Disable fsspec caching to prevent instance sharing

    def __init__(
        self,
        sandbox_id: str | None = None,
        api_key: str | None = None,
        **kwargs: Any,
    ) -> None:
        """Initialize Daytona filesystem.

        Args:
            sandbox_id: Existing sandbox ID to connect to
            api_key: Daytona API key
            **kwargs: Additional filesystem options
        """
        super().__init__(**kwargs)
        self._sandbox_id = sandbox_id
        self._api_key = api_key
        self._sandbox: AsyncSandbox | None = None
        self._daytona: AsyncDaytona | None = None
        self._session_started = False

    @staticmethod
    def _get_kwargs_from_urls(path: str) -> dict[str, Any]:
        path = path.removeprefix("daytona://")
        return {"sandbox_id": path}

    async def _get_sandbox(self):
        """Get or create Daytona sandbox instance."""
        from daytona import AsyncDaytona, DaytonaConfig

        if self._sandbox is not None:
            return self._sandbox

        config = DaytonaConfig(api_key=self._api_key)
        self._daytona = AsyncDaytona(config)
        if self._sandbox_id:  # Connect to existing
            self._sandbox = await self._daytona.get(self._sandbox_id)
        else:
            self._sandbox = await self._daytona.create()
        self._sandbox_id = self._sandbox.id
        return self._sandbox

    async def set_session(self) -> None:
        """Initialize the Daytona session."""
        if not self._session_started:
            await self._get_sandbox()
            self._session_started = True

    async def close_session(self) -> None:
        """Close the Daytona session."""
        if self._sandbox and self._session_started:
            await self._sandbox.delete()
            self._sandbox = None
            self._session_started = False

    @overload
    async def _ls(
        self, path: str, detail: Literal[True] = ..., **kwargs: Any
    ) -> list[DaytonaInfo]: ...

    @overload
    async def _ls(self, path: str, detail: Literal[False], **kwargs: Any) -> list[str]: ...

    async def _ls(
        self, path: str, detail: bool = True, **kwargs: Any
    ) -> list[DaytonaInfo] | list[str]:
        """List directory contents with caching."""
        await self.set_session()
        sandbox = await self._get_sandbox()
        try:
            file_infos = await sandbox.fs.list_files(path)
        except Exception as exc:
            if "not found" in str(exc).lower() or "no such file" in str(exc).lower():
                raise FileNotFoundError(path) from exc
            msg = f"Failed to list directory {path}: {exc}"
            raise OSError(msg) from exc

        if not detail:
            return [info.name for info in file_infos]

        return [
            DaytonaInfo(
                name=f"{path.rstrip('/')}/{info.name}",
                size=int(info.size),
                type="directory" if info.is_dir else "file",
                mtime=_parse_daytona_time(info.mod_time),
                mode=int(info.mode) if info.mode else 0,
                permissions=info.permissions,
                owner=info.owner,
                group=info.group,
            )
            for info in file_infos
        ]

    async def _cat_file(
        self, path: str, start: int | None = None, end: int | None = None, **kwargs: Any
    ) -> bytes:
        """Read file contents."""
        await self.set_session()
        sandbox = await self._get_sandbox()
        try:
            content = await sandbox.fs.download_file(path)
        except Exception as exc:
            if "not found" in str(exc).lower() or "no such file" in str(exc).lower():
                raise FileNotFoundError(path) from exc
            if "is a directory" in str(exc).lower():
                raise IsADirectoryError(path) from exc
            msg = f"Failed to read file {path}: {exc}"
            raise OSError(msg) from exc

        if isinstance(content, str):  # Ensure we have bytes
            content = content.encode()
        # Handle byte ranges if specified
        if start is not None or end is not None:
            start = start or 0
            end = end or len(content)
            content = content[start:end]
        return content

    async def _put_file(
        self,
        lpath: str,
        rpath: str,
        callback=None,
        **kwargs: Any,
    ) -> None:
        """Upload a local file to the sandbox."""
        await self.set_session()
        sandbox = await self._get_sandbox()
        try:
            await sandbox.fs.upload_file(lpath, rpath)
        except Exception as exc:
            msg = f"Failed to upload file from {lpath} to {rpath}: {exc}"
            raise OSError(msg) from exc

    async def _pipe_file(
        self, path: str, value: bytes, mode: CreationMode = "overwrite", **kwargs: Any
    ) -> None:
        """Write data to a file in the sandbox."""
        await self.set_session()
        sandbox = await self._get_sandbox()
        try:
            await sandbox.fs.upload_file(value, path)
        except Exception as exc:
            msg = f"Failed to write file {path}: {exc}"
            raise OSError(msg) from exc

    async def _mkdir(self, path: str, create_parents: bool = True, **kwargs: Any) -> None:
        """Create a directory."""
        await self.set_session()
        sandbox = await self._get_sandbox()
        try:
            # Daytona's create_folder uses octal permissions as string
            await sandbox.fs.create_folder(path, "755")
        except Exception as exc:
            if create_parents and "parent" in str(exc).lower():
                # Try to create parent directories first
                parent = os.path.dirname(path)  # noqa: PTH120
                if parent and parent not in (path, "/"):
                    await self._mkdir(parent, create_parents=True)
                    await sandbox.fs.create_folder(path, "755")
            else:
                msg = f"Failed to create directory {path}: {exc}"
                raise OSError(msg) from exc

    async def _rm_file(self, path: str, **kwargs: Any) -> None:
        """Remove a file."""
        await self.set_session()
        sandbox = await self._get_sandbox()
        try:
            await sandbox.fs.delete_file(path)
        except Exception as exc:
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
            # doesnt have delete-dir, so workaround.
            await sandbox.fs.move_files(path, "tmp/upathools")
        except Exception as exc:
            if "not found" in str(exc).lower() or "no such file" in str(exc).lower():
                raise FileNotFoundError(path) from exc
            if "not a directory" in str(exc).lower():
                raise NotADirectoryError(path) from exc
            if "not empty" in str(exc).lower():
                raise OSError(f"Directory not empty: {path}") from exc
            msg = f"Failed to remove directory {path}: {exc}"
            raise OSError(msg) from exc

    async def _exists(self, path: str, **kwargs: Any) -> bool:
        """Check if path exists."""
        await self.set_session()
        sandbox = await self._get_sandbox()
        try:
            await sandbox.fs.get_file_info(path)
        except Exception:  # noqa: BLE001
            return False
        else:
            return True

    async def _isfile(self, path: str, **kwargs: Any) -> bool:
        """Check if path is a file."""
        await self.set_session()
        sandbox = await self._get_sandbox()
        try:
            info = await sandbox.fs.get_file_info(path)
        except Exception:  # noqa: BLE001
            return False
        else:
            return not info.is_dir

    async def _isdir(self, path: str, **kwargs: Any) -> bool:
        """Check if path is a directory."""
        await self.set_session()
        sandbox = await self._get_sandbox()
        try:
            info = await sandbox.fs.get_file_info(path)
        except Exception:  # noqa: BLE001
            return False
        else:
            return info.is_dir

    async def _size(self, path: str, **kwargs: Any) -> int:
        """Get file size."""
        await self.set_session()
        sandbox = await self._get_sandbox()
        try:
            info = await sandbox.fs.get_file_info(path)
        except Exception as exc:
            if "not found" in str(exc).lower() or "no such file" in str(exc).lower():
                raise FileNotFoundError(path) from exc
            msg = f"Failed to get file size for {path}: {exc}"
            raise OSError(msg) from exc
        else:
            return int(info.size)

    async def _modified(self, path: str, **kwargs: Any) -> float:
        """Get file modification time."""
        await self.set_session()
        sandbox = await self._get_sandbox()

        try:
            info = await sandbox.fs.get_file_info(path)
            return _parse_daytona_time(info.mod_time)
        except Exception as exc:
            if "not found" in str(exc).lower() or "no such file" in str(exc).lower():
                raise FileNotFoundError(path) from exc
            msg = f"Failed to get modification time for {path}: {exc}"
            raise OSError(msg) from exc

    async def _info(self, path: str, **kwargs: Any) -> DaytonaInfo:
        """Get info about a file or directory."""
        await self.set_session()
        sandbox = await self._get_sandbox()
        try:
            info = await sandbox.fs.get_file_info(path)
            return DaytonaInfo(
                name=path,
                size=int(info.size),
                type="directory" if info.is_dir else "file",
                mtime=_parse_daytona_time(info.mod_time),
            )
        except Exception as exc:
            if "not found" in str(exc).lower() or "no such file" in str(exc).lower():
                raise FileNotFoundError(path) from exc
            msg = f"Failed to get info for {path}: {exc}"
            raise OSError(msg) from exc

    async def _mv_file(self, path1: str, path2: str, **kwargs: Any) -> None:
        """Move/rename a file."""
        await self.set_session()
        sandbox = await self._get_sandbox()
        try:
            await sandbox.fs.move_files(path1, path2)
        except Exception as exc:
            if "not found" in str(exc).lower() or "no such file" in str(exc).lower():
                raise FileNotFoundError(path1) from exc
            msg = f"Failed to move {path1} to {path2}: {exc}"
            raise OSError(msg) from exc

    @overload
    async def _find(
        self,
        path: str,
        maxdepth: int | None = None,
        withdirs: bool = False,
        *,
        detail: Literal[False] = False,
        **kwargs: Any,
    ) -> list[str]: ...

    @overload
    async def _find(
        self,
        path: str,
        maxdepth: int | None = None,
        withdirs: bool = False,
        *,
        detail: Literal[True],
        **kwargs: Any,
    ) -> dict[str, DaytonaInfo]: ...

    async def _find(
        self,
        path: str,
        maxdepth: int | None = None,
        withdirs: bool = False,
        *,
        detail: bool = False,
        **kwargs: Any,
    ) -> list[str] | dict[str, DaytonaInfo]:
        """Recursively list all files using Daytona's search_files.

        More efficient than walking the directory tree.
        """
        await self.set_session()
        sandbox = await self._get_sandbox()
        try:
            result = await sandbox.fs.search_files(path, "**/*")
        except Exception as exc:
            msg = f"Failed to find files in {path}: {exc}"
            raise OSError(msg) from exc
        else:
            files = result.files
            if detail:
                # Return dict with minimal info (search_files doesn't return metadata)
                return {
                    f: DaytonaInfo(
                        name=f,
                        type="file",  # search_files only returns files
                        size=0,
                        mtime=0.0,
                    )
                    for f in files
                }
            return files

    @overload
    async def _glob(
        self,
        path: str,
        maxdepth: int | None = None,
        *,
        detail: Literal[False] = False,
        **kwargs: Any,
    ) -> list[str]: ...

    @overload
    async def _glob(
        self,
        path: str,
        maxdepth: int | None = None,
        *,
        detail: Literal[True],
        **kwargs: Any,
    ) -> dict[str, DaytonaInfo]: ...

    async def _glob(
        self,
        path: str,
        maxdepth: int | None = None,
        *,
        detail: bool = True,
        **kwargs: Any,
    ) -> list[str] | dict[str, DaytonaInfo]:
        """Glob for files using Daytona's native search_files.

        This is more efficient than the default fsspec implementation
        which walks the directory tree.
        """
        # Check for glob magic characters
        glob_chars = {"*", "?", "["}

        # If no glob pattern, fall back to default
        if not any(c in path for c in glob_chars):
            if await self._exists(path):
                if detail:
                    info = await self._info(path)
                    return {path: info}
                return [path]
            return {} if detail else []

        await self.set_session()
        sandbox = await self._get_sandbox()

        # Split path into root directory and pattern
        # e.g., "/workspace/src/**/*.py" -> root="/workspace/src", pattern="**/*.py"
        path = self._strip_protocol(path)
        idx = min(
            (path.find(c) for c in "*?[" if path.find(c) >= 0),
            default=len(path),
        )
        if "/" in path[:idx]:
            root = path[: path[:idx].rindex("/") + 1].rstrip("/") or "/"
            pattern = path[len(root) :].lstrip("/")
        else:
            root = "/"
            pattern = path

        try:
            result = await sandbox.fs.search_files(root, pattern)
        except Exception as exc:
            msg = f"Failed to glob {path}: {exc}"
            raise OSError(msg) from exc

        files = result.files
        if not detail:
            return files

        # Return barebone info dicts (search_files doesn't return metadata)
        return {f: DaytonaInfo(name=f, type="file") for f in files}

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
        """Search for pattern in files."""
        await self.set_session()
        sandbox = await self._get_sandbox()
        try:
            matches = await sandbox.fs.find_files(path, pattern)
            result = [
                GrepMatch(path=m.file, line_number=int(m.line), text=m.content) for m in matches
            ]
        except Exception as exc:
            msg = f"Failed to grep pattern {pattern!r} in {path}: {exc}"
            raise OSError(msg) from exc
        else:
            if max_count is not None:
                return result[:max_count]
            return result

    async def _chmod(self, path: str, mode: int, **kwargs: Any) -> None:
        """Change file permissions."""
        await self.set_session()
        sandbox = await self._get_sandbox()
        try:
            # Convert integer mode to octal string
            mode_str = oct(mode)[2:]  # Remove '0o' prefix
            await sandbox.fs.set_file_permissions(path, mode=mode_str)
        except Exception as exc:
            if "not found" in str(exc).lower() or "no such file" in str(exc).lower():
                raise FileNotFoundError(path) from exc
            msg = f"Failed to change permissions for {path}: {exc}"
            raise OSError(msg) from exc

    # Sync wrappers for async methods
    ls = sync_wrapper(_ls)  # pyright: ignore[reportAssignmentType]
    cat_file = sync_wrapper(_cat_file)  # pyright: ignore[reportAssignmentType]
    put_file = sync_wrapper(_put_file)  # pyright: ignore[reportAssignmentType]
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
    mv_file = sync_wrapper(_mv_file)
    find = sync_wrapper(_find)  # pyright: ignore[reportAssignmentType]
    glob = sync_wrapper(_glob)  # pyright: ignore[reportAssignmentType]
    grep = sync_wrapper(_grep)
    chmod = sync_wrapper(_chmod)


if __name__ == "__main__":

    async def main():
        fs = DaytonaFS()
        await fs._mkdir("test")
        glob_result = await fs._glob("test")
        print(glob_result)

    import asyncio

    asyncio.run(main())
