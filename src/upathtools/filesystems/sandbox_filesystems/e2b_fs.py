"""E2B async filesystem implementation for upathtools."""

import base64
import errno
import logging
import os
from typing import TYPE_CHECKING, Any, Literal, Required, overload

from upathtools.async_helpers import sync_wrapper
from upathtools.filesystems.base import BaseAsyncFileSystem, BaseUPath, FileInfo, GrepMatch
from upathtools.filesystems.sandbox_filesystems._shell_search import (
    build_grep_script,
    parse_grep_output,
)


if TYPE_CHECKING:
    from e2b.sandbox.filesystem.filesystem import EntryInfo

    from upathtools.filesystems.base import CreationMode


logger = logging.getLogger(__name__)


class E2BInfo(FileInfo, total=False):
    """Info dict for E2B filesystem paths."""

    size: Required[int]
    mtime: Required[float]
    mode: Required[int]
    permissions: Required[str]
    owner: Required[str]
    group: Required[str]
    symlink_target: Required[str | None]


def _is_not_found(exc: Exception) -> bool:
    """Whether an e2b error means the path does not exist."""
    from e2b.exceptions import NotFoundException

    if isinstance(exc, NotFoundException):
        return True
    message = str(exc).lower()
    return "not found" in message or "no such file" in message


def _entry_info(path: str, entry: EntryInfo) -> E2BInfo:
    """Convert e2b entry metadata to an info dict."""
    from e2b.sandbox.filesystem.filesystem import FileType

    return E2BInfo(
        name=path,
        size=entry.size,
        type="directory" if entry.type == FileType.DIR else "file",
        mtime=entry.modified_time.timestamp() if entry.modified_time else 0.0,
        mode=entry.mode,
        permissions=entry.permissions,
        owner=entry.owner,
        group=entry.group,
        symlink_target=entry.symlink_target,
    )


class E2BPath(BaseUPath[E2BInfo]):
    """E2B-specific UPath implementation."""

    __slots__ = ()


class E2BFS(BaseAsyncFileSystem[E2BPath, E2BInfo]):
    """Async filesystem for E2B sandbox environments.

    This filesystem provides access to files within an E2B sandbox environment,
    allowing you to read, write, and manipulate files remotely through the
    E2B native filesystem interface.
    """

    protocol = "e2b"
    upath_cls = E2BPath
    root_marker = "/"
    cachable = False  # Disable fsspec caching to prevent instance sharing

    def __init__(
        self,
        sandbox_id: str | None = None,
        api_key: str | None = None,
        template: str = "code-interpreter-v1",
        timeout: float = 300,
        **kwargs: Any,
    ) -> None:
        """Initialize E2B filesystem.

        Args:
            sandbox_id: Existing sandbox ID to connect to
            api_key: E2B API key
            template: E2B template to use for new sandboxes
            timeout: Default timeout for operations
            **kwargs: Additional filesystem options
        """
        super().__init__(**kwargs)
        self._sandbox_id = sandbox_id
        self._api_key = api_key
        self._template = template
        self._timeout = timeout
        self._sandbox = None
        self._session_started = False

    @staticmethod
    def _get_kwargs_from_urls(path: str) -> dict[str, Any]:
        path = path.removeprefix("e2b://")
        return {"sandbox_id": path}

    async def _get_sandbox(self):
        """Get or create E2B sandbox instance."""
        from e2b_code_interpreter import AsyncSandbox

        if self._sandbox is not None:
            return self._sandbox

        if self._sandbox_id:
            # Connect to existing sandbox
            self._sandbox = await AsyncSandbox.connect(self._sandbox_id, api_key=self._api_key)
        else:
            # Create new sandbox
            self._sandbox = await AsyncSandbox.create(self._template, api_key=self._api_key)
            assert self._sandbox
            self._sandbox_id = self._sandbox.sandbox_id

        return self._sandbox

    async def set_session(self) -> None:
        """Initialize the E2B session."""
        if not self._session_started:
            await self._get_sandbox()
            self._session_started = True

    async def close_session(self) -> None:
        """Close the E2B session."""
        if self._sandbox and self._session_started:
            await self._sandbox.kill()
            self._sandbox = None
            self._session_started = False

    @overload
    async def _ls(self, path: str, detail: Literal[True] = ..., **kwargs: Any) -> list[E2BInfo]: ...

    @overload
    async def _ls(self, path: str, detail: bool, **kwargs: Any) -> list[str]: ...

    async def _ls(self, path: str, detail: bool = True, **kwargs: Any) -> list[E2BInfo] | list[str]:
        """List directory contents with caching."""
        await self.set_session()
        sandbox = await self._get_sandbox()

        try:
            items = await sandbox.files.list(path)
        except Exception as exc:
            if _is_not_found(exc):
                raise FileNotFoundError(path) from exc
            msg = f"Failed to list directory {path}: {exc}"
            raise OSError(msg) from exc

        if not detail:
            return [item.path for item in items]

        from e2b.sandbox.filesystem.filesystem import FileType

        # list() already returns full EntryInfo (size, mode, owner, mtime, ...)
        return [
            E2BInfo(
                name=item.path,
                size=item.size,
                type="directory" if item.type == FileType.DIR else "file",
                mtime=item.modified_time.timestamp() if item.modified_time else 0,
                mode=item.mode,
                permissions=item.permissions,
                owner=item.owner,
                group=item.group,
                symlink_target=item.symlink_target,
            )
            for item in items
        ]

    async def _cat_file(
        self, path: str, start: int | None = None, end: int | None = None, **kwargs: Any
    ) -> bytes:
        """Read file contents."""
        await self.set_session()
        sandbox = await self._get_sandbox()

        try:
            content = bytes(await sandbox.files.read(path, format="bytes"))
        except Exception as exc:
            if _is_not_found(exc):
                raise FileNotFoundError(path) from exc
            if "is a directory" in str(exc).lower():
                raise IsADirectoryError(path) from exc
            msg = f"Failed to read file {path}: {exc}"
            raise OSError(msg) from exc

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

        # Read local file
        with open(lpath, "rb") as f:  # noqa: PTH123
            data = f.read()

        await self._pipe_file(rpath, data, **kwargs)

    async def _pipe_file(
        self, path: str, value: bytes, mode: CreationMode = "overwrite", **kwargs: Any
    ) -> None:
        """Write data to a file in the sandbox."""
        await self.set_session()
        sandbox = await self._get_sandbox()

        try:
            # Convert bytes to string for E2B - they handle encoding
            if isinstance(value, bytes):
                try:
                    content = value.decode("utf-8")
                except UnicodeDecodeError:
                    # For binary files, encode as base64 and write a script to decode
                    encoded = base64.b64encode(value).decode("ascii")
                    decode_script = f"""
import base64
with open({path!r}, 'wb') as f:
    f.write(base64.b64decode({encoded!r}))
"""
                    # Execute the decode script
                    execution = await sandbox.run_code(decode_script)
                    if execution.error:
                        raise OSError(f"Failed to write binary file: {execution.error.value}")  # noqa: B904
                    return
            else:
                content = value

            await sandbox.files.write(path, content)
        except Exception as exc:
            msg = f"Failed to write file {path}: {exc}"
            raise OSError(msg) from exc

    async def _mkdir(self, path: str, create_parents: bool = True, **kwargs: Any) -> None:
        """Create a directory."""
        await self.set_session()
        sandbox = await self._get_sandbox()

        try:
            await sandbox.files.make_dir(path)
        except Exception as exc:
            # E2B make_dir might not have create_parents option
            if create_parents and "parent" in str(exc).lower():
                # Try to create parent directories first
                parent = os.path.dirname(path)  # noqa: PTH120
                if parent and parent not in (path, "/"):
                    await self._mkdir(parent, create_parents=True)
                    await sandbox.files.make_dir(path)
            else:
                raise OSError(f"Failed to create directory {path}: {exc}") from exc

    async def _rm_file(self, path: str, **kwargs: Any) -> None:
        """Remove a file."""
        await self.set_session()
        sandbox = await self._get_sandbox()

        # e2b's remove succeeds silently for missing paths; fsspec expects an error.
        if not await sandbox.files.exists(path):
            raise FileNotFoundError(path)
        try:
            await sandbox.files.remove(path)
        except Exception as exc:
            if _is_not_found(exc):
                raise FileNotFoundError(path) from exc
            if "is a directory" in str(exc).lower():
                raise IsADirectoryError(path) from exc
            msg = f"Failed to remove file {path}: {exc}"
            raise OSError(msg) from exc

    async def _rmdir(self, path: str, **kwargs: Any) -> None:
        """Remove a directory."""
        await self.set_session()
        sandbox = await self._get_sandbox()

        # e2b's remove succeeds silently for missing paths and deletes directories
        # recursively; fsspec's rmdir must fail for missing and non-empty directories.
        if not await sandbox.files.exists(path):
            raise FileNotFoundError(path)
        if await sandbox.files.list(path, depth=1):
            raise OSError(errno.ENOTEMPTY, "Directory not empty", path)
        try:
            await sandbox.files.remove(path)
        except Exception as exc:
            if _is_not_found(exc):
                raise FileNotFoundError(path) from exc
            if "not a directory" in str(exc).lower():
                raise NotADirectoryError(path) from exc
            if "not empty" in str(exc).lower():
                msg = f"Directory not empty: {path}"
                raise OSError(msg) from exc
            msg = f"Failed to remove directory {path}: {exc}"
            raise OSError(msg) from exc

    async def _exists(self, path: str, **kwargs: Any) -> bool:
        """Check if path exists."""
        await self.set_session()
        sandbox = await self._get_sandbox()

        try:
            return await sandbox.files.exists(path)
        except Exception:  # noqa: BLE001
            return False

    async def _isfile(self, path: str, **kwargs: Any) -> bool:
        """Check if path is a file."""
        from e2b.sandbox.filesystem.filesystem import FileType

        await self.set_session()
        sandbox = await self._get_sandbox()
        try:
            info = await sandbox.files.get_info(path)
        except Exception:  # noqa: BLE001
            return False
        else:
            return info.type == FileType.FILE

    async def _isdir(self, path: str, **kwargs: Any) -> bool:
        """Check if path is a directory."""
        from e2b.sandbox.filesystem.filesystem import FileType

        await self.set_session()
        sandbox = await self._get_sandbox()
        try:
            info = await sandbox.files.get_info(path)
        except Exception:  # noqa: BLE001
            return False
        else:
            return info.type == FileType.DIR

    async def _size(self, path: str, **kwargs: Any) -> int:
        """Get file size."""
        await self.set_session()
        sandbox = await self._get_sandbox()

        try:
            info = await sandbox.files.get_info(path)
        except Exception as exc:
            if _is_not_found(exc):
                raise FileNotFoundError(path) from exc
            raise OSError(f"Failed to get file size for {path}: {exc}") from exc
        else:
            return info.size

    async def _modified(self, path: str, **kwargs: Any) -> float:
        """Get file modification time."""
        await self.set_session()
        sandbox = await self._get_sandbox()

        try:
            info = await sandbox.files.get_info(path)
            return info.modified_time.timestamp() if info.modified_time else 0.0
        except Exception as exc:
            if _is_not_found(exc):
                raise FileNotFoundError(path) from exc
            raise OSError(f"Failed to get modification time for {path}: {exc}") from exc

    async def _info(self, path: str, **kwargs: Any) -> E2BInfo:
        """Get info about a file or directory."""
        await self.set_session()
        sandbox = await self._get_sandbox()
        try:
            return _entry_info(path, await sandbox.files.get_info(path))
        except Exception as exc:
            if _is_not_found(exc):
                raise FileNotFoundError(path) from exc
            msg = f"Failed to get info for {path}: {exc}"
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
    ) -> dict[str, E2BInfo]: ...

    async def _find(
        self,
        path: str,
        maxdepth: int | None = None,
        withdirs: bool = False,
        *,
        detail: bool = False,
        **kwargs: Any,
    ) -> list[str] | dict[str, E2BInfo]:
        """Recursively list all files using E2B's depth parameter.

        More efficient than walking the directory tree with multiple _ls calls.
        """
        from e2b.sandbox.filesystem.filesystem import FileType

        await self.set_session()
        sandbox = await self._get_sandbox()
        # Use a large depth for recursive listing (E2B doesn't support None for unlimited)
        depth = maxdepth if maxdepth is not None else 100
        try:
            items = await sandbox.files.list(path, depth=depth)
        except Exception as exc:
            if _is_not_found(exc):
                raise FileNotFoundError(path) from exc
            msg = f"Failed to find files in {path}: {exc}"
            raise OSError(msg) from exc
        else:
            filtered = items if withdirs else [item for item in items if item.type == FileType.FILE]
            if detail:
                return {item.path: _entry_info(item.path, item) for item in filtered}
            return [item.path for item in filtered]

    async def _run_shell(self, script: str) -> str:
        """Run a shell script in the sandbox and return its stdout."""
        from e2b.sandbox.commands.command_handle import CommandExitException

        await self.set_session()
        sandbox = await self._get_sandbox()
        try:
            result = await sandbox.commands.run(script)
        except CommandExitException as exc:
            return exc.stdout
        return result.stdout

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
        """Search files inside the sandbox with ripgrep (or grep as fallback).

        Runs next to the data instead of downloading every file.
        """
        script = build_grep_script(
            path,
            pattern,
            max_count=max_count,
            case_sensitive=case_sensitive,
            hidden=hidden,
            no_ignore=no_ignore,
            globs=globs,
            multiline=multiline,
        )
        return parse_grep_output(await self._run_shell(script), max_count=max_count)

    async def _mv(self, path1: str, path2: str, **kwargs: Any) -> None:
        """Move or rename a file or directory (native rename)."""
        await self.set_session()
        sandbox = await self._get_sandbox()
        try:
            await sandbox.files.rename(path1, path2)
        except Exception as exc:
            if _is_not_found(exc):
                raise FileNotFoundError(path1) from exc
            msg = f"Failed to move {path1} to {path2}: {exc}"
            raise OSError(msg) from exc

    # This would be faster than fsspec's default implementations.

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
    find = sync_wrapper(_find)  # pyright: ignore[reportAssignmentType]
    mv = sync_wrapper(_mv)


if __name__ == "__main__":
    import asyncio

    async def main():
        fs = E2BFS()
        result = await fs._isdir("somepath")
        print(result)

    asyncio.run(main())
