"""HopX async filesystem implementation for upathtools."""

from __future__ import annotations

import logging
from typing import TYPE_CHECKING, Any, Literal, Required, overload

from upathtools.async_helpers import sync_wrapper
from upathtools.filesystems.base import BaseAsyncFileSystem, BaseUPath, FileInfo


if TYPE_CHECKING:
    from hopx_ai import AsyncSandbox

    from upathtools.filesystems.base import CreationMode


logger = logging.getLogger(__name__)


class HopXInfo(FileInfo, total=False):
    """Info dict for HopX filesystem paths."""

    size: Required[int]
    mtime: Required[float]
    permissions: Required[str]


class HopXPath(BaseUPath[HopXInfo]):
    """HopX-specific UPath implementation."""

    __slots__ = ()


class HopXFS(BaseAsyncFileSystem[HopXPath, HopXInfo]):
    """Async filesystem for HopX sandbox environments.

    This filesystem provides access to files within a HopX sandbox environment,
    allowing you to read, write, and manipulate files remotely through the
    HopX Python SDK.
    """

    protocol = "hopx"
    upath_cls = HopXPath
    root_marker = "/"
    cachable = False  # Disable fsspec caching to prevent instance sharing

    def __init__(
        self,
        sandbox_id: str | None = None,
        api_key: str | None = None,
        template: str = "code-interpreter",
        timeout_seconds: int | None = None,
        **kwargs: Any,
    ) -> None:
        """Initialize HopX filesystem.

        Args:
            sandbox_id: Existing sandbox ID to connect to
            api_key: HopX API key (or set HOPX_API_KEY env var)
            template: HopX template to use for new sandboxes
            timeout_seconds: Auto-kill timeout in seconds for new sandboxes
            **kwargs: Additional filesystem options
        """
        super().__init__(**kwargs)
        self._sandbox_id = sandbox_id
        self._api_key = api_key
        self._template = template
        self._timeout_seconds = timeout_seconds
        self._sandbox: AsyncSandbox | None = None
        self._session_started = False

    @staticmethod
    def _get_kwargs_from_urls(path: str) -> dict[str, Any]:
        path = path.removeprefix("hopx://")
        return {"sandbox_id": path}

    async def _get_sandbox(self) -> AsyncSandbox:
        """Get or create HopX sandbox instance."""
        from hopx_ai import AsyncSandbox

        if self._sandbox is not None:
            return self._sandbox

        if self._sandbox_id:
            self._sandbox = await AsyncSandbox.connect(self._sandbox_id, api_key=self._api_key)
        else:
            self._sandbox = await AsyncSandbox.create(
                self._template,
                api_key=self._api_key,
                timeout_seconds=self._timeout_seconds,
            )
            self._sandbox_id = self._sandbox.sandbox_id

        return self._sandbox

    async def set_session(self) -> None:
        """Initialize the HopX session."""
        if not self._session_started:
            await self._get_sandbox()
            self._session_started = True

    async def close_session(self) -> None:
        """Close the HopX session."""
        if self._sandbox and self._session_started:
            await self._sandbox.kill()
            self._sandbox = None
            self._session_started = False

    @overload
    async def _ls(
        self, path: str, detail: Literal[True] = ..., **kwargs: Any
    ) -> list[HopXInfo]: ...

    @overload
    async def _ls(self, path: str, detail: bool, **kwargs: Any) -> list[str]: ...

    async def _ls(
        self, path: str, detail: bool = True, **kwargs: Any
    ) -> list[HopXInfo] | list[str]:
        """List directory contents."""
        await self.set_session()
        sandbox = await self._get_sandbox()

        try:
            items = await sandbox.files.list(path)
        except Exception as exc:
            if "not found" in str(exc).lower() or "no such file" in str(exc).lower():
                raise FileNotFoundError(path) from exc
            msg = f"Failed to list directory {path}: {exc}"
            raise OSError(msg) from exc

        if not detail:
            return [item.path or item.name or "" for item in items]

        result = []
        for item in items:
            mtime = item.modified_time.timestamp() if item.modified_time else 0.0
            result.append(
                HopXInfo(
                    name=item.path or item.name or "",
                    size=item.size or 0,
                    type="directory" if item.is_directory else "file",
                    mtime=mtime,
                    permissions=item.permissions or "",
                )
            )

        return result

    async def _cat_file(
        self, path: str, start: int | None = None, end: int | None = None, **kwargs: Any
    ) -> bytes:
        """Read file contents."""
        await self.set_session()
        sandbox = await self._get_sandbox()

        try:
            content = await sandbox.files.read(path)
        except Exception as exc:
            if "not found" in str(exc).lower() or "no such file" in str(exc).lower():
                raise FileNotFoundError(path) from exc
            if "is a directory" in str(exc).lower():
                raise IsADirectoryError(path) from exc
            msg = f"Failed to read file {path}: {exc}"
            raise OSError(msg) from exc

        data = content.encode("utf-8") if isinstance(content, str) else content

        if start is not None or end is not None:
            data = data[start:end]

        return data

    async def _put_file(self, lpath: str, rpath: str, callback=None, **kwargs: Any) -> None:
        """Upload a local file to the sandbox."""
        await self.set_session()

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
            if isinstance(value, bytes):
                try:
                    content = value.decode("utf-8")
                    await sandbox.files.write(path, content)
                except UnicodeDecodeError:
                    await sandbox.files.write_bytes(path, value)
            else:
                await sandbox.files.write(path, value)
        except Exception as exc:
            msg = f"Failed to write file {path}: {exc}"
            raise OSError(msg) from exc

    async def _mkdir(self, path: str, create_parents: bool = True, **kwargs: Any) -> None:
        """Create a directory."""
        await self.set_session()
        sandbox = await self._get_sandbox()

        try:
            await sandbox.files.mkdir(path)
        except Exception as exc:
            msg = f"Failed to create directory {path}: {exc}"
            raise OSError(msg) from exc

    async def _rm_file(self, path: str, **kwargs: Any) -> None:
        """Remove a file."""
        await self.set_session()
        sandbox = await self._get_sandbox()

        try:
            await sandbox.files.remove(path)
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
            await sandbox.files.remove(path)
        except Exception as exc:
            if "not found" in str(exc).lower() or "no such file" in str(exc).lower():
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
        await self.set_session()
        sandbox = await self._get_sandbox()

        try:
            await sandbox.files.list(path)
            # If listing succeeds for the path itself (not its contents),
            # it means the path is a directory. If it raises, check exists.
            # HopX list returns contents of a directory, not the entry itself.
            # So if list succeeds, it's a directory.
        except Exception:  # noqa: BLE001
            # If listing fails, check if it exists as a file
            try:
                await sandbox.files.read(path)
            except Exception:  # noqa: BLE001
                return False
            else:
                return True
        else:
            return False

    async def _isdir(self, path: str, **kwargs: Any) -> bool:
        """Check if path is a directory."""
        await self.set_session()
        sandbox = await self._get_sandbox()

        try:
            await sandbox.files.list(path)
        except Exception:  # noqa: BLE001
            return False
        else:
            return True

    async def _size(self, path: str, **kwargs: Any) -> int:
        """Get file size."""
        await self.set_session()
        sandbox = await self._get_sandbox()

        try:
            content = await sandbox.files.read(path)
            return len(content.encode("utf-8") if isinstance(content, str) else content)
        except Exception as exc:
            if "not found" in str(exc).lower() or "no such file" in str(exc).lower():
                raise FileNotFoundError(path) from exc
            msg = f"Failed to get file size for {path}: {exc}"
            raise OSError(msg) from exc

    async def _modified(self, path: str, **kwargs: Any) -> float:
        """Get file modification time."""
        await self.set_session()
        sandbox = await self._get_sandbox()

        try:
            # Get info by listing the parent directory and finding the entry
            parent = path.rsplit("/", 1)[0] or "/"
            items = await sandbox.files.list(parent)
            for item in items:
                if (item.path or item.name) == path or (item.name and path.endswith(item.name)):
                    if item.modified_time:
                        return item.modified_time.timestamp()
                    return 0.0
        except Exception as exc:
            if "not found" in str(exc).lower() or "no such file" in str(exc).lower():
                raise FileNotFoundError(path) from exc
            msg = f"Failed to get modification time for {path}: {exc}"
            raise OSError(msg) from exc

        raise FileNotFoundError(path)

    async def _info(self, path: str, **kwargs: Any) -> HopXInfo:
        """Get info about a file or directory."""
        await self.set_session()
        sandbox = await self._get_sandbox()

        # Try listing as directory first
        try:
            await sandbox.files.list(path)
            is_dir = True
        except Exception:  # noqa: BLE001
            is_dir = False

        if is_dir:
            # Get metadata from parent listing
            mtime = 0.0
            permissions = ""
            if path != "/":
                try:
                    parent = path.rsplit("/", 1)[0] or "/"
                    items = await sandbox.files.list(parent)
                    for item in items:
                        item_path = item.path or item.name or ""
                        if item_path == path or (item.name and path.endswith(item.name)):
                            mtime = item.modified_time.timestamp() if item.modified_time else 0.0
                            permissions = item.permissions or ""
                            break
                except Exception:  # noqa: BLE001
                    pass
            return HopXInfo(
                name=path,
                size=0,
                type="directory",
                mtime=mtime,
                permissions=permissions,
            )

        # It's a file - get info from parent listing
        try:
            parent = path.rsplit("/", 1)[0] or "/"
            items = await sandbox.files.list(parent)
            for item in items:
                item_path = item.path or item.name or ""
                if item_path == path or (item.name and path.endswith(item.name)):
                    mtime = item.modified_time.timestamp() if item.modified_time else 0.0
                    return HopXInfo(
                        name=path,
                        size=item.size or 0,
                        type="file",
                        mtime=mtime,
                        permissions=item.permissions or "",
                    )
        except Exception as exc:
            if "not found" in str(exc).lower() or "no such file" in str(exc).lower():
                raise FileNotFoundError(path) from exc
            msg = f"Failed to get info for {path}: {exc}"
            raise OSError(msg) from exc

        raise FileNotFoundError(path)

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
        fs = HopXFS()
        result = await fs._ls("/workspace")
        print(result)

    asyncio.run(main())
