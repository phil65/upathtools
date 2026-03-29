"""Codex CLI async filesystem implementation for upathtools."""

from __future__ import annotations

import base64
import logging
from typing import TYPE_CHECKING, Any, Literal, Required, overload

from upathtools.async_helpers import sync_wrapper
from upathtools.filesystems.base import BaseAsyncFileSystem, BaseUPath, FileInfo


if TYPE_CHECKING:
    from codexed.client import CodexClient

    from upathtools.filesystems.base import CreationMode


logger = logging.getLogger(__name__)


class CodexInfo(FileInfo, total=False):
    """Info dict for Codex filesystem paths."""

    size: Required[int]
    created: Required[float]
    mtime: Required[float]


class CodexPath(BaseUPath[CodexInfo]):
    """Codex-specific UPath implementation."""

    __slots__ = ()


class CodexFS(BaseAsyncFileSystem[CodexPath, CodexInfo]):
    """Async filesystem for Codex CLI sandbox environments.

    This filesystem provides access to files within a Codex CLI sandbox,
    allowing you to read, write, and manipulate files remotely through
    the Codex client's filesystem API.

    Requires the ``codexed`` package (optional dependency).
    """

    protocol = "codex"
    upath_cls = CodexPath
    root_marker = "/"
    cachable = False

    def __init__(self, client: CodexClient | None = None, **kwargs: Any) -> None:
        """Initialize Codex filesystem.

        Args:
            client: An existing CodexClient instance. If not provided,
                    one will be created (and must be started via set_session).
            **kwargs: Additional filesystem options.
        """
        super().__init__(**kwargs)
        self._client = client
        self._owns_client = client is None

    async def _get_client(self) -> CodexClient:
        """Get or create a CodexClient instance."""
        if self._client is None:
            from codexed.client import CodexClient

            self._client = CodexClient()
            self._owns_client = True
            await self._client.__aenter__()
        return self._client

    async def set_session(self) -> None:
        """Initialize the Codex session."""
        await self._get_client()

    async def close_session(self) -> None:
        """Close the Codex session if we own it."""
        if self._client and self._owns_client:
            await self._client.__aexit__(None, None, None)
            self._client = None

    @overload
    async def _ls(
        self, path: str, detail: Literal[True] = ..., **kwargs: Any
    ) -> list[CodexInfo]: ...

    @overload
    async def _ls(self, path: str, detail: bool, **kwargs: Any) -> list[str]: ...

    async def _ls(
        self, path: str, detail: bool = True, **kwargs: Any
    ) -> list[CodexInfo] | list[str]:
        """List directory contents."""
        client = await self._get_client()

        try:
            entries = await client.fs.read_directory(path)
        except Exception as exc:
            if "not found" in str(exc).lower() or "no such" in str(exc).lower():
                raise FileNotFoundError(path) from exc
            raise OSError(f"Failed to list directory {path}: {exc}") from exc

        if not detail:
            return [f"{path.rstrip('/')}/{e.file_name}" for e in entries]

        results: list[CodexInfo] = []
        for entry in entries:
            full_path = f"{path.rstrip('/')}/{entry.file_name}"
            file_type: Literal["file", "directory"] = "directory" if entry.is_directory else "file"
            # Try to get metadata for timestamps
            try:
                meta = await client.fs.get_metadata(full_path)
                results.append(
                    CodexInfo(
                        name=full_path,
                        type=file_type,
                        size=0,
                        created=meta.created_at_ms / 1000.0,
                        mtime=meta.modified_at_ms / 1000.0,
                    )
                )
            except Exception:  # noqa: BLE001
                results.append(
                    CodexInfo(
                        name=full_path,
                        type=file_type,
                        size=0,
                        created=0.0,
                        mtime=0.0,
                    )
                )

        return results

    async def _info(self, path: str, **kwargs: Any) -> CodexInfo:
        """Get info about a file or directory."""
        client = await self._get_client()
        try:
            meta = await client.fs.get_metadata(path)
            return CodexInfo(
                name=path,
                type="directory" if meta.is_directory else "file",
                size=0,
                created=meta.created_at_ms / 1000.0,
                mtime=meta.modified_at_ms / 1000.0,
            )
        except Exception as exc:
            if "not found" in str(exc).lower() or "no such" in str(exc).lower():
                raise FileNotFoundError(path) from exc
            raise OSError(f"Failed to get info for {path}: {exc}") from exc

    async def _cat_file(
        self, path: str, start: int | None = None, end: int | None = None, **kwargs: Any
    ) -> bytes:
        """Read file contents."""
        client = await self._get_client()

        try:
            response = await client.fs.read_file(path)
        except Exception as exc:
            if "not found" in str(exc).lower() or "no such" in str(exc).lower():
                raise FileNotFoundError(path) from exc
            if "is a directory" in str(exc).lower():
                raise IsADirectoryError(path) from exc
            msg = f"Failed to read file {path}: {exc}"
            raise OSError(msg) from exc

        content = base64.b64decode(response.data_base64)

        if start is not None or end is not None:
            content = content[start or 0 : end or len(content)]

        return content

    async def _pipe_file(
        self, path: str, value: bytes, mode: CreationMode = "overwrite", **kwargs: Any
    ) -> None:
        """Write data to a file."""
        client = await self._get_client()
        data_b64 = base64.b64encode(value).decode("ascii")

        try:
            await client.fs.write_file(path, data_b64)
        except Exception as exc:
            msg = f"Failed to write file {path}: {exc}"
            raise OSError(msg) from exc

    async def _mkdir(self, path: str, create_parents: bool = True, **kwargs: Any) -> None:
        """Create a directory."""
        client = await self._get_client()
        try:
            await client.fs.create_directory(path, recursive=create_parents)
        except Exception as exc:
            raise OSError(f"Failed to create directory {path}: {exc}") from exc

    async def _rm_file(self, path: str, **kwargs: Any) -> None:
        """Remove a file."""
        client = await self._get_client()
        try:
            await client.fs.remove(path, recursive=False)
        except Exception as exc:
            if "not found" in str(exc).lower() or "no such" in str(exc).lower():
                raise FileNotFoundError(path) from exc
            raise OSError(f"Failed to remove file {path}: {exc}") from exc

    async def _rmdir(self, path: str, **kwargs: Any) -> None:
        """Remove a directory."""
        client = await self._get_client()
        try:
            await client.fs.remove(path, recursive=False)
        except Exception as exc:
            if "not found" in str(exc).lower() or "no such" in str(exc).lower():
                raise FileNotFoundError(path) from exc
            raise OSError(f"Failed to remove directory {path}: {exc}") from exc

    async def _rm(self, path: str, recursive: bool = False, **kwargs: Any) -> None:
        """Remove a file or directory."""
        client = await self._get_client()
        try:
            await client.fs.remove(path, recursive=recursive)
        except Exception as exc:
            if "not found" in str(exc).lower() or "no such" in str(exc).lower():
                raise FileNotFoundError(path) from exc
            raise OSError(f"Failed to remove {path}: {exc}") from exc

    async def _cp_file(self, path1: str, path2: str, **kwargs: Any) -> None:
        """Copy a file."""
        client = await self._get_client()
        try:
            await client.fs.copy(path1, path2)
        except Exception as exc:
            raise OSError(f"Failed to copy {path1} to {path2}: {exc}") from exc

    async def _exists(self, path: str, **kwargs: Any) -> bool:
        """Check if path exists."""
        client = await self._get_client()
        try:
            await client.fs.get_metadata(path)
        except Exception:  # noqa: BLE001
            return False
        else:
            return True

    async def _isfile(self, path: str, **kwargs: Any) -> bool:
        """Check if path is a file."""
        client = await self._get_client()
        try:
            meta = await client.fs.get_metadata(path)
        except Exception:  # noqa: BLE001
            return False
        else:
            return meta.is_file

    async def _isdir(self, path: str, **kwargs: Any) -> bool:
        """Check if path is a directory."""
        client = await self._get_client()
        try:
            meta = await client.fs.get_metadata(path)
        except Exception:  # noqa: BLE001
            return False
        else:
            return meta.is_directory

    async def _modified(self, path: str, **kwargs: Any) -> float:
        """Get file modification time."""
        client = await self._get_client()
        try:
            meta = await client.fs.get_metadata(path)
            return meta.modified_at_ms / 1000.0
        except Exception as exc:
            if "not found" in str(exc).lower() or "no such" in str(exc).lower():
                raise FileNotFoundError(path) from exc
            raise OSError(f"Failed to get modification time for {path}: {exc}") from exc

    async def _created(self, path: str, **kwargs: Any) -> float:
        """Get file creation time."""
        client = await self._get_client()
        try:
            meta = await client.fs.get_metadata(path)
            return meta.created_at_ms / 1000.0
        except Exception as exc:
            if "not found" in str(exc).lower() or "no such" in str(exc).lower():
                raise FileNotFoundError(path) from exc
            raise OSError(f"Failed to get creation time for {path}: {exc}") from exc

    # Sync wrappers
    ls = sync_wrapper(_ls)  # pyright: ignore[reportAssignmentType]
    cat_file = sync_wrapper(_cat_file)  # pyright: ignore[reportAssignmentType]
    pipe_file = sync_wrapper(_pipe_file)  # pyright: ignore[reportAssignmentType]
    mkdir = sync_wrapper(_mkdir)
    rm_file = sync_wrapper(_rm_file)
    rmdir = sync_wrapper(_rmdir)
    rm = sync_wrapper(_rm)  # pyright: ignore[reportAssignmentType]
    cp_file = sync_wrapper(_cp_file)
    exists = sync_wrapper(_exists)  # pyright: ignore[reportAssignmentType]
    isfile = sync_wrapper(_isfile)
    isdir = sync_wrapper(_isdir)
    info = sync_wrapper(_info)
    modified = sync_wrapper(_modified)
    created = sync_wrapper(_created)


if __name__ == "__main__":
    import asyncio

    from codexed.client import CodexClient

    async def main():
        client = CodexClient()
        async with client:
            fs = CodexFS(client=client)
            files = await fs._ls("/")
            print(files)

    asyncio.run(main())
