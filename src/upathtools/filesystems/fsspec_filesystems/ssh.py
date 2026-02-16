"""Async SSH filesystem with UPath integration (requires sshfs/asyncssh).

This wraps sshfs's SSHFileSystem to allow proper async initialization,
avoiding the sync_wrapper hack that breaks when already in an async event loop.
"""

from __future__ import annotations

from contextlib import AsyncExitStack
from typing import Any
import weakref

from sshfs import SSHFileSystem as _SSHFileSystem
from sshfs.pools import SFTPSoftChannelPool
from upath.implementations.sftp import SFTPPath

from upathtools.filesystems.base import BaseAsyncFileSystem


class AsyncSSHFileSystem(BaseAsyncFileSystem[SFTPPath], _SSHFileSystem):
    """Async SSH filesystem that supports initialization within a running event loop.

    Unlike the upstream SSHFileSystem which uses sync_wrapper for connection
    during __init__ (breaking when called from async code), this class defers
    connection to the async _connect method and connects lazily on first use.
    """

    upath_cls = SFTPPath

    def __init__(
        self,
        host: str,
        *,
        pool_type: type = SFTPSoftChannelPool,
        max_sftp_channels: int = 8,
        **kwargs: Any,
    ) -> None:
        """Initialize the filesystem without establishing a connection.

        Connection is deferred and established lazily on first use via
        ``_ensure_connected()``, or explicitly via ``await fs._async_connect()``.

        Args:
            host: SSH host to connect to.
            pool_type: SFTP channel pool class to use.
            max_sftp_channels: Maximum number of SFTP channels.
            kwargs: Additional keyword arguments passed to asyncssh.connect
                    (e.g., username, password, client_keys, known_hosts, port).
        """
        # Skip SSHFileSystem.__init__ entirely — it calls sync connect.
        # Call AsyncFileSystem.__init__ directly.
        # Note: AsyncFileSystem.__init__ expects (self, *args, **kwargs)
        # and passes remaining kwargs to AbstractFileSystem.__init__.
        # We need to filter out SSH-specific kwargs so they don't confuse fsspec.
        fsspec_kwargs = {
            k: v for k, v in kwargs.items() if k in ("loop", "batch_size", "asynchronous")
        }
        super(_SSHFileSystem, self).__init__(**fsspec_kwargs)

        self._host = host
        self._pool_type = pool_type
        self._max_sftp_channels = max_sftp_channels
        self._connect_kwargs = {
            k: v for k, v in kwargs.items() if k not in ("loop", "batch_size", "asynchronous")
        }
        self._connect_kwargs.setdefault("known_hosts", None)

        # Initialize state that SSHFileSystem expects
        self._stack = AsyncExitStack()
        self.active_executors = 0
        self._client = None  # type: ignore[assignment]
        self._pool = None  # type: ignore[assignment]
        self._connected = False

    async def _async_connect(self) -> None:
        """Establish the SSH connection asynchronously."""
        if self._connected:
            return
        self._client, self._pool = await self._connect(
            self._host,
            self._pool_type,
            max_sftp_channels=self._max_sftp_channels,
            **self._connect_kwargs,
        )
        self._connected = True
        weakref.finalize(self, self._finalize, self.loop, self._pool, self._stack)

    async def _ensure_connected(self) -> None:
        """Ensure the SSH connection is established."""
        if not self._connected:
            await self._async_connect()

    # Override all async methods to ensure connection before use

    async def _info(self, path: str, **kwargs: Any) -> dict[str, Any]:
        await self._ensure_connected()
        return await super()._info(path, **kwargs)

    async def _ls(self, path: str, detail: bool = False, **kwargs: Any) -> list[Any]:
        await self._ensure_connected()
        return await super()._ls(path, detail=detail, **kwargs)

    async def _cat_file(self, path: str, **kwargs: Any) -> bytes:
        await self._ensure_connected()
        return await super()._cat_file(path, **kwargs)

    async def _pipe_file(
        self, path: str, data: bytes, chunksize: int = 50 * 2**20, **kwargs: Any
    ) -> None:
        await self._ensure_connected()
        await super()._pipe_file(path, data, chunksize=chunksize, **kwargs)

    async def _put_file(
        self, lpath: str, rpath: str, block_size: int = 2**18, callback: Any = None, **kwargs: Any
    ) -> None:
        await self._ensure_connected()
        await super()._put_file(lpath, rpath, block_size=block_size, callback=callback, **kwargs)

    async def _get_file(
        self, lpath: str, rpath: str, block_size: int = 2**18, callback: Any = None, **kwargs: Any
    ) -> None:
        await self._ensure_connected()
        await super()._get_file(lpath, rpath, block_size=block_size, callback=callback, **kwargs)

    async def _cp_file(self, lpath: str, rpath: str, **kwargs: Any) -> None:
        await self._ensure_connected()
        await super()._cp_file(lpath, rpath, **kwargs)

    async def _mv(self, lpath: str, rpath: str, **kwargs: Any) -> None:
        await self._ensure_connected()
        await super()._mv(lpath, rpath, **kwargs)

    async def _rm_file(self, path: str, **kwargs: Any) -> None:
        await self._ensure_connected()
        await super()._rm_file(path, **kwargs)

    async def _rmdir(
        self,
        path: str,
        recursive: bool = False,
        ignore_errors: bool = False,
        on_error: Any = None,
        **kwargs: Any,
    ) -> None:
        await self._ensure_connected()
        await super()._rmdir(
            path, recursive=recursive, ignore_errors=ignore_errors, on_error=on_error, **kwargs
        )

    async def _rm(self, path: str | list[str], recursive: bool = False, **kwargs: Any) -> None:
        await self._ensure_connected()
        await super()._rm(path, recursive=recursive, **kwargs)

    async def _mkdir(
        self,
        path: str,
        *,
        create_parents: bool = True,
        permissions: int = 511,
        **kwargs: Any,
    ) -> None:
        await self._ensure_connected()
        await super()._mkdir(path, create_parents=create_parents, permissions=permissions, **kwargs)

    async def _makedirs(
        self,
        path: str,
        *,
        exist_ok: bool = False,
        permissions: int = 511,
        **kwargs: Any,
    ) -> None:
        await self._ensure_connected()
        await super()._makedirs(path, exist_ok=exist_ok, permissions=permissions, **kwargs)

    async def _isdir(self, path: str) -> bool:
        await self._ensure_connected()
        return await super()._isdir(path)

    async def _modified(self, path: str, **kwargs: Any) -> Any:
        await self._ensure_connected()
        return await super()._modified(path, **kwargs)

    async def _checksum(self, path: str) -> str:
        await self._ensure_connected()
        return await super()._checksum(path)

    async def _get_system(self) -> str:
        await self._ensure_connected()
        return await super()._get_system()

    async def _execute(self, *args: Any, **kwargs: Any) -> Any:
        await self._ensure_connected()
        return await super()._execute(*args, **kwargs)
