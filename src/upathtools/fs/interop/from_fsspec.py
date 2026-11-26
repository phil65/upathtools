"""Inbound adapter wrapping any ``fsspec`` filesystem as a core :class:`~upathtools.fs.AsyncFS`."""

import asyncio
from collections.abc import Mapping
import errno
import inspect
import threading
from typing import TYPE_CHECKING, Any, cast
import weakref

from fsspec.asyn import AsyncFileSystem
from fsspec.spec import AbstractFileSystem

from upathtools.fs._sync import FSSPEC_SYNC_VERSION, FsspecSyncNote
from upathtools.fs.base import AsyncFS
from upathtools.fs.errors import UnsupportedOperation
from upathtools.fs.interop.to_fsspec import AsFsspecFileSystem
from upathtools.fs.types import FileInfo, WriteMode


if TYPE_CHECKING:
    from collections.abc import Callable


__fsspec_sync__ = FsspecSyncNote(
    version=FSSPEC_SYNC_VERSION,
    upstream=(
        "fsspec/spec.py:AbstractFileSystem",
        "fsspec/asyn.py:AsyncFileSystem",
    ),
    notes=(
        "Inbound bridge from any fsspec AbstractFileSystem / AsyncFileSystem to "
        "core AsyncFS[FileInfo]. Awaits private coroutine hooks directly only when "
        "isinstance(fs, AsyncFileSystem) and fs.asynchronous is True; all other "
        "filesystems dispatch public sync methods via asyncio.to_thread with an "
        "optional asyncio.Semaphore concurrency bound. No __getattr__ proxying."
    ),
)

_ADAPTER_CACHE_LOCK = threading.Lock()
# Map fsspec instance -> weakref to its FsspecAdapter so the cache does not hold a
# strong reference cycle back to the key via ``adapter.raw``.
_ADAPTER_CACHE: weakref.WeakKeyDictionary[
    AbstractFileSystem,
    weakref.ref[FsspecAdapter],
] = weakref.WeakKeyDictionary()


def _method_accepts_param(fn: object, param: str) -> bool:
    """Return True if ``fn`` declares ``param`` as an explicit parameter."""
    if not callable(fn):
        return False
    try:
        params = inspect.signature(fn).parameters
    except TypeError, ValueError:
        return False
    return param in params


def _detect_capabilities(raw: AbstractFileSystem, *, is_async: bool) -> frozenset[str]:
    """Infer core :attr:`AsyncFS.capabilities` from a wrapped ``fsspec`` filesystem."""
    raw_cls = type(raw)
    caps: set[str] = set()

    is_readonly_mode = getattr(raw, "mode", None) == "r"

    if not is_readonly_mode:
        if is_async and issubclass(raw_cls, AsyncFileSystem):
            has_write = (
                raw_cls._pipe_file is not AsyncFileSystem._pipe_file
                or raw_cls._open is not AbstractFileSystem._open
            )
        else:
            has_write = (
                raw_cls.pipe_file is not AbstractFileSystem.pipe_file
                or raw_cls._open is not AbstractFileSystem._open
            )
        if has_write:
            caps.add("write_bytes")

        has_mkdir = (
            raw_cls.mkdir is not AbstractFileSystem.mkdir
            or raw_cls.makedirs is not AbstractFileSystem.makedirs
            or (
                issubclass(raw_cls, AsyncFileSystem)
                and (
                    raw_cls._mkdir is not AsyncFileSystem._mkdir
                    or raw_cls._makedirs is not AsyncFileSystem._makedirs
                )
            )
        )
        if has_mkdir:
            caps.add("mkdir")

        has_rmdir = raw_cls.rmdir is not AbstractFileSystem.rmdir or hasattr(raw_cls, "_rmdir")
        if has_rmdir:
            caps.add("rmdir")

        has_rm_file = (
            raw_cls.rm_file is not AbstractFileSystem.rm_file
            or raw_cls._rm is not AbstractFileSystem._rm
            or (
                issubclass(raw_cls, AsyncFileSystem)
                and raw_cls._rm_file is not AsyncFileSystem._rm_file
            )
        )
        if has_rm_file:
            caps.add("remove_file")

    return frozenset(caps)


class FsspecAdapter(AsyncFS[FileInfo]):
    """Adapt any ``fsspec`` :class:`AbstractFileSystem` into a core :class:`AsyncFS`.

    Dispatch rules:
      - If ``fs`` is an :class:`~fsspec.asyn.AsyncFileSystem` constructed with
        ``asynchronous=True``, its private coroutine hooks (``_info``, ``_ls``,
        ``_cat_file``, ``_pipe_file``, ``_rm_file``, ``_mkdir``, ``_makedirs``,
        and ``_rmdir`` when present) are awaited directly on the caller's loop.
      - For all other filesystems (sync implementations and ``AsyncFileSystem``
        instances with ``asynchronous=False`` whose sessions belong to ``fsspec``'s
        own background loop), public sync methods are offloaded via
        :func:`asyncio.to_thread`, optionally bounded by an
        :class:`asyncio.Semaphore`.

    No ``__getattr__`` proxying is performed; access backend-specific APIs
    explicitly via :attr:`raw`.
    """

    def __init__(
        self,
        fs: AbstractFileSystem,
        *,
        max_concurrency: int | None = None,
        close_underlying: bool = False,
    ) -> None:
        """Initialize an :class:`FsspecAdapter` around ``fs``.

        Args:
            fs: Any ``fsspec`` :class:`AbstractFileSystem` instance.
            max_concurrency: Optional maximum number of concurrent ``to_thread``
                calls for sync-dispatched backends.
            close_underlying: If ``True``, :meth:`aclose` attempts to close the
                underlying ``fsspec`` filesystem. Defaults to ``False``.

        Raises:
            ValueError: If ``max_concurrency`` is provided and ``<= 0``.
        """
        if max_concurrency is not None and max_concurrency <= 0:
            raise ValueError(
                f"max_concurrency must be a positive integer or None, got {max_concurrency}"
            )
        raw_batch = getattr(fs, "batch_size", None)
        valid_batch = raw_batch if isinstance(raw_batch, int) and raw_batch > 0 else None
        super().__init__(batch_size=valid_batch)

        self.raw = fs
        self._is_async = isinstance(fs, AsyncFileSystem) and bool(
            getattr(fs, "asynchronous", False)
        )
        self._sem: asyncio.Semaphore | None = (
            asyncio.Semaphore(max_concurrency) if max_concurrency is not None else None
        )
        self._close_underlying = close_underlying
        self._detected_capabilities = _detect_capabilities(fs, is_async=self._is_async)

        pipe_target = getattr(fs, "_pipe_file", None) if self._is_async else fs.pipe_file
        self._pipe_supports_mode = _method_accepts_param(pipe_target, "mode")

        # Instance-level metadata mirrored from the wrapped fsspec filesystem.
        self.protocol = fs.protocol  # ty: ignore[invalid-attribute-access]  # per-instance override
        self.root_marker = fs.root_marker  # ty: ignore[invalid-attribute-access]  # per-instance
        self.sep = fs.sep  # ty: ignore[invalid-attribute-access]  # per-instance override
        self.local_file = bool(getattr(fs, "local_file", False))  # ty: ignore[invalid-attribute-access]  # per-instance

    @property
    def capabilities(self) -> frozenset[str]:
        """Capabilities detected on the wrapped ``fsspec`` filesystem."""
        return self._detected_capabilities

    def _norm(self, path: str) -> str:
        """Normalize ``path`` via ``self.raw._strip_protocol`` without forcing a leading ``/``."""
        stripped = self.raw._strip_protocol(path)
        if isinstance(stripped, list):
            stripped = stripped[0] if stripped else ""
        cleaned = str(stripped).rstrip("/")
        return cleaned if cleaned else self.root_marker

    def _normalize_info(self, raw_info: Mapping[str, Any]) -> FileInfo:
        """Convert an ``fsspec`` info dict to a :class:`FileInfo` with normalized ``path``."""
        info = FileInfo.from_fsspec_dict(raw_info)
        norm_path = self._norm(info.path)
        if norm_path == info.path:
            return info
        return FileInfo(
            path=norm_path,
            type=info.type,
            size=info.size,
            mtime=info.mtime,
            ctime=info.ctime,
            mode=info.mode,
            etag=info.etag,
            is_symlink=info.is_symlink,
            symlink_target=info.symlink_target,
            extra=info.extra,
        )

    async def _run_sync[T](self, fn: Callable[..., T], *args: Any, **kwargs: Any) -> T:
        if self._sem is None:
            return await asyncio.to_thread(fn, *args, **kwargs)
        async with self._sem:
            return await asyncio.to_thread(fn, *args, **kwargs)

    async def info(self, path: str) -> FileInfo:
        self._check_open()
        norm = self._norm(path)
        if self._is_async:
            raw_info = await cast("AsyncFileSystem", self.raw)._info(norm)
        else:
            raw_info = await self._run_sync(self.raw.info, norm)
        return self._normalize_info(raw_info)

    async def ls(self, path: str) -> list[FileInfo]:
        self._check_open()
        norm = self._norm(path)
        if self._is_async:
            raw_entries = await cast("AsyncFileSystem", self.raw)._ls(norm, detail=True)
        else:
            raw_entries = await self._run_sync(self.raw.ls, norm, detail=True)

        entries: list[FileInfo] = []
        for item in raw_entries:
            if isinstance(item, Mapping):
                entries.append(self._normalize_info(item))
            else:
                entries.append(await self.info(str(item)))

        if len(entries) == 1 and entries[0].path == norm and not entries[0].is_dir:
            raise NotADirectoryError(errno.ENOTDIR, "Not a directory", norm)

        children = [entry for entry in entries if not (entry.path == norm and entry.is_dir)]
        children.sort(key=lambda e: e.path)
        return children

    async def read_bytes(
        self,
        path: str,
        *,
        start: int | None = None,
        end: int | None = None,
    ) -> bytes:
        self._check_open()
        norm = self._norm(path)

        # Guard against fsspec's AbstractFileSystem.cat_file raising ValueError on
        # reversed slices (e.g. start=7, end=3 or start=-2, end=-5).
        is_reversed = (
            start is not None
            and end is not None
            and (
                (start >= 0 and end >= 0 and start >= end)
                or (start < 0 and end < 0 and start >= end)
            )
        )
        if is_reversed:
            entry = await self.info(norm)
            if entry.is_dir:
                raise IsADirectoryError(errno.EISDIR, "Is a directory", norm)
            return b""

        try:
            if self._is_async:
                data = await cast("AsyncFileSystem", self.raw)._cat_file(norm, start=start, end=end)
            else:
                data = await self._run_sync(self.raw.cat_file, norm, start=start, end=end)
        except FileNotFoundError:
            if await self.is_dir(norm):
                raise IsADirectoryError(errno.EISDIR, "Is a directory", norm) from None
            raise
        except ValueError:
            if start is not None and end is not None:
                entry = await self.info(norm)
                if entry.is_dir:
                    raise IsADirectoryError(errno.EISDIR, "Is a directory", norm) from None
                if entry.size is not None:
                    s_idx, e_idx, _ = slice(start, end).indices(entry.size)
                    if s_idx >= e_idx:
                        return b""
            raise
        return bytes(data)

    async def write_bytes(
        self,
        path: str,
        data: bytes | bytearray | memoryview,
        *,
        mode: WriteMode = "overwrite",
    ) -> None:
        """Write raw bytes to ``path`` on the wrapped ``fsspec`` filesystem.

        When ``mode="create"`` and the wrapped filesystem's ``pipe_file`` does not
        declare a ``mode`` parameter, existence is checked via :meth:`exists` before
        writing. Note that this fallback check is subject to a TOCTOU race if another
        writer creates ``path`` concurrently.
        """
        self._check_open()
        if "write_bytes" not in self._detected_capabilities:
            raise UnsupportedOperation(f"{type(self.raw).__name__} does not support write_bytes")
        if mode not in ("overwrite", "create"):
            raise ValueError(f"Unsupported write mode {mode!r}; expected 'overwrite' or 'create'")
        norm = self._norm(path)
        payload = bytes(data)

        if mode == "create" and not self._pipe_supports_mode and await self.exists(norm):
            raise FileExistsError(errno.EEXIST, "File exists", norm)

        try:
            if self._is_async:
                async_fs = cast("AsyncFileSystem", self.raw)
                if self._pipe_supports_mode:
                    await async_fs._pipe_file(norm, payload, mode=mode)
                else:
                    await async_fs._pipe_file(norm, payload)
            elif self._pipe_supports_mode:
                await self._run_sync(self.raw.pipe_file, norm, payload, mode=mode)
            else:
                await self._run_sync(self.raw.pipe_file, norm, payload)
        except NotImplementedError as exc:
            raise UnsupportedOperation(
                f"{type(self.raw).__name__} does not support write_bytes"
            ) from exc

    async def mkdir(
        self,
        path: str,
        *,
        parents: bool = True,
        exist_ok: bool = False,
    ) -> None:
        self._check_open()
        if "mkdir" not in self._detected_capabilities:
            raise UnsupportedOperation(f"{type(self.raw).__name__} does not support mkdir")
        norm = self._norm(path)
        if exist_ok and await self.is_file(norm):
            raise FileExistsError(errno.EEXIST, "File exists", norm)
        try:
            if parents:
                if self._is_async:
                    await cast("AsyncFileSystem", self.raw)._makedirs(norm, exist_ok=exist_ok)
                else:
                    await self._run_sync(self.raw.makedirs, norm, exist_ok=exist_ok)
            else:
                try:
                    if self._is_async:
                        await cast("AsyncFileSystem", self.raw)._mkdir(norm, create_parents=False)
                    else:
                        await self._run_sync(self.raw.mkdir, norm, create_parents=False)
                except FileExistsError:
                    if exist_ok and await self.is_dir(norm):
                        return
                    raise
        except NotImplementedError as exc:
            raise UnsupportedOperation(f"{type(self.raw).__name__} does not support mkdir") from exc

    async def rmdir(self, path: str) -> None:
        self._check_open()
        if "rmdir" not in self._detected_capabilities:
            raise UnsupportedOperation(f"{type(self.raw).__name__} does not support rmdir")
        norm = self._norm(path)
        try:
            async_rmdir = getattr(self.raw, "_rmdir", None) if self._is_async else None
            if inspect.iscoroutinefunction(async_rmdir):
                await async_rmdir(norm)
            else:
                await self._run_sync(self.raw.rmdir, norm)
        except NotImplementedError as exc:
            raise UnsupportedOperation(f"{type(self.raw).__name__} does not support rmdir") from exc
        except FileNotFoundError:
            if await self.is_file(norm):
                raise NotADirectoryError(errno.ENOTDIR, "Not a directory", norm) from None
            if await self.is_dir(norm) and await self.ls(norm):
                raise OSError(errno.ENOTEMPTY, "Directory not empty", norm) from None
            raise

    async def remove_file(self, path: str) -> None:
        self._check_open()
        if "remove_file" not in self._detected_capabilities:
            raise UnsupportedOperation(f"{type(self.raw).__name__} does not support remove_file")
        norm = self._norm(path)
        try:
            if self._is_async:
                await cast("AsyncFileSystem", self.raw)._rm_file(norm)
            else:
                await self._run_sync(self.raw.rm_file, norm)
        except NotImplementedError as exc:
            raise UnsupportedOperation(
                f"{type(self.raw).__name__} does not support remove_file"
            ) from exc
        except FileNotFoundError:
            if await self.is_dir(norm):
                raise IsADirectoryError(errno.EISDIR, "Is a directory", norm) from None
            raise

    def invalidate_cache(self, path: str | None = None) -> None:
        super().invalidate_cache(path)
        if path is None:
            self.raw.invalidate_cache()
        else:
            self.raw.invalidate_cache(self._norm(path))

    async def aclose(self) -> None:
        if not self._closed and self._close_underlying:
            for attr in ("aclose", "close_session", "_close"):
                hook = getattr(self.raw, attr, None)
                if inspect.iscoroutinefunction(hook):
                    await hook()
                    break
            else:
                sync_close = getattr(self.raw, "close", None)
                if callable(sync_close):
                    await self._run_sync(sync_close)
        await super().aclose()


def from_fsspec(
    fs: AbstractFileSystem,
    *,
    max_concurrency: int | None = None,
    close_underlying: bool = False,
) -> FsspecAdapter:
    """Wrap an ``fsspec`` :class:`AbstractFileSystem` in a new :class:`FsspecAdapter`."""
    return FsspecAdapter(
        fs,
        max_concurrency=max_concurrency,
        close_underlying=close_underlying,
    )


def ensure_async_fs(fs: AsyncFS[Any] | AbstractFileSystem) -> AsyncFS[Any]:
    """Return a core :class:`AsyncFS` for either a core filesystem or an ``fsspec`` instance.

    - Core :class:`AsyncFS` instances are returned unchanged.
    - :class:`AsFsspecFileSystem` instances are unwrapped back to their ``core_fs``
      instead of double-wrapping.
    - Other ``fsspec`` :class:`AbstractFileSystem` instances return a single cached
      :class:`FsspecAdapter` per instance stored in a lock-protected
      :class:`weakref.WeakKeyDictionary` (never mutating ``fs.__dict__``).
    """
    if isinstance(fs, AsyncFS):
        return fs
    if isinstance(fs, AsFsspecFileSystem):
        return fs.core_fs
    with _ADAPTER_CACHE_LOCK:
        cached_ref = _ADAPTER_CACHE.get(fs)
        adapter = cached_ref() if cached_ref is not None else None
        if adapter is None:
            adapter = FsspecAdapter(fs)
            _ADAPTER_CACHE[fs] = weakref.ref(adapter)
        return adapter
