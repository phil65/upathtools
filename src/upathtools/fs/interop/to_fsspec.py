"""Outbound adapter exposing a core :class:`~upathtools.fs.AsyncFS` as an ``fsspec`` filesystem."""

import asyncio
from datetime import UTC, datetime
import errno
import os
from pathlib import Path
import threading
from typing import TYPE_CHECKING, Any, ClassVar, Literal, cast

import fsspec
from fsspec.asyn import AsyncFileSystem, sync_wrapper
from fsspec.spec import AbstractBufferedFile
from fsspec.utils import isfilelike
from upath import UPath
import upath.registry as upath_registry

from upathtools.fs._sync import FSSPEC_SYNC_VERSION, FsspecSyncNote
from upathtools.fs.base import AsyncFS
from upathtools.fs.batch import run_bounded
from upathtools.fs.errors import UnsupportedOperation
from upathtools.fs.paths import normalize_path, parent_path, strip_protocol as strip_protocol_path
from upathtools.fs.runner import LoopRunner, get_default_runner


if TYPE_CHECKING:
    from collections.abc import Callable, Mapping

    from upathtools.fs.streams import AsyncBufferedReader, AsyncBufferedWriter


__fsspec_sync__ = FsspecSyncNote(
    version=FSSPEC_SYNC_VERSION,
    upstream=(
        "fsspec/asyn.py:AsyncFileSystem",
        "fsspec/spec.py:AbstractBufferedFile",
        "fsspec/registry.py:register_implementation",
    ),
    notes=(
        "Outbound bridge from core AsyncFS to fsspec.asyn.AsyncFileSystem with "
        "cachable=False, explicit LoopRunner loop binding (never spawning fsspec's "
        "global fsspecIO thread), and opt-in fsspec/UPath protocol bridge helpers."
    ),
)

_CLASS_CACHE_LOCK = threading.Lock()
_CLASS_CACHE: dict[
    tuple[type[AsyncFS[Any]], str | tuple[str, ...], str, str],
    type[AsFsspecFileSystem],
] = {}


def _as_str_path(path: str | os.PathLike[str]) -> str:
    """Convert a string or PathLike to a plain string."""
    return os.fspath(path)


class _AsFsspecBufferedFile(AbstractBufferedFile):
    """Sync buffered file over :class:`AsFsspecFileSystem` for ``fsspec.open`` and ``UPath``."""

    def __init__(
        self,
        fs: AsFsspecFileSystem,
        path: str,
        mode: str = "rb",
        block_size: int | str | None = "default",
        autocommit: bool = True,
        cache_type: str = "readahead",
        cache_options: dict[str, Any] | None = None,
        size: int | None = None,
        **kwargs: Any,
    ) -> None:
        norm_path = fs._strip_protocol(path)
        self._chunks: list[bytes] = []
        self._pending_data: bytes | None = None

        if mode == "xb" and fs.exists(norm_path):
            raise FileExistsError(errno.EEXIST, "File exists", norm_path)

        details: dict[str, Any] | None = None
        if mode == "rb" and size is None:
            details = fs.info(norm_path)
            if details.get("type") == "directory":
                raise IsADirectoryError(errno.EISDIR, "Is a directory", norm_path)
            size = details.get("size")
            if size is None:
                size = 0
        elif mode == "ab":
            try:
                existing = fs.cat_file(norm_path)
            except FileNotFoundError:
                existing = b""
            if existing:
                self._chunks.append(existing)

        super().__init__(
            fs,
            norm_path,
            mode=mode,
            block_size=block_size,
            autocommit=autocommit,
            cache_type=cache_type,
            cache_options=cache_options,
            size=size,
            **kwargs,
        )
        if details is not None:
            self._details = details

    def _fetch_range(self, start: int, end: int) -> bytes:
        return cast("bytes", self.fs.cat_file(self.path, start=start, end=end))

    def _upload_chunk(self, final: bool = False) -> bool:
        chunk = self.buffer.getvalue()
        if chunk:
            self._chunks.append(chunk)
        if not final:
            return True
        self._pending_data = b"".join(self._chunks)
        self._chunks.clear()
        if self.autocommit:
            self.commit()
        return True

    def commit(self) -> None:
        if self._pending_data is not None:
            data = self._pending_data
            self._pending_data = None
            write_mode: Literal["overwrite", "create"] = (
                "create" if self.mode == "xb" else "overwrite"
            )
            self.fs.pipe_file(self.path, data, mode=write_mode)

    def discard(self) -> None:
        self._chunks.clear()
        self._pending_data = None


class AsFsspecFileSystem(AsyncFileSystem):
    """Expose a core :class:`~upathtools.fs.AsyncFS` as an ``fsspec`` :class:`AsyncFileSystem`.

    Sync operations execute on the configured :class:`~upathtools.fs.LoopRunner` loop
    rather than ``fsspec``'s global ``fsspecIO`` thread.
    """

    cachable = False
    mirror_sync_methods = True
    _core_cls: ClassVar[type[AsyncFS[Any]] | None] = None
    protocol: str | tuple[str, ...] = "abstract"
    root_marker: str = "/"
    sep: str = "/"

    def __init__(
        self,
        core_fs: AsyncFS[Any],
        *,
        asynchronous: bool = False,
        loop: asyncio.AbstractEventLoop | None = None,
        runner: LoopRunner | None = None,
        **kwargs: Any,
    ) -> None:
        """Initialize an ``fsspec`` adapter wrapping ``core_fs``.

        Args:
            core_fs: Core :class:`~upathtools.fs.AsyncFS` instance to delegate to.
            asynchronous: When ``True``, the instance is intended for async callers
                and does not bind a background sync event loop.
            loop: Explicit event loop for sync bridging (takes precedence over ``runner``).
            runner: Explicit :class:`~upathtools.fs.LoopRunner` for sync bridging.
                Defaults to :func:`~upathtools.fs.get_default_runner` when
                ``asynchronous=False`` and ``loop is None``.
            **kwargs: Extra storage options accepted and forwarded to ``AsyncFileSystem``.
        """
        if asynchronous:
            resolved_runner = runner
            resolved_loop = loop
        else:
            resolved_runner = runner if runner is not None else get_default_runner()
            resolved_loop = loop if loop is not None else resolved_runner.loop

        self.core_fs = core_fs
        self._runner = resolved_runner
        self.local_file = core_fs.local_file
        batch_size = kwargs.pop("batch_size", core_fs.batch_size)
        super().__init__(
            asynchronous=asynchronous,
            loop=resolved_loop,
            batch_size=batch_size,
            **kwargs,
        )

    @classmethod
    def _strip_protocol(cls, path: Any) -> Any:
        if isinstance(path, list):
            return [cls._strip_protocol(item) for item in path]
        path_str = _as_str_path(path)
        protos_list: list[str] = []
        cls_protos = (cls.protocol,) if isinstance(cls.protocol, str) else tuple(cls.protocol)
        protos_list.extend(cls_protos)
        if cls._core_cls is not None:
            core_protos = (
                (cls._core_cls.protocol,)
                if isinstance(cls._core_cls.protocol, str)
                else tuple(cls._core_cls.protocol)
            )
            for p in core_protos:
                if p not in protos_list:
                    protos_list.append(p)
        stripped = strip_protocol_path(path_str, protos_list, root_marker="")
        norm = normalize_path(stripped)
        if cls.root_marker == "/":
            return "/" if not norm else (norm if norm.startswith("/") else f"/{norm}")
        if cls.root_marker:
            if not norm:
                return cls.root_marker
            if norm.startswith(cls.root_marker):
                return norm
            return f"{cls.root_marker}{norm.lstrip('/')}"
        return norm.lstrip("/")

    @classmethod
    def _get_kwargs_from_urls(cls, path: str) -> dict[str, Any]:
        if cls._core_cls is not None:
            return cls._core_cls.kwargs_from_url(path)
        return {}

    def invalidate_cache(self, path: str | None = None) -> None:
        self.core_fs.invalidate_cache(path)
        super().invalidate_cache(path)

    async def _info(self, path: str, **kwargs: Any) -> dict[str, Any]:
        entry = await self.core_fs.info(_as_str_path(path))
        return entry.to_fsspec_dict()

    async def _ls(
        self,
        path: str,
        detail: bool = True,
        **kwargs: Any,
    ) -> list[Any]:
        str_path = _as_str_path(path)
        try:
            entries = await self.core_fs.ls(str_path)
        except NotADirectoryError:
            entry = await self.core_fs.info(str_path)
            return [entry.to_fsspec_dict()] if detail else [entry.path]
        if detail:
            return [entry.to_fsspec_dict() for entry in entries]
        return [entry.path for entry in entries]

    async def _cat_file(
        self,
        path: str,
        start: int | None = None,
        end: int | None = None,
        **kwargs: Any,
    ) -> bytes:
        return await self.core_fs.read_bytes(_as_str_path(path), start=start, end=end)

    async def _pipe_file(
        self,
        path: str,
        value: bytes | bytearray | memoryview,
        mode: str = "overwrite",
        **kwargs: Any,
    ) -> None:
        write_mode: Literal["overwrite", "create"]
        if mode == "overwrite":
            write_mode = "overwrite"
        elif mode == "create":
            write_mode = "create"
        else:
            msg = f"Unsupported pipe_file mode {mode!r}; expected 'overwrite' or 'create'"
            raise ValueError(msg)
        try:
            await self.core_fs.write_bytes(_as_str_path(path), value, mode=write_mode)
        except UnsupportedOperation as exc:
            raise NotImplementedError(str(exc)) from exc

    async def _rm_file(self, path: str, **kwargs: Any) -> None:
        try:
            await self.core_fs.remove_file(_as_str_path(path))
        except UnsupportedOperation as exc:
            raise NotImplementedError(str(exc)) from exc

    async def _rm(
        self,
        path: str | list[str],
        recursive: bool = False,
        batch_size: int | None = None,
        maxdepth: int | None = None,
        **kwargs: Any,
    ) -> None:
        expanded = await self._expand_path(path, recursive=recursive, maxdepth=maxdepth)
        files: list[str] = []
        dirs: list[str] = []
        for p in expanded:
            info = await self.core_fs.info(p)
            if info.is_dir:
                dirs.append(info.path)
            else:
                files.append(info.path)

        effective_batch = batch_size if (isinstance(batch_size, int) and batch_size > 0) else None
        limit = self.core_fs.batch_size if effective_batch is None else effective_batch

        if files:
            await run_bounded(
                (self._rm_file(f) for f in files),
                limit=limit,
            )

        dirs.sort(key=lambda d: (d.count("/"), len(d)), reverse=True)
        for d in dirs:
            if d == self.core_fs.root_marker:
                continue
            try:
                await self._rmdir(d)
            except FileNotFoundError:
                # An implied pseudo-directory on an object-store / fsspec memory backend
                # can vanish once its child files are deleted above.
                continue

    async def _cp_file(self, path1: str, path2: str, **kwargs: Any) -> None:
        src = _as_str_path(path1)
        dst = _as_str_path(path2)
        src_info = await self.core_fs.info(src)
        try:
            if src_info.is_dir:
                await self.core_fs.mkdir(dst, parents=True, exist_ok=True)
                return
            try:
                await self.core_fs.copy_file(src, dst, overwrite=True)
            except FileNotFoundError:
                # Ensure destination parent directory exists for non-object-store backends.
                dst_norm = self.core_fs._norm(dst)
                parent = parent_path(dst_norm, root_marker=self.core_fs.root_marker)
                if parent != dst_norm and "mkdir" in self.core_fs.capabilities:
                    await self.core_fs.mkdir(parent, parents=True, exist_ok=True)
                    await self.core_fs.copy_file(src, dst, overwrite=True)
                else:
                    raise
        except UnsupportedOperation as exc:
            raise NotImplementedError(str(exc)) from exc

    async def _mkdir(
        self,
        path: str,
        create_parents: bool = True,
        **kwargs: Any,
    ) -> None:
        try:
            await self.core_fs.mkdir(_as_str_path(path), parents=create_parents, exist_ok=False)
        except UnsupportedOperation as exc:
            raise NotImplementedError(str(exc)) from exc

    async def _makedirs(self, path: str, exist_ok: bool = False) -> None:
        try:
            await self.core_fs.mkdir(_as_str_path(path), parents=True, exist_ok=exist_ok)
        except UnsupportedOperation as exc:
            raise NotImplementedError(str(exc)) from exc

    async def _rmdir(self, path: str) -> None:
        try:
            await self.core_fs.rmdir(_as_str_path(path))
        except UnsupportedOperation as exc:
            raise NotImplementedError(str(exc)) from exc

    rmdir = sync_wrapper(_rmdir)

    async def _exists(self, path: str, **kwargs: Any) -> bool:
        return await self.core_fs.exists(_as_str_path(path))

    async def _isfile(self, path: str) -> bool:
        return await self.core_fs.is_file(_as_str_path(path))

    async def _isdir(self, path: str) -> bool:
        return await self.core_fs.is_dir(_as_str_path(path))

    async def _size(self, path: str) -> int | None:
        return await self.core_fs.size(_as_str_path(path))

    async def _put_file(
        self,
        lpath: Any,
        rpath: str,
        mode: str = "overwrite",
        **kwargs: Any,
    ) -> None:
        rpath_str = _as_str_path(rpath)
        lpath_str = _as_str_path(lpath)
        local_path = Path(lpath_str)
        if local_path.is_dir():
            await self._makedirs(rpath_str, exist_ok=True)
            return
        write_mode: Literal["overwrite", "create"]
        if mode == "overwrite":
            write_mode = "overwrite"
        elif mode == "create":
            write_mode = "create"
        else:
            msg = f"Unsupported put_file mode {mode!r}; expected 'overwrite' or 'create'"
            raise ValueError(msg)
        try:
            try:
                await self.core_fs.put_file(lpath_str, rpath_str, mode=write_mode)
            except FileNotFoundError:
                if local_path.exists() and "mkdir" in self.core_fs.capabilities:
                    r_norm = self.core_fs._norm(rpath_str)
                    parent = parent_path(r_norm, root_marker=self.core_fs.root_marker)
                    if parent != r_norm:
                        await self.core_fs.mkdir(parent, parents=True, exist_ok=True)
                        await self.core_fs.put_file(lpath_str, rpath_str, mode=write_mode)
                        return
                raise
        except UnsupportedOperation as exc:
            raise NotImplementedError(str(exc)) from exc

    async def _get_file(self, rpath: str, lpath: Any, **kwargs: Any) -> None:
        rpath_str = _as_str_path(rpath)
        if isfilelike(lpath):
            data = await self.core_fs.read_bytes(rpath_str)
            lpath.write(data)
            return
        lpath_str = _as_str_path(lpath)
        info = await self.core_fs.info(rpath_str)
        if info.is_dir:
            await asyncio.to_thread(os.makedirs, lpath_str, exist_ok=True)
            return
        await self.core_fs.get_file(rpath_str, lpath_str)

    async def _glob(
        self,
        path: str,
        maxdepth: int | None = None,
        **kwargs: Any,
    ) -> list[str] | dict[str, dict[str, Any]]:
        detail = bool(kwargs.pop("detail", False))
        withdirs = bool(kwargs.pop("withdirs", True))
        entries = await self.core_fs.glob(_as_str_path(path), maxdepth=maxdepth)
        if not withdirs:
            entries = [e for e in entries if not e.is_dir]
        if detail:
            return {e.path: e.to_fsspec_dict() for e in entries}
        return [e.path for e in entries]

    async def _find(
        self,
        path: str,
        maxdepth: int | None = None,
        withdirs: bool = False,
        **kwargs: Any,
    ) -> list[str] | dict[str, dict[str, Any]]:
        detail = bool(kwargs.pop("detail", False))
        entries = await self.core_fs.find(
            _as_str_path(path),
            maxdepth=maxdepth,
            withdirs=withdirs,
        )
        if detail:
            return {e.path: e.to_fsspec_dict() for e in entries}
        return [e.path for e in entries]

    async def _modified(self, path: str) -> datetime:
        info = await self.core_fs.info(_as_str_path(path))
        if info.mtime is None:
            raise NotImplementedError(f"Modification time is not available for {path!r}")
        return datetime.fromtimestamp(info.mtime, tz=UTC)

    modified = sync_wrapper(_modified)

    async def _created(self, path: str) -> datetime:
        info = await self.core_fs.info(_as_str_path(path))
        if info.ctime is None:
            raise NotImplementedError(f"Creation time is not available for {path!r}")
        return datetime.fromtimestamp(info.ctime, tz=UTC)

    created = sync_wrapper(_created)

    async def _touch(self, path: str, truncate: bool = True, **kwargs: Any) -> None:
        try:
            await self.core_fs.touch(_as_str_path(path), truncate=truncate)
        except UnsupportedOperation as exc:
            raise NotImplementedError(str(exc)) from exc

    touch = sync_wrapper(_touch)

    def _open(
        self,
        path: str,
        mode: str = "rb",
        block_size: int | None = None,
        autocommit: bool = True,
        cache_options: dict[str, Any] | None = None,
        **kwargs: Any,
    ) -> _AsFsspecBufferedFile:
        return _AsFsspecBufferedFile(
            self,
            _as_str_path(path),
            mode=mode,
            block_size=block_size,
            autocommit=autocommit,
            cache_options=cache_options,
            **kwargs,
        )

    async def open_async(
        self,
        path: str,
        mode: str = "rb",
        **kwargs: Any,
    ) -> AsyncBufferedReader | AsyncBufferedWriter:
        if "b" not in mode or kwargs.get("compression"):
            raise ValueError("open_async only supports uncompressed binary modes")
        str_path = _as_str_path(path)
        block_size: int | None = kwargs.get("block_size")
        if mode == "rb":
            return self.core_fs.open(str_path, "rb", block_size=block_size)
        if mode in ("wb", "xb", "ab"):
            return self.core_fs.open(str_path, mode, block_size=block_size)
        raise ValueError(f"Unsupported mode {mode!r}; expected one of 'rb', 'wb', 'xb', 'ab'")


def _protocol_label(protocol: str | tuple[str, ...]) -> str:
    first = protocol if isinstance(protocol, str) else protocol[0]
    cleaned = "".join(ch if ch.isalnum() else "_" for ch in first)
    return cleaned.title().replace("_", "") or "Custom"


def fsspec_class_for(
    core_cls: type[AsyncFS[Any]],
    *,
    protocol: str | tuple[str, ...] | None = None,
    root_marker: str | None = None,
    sep: str | None = None,
) -> type[AsFsspecFileSystem]:
    """Return a cached :class:`AsFsspecFileSystem` subclass specialized for ``core_cls``.

    The generated class carries ``protocol``, ``root_marker``, ``sep``, ``local_file``,
    and ``_strip_protocol`` derived from ``core_cls`` (with optional overrides) so
    ``fsspec`` and ``upath`` path flavours work without instantiating the filesystem.
    """
    resolved_protocol = core_cls.protocol if protocol is None else protocol
    resolved_root = core_cls.root_marker if root_marker is None else root_marker
    resolved_sep = core_cls.sep if sep is None else sep
    key = (core_cls, resolved_protocol, resolved_root, resolved_sep)

    with _CLASS_CACHE_LOCK:
        cached = _CLASS_CACHE.get(key)
        if cached is not None:
            return cached

        cls_name = f"AsFsspec{core_cls.__name__}_{_protocol_label(resolved_protocol)}"
        subcls = cast(
            "type[AsFsspecFileSystem]",
            type(
                cls_name,
                (AsFsspecFileSystem,),
                {
                    "__module__": __name__,
                    "cachable": False,
                    "_core_cls": core_cls,
                    "protocol": resolved_protocol,
                    "root_marker": resolved_root,
                    "sep": resolved_sep,
                    "local_file": core_cls.local_file,
                },
            ),
        )
        _CLASS_CACHE[key] = subcls
        return subcls


def to_fsspec(
    core_fs: AsyncFS[Any],
    *,
    asynchronous: bool = False,
    runner: LoopRunner | None = None,
    protocol: str | tuple[str, ...] | None = None,
) -> AsFsspecFileSystem:
    """Wrap a core :class:`~upathtools.fs.AsyncFS` as an :class:`AsFsspecFileSystem`.

    Args:
        core_fs: Core async filesystem instance.
        asynchronous: If ``True``, create an async-only fsspec instance without
            binding a sync :class:`~upathtools.fs.LoopRunner`.
        runner: Optional :class:`~upathtools.fs.LoopRunner` for sync calls.
        protocol: Optional protocol override for the generated fsspec adapter class.

    Returns:
        An instance of the specialized :class:`AsFsspecFileSystem` subclass for
        ``type(core_fs)``.
    """
    resolved_protocol = core_fs.protocol if protocol is None else protocol
    cls = fsspec_class_for(
        type(core_fs),
        protocol=resolved_protocol,
        root_marker=core_fs.root_marker,
        sep=core_fs.sep,
    )
    return cls(core_fs, asynchronous=asynchronous, runner=runner)


def register_fsspec_bridge(
    protocol: str,
    factory: Callable[..., AsyncFS[Any]],
    *,
    clobber: bool = False,
    root_marker: str | None = None,
    sep: str | None = None,
) -> type[AsFsspecFileSystem]:
    """Generate and register an ``fsspec`` bridge class for ``protocol``.

    After registration, ``fsspec.filesystem(protocol, **opts)``,
    ``fsspec.open(f"{protocol}://...")``, and ``upath.UPath(f"{protocol}://...")``
    resolve to the generated :class:`AsFsspecFileSystem` subclass.

    Args:
        protocol: URI scheme / fsspec protocol name to register.
        factory: Callable ``(**storage_options) -> AsyncFS`` constructing the core fs.
        clobber: Whether to overwrite an existing ``fsspec`` registration.
        root_marker: Optional root marker override (defaults to ``factory.root_marker``
            if ``factory`` is an :class:`AsyncFS` subclass, else ``"/"``).
        sep: Optional path separator override (defaults to ``factory.sep`` or ``"/"``).

    Returns:
        The generated and registered :class:`AsFsspecFileSystem` subclass.
    """
    core_cls: type[AsyncFS[Any]] | None = (
        factory if isinstance(factory, type) and issubclass(factory, AsyncFS) else None
    )
    resolved_root = (
        root_marker
        if root_marker is not None
        else (core_cls.root_marker if core_cls is not None else "/")
    )
    resolved_sep = sep if sep is not None else (core_cls.sep if core_cls is not None else "/")
    resolved_local = core_cls.local_file if core_cls is not None else False

    def _bridge_init(
        self: AsFsspecFileSystem,
        *args: Any,
        asynchronous: bool = False,
        loop: asyncio.AbstractEventLoop | None = None,
        runner: LoopRunner | None = None,
        **storage_options: Any,
    ) -> None:
        core_fs = factory(*args, **storage_options)
        AsFsspecFileSystem.__init__(
            self,
            core_fs,
            asynchronous=asynchronous,
            loop=loop,
            runner=runner,
        )

    cls_name = f"Bridged{_protocol_label(protocol)}FileSystem"
    bridge_cls = cast(
        "type[AsFsspecFileSystem]",
        type(
            cls_name,
            (AsFsspecFileSystem,),
            {
                "__module__": __name__,
                "__init__": _bridge_init,
                "cachable": False,
                "_core_cls": core_cls,
                "protocol": protocol,
                "root_marker": resolved_root,
                "sep": resolved_sep,
                "local_file": resolved_local,
            },
        ),
    )
    fsspec.register_implementation(protocol, bridge_cls, clobber=clobber)
    return bridge_cls


def register_upath_bridge(
    protocol: str,
    factory: Callable[..., AsyncFS[Any]],
    *,
    clobber: bool = False,
    root_marker: str | None = None,
    sep: str | None = None,
) -> type[UPath]:
    """Register both an ``fsspec`` bridge and a :class:`upath.UPath` subclass for ``protocol``.

    The generated :class:`~upath.UPath` subclass overrides ``_fs_factory`` to construct
    the core filesystem via ``factory(**storage_options)`` and wrap it with
    :func:`to_fsspec`.

    Args:
        protocol: URI scheme to register in both ``fsspec`` and ``upath``.
        factory: Callable ``(**storage_options) -> AsyncFS`` constructing the core fs.
        clobber: Whether to overwrite existing registrations in ``fsspec`` and ``upath``.
        root_marker: Optional root marker override for the path flavour.
        sep: Optional path separator override for the path flavour.

    Returns:
        The generated and registered :class:`~upath.UPath` subclass.
    """
    register_fsspec_bridge(
        protocol,
        factory,
        clobber=clobber,
        root_marker=root_marker,
        sep=sep,
    )
    core_cls: type[AsyncFS[Any]] | None = (
        factory if isinstance(factory, type) and issubclass(factory, AsyncFS) else None
    )

    @classmethod
    def _fs_factory(
        cls: type[UPath],
        urlpath: str,
        protocol_arg: str,
        storage_options: Mapping[str, Any],
    ) -> AsFsspecFileSystem:
        opts = dict(storage_options)
        if core_cls is not None:
            inferred = core_cls.kwargs_from_url(urlpath)
            opts = {**inferred, **opts}
        core_fs = factory(**opts)
        return to_fsspec(core_fs, protocol=protocol_arg or protocol)

    path_cls_name = f"Bridged{_protocol_label(protocol)}Path"
    path_cls = cast(
        "type[UPath]",
        type(
            path_cls_name,
            (UPath,),
            {
                "__module__": __name__,
                "_fs_factory": _fs_factory,
            },
        ),
    )
    upath_registry.register_implementation(protocol, path_cls, clobber=clobber)
    return path_cls
