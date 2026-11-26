"""Thread-safe protocol registry and URL resolver for ``upathtools.fs``."""

from importlib.metadata import EntryPoint, entry_points
import os
import threading
from typing import TYPE_CHECKING, Any

from fsspec.asyn import AsyncFileSystem
from fsspec.registry import get_filesystem_class as fsspec_get_filesystem_class

from upathtools.fs._sync import FSSPEC_SYNC_VERSION, FsspecSyncNote
from upathtools.fs.implementations.local import LocalFS
from upathtools.fs.implementations.memory import MemoryFS
from upathtools.fs.interop.from_fsspec import FsspecAdapter
from upathtools.fs.paths import split_protocol


if TYPE_CHECKING:
    from collections.abc import Callable

    from upathtools.fs.base import AsyncFS


__fsspec_sync__ = FsspecSyncNote(
    version=FSSPEC_SYNC_VERSION,
    upstream=(
        "fsspec/registry.py:register_implementation",
        "fsspec/registry.py:get_filesystem_class",
        "fsspec/core.py:url_to_fs",
    ),
    notes=(
        "Thread-safe ProtocolRegistry with explicit factories, lazy one-time "
        "importlib.metadata entry-point loading under group 'upathtools.filesystems', "
        "and fsspec fallback wrapped in FsspecAdapter (passing asynchronous=True for "
        "AsyncFileSystem subclasses and skip_instance_cache=True). Chained 'a::b' URLs "
        "currently raise NotImplementedError."
    ),
)

ENTRY_POINT_GROUP = "upathtools.filesystems"


class ProtocolRegistry:
    """Thread-safe registry mapping protocol names to core :class:`AsyncFS` factories.

    Lookup order for :meth:`filesystem` and :meth:`url_to_fs`:
      1. Explicitly registered factories via :meth:`register`.
      2. Entry points in group ``"upathtools.filesystems"`` (discovered lazily once).
      3. ``fsspec``'s protocol registry, wrapped in :class:`~upathtools.fs.FsspecAdapter`
         (with ``asynchronous=True`` when the ``fsspec`` class is an ``AsyncFileSystem``).
    """

    def __init__(
        self,
        *,
        entry_point_group: str | None = ENTRY_POINT_GROUP,
        fsspec_fallback: bool = True,
    ) -> None:
        self._lock = threading.RLock()
        self._factories: dict[str, Callable[..., AsyncFS[Any]]] = {}
        self._ep_factories: dict[str, Callable[..., AsyncFS[Any]]] = {}
        self._entry_point_group = entry_point_group
        self._entry_points_loaded = entry_point_group is None
        self._entry_points: dict[str, EntryPoint] = {}
        self._fsspec_fallback = fsspec_fallback

    def _ensure_entry_points_locked(self) -> None:
        if self._entry_points_loaded:
            return
        assert self._entry_point_group is not None
        eps = entry_points(group=self._entry_point_group)
        self._entry_points = {ep.name: ep for ep in eps}
        self._entry_points_loaded = True

    def register(
        self,
        protocol: str,
        factory: Callable[..., AsyncFS[Any]],
        *,
        clobber: bool = False,
    ) -> None:
        """Register ``factory`` for ``protocol``.

        Args:
            protocol: Non-empty protocol name (e.g. ``"memory"``, ``"s3"``).
            factory: Callable ``(**storage_options) -> AsyncFS``.
            clobber: If ``False`` (default), raises :class:`ValueError` when
                ``protocol`` is already explicitly registered.
        """
        if not protocol:
            raise ValueError("protocol must be a non-empty string")
        with self._lock:
            if not clobber and protocol in self._factories:
                raise ValueError(
                    f"Protocol {protocol!r} is already registered (pass clobber=True to replace)"
                )
            self._factories[protocol] = factory

    def unregister(self, protocol: str) -> None:
        """Remove an explicitly registered ``protocol``.

        Raises:
            KeyError: If ``protocol`` is not explicitly registered.
        """
        with self._lock:
            if protocol not in self._factories:
                raise KeyError(f"Protocol {protocol!r} is not registered")
            del self._factories[protocol]

    def get_factory(self, protocol: str) -> Callable[..., AsyncFS[Any]] | None:
        """Return the registered or entry-point factory for ``protocol``, or ``None``."""
        with self._lock:
            factory = self._factories.get(protocol)
            if factory is not None:
                return factory
            self._ensure_entry_points_locked()
            cached_ep = self._ep_factories.get(protocol)
            if cached_ep is not None:
                return cached_ep
            ep = self._entry_points.get(protocol)
            if ep is None:
                return None
            loaded: Callable[..., AsyncFS[Any]] = ep.load()
            self._ep_factories[protocol] = loaded
            return loaded

    def protocols(self) -> frozenset[str]:
        """Return all protocol names known natively (registered plus entry points)."""
        with self._lock:
            self._ensure_entry_points_locked()
            return frozenset(self._factories) | frozenset(self._entry_points)

    def filesystem(self, protocol: str, **storage_options: Any) -> AsyncFS[Any]:
        """Instantiate a core :class:`AsyncFS` for ``protocol`` and ``storage_options``."""
        proto = protocol or "file"
        factory = self.get_factory(proto)
        if factory is not None:
            return factory(**storage_options)

        if not self._fsspec_fallback:
            raise ValueError(f"Protocol not known: {proto}")

        fs_cls = fsspec_get_filesystem_class(proto)
        opts = dict(storage_options)
        if issubclass(fs_cls, AsyncFileSystem):
            opts.setdefault("asynchronous", True)
        opts.setdefault("skip_instance_cache", True)
        raw_fs = fs_cls(**opts)
        return FsspecAdapter(raw_fs)

    def url_to_fs(
        self,
        url: str | os.PathLike[str],
        **storage_options: Any,
    ) -> tuple[AsyncFS[Any], str]:
        """Resolve ``url`` into a ``(core_fs, normalized_path)`` pair.

        Chained ``fsspec`` URLs (containing ``"::"``) are not yet supported and raise
        :class:`NotImplementedError`.
        """
        url_str = os.fspath(url)
        if "::" in url_str:
            raise NotImplementedError(f"Chained URLs are not yet supported: {url_str!r}")

        protocol, _ = split_protocol(url_str)
        proto = protocol or "file"

        factory = self.get_factory(proto)
        if factory is not None:
            kw_extractor = getattr(factory, "kwargs_from_url", None)
            url_kwargs: dict[str, Any] = kw_extractor(url_str) if callable(kw_extractor) else {}
        elif self._fsspec_fallback:
            fs_cls = fsspec_get_filesystem_class(proto)
            url_kwargs = fs_cls._get_kwargs_from_urls(url_str)
        else:
            raise ValueError(f"Protocol not known: {proto}")

        merged = {**url_kwargs, **storage_options}
        fs = self.filesystem(proto, **merged)
        return fs, fs._norm(url_str)


default_registry = ProtocolRegistry()
default_registry.register("memory", MemoryFS)
default_registry.register("file", LocalFS)
default_registry.register("local", LocalFS)
