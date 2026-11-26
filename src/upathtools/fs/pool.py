"""Loop-scoped, thread-safe filesystem instance pool for ``upathtools.fs``."""

import asyncio
import threading
from typing import TYPE_CHECKING, Any
import weakref

from upathtools.fs.paths import tokenize
from upathtools.fs.registry import ProtocolRegistry, default_registry


if TYPE_CHECKING:
    from upathtools.fs.base import AsyncFS


class FilesystemPool:
    """Loop-scoped cache of core :class:`~upathtools.fs.AsyncFS` instances.

    Instances are keyed by ``(event_loop, protocol, tokenized_storage_options)``
    using a :class:`weakref.WeakKeyDictionary` over event loops guarded by a
    :class:`threading.Lock`.

    - Within the same running event loop, repeated :meth:`get` calls with the same
      protocol and storage options return the exact same :class:`AsyncFS` instance.
    - Different event loops (sequential ``asyncio.run()`` calls on one thread or
      concurrent loops across threads) always receive independent instances.
    - Entries belonging to closed loops are pruned on access, and garbage-collected
      loops drop their entries automatically.
    """

    def __init__(self, registry: ProtocolRegistry | None = None) -> None:
        """Initialize a :class:`FilesystemPool`.

        Args:
            registry: Optional :class:`ProtocolRegistry` used to construct new
                filesystem instances. Defaults to :data:`default_registry`.
        """
        self._registry = default_registry if registry is None else registry
        self._lock = threading.Lock()
        self._pools: weakref.WeakKeyDictionary[
            asyncio.AbstractEventLoop,
            dict[tuple[str, str], AsyncFS[Any]],
        ] = weakref.WeakKeyDictionary()

    @property
    def registry(self) -> ProtocolRegistry:
        """The :class:`ProtocolRegistry` backing this pool."""
        return self._registry

    def _prune_closed_loops_locked(self) -> None:
        closed_loops = [loop for loop in list(self._pools) if loop.is_closed()]
        for loop in closed_loops:
            self._pools.pop(loop, None)

    def __len__(self) -> int:
        """Return total cached filesystem count across live (non-closed) loops."""
        with self._lock:
            self._prune_closed_loops_locked()
            return sum(len(bucket) for bucket in self._pools.values())

    def get(self, protocol: str, **storage_options: Any) -> AsyncFS[Any]:
        """Return the cached :class:`AsyncFS` for the running loop and options.

        Must be called while an asyncio event loop is running.

        Raises:
            RuntimeError: If no event loop is currently running.
        """
        loop = asyncio.get_running_loop()
        proto = protocol or "file"
        key = (proto, tokenize(storage_options))

        with self._lock:
            self._prune_closed_loops_locked()
            bucket = self._pools.get(loop)
            if bucket is not None:
                existing = bucket.get(key)
                if existing is not None and not existing.closed:
                    return existing

        created = self._registry.filesystem(proto, **storage_options)

        with self._lock:
            self._prune_closed_loops_locked()
            bucket = self._pools.setdefault(loop, {})
            existing = bucket.get(key)
            if existing is not None and not existing.closed:
                return existing
            bucket[key] = created
            return created

    async def aclose_loop(self, loop: asyncio.AbstractEventLoop | None = None) -> None:
        """Close and drop every cached filesystem associated with ``loop``.

        Args:
            loop: Target event loop. Defaults to the currently running event loop.
        """
        target_loop = asyncio.get_running_loop() if loop is None else loop
        with self._lock:
            self._prune_closed_loops_locked()
            bucket = self._pools.pop(target_loop, None)

        if not bucket:
            return

        errors: list[Exception] = []
        for fs in bucket.values():
            try:
                await fs.aclose()
            except Exception as exc:  # noqa: BLE001
                errors.append(exc)

        if len(errors) == 1:
            raise errors[0]
        if errors:
            raise ExceptionGroup("Errors closing pooled filesystems", errors)

    async def aclose_all(self) -> None:
        """Close and drop every cached filesystem for the current event loop.

        Also prunes entries for any closed loops. Filesystems belonging to other
        still-running loops are left untouched because closing loop-bound clients
        from a foreign loop is unsafe.
        """
        await self.aclose_loop()
