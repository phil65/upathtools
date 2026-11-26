"""Thread-safe directory listing cache with TTL expiry and LRU eviction."""

from collections import OrderedDict
import threading
import time
from typing import TYPE_CHECKING

from upathtools.fs._sync import FSSPEC_SYNC_VERSION, FsspecSyncNote
from upathtools.fs.types import FileInfo


if TYPE_CHECKING:
    from collections.abc import Callable, Iterable


__fsspec_sync__ = FsspecSyncNote(
    version=FSSPEC_SYNC_VERSION,
    upstream=("fsspec/dircache.py:DirCache",),
    notes=(
        "Thread-safe rewrite guarded by a threading.RLock (free-threading safe, "
        "no unlocked read-modify-write and no functools.lru_cache side-effect hook). "
        "Stores immutable tuple[TInfo, ...] snapshots, uses an injectable monotonic "
        "clock for TTL expiry, maintains true LRU ordering in an OrderedDict, and "
        "provides prefix-aware invalidate()."
    ),
)


def _normalize_cache_key(path: str) -> str:
    """Normalize trailing slashes while preserving absolute root ``'/'``."""
    stripped = path.rstrip("/")
    if not stripped and path.startswith("/"):
        return "/"
    return stripped


class DirCache[TInfo: FileInfo = FileInfo]:
    """Thread-safe directory listing cache with optional TTL and LRU bounds.

    Maps a normalized directory path to an immutable ``tuple[TInfo, ...]`` snapshot.
    All mutations and lookups are synchronized under an internal ``threading.RLock``
    so concurrent access under free-threaded Python is safe.

    Attributes:
        expiry_time: Entry time-to-live in seconds, or ``None`` for no expiry.
        max_paths: Maximum number of cached directory paths, or ``None`` for unbounded.
    """

    __slots__ = ("_clock", "_entries", "_lock", "expiry_time", "max_paths")

    def __init__(
        self,
        expiry_time: float | None = None,
        max_paths: int | None = None,
        *,
        clock: Callable[[], float] = time.monotonic,
    ) -> None:
        """Initialize the directory cache.

        Args:
            expiry_time: Entry TTL in seconds (must be ``>= 0``), or ``None``.
            max_paths: Maximum number of cached paths (must be ``>= 0``), or ``None``.
            clock: Monotonic clock callable returning current seconds (injectable for tests).

        Raises:
            ValueError: If ``expiry_time`` or ``max_paths`` is negative.
        """
        if expiry_time is not None and expiry_time < 0:
            raise ValueError(f"expiry_time must be non-negative or None, got {expiry_time}")
        if max_paths is not None and max_paths < 0:
            raise ValueError(f"max_paths must be non-negative or None, got {max_paths}")

        self.expiry_time = expiry_time
        self.max_paths = max_paths
        self._clock = clock
        self._lock = threading.RLock()
        self._entries: OrderedDict[str, tuple[tuple[TInfo, ...], float]] = OrderedDict()

    def _is_expired(self, stored_at: float, now: float) -> bool:
        return self.expiry_time is not None and (now - stored_at) >= self.expiry_time

    def get(self, path: str) -> tuple[TInfo, ...] | None:
        """Return the cached listing snapshot for ``path``, or ``None`` if absent/expired.

        A cache hit refreshes the entry's LRU recency.
        """
        key = _normalize_cache_key(path)
        with self._lock:
            entry = self._entries.get(key)
            if entry is None:
                return None
            snapshot, stored_at = entry
            if self._is_expired(stored_at, self._clock()):
                del self._entries[key]
                return None
            self._entries.move_to_end(key)
            return snapshot

    def set(self, path: str, entries: Iterable[TInfo]) -> None:
        """Store an immutable snapshot of ``entries`` for ``path``.

        Refreshes the entry's timestamp and LRU recency, evicting the least-recently-used
        entries if ``max_paths`` is exceeded.
        """
        snapshot = tuple(entries)
        key = _normalize_cache_key(path)
        with self._lock:
            if self.max_paths == 0:
                return
            now = self._clock()
            self._entries[key] = (snapshot, now)
            self._entries.move_to_end(key)
            if self.max_paths is not None:
                while len(self._entries) > self.max_paths:
                    self._entries.popitem(last=False)

    def invalidate(self, path: str | None = None) -> None:
        """Drop cached entries.

        Args:
            path: When ``None``, clears the entire cache. Otherwise removes ``path``
                and every cached path nested beneath it (e.g., ``"/a"`` drops ``"/a"``
                and ``"/a/b"`` without affecting ``"/ab"``), plus the listing of its
                parent directory (which lists ``path``).
        """
        with self._lock:
            if path is None:
                self._entries.clear()
                return
            key = _normalize_cache_key(path)
            if key == "":
                to_remove = [k for k in self._entries if not k.startswith("/")]
            elif key == "/":
                to_remove = [k for k in self._entries if k.startswith("/")]
            else:
                prefix = f"{key}/"
                # The parent's listing contains ``path`` itself, so it is stale as well.
                parent = (key.rsplit("/", 1)[0] or "/") if "/" in key else ""
                to_remove = [k for k in self._entries if k in {key, parent} or k.startswith(prefix)]
            for k in to_remove:
                del self._entries[k]

    def __contains__(self, path: object) -> bool:
        if not isinstance(path, str):
            return False
        key = _normalize_cache_key(path)
        with self._lock:
            entry = self._entries.get(key)
            if entry is None:
                return False
            _, stored_at = entry
            if self._is_expired(stored_at, self._clock()):
                del self._entries[key]
                return False
            return True

    def __len__(self) -> int:
        with self._lock:
            if self.expiry_time is not None:
                now = self._clock()
                expired = [
                    k
                    for k, (_, stored_at) in self._entries.items()
                    if self._is_expired(stored_at, now)
                ]
                for k in expired:
                    del self._entries[k]
            return len(self._entries)
