"""Isolated memory filesystem with per-key storage."""

import threading
from typing import Any, ClassVar
import uuid
import weakref

from fsspec.implementations.memory import MemoryFileSystem
from upath.implementations.memory import MemoryPath

from upathtools.filesystems.base import BaseFileSystem


class IsolatedMemoryFileSystem(BaseFileSystem[MemoryPath], MemoryFileSystem):
    """MemoryFileSystem with per-key isolated storage.

    Unlike the standard MemoryFileSystem which uses global class-level storage,
    this implementation allows for isolated storage namespaces via a key parameter.

    Usage:
        # Fully isolated (unique UUID key, no sharing)
        fs1 = IsolatedMemoryFileSystem()
        fs2 = IsolatedMemoryFileSystem()
        # fs1 and fs2 have completely separate storage

        # Shared by key (same key = same storage)
        fs3 = IsolatedMemoryFileSystem(key="session_123")
        fs4 = IsolatedMemoryFileSystem(key="session_123")
        # fs3 and fs4 share the same storage

        # Explicit global sharing
        fs5 = IsolatedMemoryFileSystem(key="global")
        fs6 = IsolatedMemoryFileSystem(key="global")
        # All instances with key="global" share storage
    """

    upath_cls = MemoryPath

    # Class-level storage maps, keyed by isolation key
    _stores: ClassVar[dict[str, dict[str, Any]]] = {}
    _pseudo_dirs_map: ClassVar[dict[str, list[str]]] = {}
    _stores_lock: ClassVar[threading.Lock] = threading.Lock()

    # Override class attributes to prevent using parent's global storage
    store: dict[str, Any]  # type: ignore[assignment]
    pseudo_dirs: list[str]  # type: ignore[assignment]

    protocol = "memory"

    # Disable fsspec's instance caching - each call should create a new instance
    # with its own isolated storage (unless using the same key)
    cachable = False

    def __init__(self, key: str | None = None, **storage_options: Any) -> None:
        """Initialize an isolated memory filesystem.

        Args:
            key: Isolation key for storage namespace. If None, a unique UUID
                is generated, making this instance fully isolated. If provided,
                all instances with the same key share the same storage.
            **storage_options: Additional options passed to parent class.
        """
        anonymous = key is None
        # Generate unique key if not provided
        if key is None:
            key = uuid.uuid4().hex
        self._key = key

        # Initialize storage for this key if it doesn't exist (atomic setdefault)
        with self._stores_lock:
            self.store = self._stores.setdefault(key, {})
            self.pseudo_dirs = self._pseudo_dirs_map.setdefault(key, [""])
        if anonymous:
            # Nobody else knows the key: drop the store with the instance
            weakref.finalize(self, self.remove_key, key)

        # Call parent init
        super().__init__(**storage_options)

    @property
    def key(self) -> str:
        """Return the isolation key for this filesystem."""
        return self._key

    def clear(self) -> None:
        """Clear all data in this filesystem's storage namespace."""
        self.store.clear()
        self.pseudo_dirs.clear()
        self.pseudo_dirs.append("")

    @classmethod
    def clear_key(cls, key: str) -> None:
        """Clear storage for a specific key.

        Args:
            key: The isolation key to clear.
        """
        if key in cls._stores:
            cls._stores[key].clear()
        if key in cls._pseudo_dirs_map:
            cls._pseudo_dirs_map[key].clear()
            cls._pseudo_dirs_map[key].append("")

    @classmethod
    def remove_key(cls, key: str) -> None:
        """Remove a key's storage entirely.

        Args:
            key: The isolation key to remove.
        """
        with cls._stores_lock:
            cls._stores.pop(key, None)
            cls._pseudo_dirs_map.pop(key, None)

    @classmethod
    def list_keys(cls) -> list[str]:
        """List all active isolation keys.

        Returns:
            List of isolation keys currently in use.
        """
        return list(cls._stores.keys())


if __name__ == "__main__":
    # Quick test
    fs1 = IsolatedMemoryFileSystem()
    fs1.pipe("/test/file.txt", b"hello from fs1")

    fs2 = IsolatedMemoryFileSystem()
    print(f"fs2 sees fs1 file: {fs2.exists('/test/file.txt')}")  # False

    fs3 = IsolatedMemoryFileSystem(key="shared")
    fs3.pipe("/shared/data.txt", b"shared data")

    fs4 = IsolatedMemoryFileSystem(key="shared")
    print(f"fs4 sees fs3 file: {fs4.exists('/shared/data.txt')}")  # True
    print(f"fs4 content: {fs4.cat('/shared/data.txt')}")  # b'shared data'

    print(f"Active keys: {IsolatedMemoryFileSystem.list_keys()}")
