"""Isolated memory filesystem with per-key storage."""

from __future__ import annotations

from typing import Any, ClassVar
import uuid

from fsspec.implementations.memory import MemoryFileSystem


class IsolatedMemoryFileSystem(MemoryFileSystem):
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

    # Class-level storage maps, keyed by isolation key
    _stores: ClassVar[dict[str, dict[str, Any]]] = {}
    _pseudo_dirs_map: ClassVar[dict[str, list[str]]] = {}

    # Override class attributes to prevent using parent's global storage
    store: dict[str, Any]  # type: ignore[assignment]
    pseudo_dirs: list[str]  # type: ignore[assignment]

    protocol = "isolated-memory"

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
        # Generate unique key if not provided
        if key is None:
            key = uuid.uuid4().hex
        self._key = key

        # Initialize storage for this key if it doesn't exist
        if key not in self._stores:
            self._stores[key] = {}
            self._pseudo_dirs_map[key] = [""]

        # Set instance-level storage (overrides class attributes)
        self.store = self._stores[key]
        self.pseudo_dirs = self._pseudo_dirs_map[key]

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
