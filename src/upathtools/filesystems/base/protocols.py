"""Optional coroutines of async filesystems.

fsspec's `AsyncFileSystem` does not define these, while `AsyncFileSystemWrapper`
and many implementations do. Check with `isinstance()` before calling.
"""

from __future__ import annotations

from typing import Any, Protocol, runtime_checkable


@runtime_checkable
class SupportsRmdir(Protocol):
    async def _rmdir(self, path: str, **kwargs: Any) -> None: ...


@runtime_checkable
class SupportsMv(Protocol):
    async def _mv(self, path1: str, path2: str, recursive: bool = False, **kwargs: Any) -> None: ...


@runtime_checkable
class SupportsChecksum(Protocol):
    async def _checksum(self, path: str, **kwargs: Any) -> Any: ...


@runtime_checkable
class SupportsModified(Protocol):
    async def _modified(self, path: str, **kwargs: Any) -> Any: ...


@runtime_checkable
class SupportsCreated(Protocol):
    async def _created(self, path: str, **kwargs: Any) -> Any: ...
