"""Runtime-checkable capability protocols for ``upathtools.fs`` filesystems."""

from typing import TYPE_CHECKING, Any, Protocol, runtime_checkable


if TYPE_CHECKING:
    from upathtools.fs.types import GrepMatch, WriteMode


@runtime_checkable
class SupportsRangeRead(Protocol):
    """Protocol for filesystems supporting byte-range reads."""

    async def read_range(
        self,
        path: str,
        start: int | None = None,
        end: int | None = None,
    ) -> bytes: ...


@runtime_checkable
class SupportsWrite(Protocol):
    """Protocol for filesystems supporting write, delete, and directory mutations."""

    async def write_bytes(
        self,
        path: str,
        data: bytes,
        *,
        mode: WriteMode = "overwrite",
    ) -> None: ...

    async def remove(
        self,
        path: str,
        *,
        recursive: bool = False,
        maxdepth: int | None = None,
    ) -> None: ...

    async def mkdir(
        self,
        path: str,
        *,
        parents: bool = True,
        exist_ok: bool = False,
    ) -> None: ...

    async def rmdir(self, path: str) -> None: ...


@runtime_checkable
class SupportsMove(Protocol):
    """Protocol for filesystems supporting native move/rename operations."""

    async def move(
        self,
        src: str,
        dst: str,
        *,
        recursive: bool = False,
        overwrite: bool = True,
    ) -> None: ...


@runtime_checkable
class SupportsGrep(Protocol):
    """Protocol for filesystems supporting native content search."""

    async def grep(
        self,
        path: str,
        pattern: str,
        *,
        max_count: int | None = None,
        case_sensitive: bool | None = None,
        hidden: bool = False,
        no_ignore: bool = False,
        globs: list[str] | None = None,
        context_before: int | None = None,
        context_after: int | None = None,
        multiline: bool = False,
    ) -> list[GrepMatch]: ...


@runtime_checkable
class SupportsSign(Protocol):
    """Protocol for filesystems supporting signed URL generation."""

    async def sign(self, path: str, *, expiration: int = 100, **kwargs: Any) -> str: ...
