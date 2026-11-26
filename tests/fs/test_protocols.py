from typing import Any

from upathtools.fs import (
    GrepMatch,
    SupportsGrep,
    SupportsMove,
    SupportsRangeRead,
    SupportsSign,
    SupportsWrite,
    WriteMode,
)


class _WritableFS:
    async def write_bytes(self, path: str, data: bytes, *, mode: WriteMode = "overwrite") -> None:
        pass

    async def remove(
        self, path: str, *, recursive: bool = False, maxdepth: int | None = None
    ) -> None:
        pass

    async def mkdir(self, path: str, *, parents: bool = True, exist_ok: bool = False) -> None:
        pass

    async def rmdir(self, path: str) -> None:
        pass


class _PartialWritableFS:
    async def write_bytes(self, path: str, data: bytes, *, mode: WriteMode = "overwrite") -> None:
        pass

    async def remove(
        self, path: str, *, recursive: bool = False, maxdepth: int | None = None
    ) -> None:
        pass

    async def mkdir(self, path: str, *, parents: bool = True, exist_ok: bool = False) -> None:
        pass


class _FullCapabilitiesFS(_WritableFS):
    async def read_range(
        self, path: str, start: int | None = None, end: int | None = None
    ) -> bytes:
        return b""

    async def move(
        self, src: str, dst: str, *, recursive: bool = False, overwrite: bool = True
    ) -> None:
        pass

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
    ) -> list[GrepMatch]:
        return []

    async def sign(self, path: str, *, expiration: int = 100, **kwargs: Any) -> str:
        return f"https://example.invalid/{path}?exp={expiration}"


class _EmptyFS:
    pass


def test_supports_write_protocol() -> None:
    assert isinstance(_WritableFS(), SupportsWrite)
    assert not isinstance(_PartialWritableFS(), SupportsWrite)
    assert not isinstance(_EmptyFS(), SupportsWrite)


def test_other_capability_protocols() -> None:
    full = _FullCapabilitiesFS()
    empty = _EmptyFS()

    assert isinstance(full, SupportsRangeRead)
    assert isinstance(full, SupportsWrite)
    assert isinstance(full, SupportsMove)
    assert isinstance(full, SupportsGrep)
    assert isinstance(full, SupportsSign)

    assert not isinstance(empty, SupportsRangeRead)
    assert not isinstance(empty, SupportsMove)
    assert not isinstance(empty, SupportsGrep)
    assert not isinstance(empty, SupportsSign)
