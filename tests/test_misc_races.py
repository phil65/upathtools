"""Concurrent HTTP session setup and AsyncUPath.aglob fallback."""

import asyncio
from typing import Any

import fsspec
import pytest

from upathtools import AsyncUPath
from upathtools.filesystems import HTTPFileSystem


async def test_concurrent_set_session_keeps_one_client() -> None:
    created: list[Any] = []
    closed: list[Any] = []

    class Client:
        async def aclose(self) -> None:
            closed.append(self)

    async def get_client(**kwargs: Any) -> Any:
        await asyncio.sleep(0.01)
        client = Client()
        created.append(client)
        return client

    fs = HTTPFileSystem(get_client=get_client, asynchronous=True, skip_instance_cache=True)
    a, b = await asyncio.gather(fs.set_session(), fs.set_session())
    assert a is b
    assert len(created) == 2  # noqa: PLR2004
    assert closed == [c for c in created if c is not a]


async def test_aglob_failure_midway_does_not_duplicate(monkeypatch: pytest.MonkeyPatch) -> None:
    mem = fsspec.filesystem("memory")
    mem.pipe({"/aglob-t/a.txt": b"a", "/aglob-t/b.txt": b"b"})
    try:
        base = AsyncUPath("memory:///aglob-t")
        fs = await base.afs()

        async def broken_glob(path: str, **kw: Any) -> list[Any]:
            return ["/aglob-t/a.txt", 42]  # second entry can't become a path

        monkeypatch.setattr(fs, "_glob", broken_glob)
        seen: list[str] = []

        async def collect() -> None:
            async for p in base.aglob("*.txt"):
                seen.append(p.path)  # noqa: PERF401

        with pytest.raises(TypeError):
            await collect()
        assert seen == ["/aglob-t/a.txt"]
    finally:
        mem.rm("/aglob-t", recursive=True)
