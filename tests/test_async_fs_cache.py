"""get_async_fs never shares filesystems between instances, loops or threads."""

from __future__ import annotations

import asyncio
import sys
import threading
from typing import Any

import fsspec
from fsspec.asyn import AsyncFileSystem
import pytest

from upathtools import AsyncUPath
from upathtools.async_ops import get_async_fs
from upathtools.filesystems import IsolatedMemoryFileSystem, UnionFileSystem


async def test_equal_instances_are_not_shared() -> None:
    a = IsolatedMemoryFileSystem()
    b = IsolatedMemoryFileSystem()
    a.pipe("/f.txt", b"from A")
    b.pipe("/f.txt", b"from B")
    path = AsyncUPath._from_upath(b.get_upath("/f.txt"))
    assert await path.aread_bytes() == b"from B"
    assert await (await get_async_fs(a))._cat_file("/f.txt") == b"from A"
    assert await (await get_async_fs(b))._cat_file("/f.txt") == b"from B"


async def test_union_instances_are_not_shared() -> None:
    u1, u2 = UnionFileSystem({}), UnionFileSystem({})
    assert await get_async_fs(u1) is u1
    assert await get_async_fs(u2) is u2


class LoopBoundFS(AsyncFileSystem):
    """Async fs that, like an HTTP session, only works on the loop that created it."""

    protocol = "loopboundtest"

    def __init__(self, **kwargs: Any) -> None:
        super().__init__(**kwargs)
        self.created_on = asyncio.get_running_loop()

    async def _cat_file(self, path: str, start: Any = None, end: Any = None, **kw: Any) -> bytes:
        if asyncio.get_running_loop() is not self.created_on:
            raise RuntimeError("Event loop is closed")
        return b"ok"


@pytest.fixture
def loopbound_protocol():
    fsspec.register_implementation("loopboundtest", LoopBoundFS, clobber=True)
    yield "loopboundtest"
    sys.modules["fsspec.registry"]._registry.pop("loopboundtest", None)


def test_protocol_fs_per_event_loop(loopbound_protocol: str) -> None:
    async def read() -> bytes:
        fs = await get_async_fs(f"{loopbound_protocol}://x")
        return await fs._cat_file("x")

    assert asyncio.run(read()) == b"ok"
    assert asyncio.run(read()) == b"ok"  # a second loop gets its own instance

    results: list[Any] = []

    def worker() -> None:
        try:
            results.append(asyncio.run(read()))
        except Exception as e:  # noqa: BLE001
            results.append(e)

    t = threading.Thread(target=worker)
    t.start()
    t.join()
    assert results == [b"ok"]


async def test_protocol_fs_reused_within_a_loop(loopbound_protocol: str) -> None:
    first = await get_async_fs(f"{loopbound_protocol}://x")
    assert await get_async_fs(f"{loopbound_protocol}://y") is first
