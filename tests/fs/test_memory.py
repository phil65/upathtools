"""Tests for :class:`upathtools.fs.MemoryFS` and :class:`upathtools.fs.MemoryStore`."""

import asyncio
from concurrent.futures import ThreadPoolExecutor
import contextlib
from typing import TYPE_CHECKING

import pytest

from tests.fs.contract import AsyncFSContract
from upathtools.fs import (
    FSSPEC_SYNC_VERSION,
    FilesystemClosedError,
    MemoryFS,
    MemoryStore,
)
from upathtools.fs.implementations import memory as memory_mod


if TYPE_CHECKING:
    from collections.abc import AsyncIterator


class TestMemoryFSContract(AsyncFSContract):
    """Run the full AsyncFS behavioral contract against MemoryFS."""

    @pytest.fixture
    async def fs(self) -> AsyncIterator[MemoryFS]:
        mem = MemoryFS()
        yield mem
        await mem.aclose()

    @pytest.fixture
    async def root(self, fs: MemoryFS) -> str:
        await fs.mkdir("/work", exist_ok=True)
        return "/work"


async def test_memory_protocol_stripping_and_normalization() -> None:
    fs = MemoryFS()
    await fs.write_bytes("memory://a/b/c.txt", b"hello")
    assert await fs.read_bytes("a/b/c.txt") == b"hello"
    assert await fs.read_bytes("/a/b/c.txt") == b"hello"
    assert await fs.read_bytes("coremem:///a/b/c.txt") == b"hello"
    assert (await fs.info("memory://a/b/")).path == "/a/b"
    assert MemoryFS.strip_protocol("memory://") == "/"
    assert MemoryFS.strip_protocol("coremem:///x/y/") == "/x/y"


async def test_memory_instance_isolation() -> None:
    fs1 = MemoryFS()
    fs2 = MemoryFS()

    await fs1.write_bytes("/only_in_1.txt", b"1")
    assert await fs1.exists("/only_in_1.txt")
    assert not await fs2.exists("/only_in_1.txt")


async def test_memory_shared_store() -> None:
    store = MemoryStore()
    fs1 = MemoryFS(store=store)
    fs2 = MemoryFS(store=store)

    await fs1.write_bytes("/shared/data.bin", b"shared")
    assert await fs2.read_bytes("/shared/data.bin") == b"shared"

    # Closing fs1 closes only fs1; fs2 remains usable on the same store.
    await fs1.aclose()
    with pytest.raises(FilesystemClosedError):
        await fs1.info("/shared/data.bin")
    assert await fs2.read_bytes("/shared/data.bin") == b"shared"


async def test_memory_implicit_parent_creation_and_copy_input() -> None:
    fs = MemoryFS()
    buf = bytearray(b"mutable")
    await fs.write_bytes("/deep/nested/dir/file.bin", buf)

    # Mutating caller buffer after write must not affect stored bytes.
    buf[:] = b"CHANGED"
    assert await fs.read_bytes("/deep/nested/dir/file.bin") == b"mutable"
    assert await fs.is_dir("/deep")
    assert await fs.is_dir("/deep/nested")
    assert await fs.is_dir("/deep/nested/dir")

    info = await fs.info("/deep/nested/dir/file.bin")
    assert info.mtime is not None
    assert info.ctime is not None

    with pytest.raises(NotADirectoryError):
        await fs.write_bytes("/deep/nested/dir/file.bin/child", b"nope")


async def test_memory_atomic_subtree_move() -> None:
    fs = MemoryFS()
    await fs.write_bytes("/src/a.txt", b"A")
    await fs.write_bytes("/src/sub/b.txt", b"B")
    await fs.mkdir("/src/empty")

    orig_info = await fs.info("/src/a.txt")
    await fs.move("/src", "/dst")

    assert not await fs.exists("/src")
    assert await fs.read_bytes("/dst/a.txt") == b"A"
    assert await fs.read_bytes("/dst/sub/b.txt") == b"B"
    assert await fs.is_dir("/dst/empty")
    assert (await fs.info("/dst/a.txt")).ctime == orig_info.ctime


async def test_memory_closed_raises_on_all_operations() -> None:
    fs = MemoryFS()
    await fs.write_bytes("/a.txt", b"x")
    await fs.aclose()
    assert fs.closed
    await fs.aclose()  # idempotent

    with pytest.raises(FilesystemClosedError):
        await fs.info("/a.txt")
    with pytest.raises(FilesystemClosedError):
        await fs.ls("/")
    with pytest.raises(FilesystemClosedError):
        await fs.read_bytes("/a.txt")
    with pytest.raises(FilesystemClosedError):
        await fs.write_bytes("/b.txt", b"y")
    with pytest.raises(FilesystemClosedError):
        await fs.mkdir("/d")
    with pytest.raises(FilesystemClosedError):
        await fs.rmdir("/d")
    with pytest.raises(FilesystemClosedError):
        await fs.remove_file("/a.txt")
    with pytest.raises(FilesystemClosedError):
        await fs.move("/a.txt", "/b.txt")
    with pytest.raises(FilesystemClosedError):
        fs.open("/a.txt", "rb")


async def test_memory_capabilities_and_sync_note() -> None:
    fs = MemoryFS()
    assert not fs.readonly
    assert {"write_bytes", "mkdir", "rmdir", "remove_file", "move"}.issubset(fs.capabilities)
    assert memory_mod.__fsspec_sync__.version == FSSPEC_SYNC_VERSION


def test_memory_eight_thread_stress() -> None:
    """Eight threads each running asyncio.run() against ONE shared MemoryFS."""
    fs = MemoryFS()
    asyncio.run(fs.mkdir("/shared"))
    num_threads = 8
    iterations = 150
    slot_count = 6

    def worker(tid: int) -> None:
        async def run() -> None:
            for i in range(iterations):
                shared_path = f"/shared/slot_{(tid + i) % slot_count}.bin"
                own_path = f"/shared/thread_{tid}.bin"
                payload = f"t{tid}-i{i}".encode()

                await fs.write_bytes(shared_path, payload)
                await fs.write_bytes(own_path, payload)

                try:
                    data = await fs.read_bytes(shared_path)
                    assert data.startswith(b"t")
                except FileNotFoundError:
                    pass

                entries = await fs.ls("/shared")
                assert all(e.path.startswith("/shared/") for e in entries)

                if i % 3 == 0:
                    with contextlib.suppress(FileNotFoundError):
                        await fs.remove_file(shared_path)

            # Leave each thread's own file in a known final state.
            await fs.write_bytes(f"/shared/final_{tid}.bin", f"done-{tid}".encode())

        asyncio.run(run())

    with ThreadPoolExecutor(max_workers=num_threads) as pool:
        futures = [pool.submit(worker, tid) for tid in range(num_threads)]
        for fut in futures:
            fut.result()

    async def verify() -> None:
        for tid in range(num_threads):
            assert await fs.read_bytes(f"/shared/final_{tid}.bin") == f"done-{tid}".encode()
        entries = await fs.ls("/shared")
        paths = [e.path for e in entries]
        assert paths == sorted(paths)
        for entry in entries:
            data = await fs.read_bytes(entry.path)
            assert entry.size == len(data)

    asyncio.run(verify())
