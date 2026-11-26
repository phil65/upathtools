"""Tests for loop-scoped, thread-safe :class:`upathtools.fs.FilesystemPool`."""

import asyncio
from concurrent.futures import ThreadPoolExecutor
import gc
import weakref

import pytest

from upathtools.fs import FilesystemPool, MemoryFS, ProtocolRegistry


def test_pool_get_requires_running_loop() -> None:
    pool = FilesystemPool()
    with pytest.raises(RuntimeError, match="no running event loop"):
        pool.get("memory")


async def test_pool_returns_same_instance_within_one_loop() -> None:
    pool = FilesystemPool()
    try:
        fs1 = pool.get("memory")
        fs2 = pool.get("memory")
        assert isinstance(fs1, MemoryFS)
        assert fs1 is fs2

        # Option order does not change tokenization
        fs_a = pool.get("memory", batch_size=4, use_listings_cache=True)
        fs_b = pool.get("memory", use_listings_cache=True, batch_size=4)
        assert fs_a is fs_b
        assert fs_a is not fs1

        # Different option values yield distinct instances
        fs_c = pool.get("memory", batch_size=8, use_listings_cache=True)
        assert fs_c is not fs_a
    finally:
        await pool.aclose_loop()


def test_pool_isolates_two_sequential_asyncio_run_calls_on_same_thread() -> None:
    pool = FilesystemPool()

    async def acquire() -> MemoryFS:
        fs = pool.get("memory")
        assert isinstance(fs, MemoryFS)
        assert fs is pool.get("memory")
        return fs

    fs1 = asyncio.run(acquire())
    fs2 = asyncio.run(acquire())
    assert fs1 is not fs2


def test_pool_isolates_across_threads_and_handles_8_thread_concurrency() -> None:
    pool = FilesystemPool()

    def worker(worker_id: int) -> tuple[MemoryFS, MemoryFS]:
        async def run() -> tuple[MemoryFS, MemoryFS]:
            first = pool.get("memory")
            assert isinstance(first, MemoryFS)
            for _ in range(50):
                assert pool.get("memory") is first
            per_worker = pool.get("memory", batch_size=worker_id + 1)
            assert isinstance(per_worker, MemoryFS)
            assert per_worker is not first
            assert pool.get("memory", batch_size=worker_id + 1) is per_worker
            await pool.aclose_loop()
            assert first.closed
            assert per_worker.closed
            return (first, per_worker)

        return asyncio.run(run())

    num_threads = 8
    with ThreadPoolExecutor(max_workers=num_threads) as ex:
        results = list(ex.map(worker, range(num_threads)))

    # Keep all 8 instances alive simultaneously so memory addresses cannot be reused.
    primary_instances = [r[0] for r in results]
    assert len({id(fs) for fs in primary_instances}) == num_threads
    assert len(pool) == 0


async def test_pool_aclose_loop_and_aclose_all() -> None:
    pool = FilesystemPool()
    fs1 = pool.get("memory")
    fs2 = pool.get("memory", batch_size=2)
    assert isinstance(fs1, MemoryFS)
    assert isinstance(fs2, MemoryFS)
    assert not fs1.closed
    assert not fs2.closed

    await pool.aclose_loop()
    assert fs1.closed
    assert fs2.closed
    assert len(pool) == 0

    # Getting again after close creates a fresh open instance
    fs3 = pool.get("memory")
    assert fs3 is not fs1
    assert not fs3.closed

    await pool.aclose_all()
    assert fs3.closed
    assert len(pool) == 0


def test_pool_prunes_closed_loops_even_while_strongly_referenced() -> None:
    pool = FilesystemPool()

    loop1 = asyncio.new_event_loop()
    try:

        async def grab() -> MemoryFS:
            fs = pool.get("memory")
            assert isinstance(fs, MemoryFS)
            return fs

        fs1 = loop1.run_until_complete(grab())
        assert not fs1.closed
    finally:
        loop1.close()

    # loop1 is still strongly referenced here, yet closed -> pruned on next access
    assert loop1.is_closed()

    async def check_pruned() -> None:
        fs2 = pool.get("memory")
        assert fs2 is not fs1
        assert len(pool) == 1
        assert loop1 not in pool._pools
        await pool.aclose_loop()

    asyncio.run(check_pruned())


def test_pool_gc_loop_drops_entries() -> None:
    pool = FilesystemPool()
    loop_ref: list[weakref.ref[asyncio.AbstractEventLoop]] = []
    fs_ref: list[weakref.ref[MemoryFS]] = []

    def run_ephemeral_loop() -> None:
        loop = asyncio.new_event_loop()
        loop_ref.append(weakref.ref(loop))
        try:

            async def grab() -> None:
                fs = pool.get("memory")
                assert isinstance(fs, MemoryFS)
                fs_ref.append(weakref.ref(fs))

            loop.run_until_complete(grab())
        finally:
            loop.close()

    run_ephemeral_loop()
    gc.collect()

    assert loop_ref[0]() is None
    assert fs_ref[0]() is None
    assert len(pool._pools) == 0


async def test_pool_injectable_registry() -> None:
    reg = ProtocolRegistry(entry_point_group=None, fsspec_fallback=False)
    reg.register("custom", MemoryFS)
    pool = FilesystemPool(registry=reg)
    assert pool.registry is reg

    try:
        fs = pool.get("custom")
        assert isinstance(fs, MemoryFS)
        assert pool.get("custom") is fs

        with pytest.raises(ValueError, match="Protocol not known"):
            pool.get("memory")
    finally:
        await pool.aclose_loop()
