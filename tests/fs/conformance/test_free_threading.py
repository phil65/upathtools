"""Multi-threaded free-threading (PEP 703) stress suite for ``upathtools.fs``.

Hammers every stateful component in ``upathtools.fs`` concurrently across 8 OS threads
(each running its own ``asyncio.run()`` event loop where async code is involved):

  - Shared ``MemoryFS`` (overlapping writes, reads, ls, move, remove)
  - Shared ``LocalFS`` in ``tmp_path`` (atomic overwrite, ``O_EXCL`` create, reads, ls, move)
  - Shared ``DirCache`` (set, get, contains, len, prefix invalidation, clear)
  - Shared ``ProtocolRegistry`` (register, unregister, clobber, filesystem, protocols)
  - Shared ``FilesystemPool`` (concurrent ``get`` and ``aclose_loop`` across many loops)
  - Shared ``LoopRunner`` (concurrent ``run`` from all threads)
  - Shared ``SyncFS`` over ``MemoryFS`` (concurrent sync writes, reads, ls, move, remove, open)

Verifies:
  - No unexpected exceptions (only documented concurrent race errors are permitted and counted)
  - Consistent, non-torn final states (every existing file holds a complete payload)
  - No leaked threads left running after each stress test
"""

import asyncio
from concurrent.futures import ThreadPoolExecutor
import logging
import re
import sys
import threading
import time
from typing import TYPE_CHECKING, Any

import pytest

from upathtools.fs import (
    DirCache,
    FileInfo,
    FilesystemPool,
    LocalFS,
    LoopRunner,
    MemoryFS,
    ProtocolRegistry,
    SyncFS,
)


if TYPE_CHECKING:
    from pathlib import Path


pytestmark = pytest.mark.free_threading

logger = logging.getLogger(__name__)

_WORKERS = 8
_PAYLOAD_RE = re.compile(rb"^W(\d{2})-I(\d{4})-([A-Z]+)-END-(\d{2})-(\d{4})$")


def _make_payload(tid: int, step: int, body_len: int) -> bytes:
    """Create a self-validating payload whose prefix, body, and suffix must all match."""
    fill = bytes([ord("A") + ((tid + step) % 26)]) * body_len
    return f"W{tid:02d}-I{step:04d}-".encode() + fill + f"-END-{tid:02d}-{step:04d}".encode()


def _is_valid_payload(data: bytes) -> bool:
    """Return True iff ``data`` is a complete, untorn payload from :func:`_make_payload`."""
    match = _PAYLOAD_RE.match(data)
    if match is None:
        return False
    tid_start, step_start, body, tid_end, step_end = match.groups()
    if tid_start != tid_end or step_start != step_end:
        return False
    expected_byte = ord("A") + ((int(tid_start) + int(step_start)) % 26)
    return body == bytes([expected_byte]) * len(body)


def _snapshot_threads() -> set[int]:
    """Capture ident set of all currently alive threads."""
    return {t.ident for t in threading.enumerate() if t.is_alive() and t.ident is not None}


def _assert_no_leaked_threads(before_idents: set[int], *, timeout: float = 1.0) -> None:
    """Assert no threads created after ``before_idents`` remain alive."""
    deadline = time.monotonic() + timeout
    while True:
        leaked = [
            t
            for t in threading.enumerate()
            if t.is_alive() and t.ident is not None and t.ident not in before_idents
        ]
        if not leaked or time.monotonic() >= deadline:
            assert leaked == [], f"Leaked threads after stress test: {[t.name for t in leaked]}"
            return
        time.sleep(0.01)


def test_report_gil_status(record_property: pytest.RecordProperty) -> None:
    """Report ``sys._is_gil_enabled()`` so standard vs. 3.14t free-threaded runs are visible."""
    gil_fn = getattr(sys, "_is_gil_enabled", None)
    gil_enabled = True if gil_fn is None else bool(gil_fn())
    record_property("gil_enabled", gil_enabled)
    logger.info(
        "free-threading runtime: python=%s gil_enabled=%s",
        sys.version.split()[0],
        gil_enabled,
    )
    assert isinstance(gil_enabled, bool)


def test_memory_fs_concurrent_stress() -> None:
    """Hammer one shared ``MemoryFS`` from 8 threads each running ``asyncio.run()``."""
    before_threads = _snapshot_threads()
    fs = MemoryFS()
    asyncio.run(fs.mkdir("/work"))
    iterations = 100
    slots = 5
    race_counts = {"not_found": 0, "exists": 0}
    count_lock = threading.Lock()

    def worker(tid: int) -> None:
        local_not_found = 0
        local_exists = 0

        async def run() -> None:
            nonlocal local_not_found, local_exists
            for step in range(iterations):
                slot_a = f"/work/slot_{(tid + step) % slots}.bin"
                slot_b = f"/work/slot_{(tid + step + 1) % slots}.bin"
                create_slot = f"/work/create_{step % slots}.bin"
                own = f"/work/own_{tid}.bin"
                payload = _make_payload(tid, step, body_len=256)

                await fs.write_bytes(slot_a, payload, mode="overwrite")
                await fs.write_bytes(own, payload, mode="overwrite")

                try:
                    await fs.write_bytes(create_slot, payload, mode="create")
                except FileExistsError:
                    local_exists += 1

                try:
                    data = await fs.read_bytes(slot_a)
                except FileNotFoundError:
                    local_not_found += 1
                else:
                    assert _is_valid_payload(data), f"Torn read on {slot_a}: len={len(data)}"

                entries = await fs.ls("/work")
                paths = [e.path for e in entries]
                assert paths == sorted(paths)
                assert all(p.startswith("/work/") for p in paths)

                try:
                    await fs.move(slot_a, slot_b, overwrite=(step % 2 == 0))
                except FileNotFoundError:
                    local_not_found += 1
                except FileExistsError:
                    local_exists += 1

                if step % 3 == 0:
                    try:
                        await fs.remove(create_slot)
                    except FileNotFoundError:
                        local_not_found += 1

            await fs.write_bytes(
                f"/work/final_{tid}.bin",
                _make_payload(tid, iterations, body_len=256),
            )

        asyncio.run(run())
        with count_lock:
            race_counts["not_found"] += local_not_found
            race_counts["exists"] += local_exists

    with ThreadPoolExecutor(max_workers=_WORKERS) as pool:
        futures = [pool.submit(worker, tid) for tid in range(_WORKERS)]
        for fut in futures:
            fut.result()

    async def verify() -> None:
        for tid in range(_WORKERS):
            expected = _make_payload(tid, iterations, body_len=256)
            assert await fs.read_bytes(f"/work/final_{tid}.bin") == expected
        entries = await fs.ls("/work")
        assert [e.path for e in entries] == sorted(e.path for e in entries)
        for entry in entries:
            data = await fs.read_bytes(entry.path)
            assert entry.size == len(data)
            assert _is_valid_payload(data), f"Corrupt final file {entry.path}"
        await fs.aclose()

    asyncio.run(verify())
    assert race_counts["not_found"] >= 0
    assert race_counts["exists"] >= 0
    _assert_no_leaked_threads(before_threads)


def test_local_fs_concurrent_stress(tmp_path: Path) -> None:
    """Hammer one shared ``LocalFS`` in ``tmp_path`` from 8 threads running ``asyncio.run()``."""
    before_threads = _snapshot_threads()
    work_dir = tmp_path / "local_stress"
    work_dir.mkdir()
    work_posix = work_dir.as_posix()
    fs = LocalFS(auto_mkdir=True)
    iterations = 35
    slots = 4
    body_len = 8192
    race_counts = {"not_found": 0, "exists": 0}
    count_lock = threading.Lock()

    def worker(tid: int) -> None:
        local_not_found = 0
        local_exists = 0

        async def run() -> None:
            nonlocal local_not_found, local_exists
            for step in range(iterations):
                slot_a = f"{work_posix}/slot_{(tid + step) % slots}.bin"
                slot_b = f"{work_posix}/slot_{(tid + step + 1) % slots}.bin"
                create_slot = f"{work_posix}/create_{step % slots}.bin"
                own = f"{work_posix}/own_{tid}.bin"
                payload = _make_payload(tid, step, body_len=body_len)

                await fs.write_bytes(slot_a, payload, mode="overwrite")
                await fs.write_bytes(own, payload, mode="overwrite")

                try:
                    await fs.write_bytes(create_slot, payload, mode="create")
                except FileExistsError:
                    local_exists += 1

                try:
                    data = await fs.read_bytes(slot_a)
                except FileNotFoundError:
                    local_not_found += 1
                else:
                    assert _is_valid_payload(data), (
                        f"Torn LocalFS read on {slot_a}: len={len(data)}"
                    )

                entries = await fs.ls(work_posix)
                paths = [e.path for e in entries]
                assert paths == sorted(paths)

                try:
                    await fs.move(slot_a, slot_b, overwrite=(step % 2 == 0))
                except FileNotFoundError:
                    local_not_found += 1
                except FileExistsError:
                    local_exists += 1

                if step % 3 == 0:
                    try:
                        await fs.remove_file(create_slot)
                    except FileNotFoundError:
                        local_not_found += 1

            await fs.write_bytes(
                f"{work_posix}/final_{tid}.bin",
                _make_payload(tid, iterations, body_len=body_len),
            )

        asyncio.run(run())
        with count_lock:
            race_counts["not_found"] += local_not_found
            race_counts["exists"] += local_exists

    with ThreadPoolExecutor(max_workers=_WORKERS) as pool:
        futures = [pool.submit(worker, tid) for tid in range(_WORKERS)]
        for fut in futures:
            fut.result()

    async def verify() -> None:
        leftovers = [p.name for p in work_dir.iterdir() if p.name.endswith(".upathtools-tmp")]
        assert leftovers == [], f"Leftover atomic temp files: {leftovers}"
        for tid in range(_WORKERS):
            expected = _make_payload(tid, iterations, body_len=body_len)
            assert await fs.read_bytes(f"{work_posix}/final_{tid}.bin") == expected
        entries = await fs.ls(work_posix)
        for entry in entries:
            data = await fs.read_bytes(entry.path)
            assert entry.size == len(data)
            assert _is_valid_payload(data), f"Corrupt LocalFS file {entry.path}"
        await fs.aclose()

    asyncio.run(verify())
    assert race_counts["not_found"] >= 0
    assert race_counts["exists"] >= 0
    _assert_no_leaked_threads(before_threads)


def test_dircache_registry_pool_runner_and_syncfs_simultaneous_stress(tmp_path: Path) -> None:
    """Simultaneously hammer all 7 core components across 8 OS threads.

    Exercises ``MemoryFS``, ``LocalFS``, ``DirCache``, ``ProtocolRegistry``,
    ``FilesystemPool``, ``LoopRunner``, and ``SyncFS`` concurrently in one
    ``ThreadPoolExecutor(max_workers=8)`` run and asserts untorn final states
    and zero leaked threads.
    """
    before_threads = _snapshot_threads()

    mem_fs = MemoryFS()
    asyncio.run(mem_fs.mkdir("/combo"))

    local_dir = tmp_path / "combo_local"
    local_dir.mkdir()
    local_posix = local_dir.as_posix()
    local_fs = LocalFS(auto_mkdir=True)

    max_cached = 24
    cache: DirCache[FileInfo] = DirCache(expiry_time=30.0, max_paths=max_cached)

    registry = ProtocolRegistry(entry_point_group=None, fsspec_fallback=False)
    registry.register("stable", MemoryFS)

    fs_pool = FilesystemPool(registry=registry)

    sync_backing = MemoryFS()
    asyncio.run(sync_backing.mkdir("/sync"))

    iterations = 40
    slots = 4
    race_counts = {
        "file_not_found": 0,
        "file_exists": 0,
        "registry_race": 0,
    }
    count_lock = threading.Lock()
    pool_instances: list[list[Any]] = [[] for _ in range(_WORKERS)]

    with LoopRunner(name="ft-stress-runner") as runner:
        sync_fs = SyncFS(sync_backing, runner=runner)

        def worker(tid: int) -> None:
            local_fnf = 0
            local_fe = 0
            local_reg = 0

            async def async_pass(step: int) -> None:
                nonlocal local_fnf, local_fe
                payload = _make_payload(tid, step, body_len=512)
                slot = f"s_{(tid + step) % slots}.bin"
                next_slot = f"s_{(tid + step + 1) % slots}.bin"

                # 1. Shared MemoryFS
                m_src = f"/combo/{slot}"
                m_dst = f"/combo/{next_slot}"
                await mem_fs.write_bytes(m_src, payload)
                try:
                    m_data = await mem_fs.read_bytes(m_src)
                except FileNotFoundError:
                    local_fnf += 1
                else:
                    assert _is_valid_payload(m_data)
                try:
                    await mem_fs.move(m_src, m_dst, overwrite=(step % 2 == 0))
                except FileNotFoundError:
                    local_fnf += 1
                except FileExistsError:
                    local_fe += 1

                # 2. Shared LocalFS
                l_src = f"{local_posix}/{slot}"
                l_own = f"{local_posix}/own_{tid}.bin"
                await local_fs.write_bytes(l_src, payload)
                await local_fs.write_bytes(l_own, payload)
                try:
                    l_data = await local_fs.read_bytes(l_src)
                except FileNotFoundError:
                    local_fnf += 1
                else:
                    assert _is_valid_payload(l_data)

                # 3. Shared FilesystemPool (fresh loop per asyncio.run batch)
                pooled_a = fs_pool.get("stable")
                pooled_b = fs_pool.get("stable")
                assert pooled_a is pooled_b
                if step == 0:
                    pool_instances[tid].append(pooled_a)
                await pooled_a.write_bytes(f"/p_{tid}_{step}.bin", payload)
                assert await pooled_a.read_bytes(f"/p_{tid}_{step}.bin") == payload

            # Run a batch of async steps inside two sequential event loops per thread.
            half = iterations // 2
            for loop_idx, step_range in enumerate((range(half), range(half, iterations))):

                async def run_loop(steps: range = step_range, idx: int = loop_idx) -> None:
                    for step in steps:
                        await async_pass(step)
                    if idx == 0:
                        await fs_pool.aclose_loop()

                asyncio.run(run_loop())

            # Synchronous concurrent hammering of DirCache, ProtocolRegistry, LoopRunner, SyncFS.
            for step in range(iterations):
                payload = _make_payload(tid, step, body_len=256)

                # 4. Shared DirCache
                dir_key = f"/tree/d_{(tid + step) % 16}"
                entry = FileInfo(path=f"{dir_key}/f_{step}.bin", type="file", size=len(payload))
                cache.set(dir_key, [entry])
                got = cache.get(dir_key)
                if got is not None:
                    assert isinstance(got, tuple)
                    assert all(item.path.startswith(f"{dir_key}/") for item in got)
                assert len(cache) <= max_cached
                if step % 5 == 0:
                    cache.invalidate(f"/tree/d_{step % 16}")

                # 5. Shared ProtocolRegistry
                proto = f"dyn_{(tid + step) % 6}"
                try:
                    registry.register(proto, MemoryFS, clobber=(step % 2 == 0))
                except ValueError:
                    local_reg += 1
                assert "stable" in registry.protocols()
                assert isinstance(registry.filesystem("stable"), MemoryFS)
                try:
                    assert isinstance(registry.filesystem(proto), MemoryFS)
                except ValueError:
                    local_reg += 1
                if step % 2 == 1:
                    try:
                        registry.unregister(proto)
                    except KeyError:
                        local_reg += 1

                # 6. Shared LoopRunner direct coroutine submission
                async def compute(t: int = tid, s: int = step) -> tuple[int, int]:
                    if s % 10 == 0:
                        await asyncio.sleep(0)
                    return (t, s)

                assert runner.run(compute()) == (tid, step)

                # 7. Shared SyncFS over MemoryFS
                s_src = f"/sync/slot_{(tid + step) % slots}.bin"
                s_dst = f"/sync/slot_{(tid + step + 1) % slots}.bin"
                s_own = f"/sync/own_{tid}.bin"
                sync_fs.write_bytes(s_src, payload)
                sync_fs.write_bytes(s_own, payload)
                try:
                    s_data = sync_fs.read_bytes(s_src)
                except FileNotFoundError:
                    local_fnf += 1
                else:
                    assert _is_valid_payload(s_data)
                entries = sync_fs.ls("/sync")
                assert [e.path for e in entries] == sorted(e.path for e in entries)
                try:
                    sync_fs.move(s_src, s_dst, overwrite=(step % 2 == 0))
                except FileNotFoundError:
                    local_fnf += 1
                except FileExistsError:
                    local_fe += 1
                if step % 4 == 0:
                    try:
                        sync_fs.remove(s_dst)
                    except FileNotFoundError:
                        local_fnf += 1

            # Deterministic final markers per worker.
            final_payload = _make_payload(tid, iterations, body_len=256)
            sync_fs.write_bytes(f"/sync/final_{tid}.bin", final_payload)
            registry.register(f"worker_{tid}", MemoryFS, clobber=True)

            with count_lock:
                race_counts["file_not_found"] += local_fnf
                race_counts["file_exists"] += local_fe
                race_counts["registry_race"] += local_reg

        with ThreadPoolExecutor(max_workers=_WORKERS) as pool:
            futures = [pool.submit(worker, tid) for tid in range(_WORKERS)]
            for fut in futures:
                fut.result()

        # Verify SyncFS final state before closing the runner.
        for tid in range(_WORKERS):
            expected = _make_payload(tid, iterations, body_len=256)
            assert sync_fs.read_bytes(f"/sync/final_{tid}.bin") == expected
        for entry in sync_fs.ls("/sync"):
            data = sync_fs.read_bytes(entry.path)
            assert entry.size == len(data)
            assert _is_valid_payload(data)
        sync_fs.close()

    # All 8 threads received distinct FilesystemPool instances; closed loops are pruned.
    first_loop_instances = [items[0] for items in pool_instances]
    assert len({id(inst) for inst in first_loop_instances}) == _WORKERS
    assert len(fs_pool) == 0

    # Registry retains stable + every worker's final registration.
    assert "stable" in registry.protocols()
    for tid in range(_WORKERS):
        assert f"worker_{tid}" in registry.protocols()
        assert isinstance(registry.filesystem(f"worker_{tid}"), MemoryFS)

    # DirCache respects its LRU bound.
    assert len(cache) <= max_cached

    # MemoryFS and LocalFS final states are completely consistent and untorn.
    async def verify_async_stores() -> None:
        for entry in await mem_fs.ls("/combo"):
            data = await mem_fs.read_bytes(entry.path)
            assert entry.size == len(data)
            assert _is_valid_payload(data)
        await mem_fs.aclose()

        leftovers = [p.name for p in local_dir.iterdir() if p.name.endswith(".upathtools-tmp")]
        assert leftovers == []
        for entry in await local_fs.ls(local_posix):
            data = await local_fs.read_bytes(entry.path)
            assert entry.size == len(data)
            assert _is_valid_payload(data)
        await local_fs.aclose()

    asyncio.run(verify_async_stores())
    assert all(count >= 0 for count in race_counts.values())
    _assert_no_leaked_threads(before_threads)
