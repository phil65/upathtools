from concurrent.futures import ThreadPoolExecutor
from dataclasses import dataclass
import random

import pytest

from upathtools.fs import FSSPEC_SYNC_VERSION, DirCache, FileInfo, cache as cache_mod


class FakeClock:
    def __init__(self, start: float = 100.0) -> None:
        self.now = start

    def __call__(self) -> float:
        return self.now

    def advance(self, seconds: float) -> None:
        self.now += seconds


@dataclass(frozen=True, slots=True, kw_only=True)
class CustomInfo(FileInfo):
    owner: str = "root"


def _make_entries(prefix: str, count: int = 2) -> list[FileInfo]:
    return [FileInfo(path=f"{prefix}/file_{i}.txt", type="file", size=i) for i in range(count)]


def test_dircache_basic_operations_and_snapshot_immutability() -> None:
    expected_count = 2
    non_str_key: object = 123
    cache: DirCache[FileInfo] = DirCache()
    entries = _make_entries("/a", expected_count)
    cache.set("/a/", entries)

    # Mutating caller's list does not affect the cached immutable tuple snapshot.
    entries.append(FileInfo(path="/a/injected.txt", type="file"))

    assert "/a" in cache
    assert "/a/" in cache
    assert "/missing" not in cache
    assert non_str_key not in cache
    assert len(cache) == 1

    got = cache.get("/a")
    assert isinstance(got, tuple)
    assert len(got) == expected_count
    assert got[0].path == "/a/file_0.txt"
    assert cache.get("/missing") is None


def test_dircache_preserves_custom_fileinfo_subtype() -> None:
    cache: DirCache[CustomInfo] = DirCache()
    item = CustomInfo(path="/x/f.txt", type="file", owner="alice")
    cache.set("/x", [item])
    result = cache.get("/x")
    assert result == (item,)
    assert result is not None
    assert result[0].owner == "alice"


def test_dircache_ttl_expiry_with_injected_clock() -> None:
    clock = FakeClock(start=1000.0)
    cache: DirCache[FileInfo] = DirCache(expiry_time=10.0, clock=clock)

    cache.set("/a", _make_entries("/a"))
    clock.advance(5.0)
    cache.set("/b", _make_entries("/b"))

    expected_both = 2
    assert "/a" in cache
    assert "/b" in cache
    assert len(cache) == expected_both

    # Advance to t=1010.0: /a has aged 10.0s (expired), /b has aged 5.0s (fresh).
    clock.advance(5.0)
    assert "/a" not in cache
    assert cache.get("/a") is None
    assert "/b" in cache
    assert cache.get("/b") is not None
    assert len(cache) == 1

    # Re-setting /b refreshes its expiry timestamp.
    clock.advance(4.0)  # t=1014.0
    cache.set("/b", _make_entries("/b", 3))
    clock.advance(9.0)  # t=1023.0 -> 9s since refresh (< 10s)
    assert "/b" in cache
    assert len(cache) == 1

    clock.advance(1.0)  # t=1024.0 -> 10s since refresh (expired)
    assert len(cache) == 0
    assert cache.get("/b") is None


def test_dircache_zero_ttl_expires_immediately() -> None:
    clock = FakeClock()
    cache: DirCache[FileInfo] = DirCache(expiry_time=0.0, clock=clock)
    cache.set("/a", _make_entries("/a"))
    assert cache.get("/a") is None
    assert "/a" not in cache
    assert len(cache) == 0


def test_dircache_lru_eviction_and_recency_refresh() -> None:
    cap = 3
    cache: DirCache[FileInfo] = DirCache(max_paths=cap)
    cache.set("/a", _make_entries("/a"))
    cache.set("/b", _make_entries("/b"))
    cache.set("/c", _make_entries("/c"))
    assert len(cache) == cap

    # Touch /a via get() so /b becomes the least-recently-used entry.
    assert cache.get("/a") is not None

    # Inserting /d should evict /b, keeping /c, /a, /d.
    cache.set("/d", _make_entries("/d"))
    assert len(cache) == cap
    assert "/b" not in cache
    assert "/a" in cache
    assert "/c" in cache
    assert "/d" in cache

    # Re-setting /c refreshes its recency, so /a becomes LRU and is evicted next.
    cache.set("/c", _make_entries("/c", 1))
    cache.set("/e", _make_entries("/e"))
    assert "/a" not in cache
    assert "/c" in cache
    assert "/d" in cache
    assert "/e" in cache


def test_dircache_zero_max_paths_and_validation() -> None:
    cache: DirCache[FileInfo] = DirCache(max_paths=0)
    cache.set("/a", _make_entries("/a"))
    assert len(cache) == 0
    assert cache.get("/a") is None

    with pytest.raises(ValueError, match="expiry_time"):
        DirCache(expiry_time=-1.0)
    with pytest.raises(ValueError, match="max_paths"):
        DirCache(max_paths=-1)


def test_dircache_prefix_invalidation() -> None:
    cache: DirCache[FileInfo] = DirCache()
    for p in ("/", "/a", "/a/b", "/a/b/c", "/ab", "/ab/c", "/other"):
        cache.set(p, _make_entries(p))

    cache.invalidate("/a")
    assert "/a" not in cache
    assert "/a/b" not in cache
    assert "/a/b/c" not in cache
    # The parent "/" lists "/a", so it goes too; the sibling prefix "/ab" stays.
    assert "/" not in cache
    assert "/ab" in cache
    assert "/ab/c" in cache
    assert "/other" in cache

    # Trailing slash on invalidate path normalizes cleanly.
    cache.set("/", _make_entries("/"))
    cache.invalidate("/ab/c/")
    assert "/ab/c" not in cache
    assert "/ab" not in cache  # parent of /ab/c
    assert "/" in cache
    assert "/other" in cache

    # Invalidating "/" drops all absolute paths.
    cache.invalidate("/")
    assert len(cache) == 0


def test_dircache_invalidate_relative_and_all() -> None:
    cache: DirCache[FileInfo] = DirCache()
    for p in ("", "a", "a/b", "ab", "/abs"):
        cache.set(p, _make_entries(p))

    cache.invalidate("a")
    assert "a" not in cache
    assert "a/b" not in cache
    assert "ab" in cache
    assert "" not in cache  # parent of "a"
    assert "/abs" in cache
    cache.set("", _make_entries(""))

    cache.invalidate("")
    assert "" not in cache
    assert "ab" not in cache
    assert "/abs" in cache

    cache.invalidate(None)
    assert len(cache) == 0


def test_dircache_multithreaded_stress() -> None:
    max_paths = 32
    cache: DirCache[FileInfo] = DirCache(expiry_time=5.0, max_paths=max_paths)
    path_pool = [f"/dir_{i // 4}/sub_{i % 4}" for i in range(80)]
    ops_per_thread = 300

    set_threshold = 4
    get_threshold = 8
    clear_all_prob = 0.05

    def worker(seed: int) -> None:
        rng = random.Random(seed)
        for step in range(ops_per_thread):
            path = rng.choice(path_pool)
            op = rng.randrange(10)
            if op < set_threshold:
                cache.set(path, (FileInfo(path=f"{path}/f_{step}", type="file", size=step),))
            elif op < get_threshold:
                res = cache.get(path)
                if res is not None:
                    assert isinstance(res, tuple)
                    assert len(res) == 1
            elif op == get_threshold:
                _ = path in cache
                _ = len(cache)
            else:
                target = None if rng.random() < clear_all_prob else path.rsplit("/", 1)[0]
                cache.invalidate(target)

    with ThreadPoolExecutor(max_workers=8) as pool:
        futures = [pool.submit(worker, tid) for tid in range(8)]
        for fut in futures:
            fut.result()

    assert len(cache) <= max_paths


def test_cache_sync_note() -> None:
    assert cache_mod.__fsspec_sync__.version == FSSPEC_SYNC_VERSION
    assert "fsspec/dircache.py:DirCache" in cache_mod.__fsspec_sync__.upstream
