"""Unit tests for :class:`upathtools.fs.SyncFS` and :class:`SyncFileBridge`."""

import annotationlib
import asyncio
from collections.abc import Iterator
import gc
import inspect
import io
from typing import TYPE_CHECKING
import warnings

import pytest

from upathtools.fs import (
    AsyncFS,
    FileInfo,
    FilesystemClosedError,
    FSTimeoutError,
    LoopRunner,
    MemoryFS,
    SyncFileBridge,
    SyncFS,
    get_default_runner,
)


if TYPE_CHECKING:
    from pathlib import Path


@pytest.fixture
def runner() -> Iterator[LoopRunner]:
    with LoopRunner(name="test-sync-fs") as r:
        yield r


def test_syncfs_properties_and_default_runner() -> None:
    fs = MemoryFS()
    expected_timeout = 2.5
    with LoopRunner(name="explicit-sync") as custom_runner:
        sync = SyncFS(fs, runner=custom_runner, timeout=expected_timeout)
        assert sync.fs is fs
        assert sync.runner is custom_runner
        assert sync.timeout == expected_timeout

    default_sync = SyncFS(fs)
    try:
        assert default_sync.runner is get_default_runner()
        default_sync.write_bytes("/hello.txt", b"hi")
        assert default_sync.read_bytes("/hello.txt") == b"hi"
    finally:
        default_sync.runner.close()


def test_syncfs_read_write_metadata_and_read_many(runner: LoopRunner) -> None:
    fs = MemoryFS()
    s = SyncFS(fs, runner=runner)

    s.write_bytes("/data.bin", b"0123456789")
    assert s.read_bytes("/data.bin") == b"0123456789"
    assert s.read_bytes("/data.bin", start=2, end=6) == b"2345"

    greeting = "hello world\n"
    s.write_text("/greeting.txt", greeting, encoding="utf-8")
    assert s.read_text("/greeting.txt") == greeting

    info = s.info("/greeting.txt")
    assert info.path == "/greeting.txt"
    assert info.is_file
    assert info.size == len(greeting)
    assert s.size("/greeting.txt") == len(greeting)
    assert s.exists("/greeting.txt")
    assert s.is_file("/greeting.txt")
    assert not s.is_dir("/greeting.txt")
    assert not s.exists("/missing.txt")

    # touch: create, preserve when truncate=False, truncate when truncate=True
    s.touch("/empty.txt")
    assert s.read_bytes("/empty.txt") == b""
    s.write_bytes("/empty.txt", b"keep")
    s.touch("/empty.txt", truncate=False)
    assert s.read_bytes("/empty.txt") == b"keep"
    s.touch("/empty.txt", truncate=True)
    assert s.read_bytes("/empty.txt") == b""

    # read_many in all on_error modes
    assert s.read_many(["/data.bin", "/greeting.txt"]) == {
        "/data.bin": b"0123456789",
        "/greeting.txt": b"hello world\n",
    }
    assert s.read_many(["/data.bin", "/nope"], on_error="omit") == {
        "/data.bin": b"0123456789",
    }
    mixed = s.read_many(["/data.bin", "/nope"], on_error="return")
    assert mixed["/data.bin"] == b"0123456789"
    assert isinstance(mixed["/nope"], FileNotFoundError)


def test_syncfs_directories_removal_and_invalidate_cache(runner: LoopRunner) -> None:
    fs = MemoryFS(use_listings_cache=True)
    s = SyncFS(fs, runner=runner)

    s.mkdir("/a/b", parents=True)
    assert s.is_dir("/a")
    assert s.is_dir("/a/b")
    with pytest.raises(FileExistsError):
        s.mkdir("/a/b", exist_ok=False)
    s.mkdir("/a/b", exist_ok=True)

    s.write_bytes("/a/b/one.txt", b"1")
    s.write_bytes("/a/b/two.txt", b"2")

    entries = s.ls("/a/b")
    assert [e.path for e in entries] == ["/a/b/one.txt", "/a/b/two.txt"]
    assert s.ls_names("/a/b") == ["/a/b/one.txt", "/a/b/two.txt"]

    # invalidate_cache runs synchronously without touching the runner.
    assert fs.dircache is not None
    fs.dircache.set("/a/b", entries)
    assert "/a/b" in fs.dircache
    s.invalidate_cache("/a/b")
    assert "/a/b" not in fs.dircache

    s.remove_file("/a/b/one.txt")
    assert not s.exists("/a/b/one.txt")

    s.remove("/a/b/two.txt")
    s.rmdir("/a/b")
    assert not s.exists("/a/b")

    s.write_bytes("/a/sub/nested.txt", b"x")
    s.remove("/a", recursive=True)
    assert not s.exists("/a")


def test_syncfs_copy_and_move(runner: LoopRunner) -> None:
    s = SyncFS(MemoryFS(), runner=runner)

    s.write_bytes("/src.txt", b"original")
    s.copy_file("/src.txt", "/dst.txt")
    assert s.read_bytes("/dst.txt") == b"original"
    with pytest.raises(FileExistsError):
        s.copy_file("/src.txt", "/dst.txt", overwrite=False)

    s.write_bytes("/tree/a.txt", b"A")
    s.write_bytes("/tree/sub/b.txt", b"B")
    s.copy("/tree", "/tree_copy")
    assert s.read_bytes("/tree_copy/a.txt") == b"A"
    assert s.read_bytes("/tree_copy/sub/b.txt") == b"B"

    s.move("/tree_copy", "/tree_moved")
    assert not s.exists("/tree_copy")
    assert s.read_bytes("/tree_moved/a.txt") == b"A"
    assert s.read_bytes("/tree_moved/sub/b.txt") == b"B"


def test_syncfs_traversal_search_and_diff(runner: LoopRunner) -> None:
    s = SyncFS(MemoryFS(), runner=runner)

    s.write_text("/root/a.txt", "alpha\nbeta\n")
    s.write_text("/root/b.txt", "alpha\ngamma\n")
    s.write_text("/root/sub/c.md", "alpha beta\n")

    walked_iter = s.walk("/root")
    assert isinstance(walked_iter, Iterator)
    walked = list(walked_iter)
    assert [dp for dp, _, _ in walked] == ["/root", "/root/sub"]

    walked_bottom = list(s.walk("/root", topdown=False))
    assert [dp for dp, _, _ in walked_bottom] == ["/root/sub", "/root"]

    assert [e.path for e in s.find("/root")] == [
        "/root/a.txt",
        "/root/b.txt",
        "/root/sub/c.md",
    ]
    assert s.find_paths("/root", maxdepth=1) == ["/root/a.txt", "/root/b.txt"]

    assert [e.path for e in s.glob("/root/**/*.txt")] == ["/root/a.txt", "/root/b.txt"]
    assert s.glob_paths("/root/**/*.md") == ["/root/sub/c.md"]

    assert s.du("/root") == {
        "/root/a.txt": 11,
        "/root/b.txt": 12,
        "/root/sub/c.md": 11,
    }

    matches = s.grep("/root", "alpha")
    assert [m.path for m in matches] == ["/root/a.txt", "/root/b.txt", "/root/sub/c.md"]

    diff_text = s.diff("/root/a.txt", "/root/b.txt")
    assert "-beta" in diff_text
    assert "+gamma" in diff_text


def test_syncfs_get_file_put_file_and_lifecycle(runner: LoopRunner, tmp_path: Path) -> None:
    fs = MemoryFS()
    src_local = tmp_path / "upload.bin"
    dst_local = tmp_path / "download.bin"
    src_local.write_bytes(b"payload-123")

    with SyncFS(fs, runner=runner) as s:
        s.put_file(src_local, "/remote.bin")
        assert s.read_bytes("/remote.bin") == b"payload-123"
        s.get_file("/remote.bin", dst_local)
        assert dst_local.read_bytes() == b"payload-123"
        assert not fs.closed

    assert fs.closed
    # close() is idempotent.
    s.close()

    with pytest.raises(FilesystemClosedError):
        s.info("/remote.bin")


def test_syncfs_open_binary_modes_and_operations(runner: LoopRunner) -> None:
    s = SyncFS(MemoryFS(), runner=runner)

    # wb
    first_chunk = b"line1\n"
    second_chunk = b"line2\nline3"
    total_len = len(first_chunk) + len(second_chunk)
    with s.open("/file.bin", "wb") as f:
        assert isinstance(f, SyncFileBridge)
        assert f.name == "/file.bin"
        assert f.writable()
        assert not f.readable()
        assert not f.seekable()
        assert f.write(first_chunk) == len(first_chunk)
        assert f.write(second_chunk) == len(second_chunk)
        f.flush()
        with pytest.raises(io.UnsupportedOperation):
            f.read()
        with pytest.raises(io.UnsupportedOperation):
            f.tell()
    assert f.closed
    f.close()  # idempotent
    with pytest.raises(ValueError, match="closed"):
        f.write(b"x")

    # rb: read, readinto, readline, seek, tell, iteration
    with s.open("/file.bin", "rb", block_size=5) as rf:
        assert isinstance(rf, SyncFileBridge)
        assert rf.readable()
        assert not rf.writable()
        assert rf.seekable()
        assert rf.tell() == 0
        assert rf.readline() == first_chunk
        assert rf.tell() == len(first_chunk)

        buf = bytearray(5)
        assert rf.readinto(buf) == len(buf)
        assert bytes(buf) == b"line2"

        assert rf.seek(0, io.SEEK_SET) == 0
        assert list(rf) == [b"line1\n", b"line2\n", b"line3"]

        assert rf.seek(-5, io.SEEK_END) == total_len - 5
        assert rf.read() == b"line3"
        assert rf.seek(-3, io.SEEK_CUR) == total_len - 3
        assert rf.read(2) == b"ne"

        with pytest.raises(io.UnsupportedOperation):
            rf.write(b"nope")
    assert rf.closed

    # xb: fails on existing, succeeds on new
    with pytest.raises(FileExistsError):
        s.open("/file.bin", "xb")
    with s.open("/exclusive.bin", "xb") as xf:
        xf.write(b"fresh")
    assert s.read_bytes("/exclusive.bin") == b"fresh"

    # ab: appends to existing content
    with s.open("/exclusive.bin", "ab") as af:
        af.write(b"-more")
    assert s.read_bytes("/exclusive.bin") == b"fresh-more"

    # Missing file and directory errors at open() time
    with pytest.raises(FileNotFoundError):
        s.open("/does-not-exist.bin", "rb")
    s.mkdir("/adir")
    with pytest.raises(IsADirectoryError):
        s.open("/adir", "rb")

    # Binary mode rejects text kwargs; invalid mode raises ValueError
    with pytest.raises(ValueError, match="encoding"):
        s.open("/file.bin", "rb", encoding="utf-8")  # type: ignore[call-overload]
    with pytest.raises(ValueError, match="Unsupported mode"):
        s.open("/file.bin", "r+")


def test_syncfs_open_text_modes(runner: LoopRunner) -> None:
    s = SyncFS(MemoryFS(), runner=runner)

    with s.open("/notes.txt", "w", encoding="utf-8") as wf:
        assert isinstance(wf, io.TextIOWrapper)
        wf.write("first line\n")
        wf.write("second line\n")

    with s.open("/notes.txt", "a", encoding="utf-8") as af:
        af.write("third line\n")

    with s.open("/notes.txt", "r", encoding="utf-8") as rf:
        assert isinstance(rf, io.TextIOWrapper)
        assert rf.readline() == "first line\n"
        assert list(rf) == ["second line\n", "third line\n"]
        rf.seek(0)
        assert rf.read() == "first line\nsecond line\nthird line\n"

    with pytest.raises(FileExistsError):
        s.open("/notes.txt", "x")
    with s.open("/brand_new.txt", "x") as xf:
        xf.write("created")
    assert s.read_text("/brand_new.txt") == "created"


def test_syncfs_writer_discards_on_exception(runner: LoopRunner) -> None:
    s = SyncFS(MemoryFS(), runner=runner)
    s.write_bytes("/existing.bin", b"untouched")
    s.write_text("/existing.txt", "untouched")

    closed_flags: list[bool] = []

    def _abort_binary(path: str, payload: bytes, msg: str) -> None:
        with s.open(path, "wb") as bf:
            bf.write(payload)
            raise RuntimeError(msg)

    def _abort_text(path: str, payload: str, msg: str) -> None:
        with s.open(path, "w") as tf:
            tf.write(payload)
            closed_flags.append(tf.closed)
            raise RuntimeError(msg)

    with pytest.raises(RuntimeError, match="abort binary"):
        _abort_binary("/existing.bin", b"corrupted", "abort binary")
    assert s.read_bytes("/existing.bin") == b"untouched"

    with pytest.raises(RuntimeError, match="abort text"):
        _abort_text("/existing.txt", "corrupted", "abort text")
    assert closed_flags == [False]
    assert s.read_text("/existing.txt") == "untouched"

    with pytest.raises(RuntimeError, match="abort new"):
        _abort_binary("/never_created.bin", b"partial", "abort new")
    assert not s.exists("/never_created.bin")


def test_syncfs_reentry_from_runner_loop_raises(runner: LoopRunner) -> None:
    s = SyncFS(MemoryFS(), runner=runner)

    async def bad_reentry() -> None:
        s.exists("/anything")

    with warnings.catch_warnings(record=True) as caught:
        warnings.simplefilter("always")
        with pytest.raises(
            RuntimeError,
            match="Cannot call the sync facade from the runner's own event loop",
        ):
            runner.run(bad_reentry())
        gc.collect()

    runtime_warnings = [w for w in caught if issubclass(w.category, RuntimeWarning)]
    assert runtime_warnings == []


def test_syncfs_timeout_raises_fs_timeout_error(runner: LoopRunner) -> None:
    class _SlowFS(AsyncFS[FileInfo]):
        async def info(self, path: str) -> FileInfo:
            await asyncio.sleep(10)
            return FileInfo(path=path, type="file", size=0)  # pragma: no cover

        async def ls(self, path: str) -> list[FileInfo]:
            await asyncio.sleep(10)
            return []  # pragma: no cover

        async def read_bytes(
            self,
            path: str,
            *,
            start: int | None = None,
            end: int | None = None,
        ) -> bytes:
            await asyncio.sleep(10)
            return b""  # pragma: no cover

    s = SyncFS(_SlowFS(), runner=runner, timeout=0.05)
    with pytest.raises(FSTimeoutError):
        s.info("/slow")


def test_syncfs_drift_guard_matches_asyncfs_methods_and_signatures() -> None:
    # Design invariant: AsyncFS must NOT expose a hidden .sync property, and SyncFS
    # must NOT use __getattr__ forwarding.
    assert not hasattr(AsyncFS, "sync")
    assert "__getattr__" not in SyncFS.__dict__

    async_coro_methods = {
        name
        for name, val in AsyncFS.__dict__.items()
        if not name.startswith("_") and inspect.iscoroutinefunction(val)
    }
    async_gen_methods = {
        name
        for name, val in AsyncFS.__dict__.items()
        if not name.startswith("_") and inspect.isasyncgenfunction(val)
    }
    sync_methods = {
        name
        for name, val in SyncFS.__dict__.items()
        if not name.startswith("_") and inspect.isfunction(val)
    }

    # Explicit, minimal allowlist for intentional async/sync name or kind differences:
    # - aclose <-> close: standard async/sync close naming convention.
    # - walk: AsyncFS.walk is an async generator function; SyncFS.walk returns an Iterator.
    # - open: AsyncFS.open is a sync factory; SyncFS.open adds text modes & kwargs.
    # - invalidate_cache: plain synchronous method on both AsyncFS and SyncFS.
    async_only_allowlist = {"aclose"}
    sync_only_allowlist = {"close", "walk", "open", "invalidate_cache"}

    assert async_gen_methods == {"walk"}
    assert async_coro_methods - async_only_allowlist == sync_methods - sync_only_allowlist

    # Every mirrored method (all coroutine methods + walk + invalidate_cache) must have
    # identical parameter names, kinds, and defaults.
    mirrored = (async_coro_methods - async_only_allowlist) | {"walk", "invalidate_cache"}
    for name in sorted(mirrored):
        async_sig = inspect.signature(
            AsyncFS.__dict__[name],
            annotation_format=annotationlib.Format.FORWARDREF,
        )
        sync_sig = inspect.signature(
            SyncFS.__dict__[name],
            annotation_format=annotationlib.Format.FORWARDREF,
        )
        async_params = [
            (p.name, p.kind, p.default) for p in async_sig.parameters.values() if p.name != "self"
        ]
        sync_params = [
            (p.name, p.kind, p.default) for p in sync_sig.parameters.values() if p.name != "self"
        ]
        assert sync_params == async_params, (
            f"Signature mismatch on SyncFS.{name}: {sync_params!r} != {async_params!r}"
        )

    # Also verify SyncFS.open's leading parameters match AsyncFS.open.
    async_open_params = [
        (p.name, p.kind, p.default)
        for p in inspect.signature(
            AsyncFS.open,
            annotation_format=annotationlib.Format.FORWARDREF,
        ).parameters.values()
        if p.name != "self"
    ]
    sync_open_params = [
        (p.name, p.kind, p.default)
        for p in inspect.signature(
            SyncFS.open,
            annotation_format=annotationlib.Format.FORWARDREF,
        ).parameters.values()
        if p.name != "self"
    ]
    assert sync_open_params[: len(async_open_params)] == async_open_params
