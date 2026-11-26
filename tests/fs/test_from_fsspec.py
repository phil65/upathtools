"""Tests for inbound ``from_fsspec`` / ``FsspecAdapter`` and ``ensure_async_fs``."""

import asyncio
import gc
from typing import TYPE_CHECKING, Any

import fsspec
from fsspec.asyn import AsyncFileSystem
from fsspec.implementations.local import LocalFileSystem
from fsspec.spec import AbstractFileSystem
import pytest

from tests.fs.contract import AsyncFSContract
from upathtools.fs import (
    FileInfo,
    FsspecAdapter,
    MemoryFS,
    UnsupportedOperation,
    ensure_async_fs,
    from_fsspec,
    to_fsspec,
)


if TYPE_CHECKING:
    from collections.abc import AsyncIterator
    from pathlib import Path

    from upathtools.fs import AsyncFS


# --- Contract suites against real fsspec filesystems ---


class TestFsspecMemoryContract(AsyncFSContract):
    """Run :class:`AsyncFSContract` against ``from_fsspec(MemoryFileSystem)``."""

    @pytest.fixture
    async def fs(self) -> AsyncIterator[AsyncFS[Any]]:
        raw = fsspec.filesystem("memory", global_store=False, skip_instance_cache=True)
        raw.store.clear()
        raw.pseudo_dirs[:] = [""]
        adapted = from_fsspec(raw)
        yield adapted
        await adapted.aclose()

    @pytest.fixture
    async def root(self, fs: AsyncFS[Any]) -> str:
        await fs.mkdir("/work", parents=True, exist_ok=True)
        return "/work"

    @pytest.mark.xfail(
        strict=True,
        reason=(
            "fsspec MemoryFileSystem._open raises FileExistsError (not NotADirectoryError) "
            "when writing a path whose ancestor is a regular file"
        ),
    )
    async def test_error_types(self, fs: AsyncFS[Any], root: str) -> None:
        await super().test_error_types(fs, root)

    @pytest.mark.xfail(
        strict=True,
        reason=(
            "fsspec MemoryFileSystem.mkdir(create_parents=False) does not check whether "
            "the parent directory exists and silently succeeds"
        ),
    )
    async def test_mkdir_and_rmdir(self, fs: AsyncFS[Any], root: str) -> None:
        await super().test_mkdir_and_rmdir(fs, root)


class TestFsspecLocalContract(AsyncFSContract):
    """Run :class:`AsyncFSContract` against ``from_fsspec(LocalFileSystem)``."""

    @pytest.fixture
    async def fs(self) -> AsyncIterator[AsyncFS[Any]]:
        raw = LocalFileSystem(auto_mkdir=True, skip_instance_cache=True)
        adapted = from_fsspec(raw)
        yield adapted
        await adapted.aclose()

    @pytest.fixture
    async def root(self, fs: AsyncFS[Any], tmp_path: Path) -> str:
        work = tmp_path / "work"
        work.mkdir()
        return work.as_posix()

    @pytest.mark.xfail(
        strict=True,
        reason=(
            "LocalFileSystem(auto_mkdir=True)._open calls os.makedirs(parent, exist_ok=True), "
            "which raises FileExistsError instead of NotADirectoryError when an ancestor is a file"
        ),
    )
    async def test_error_types(self, fs: AsyncFS[Any], root: str) -> None:
        await super().test_error_types(fs, root)


# --- Dispatch, caching, unwrapping, capabilities, lifecycle, S3-style paths ---


class _FakeAsyncFsspecFS(AsyncFileSystem):
    """Minimal AsyncFileSystem subclass to verify direct-await vs to_thread dispatch."""

    protocol = "fakeasync"
    root_marker = "/"
    cachable = False

    def __init__(self, *, asynchronous: bool = True, **kwargs: Any) -> None:
        super().__init__(asynchronous=asynchronous, **kwargs)
        self.files: dict[str, bytes] = {"/hello.txt": b"hi"}
        self.dirs: set[str] = {"/"}
        self.closed_async = False

    async def _info(self, path: str, **kwargs: Any) -> dict[str, Any]:
        p = self._strip_protocol(path) or "/"
        if p in self.files:
            return {"name": p, "type": "file", "size": len(self.files[p])}
        if p in self.dirs:
            return {"name": p, "type": "directory", "size": 0}
        raise FileNotFoundError(p)

    async def _ls(self, path: str, detail: bool = True, **kwargs: Any) -> list[Any]:
        p = self._strip_protocol(path) or "/"
        if p in self.files:
            info = await self._info(p)
            return [info] if detail else [p]
        if p not in self.dirs:
            raise FileNotFoundError(p)
        prefix = "/" if p == "/" else f"{p}/"
        out: list[dict[str, Any]] = []
        for f, data in self.files.items():
            if f.startswith(prefix) and "/" not in f[len(prefix) :]:
                out.append({"name": f, "type": "file", "size": len(data)})
        out.extend(
            {"name": d, "type": "directory", "size": 0}
            for d in self.dirs
            if d != p and d.startswith(prefix) and "/" not in d[len(prefix) :]
        )
        return out if detail else [e["name"] for e in out]

    async def _cat_file(
        self, path: str, start: int | None = None, end: int | None = None, **kwargs: Any
    ) -> bytes:
        p = self._strip_protocol(path) or "/"
        if p not in self.files:
            raise FileNotFoundError(p)
        return self.files[p][start:end]

    async def _pipe_file(
        self, path: str, value: bytes, mode: str = "overwrite", **kwargs: Any
    ) -> None:
        p = self._strip_protocol(path) or "/"
        if mode == "create" and p in self.files:
            raise FileExistsError(p)
        self.files[p] = value

    async def _rm_file(self, path: str, **kwargs: Any) -> None:
        p = self._strip_protocol(path) or "/"
        if p not in self.files:
            raise FileNotFoundError(p)
        del self.files[p]

    async def _mkdir(self, path: str, create_parents: bool = True, **kwargs: Any) -> None:
        p = self._strip_protocol(path) or "/"
        self.dirs.add(p)

    async def _makedirs(self, path: str, exist_ok: bool = False) -> None:
        p = self._strip_protocol(path) or "/"
        if not exist_ok and p in self.dirs:
            raise FileExistsError(p)
        self.dirs.add(p)

    async def _rmdir(self, path: str) -> None:
        p = self._strip_protocol(path) or "/"
        if p not in self.dirs:
            raise FileNotFoundError(p)
        self.dirs.remove(p)

    async def close_session(self) -> None:
        self.closed_async = True


class _FakeSyncReadOnlyFS(AbstractFileSystem):
    """Minimal sync read-only AbstractFileSystem."""

    protocol = "fakesyncro"
    root_marker = "/"
    cachable = False

    def __init__(self, **kwargs: Any) -> None:
        super().__init__(**kwargs)
        self.closed_sync = False

    def info(self, path: str, **kwargs: Any) -> dict[str, Any]:
        p = self._strip_protocol(path) or "/"
        if p == "/":
            return {"name": "/", "type": "directory", "size": 0}
        if p == "/only.txt":
            return {"name": "/only.txt", "type": "file", "size": 4}
        raise FileNotFoundError(p)

    def ls(self, path: str, detail: bool = True, **kwargs: Any) -> list[Any]:
        p = self._strip_protocol(path) or "/"
        if p == "/only.txt":
            item = {"name": "/only.txt", "type": "file", "size": 4}
            return [item] if detail else ["/only.txt"]
        if p == "/":
            item = {"name": "/only.txt", "type": "file", "size": 4}
            return [item] if detail else ["/only.txt"]
        raise FileNotFoundError(p)

    def cat_file(
        self, path: str, start: int | None = None, end: int | None = None, **kwargs: Any
    ) -> bytes:
        p = self._strip_protocol(path) or "/"
        if p == "/only.txt":
            return b"read"[start:end]
        raise FileNotFoundError(p)

    def close(self) -> None:
        self.closed_sync = True


class _FakeS3StyleFS(AbstractFileSystem):
    """Small S3-style fsspec filesystem with ``root_marker = ''`` (no leading slash)."""

    protocol = ("s3", "s3a")
    root_marker = ""
    cachable = False

    def __init__(self, **kwargs: Any) -> None:
        super().__init__(**kwargs)
        self.objects: dict[str, bytes] = {
            "mybucket/dir/a.txt": b"alpha",
            "mybucket/dir/b.txt": b"beta",
            "mybucket/top.txt": b"top",
        }

    @classmethod
    def _strip_protocol(cls, path: Any) -> Any:
        if isinstance(path, list):
            return [cls._strip_protocol(p) for p in path]
        p = str(path)
        for prefix in ("s3://", "s3a://"):
            if p.startswith(prefix):
                p = p[len(prefix) :]
        return p.strip("/")

    def info(self, path: str, **kwargs: Any) -> dict[str, Any]:
        p = self._strip_protocol(path)
        if p in self.objects:
            return {"name": p, "type": "file", "size": len(self.objects[p])}
        prefix = f"{p}/" if p else ""
        if p == "" or any(k.startswith(prefix) for k in self.objects):
            return {"name": p, "type": "directory", "size": 0}
        raise FileNotFoundError(p)

    def ls(self, path: str, detail: bool = True, **kwargs: Any) -> list[Any]:
        p = self._strip_protocol(path)
        if p in self.objects:
            item = {"name": p, "type": "file", "size": len(self.objects[p])}
            return [item] if detail else [p]
        prefix = f"{p}/" if p else ""
        entries: dict[str, dict[str, Any]] = {}
        for key, data in self.objects.items():
            if not key.startswith(prefix):
                continue
            rest = key[len(prefix) :]
            if "/" in rest:
                child_dir = f"{prefix}{rest.split('/', 1)[0]}"
                entries[child_dir] = {"name": child_dir, "type": "directory", "size": 0}
            else:
                entries[key] = {"name": key, "type": "file", "size": len(data)}
        if not entries and p != "":
            raise FileNotFoundError(p)
        result = [entries[k] for k in sorted(entries)]
        return result if detail else [e["name"] for e in result]

    def cat_file(
        self, path: str, start: int | None = None, end: int | None = None, **kwargs: Any
    ) -> bytes:
        p = self._strip_protocol(path)
        if p not in self.objects:
            raise FileNotFoundError(p)
        return self.objects[p][start:end]

    def pipe_file(self, path: str, value: bytes, **kwargs: Any) -> None:
        # Intentionally does NOT declare a `mode` parameter to test fallback create check.
        p = self._strip_protocol(path)
        self.objects[p] = value


async def test_async_dispatch_awaits_directly_without_to_thread(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    raw = _FakeAsyncFsspecFS(asynchronous=True)
    fs = from_fsspec(raw)

    called_to_thread: list[Any] = []
    real_to_thread = asyncio.to_thread

    async def spy_to_thread(fn: Any, *args: Any, **kwargs: Any) -> Any:
        called_to_thread.append(fn)
        return await real_to_thread(fn, *args, **kwargs)

    monkeypatch.setattr(asyncio, "to_thread", spy_to_thread)

    assert (await fs.info("/hello.txt")).path == "/hello.txt"
    assert [e.path for e in await fs.ls("/")] == ["/hello.txt"]
    assert await fs.read_bytes("/hello.txt") == b"hi"
    await fs.write_bytes("/new.txt", b"new", mode="create")
    await fs.mkdir("/sub", parents=True)
    await fs.rmdir("/sub")
    await fs.remove_file("/new.txt")

    assert called_to_thread == []


async def test_sync_dispatch_uses_to_thread_and_respects_max_concurrency(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    raw = _FakeSyncReadOnlyFS()
    max_conc = 2
    fs = from_fsspec(raw, max_concurrency=max_conc)

    called_to_thread: list[Any] = []
    active = 0
    peak = 0
    real_to_thread = asyncio.to_thread

    async def spy_to_thread(fn: Any, *args: Any, **kwargs: Any) -> Any:
        nonlocal active, peak
        called_to_thread.append(fn)
        active += 1
        peak = max(peak, active)
        try:
            await asyncio.sleep(0.01)
            return await real_to_thread(fn, *args, **kwargs)
        finally:
            active -= 1

    monkeypatch.setattr(asyncio, "to_thread", spy_to_thread)

    total_calls = 8
    results = await asyncio.gather(*(fs.read_bytes("/only.txt") for _ in range(total_calls)))
    assert results == [b"read"] * total_calls
    assert len(called_to_thread) == total_calls
    assert peak <= max_conc


async def test_async_fs_with_asynchronous_false_uses_to_thread(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    # An AsyncFileSystem constructed with asynchronous=False is bound to its own loop,
    # so FsspecAdapter must dispatch via to_thread rather than awaiting its _* hooks.
    from upathtools.fs import LoopRunner

    with LoopRunner(name="test-async-false") as runner:
        raw = _FakeAsyncFsspecFS(asynchronous=False, loop=runner.loop)
        fs = from_fsspec(raw)

        called: list[Any] = []
        real_to_thread = asyncio.to_thread

        async def spy(fn: Any, *args: Any, **kwargs: Any) -> Any:
            called.append(fn)
            return await real_to_thread(fn, *args, **kwargs)

        monkeypatch.setattr(asyncio, "to_thread", spy)

        assert await fs.read_bytes("/hello.txt") == b"hi"
        assert len(called) == 1


async def test_readonly_fsspec_capabilities_and_unsupported_operation() -> None:
    raw = _FakeSyncReadOnlyFS()
    fs = from_fsspec(raw)

    assert fs.readonly is True
    assert "write_bytes" not in fs.capabilities
    assert "mkdir" not in fs.capabilities
    assert "rmdir" not in fs.capabilities
    assert "remove_file" not in fs.capabilities

    with pytest.raises(UnsupportedOperation):
        await fs.write_bytes("/x.txt", b"1")
    with pytest.raises(UnsupportedOperation):
        await fs.remove_file("/only.txt")
    with pytest.raises(UnsupportedOperation):
        await fs.rmdir("/")


async def test_aclose_close_underlying_flag() -> None:
    # Default close_underlying=False leaves underlying fs untouched.
    raw_async = _FakeAsyncFsspecFS(asynchronous=True)
    fs_no_close = from_fsspec(raw_async, close_underlying=False)
    await fs_no_close.aclose()
    assert fs_no_close.closed
    assert raw_async.closed_async is False

    # close_underlying=True invokes async close_session hook.
    fs_with_close = from_fsspec(raw_async, close_underlying=True)
    await fs_with_close.aclose()
    assert raw_async.closed_async is True

    # close_underlying=True invokes sync close() hook on sync fs.
    raw_sync = _FakeSyncReadOnlyFS()
    fs_sync_close = from_fsspec(raw_sync, close_underlying=True)
    await fs_sync_close.aclose()
    assert raw_sync.closed_sync is True


def test_ensure_async_fs_identity_caching_unwrapping_and_no_dict_mutation() -> None:
    core = MemoryFS()
    assert ensure_async_fs(core) is core

    # Unwraps AsFsspecFileSystem back to core_fs without double-wrapping
    bridged = to_fsspec(core, asynchronous=True)
    assert ensure_async_fs(bridged) is core

    # Caches one adapter per fsspec instance without mutating __dict__
    raw = _FakeSyncReadOnlyFS()
    before_keys = set(raw.__dict__)
    a1 = ensure_async_fs(raw)
    a2 = ensure_async_fs(raw)
    assert isinstance(a1, FsspecAdapter)
    assert a1 is a2
    assert a1.raw is raw
    assert set(raw.__dict__) == before_keys
    assert "__getattr__" not in FsspecAdapter.__dict__

    # WeakKeyDictionary drops adapter when raw is garbage-collected
    import weakref

    ref = weakref.ref(a1)
    del a1, a2, raw
    gc.collect()
    assert ref() is None


async def test_s3_style_empty_root_marker_path_handling() -> None:
    raw = _FakeS3StyleFS()
    fs = from_fsspec(raw)

    assert fs.root_marker == ""
    assert fs.protocol == ("s3", "s3a")

    # Protocol stripped, no forced leading "/"
    info = await fs.info("s3://mybucket/dir/a.txt")
    assert isinstance(info, FileInfo)
    assert info.path == "mybucket/dir/a.txt"
    assert info.size == len(b"alpha")

    # Root keeps root_marker ""
    root_info = await fs.info("")
    assert root_info.path == ""
    assert root_info.is_dir

    # ls returns bucket-prefixed paths without leading "/"
    assert await fs.ls_names("s3://mybucket") == ["mybucket/dir", "mybucket/top.txt"]
    assert await fs.ls_names("mybucket/dir/") == ["mybucket/dir/a.txt", "mybucket/dir/b.txt"]
    with pytest.raises(NotADirectoryError):
        await fs.ls("mybucket/top.txt")

    # find & glob work with root_marker == ""
    assert await fs.find_paths("mybucket") == [
        "mybucket/dir/a.txt",
        "mybucket/dir/b.txt",
        "mybucket/top.txt",
    ]
    assert await fs.glob_paths("s3://mybucket/**/*.txt") == [
        "mybucket/dir/a.txt",
        "mybucket/dir/b.txt",
        "mybucket/top.txt",
    ]

    # write_bytes(mode="create") fallback check when backend pipe_file has no `mode` param
    with pytest.raises(FileExistsError):
        await fs.write_bytes("mybucket/top.txt", b"dup", mode="create")
    await fs.write_bytes("mybucket/fresh.txt", b"fresh", mode="create")
    assert await fs.read_bytes("s3://mybucket/fresh.txt") == b"fresh"

    # Cannot remove root ""
    with pytest.raises(ValueError, match="root"):
        await fs.remove("", recursive=True)
