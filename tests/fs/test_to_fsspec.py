"""Tests for outbound ``to_fsspec`` / ``AsFsspecFileSystem`` and fsspec/UPath bridges."""

from contextlib import contextmanager
from datetime import UTC, datetime
import importlib
import threading
from typing import TYPE_CHECKING
import uuid

import fsspec
import fsspec.asyn as fsspec_asyn
from fsspec.registry import get_filesystem_class, registry as fsspec_reg_view
import pytest
from upath import UPath
import upath.registry as upath_registry

from upathtools.fs import (
    AsFsspecFileSystem,
    AsyncFS,
    FileInfo,
    LoopRunner,
    MemoryFS,
    MemoryStore,
    fsspec_class_for,
    register_fsspec_bridge,
    register_upath_bridge,
    to_fsspec,
)


if TYPE_CHECKING:
    from collections.abc import Iterator


@pytest.fixture
def runner() -> Iterator[LoopRunner]:
    with LoopRunner(name="test-to-fsspec") as loop_runner:
        yield loop_runner


@contextmanager
def _temp_protocol(prefix: str = "coretest") -> Iterator[str]:
    proto = f"{prefix}{uuid.uuid4().hex[:8]}"
    fsspec_reg_mod = importlib.import_module("fsspec.registry")
    try:
        yield proto
    finally:
        fsspec_reg_mod._registry.pop(proto, None)
        fsspec_reg_mod.known_implementations.pop(proto, None)
        upath_registry._registry._m.maps[0].pop(proto, None)
        upath_registry.get_upath_class.cache_clear()
        upath_registry._get_implementation_protocols.cache_clear()


class _ReadOnlyFS(AsyncFS[FileInfo]):
    """Minimal read-only AsyncFS for testing error mapping to NotImplementedError."""

    protocol = "rotest"

    def __init__(self, files: dict[str, bytes]) -> None:
        super().__init__()
        self._files = dict(files)

    async def info(self, path: str) -> FileInfo:
        norm = self._norm(path)
        if norm == "/":
            return FileInfo(path="/", type="directory", size=0)
        if norm in self._files:
            return FileInfo(path=norm, type="file", size=len(self._files[norm]))
        raise FileNotFoundError(norm)

    async def ls(self, path: str) -> list[FileInfo]:
        norm = self._norm(path)
        if norm == "/":
            return [
                FileInfo(path=p, type="file", size=len(data))
                for p, data in sorted(self._files.items())
            ]
        if norm in self._files:
            raise NotADirectoryError(norm)
        raise FileNotFoundError(norm)

    async def read_bytes(
        self,
        path: str,
        *,
        start: int | None = None,
        end: int | None = None,
    ) -> bytes:
        norm = self._norm(path)
        if norm not in self._files:
            raise FileNotFoundError(norm)
        return self._files[norm][start:end]


def test_import_does_not_register_in_fsspec_or_upath() -> None:
    from fsspec.implementations.memory import MemoryFileSystem

    assert get_filesystem_class("memory") is MemoryFileSystem
    assert not issubclass(get_filesystem_class("memory"), AsFsspecFileSystem)
    assert not any(
        isinstance(cls, type) and issubclass(cls, AsFsspecFileSystem)
        for cls in fsspec_reg_view.values()
    )


def test_fsspec_class_for_caches_per_class_and_protocol() -> None:
    cls_default_1 = fsspec_class_for(MemoryFS)
    cls_default_2 = fsspec_class_for(MemoryFS)
    assert cls_default_1 is cls_default_2
    assert cls_default_1.protocol == MemoryFS.protocol
    assert cls_default_1.root_marker == MemoryFS.root_marker
    assert cls_default_1.sep == MemoryFS.sep
    assert cls_default_1.cachable is False
    assert cls_default_1._strip_protocol("memory:///a/b/") == "/a/b"
    assert cls_default_1._strip_protocol(["memory:///a", "memory:///b/"]) == ["/a", "/b"]

    cls_custom = fsspec_class_for(MemoryFS, protocol="customproto")
    assert cls_custom is not cls_default_1
    assert cls_custom.protocol == "customproto"
    assert cls_custom._strip_protocol("customproto:///x/y/") == "/x/y"


def test_to_fsspec_sync_uses_runner_loop_never_fsspec_io_thread(runner: LoopRunner) -> None:
    core = MemoryFS()
    fs = to_fsspec(core, runner=runner)

    assert isinstance(fs, AsFsspecFileSystem)
    assert fs.core_fs is core
    assert fs.loop is runner.loop
    assert fs.loop is not fsspec_asyn.loop[0]
    assert "fsspecIO" not in {t.name for t in threading.enumerate()}

    fs.pipe_file("/hello.txt", b"hello world")
    assert fs.cat_file("/hello.txt") == b"hello world"
    assert fs.cat_file("/hello.txt", start=6, end=11) == b"world"
    assert fs.exists("/hello.txt")
    assert fs.isfile("/hello.txt")
    assert not fs.isdir("/hello.txt")
    assert fs.size("/hello.txt") == len(b"hello world")

    info = fs.info("/hello.txt")
    assert info["name"] == "/hello.txt"
    assert info["type"] == "file"
    assert info["size"] == len(b"hello world")
    assert isinstance(info["mtime"], float)
    assert isinstance(info["created"], float)

    mod = fs.modified("/hello.txt")
    cre = fs.created("/hello.txt")
    assert isinstance(mod, datetime)
    assert mod.tzinfo == UTC
    assert isinstance(cre, datetime)
    assert cre.tzinfo == UTC


def test_to_fsspec_ls_on_file_and_directory(runner: LoopRunner) -> None:
    fs = to_fsspec(MemoryFS(), runner=runner)
    fs.makedirs("/dir/sub")
    fs.pipe_file("/dir/a.txt", b"aaa")

    file_detail = fs.ls("/dir/a.txt", detail=True)
    assert len(file_detail) == 1
    assert file_detail[0]["name"] == "/dir/a.txt"
    assert file_detail[0]["type"] == "file"

    file_names = fs.ls("/dir/a.txt", detail=False)
    assert file_names == ["/dir/a.txt"]

    dir_names = fs.ls("/dir", detail=False)
    assert dir_names == ["/dir/a.txt", "/dir/sub"]

    dir_detail = fs.ls("/dir", detail=True)
    assert [d["name"] for d in dir_detail] == ["/dir/a.txt", "/dir/sub"]


async def test_to_fsspec_asynchronous_mode() -> None:
    core = MemoryFS()
    fs = to_fsspec(core, asynchronous=True)

    assert fs.asynchronous is True
    assert fs.loop is None

    await fs._pipe_file("/async.txt", b"async-data")
    assert await fs._exists("/async.txt")
    assert await fs._isfile("/async.txt")
    assert await fs._cat_file("/async.txt") == b"async-data"
    assert (await fs._info("/async.txt"))["size"] == len(b"async-data")

    await fs._mkdir("/adir", create_parents=True)
    assert await fs._isdir("/adir")
    await fs._pipe_file("/adir/f.txt", b"123")

    found_paths = await fs._find("/adir")
    assert found_paths == ["/adir/f.txt"]
    found_detail = await fs._find("/adir", withdirs=True, detail=True)
    assert isinstance(found_detail, dict)
    assert set(found_detail) == {"/adir", "/adir/f.txt"}

    glob_paths = await fs._glob("/adir/*")
    assert glob_paths == ["/adir/f.txt"]
    glob_nodirs = await fs._glob("/*", withdirs=False)
    assert glob_nodirs == ["/async.txt"]
    glob_detail = await fs._glob("/adir/*", detail=True)
    assert isinstance(glob_detail, dict)
    assert "/adir/f.txt" in glob_detail

    # open_async returns core AsyncBufferedReader / AsyncBufferedWriter
    async with await fs.open_async("/stream.bin", "wb") as writer:
        await writer.write(b"streamed")
    async with await fs.open_async("/stream.bin", "rb") as reader:
        assert await reader.read() == b"streamed"

    await fs._rm("/adir", recursive=True)
    assert not await fs._exists("/adir")


def test_to_fsspec_open_modes_seek_ranges_and_multiblock(runner: LoopRunner) -> None:
    fs = to_fsspec(MemoryFS(), runner=runner)

    # Write larger than block_size to exercise multi-chunk flush + single commit on close
    payload = b"0123456789abcdef" * 4
    with fs.open("/data.bin", "wb", block_size=10) as wf:
        wf.write(payload[:15])
        wf.write(payload[15:])

    with fs.open("/data.bin", "rb", block_size=8) as rf:
        assert rf.read(5) == b"01234"
        assert rf.tell() == len(b"01234")
        seek_pos = 10
        assert rf.seek(seek_pos) == seek_pos
        assert rf.read(6) == b"abcdef"
        assert rf.seek(-6, 2) == len(payload) - 6
        assert rf.read() == b"abcdef"

    # Append mode
    with fs.open("/data.bin", "ab") as af:
        af.write(b"XYZ")
    assert fs.cat_file("/data.bin") == payload + b"XYZ"

    # Exclusive mode raises at open time if file exists, succeeds on new
    with pytest.raises(FileExistsError):
        fs.open("/data.bin", "xb")
    with fs.open("/exclusive.bin", "xb") as xf:
        xf.write(b"once")
    assert fs.cat_file("/exclusive.bin") == b"once"

    # autocommit=False buffers until commit(), or drops on discard()
    f_discard = fs.open("/discarded.bin", "wb", autocommit=False)
    f_discard.write(b"nope")
    f_discard.close()
    assert not fs.exists("/discarded.bin")
    f_discard.discard()
    assert not fs.exists("/discarded.bin")

    f_commit = fs.open("/committed.bin", "wb", autocommit=False)
    f_commit.write(b"yes")
    f_commit.close()
    assert not fs.exists("/committed.bin")
    f_commit.commit()
    assert fs.cat_file("/committed.bin") == b"yes"


def test_to_fsspec_sync_reentry_from_runner_loop_raises_runtime_error(runner: LoopRunner) -> None:
    fs = to_fsspec(MemoryFS(), runner=runner)
    fs.pipe_file("/f.txt", b"ok")

    async def bad_reentry() -> None:
        fs.cat_file("/f.txt")

    with pytest.raises(RuntimeError):
        runner.run(bad_reentry(), timeout=2.0)


def test_to_fsspec_readonly_maps_unsupported_operation_to_not_implemented(
    runner: LoopRunner,
) -> None:
    fs = to_fsspec(_ReadOnlyFS({"/a.txt": b"hello"}), runner=runner)
    assert fs.cat_file("/a.txt") == b"hello"

    with pytest.raises(NotImplementedError):
        fs.pipe_file("/b.txt", b"x")
    with pytest.raises(NotImplementedError):
        fs.rm_file("/a.txt")
    with pytest.raises(NotImplementedError):
        fs.mkdir("/d")
    with pytest.raises(NotImplementedError):
        fs.makedirs("/d/e")
    with pytest.raises(NotImplementedError):
        fs.rmdir("/d")
    with pytest.raises(NotImplementedError):
        fs.touch("/b.txt")
    with pytest.raises(NotImplementedError):
        fs.modified("/a.txt")
    with pytest.raises(NotImplementedError):
        fs.created("/a.txt")


def test_register_fsspec_bridge_filesystem_and_fsspec_open_text_and_gzip() -> None:
    store = MemoryStore()
    with _temp_protocol("corebridge") as proto:
        cls = register_fsspec_bridge(proto, lambda **kw: MemoryFS(store=store, **kw))

        fs = fsspec.filesystem(proto)
        assert isinstance(fs, cls)
        assert isinstance(fs, AsFsspecFileSystem)
        fs.pipe_file("/plain.txt", b"hello")
        assert fs.cat_file("/plain.txt") == b"hello"

        # Text mode via fsspec.open
        with fsspec.open(f"{proto}:///notes.txt", "w", encoding="utf-8") as wf:
            wf.write("line1\nline2\n")
        with fsspec.open(f"{proto}:///notes.txt", "r", encoding="utf-8") as rf:
            assert rf.read() == "line1\nline2\n"

        # Gzip compression via fsspec.open
        with fsspec.open(
            f"{proto}:///compressed.txt.gz", "wt", compression="gzip", encoding="utf-8"
        ) as gwf:
            gwf.write("compressed payload")
        raw_bytes = fs.cat_file("/compressed.txt.gz")
        assert raw_bytes[:2] == b"\x1f\x8b"  # gzip magic header
        with fsspec.open(
            f"{proto}:///compressed.txt.gz", "rt", compression="gzip", encoding="utf-8"
        ) as grf:
            assert grf.read() == "compressed payload"

        # Re-registering without clobber raises ValueError; clobber=True replaces
        with pytest.raises(ValueError, match="clobber"):
            register_fsspec_bridge(proto, lambda **kw: MemoryFS(store=store, **kw))
        cls2 = register_fsspec_bridge(proto, lambda **kw: MemoryFS(store=store, **kw), clobber=True)
        assert isinstance(fsspec.filesystem(proto), cls2)


def test_register_upath_bridge_full_upath_operations() -> None:
    store = MemoryStore()
    with _temp_protocol("coreupath") as proto:
        path_cls = register_upath_bridge(proto, lambda **kw: MemoryFS(store=store, **kw))

        root = UPath(f"{proto}:///workspace")
        assert isinstance(root, path_cls)
        assert isinstance(root, UPath)
        assert isinstance(root.fs, AsFsspecFileSystem)

        root.mkdir(parents=True, exist_ok=True)
        assert root.exists()
        assert root.is_dir()

        sub = root / "sub"
        sub.mkdir()
        assert sub.is_dir()

        f1 = root / "hello.txt"
        f2 = sub / "nested.txt"
        f1.write_text("hello upath", encoding="utf-8")
        f2.write_text("deep text", encoding="utf-8")

        assert f1.exists()
        assert f1.is_file()
        assert f1.read_text(encoding="utf-8") == "hello upath"
        assert f2.read_text(encoding="utf-8") == "deep text"

        # iterdir
        children = sorted(p.name for p in root.iterdir())
        assert children == ["hello.txt", "sub"]

        # glob
        matched = sorted(p.name for p in root.glob("**/*.txt"))
        assert matched == ["hello.txt", "nested.txt"]

        # rename file
        moved = root / "renamed.txt"
        f1.rename(moved)
        assert not f1.exists()
        assert moved.exists()
        assert moved.read_text(encoding="utf-8") == "hello upath"

        # unlink & rmdir
        f2.unlink()
        assert not f2.exists()
        sub.rmdir()
        assert not sub.exists()
        moved.unlink()
        assert not moved.exists()


def test_to_fsspec_mv_file_and_directory(runner: LoopRunner) -> None:
    fs = to_fsspec(MemoryFS(), runner=runner)
    fs.makedirs("/src/sub")
    fs.pipe_file("/src/a.txt", b"A")
    fs.pipe_file("/src/sub/b.txt", b"B")

    fs.mv("/src/a.txt", "/moved.txt")
    assert not fs.exists("/src/a.txt")
    assert fs.cat_file("/moved.txt") == b"A"

    fs.mv("/src", "/dst", recursive=True)
    assert not fs.exists("/src")
    assert fs.cat_file("/dst/sub/b.txt") == b"B"
