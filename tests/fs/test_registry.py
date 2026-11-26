"""Tests for :class:`upathtools.fs.ProtocolRegistry` and ``default_registry``."""

from concurrent.futures import ThreadPoolExecutor
import importlib
from importlib.metadata import EntryPoint
from typing import TYPE_CHECKING, Any

from fsspec.asyn import AsyncFileSystem
from fsspec.implementations.local import LocalFileSystem
from fsspec.implementations.memory import MemoryFileSystem
from fsspec.registry import get_filesystem_class, register_implementation
import pytest

from upathtools.fs import (
    FsspecAdapter,
    LocalFS,
    MemoryFS,
    MemoryStore,
    ProtocolRegistry,
    default_registry,
)
import upathtools.fs.registry as registry_mod


if TYPE_CHECKING:
    from pathlib import Path


class _UrlKwargsFS(MemoryFS):
    """MemoryFS subclass that extracts constructor kwargs from URLs."""

    protocol = "urlkw"

    def __init__(self, *, tag: str = "default", **kwargs: Any) -> None:
        super().__init__(**kwargs)
        self.tag = tag

    @classmethod
    def kwargs_from_url(cls, url: str) -> dict[str, Any]:
        # Encode tag as authority: urlkw://<tag>/path
        rest = url.removeprefix("urlkw://")
        tag = rest.split("/", 1)[0]
        return {"tag": tag} if tag else {}


class _DummyAsyncFsspecFS(AsyncFileSystem):
    """Dummy fsspec AsyncFileSystem to test asynchronous=True fallback."""

    protocol = "dummyasync_reg"
    cachable = False

    def __init__(self, *args: Any, **kwargs: Any) -> None:
        super().__init__(*args, **kwargs)


def test_register_unregister_lookup_and_clobber() -> None:
    reg = ProtocolRegistry(entry_point_group=None, fsspec_fallback=False)
    assert reg.protocols() == frozenset()
    assert reg.get_factory("mem") is None

    reg.register("mem", MemoryFS)
    assert "mem" in reg.protocols()
    assert reg.get_factory("mem") is MemoryFS
    assert isinstance(reg.filesystem("mem"), MemoryFS)

    with pytest.raises(ValueError, match="already registered"):
        reg.register("mem", _UrlKwargsFS)

    reg.register("mem", _UrlKwargsFS, clobber=True)
    assert reg.get_factory("mem") is _UrlKwargsFS

    reg.unregister("mem")
    assert reg.get_factory("mem") is None
    assert "mem" not in reg.protocols()

    with pytest.raises(KeyError):
        reg.unregister("mem")

    with pytest.raises(ValueError, match="non-empty"):
        reg.register("", MemoryFS)

    with pytest.raises(ValueError, match="Protocol not known"):
        reg.filesystem("unknown_proto")


def test_default_registry_has_memory_only_locally_never_in_fsspec() -> None:
    assert "memory" in default_registry.protocols()
    assert "file" in default_registry.protocols()
    assert "local" in default_registry.protocols()
    fs = default_registry.filesystem("memory")
    assert isinstance(fs, MemoryFS)
    assert isinstance(default_registry.filesystem("file"), LocalFS)
    assert isinstance(default_registry.filesystem("local"), LocalFS)
    assert get_filesystem_class("memory") is MemoryFileSystem
    assert get_filesystem_class("file") is LocalFileSystem
    assert get_filesystem_class("local") is LocalFileSystem


async def test_fsspec_fallback_for_file_and_async_fs(tmp_path: Path) -> None:
    reg = ProtocolRegistry(entry_point_group=None, fsspec_fallback=True)

    # Local "file" falls back to fsspec's LocalFileSystem wrapped in FsspecAdapter
    fs = reg.filesystem("file")
    assert isinstance(fs, FsspecAdapter)
    assert isinstance(fs.raw, LocalFileSystem)

    target = tmp_path / "hello.txt"
    target.write_text("from disk", encoding="utf-8")
    assert await fs.read_text(target.as_posix()) == "from disk"

    # AsyncFileSystem fallback is constructed with asynchronous=True
    proto = "dummyasync_reg_test"
    fsspec_reg_mod = importlib.import_module("fsspec.registry")
    register_implementation(proto, _DummyAsyncFsspecFS, clobber=True)
    try:
        async_adapted = reg.filesystem(proto)
        assert isinstance(async_adapted, FsspecAdapter)
        assert isinstance(async_adapted.raw, _DummyAsyncFsspecFS)
        assert async_adapted.raw.asynchronous is True
        assert async_adapted.raw.loop is None
    finally:
        fsspec_reg_mod._registry.pop(proto, None)


def test_entry_points_loaded_lazily_once(monkeypatch: pytest.MonkeyPatch) -> None:
    calls = 0

    def fake_entry_points(*, group: str) -> list[EntryPoint]:
        nonlocal calls
        calls += 1
        assert group == "upathtools.filesystems"
        return [
            EntryPoint(
                name="epmem",
                value="upathtools.fs.implementations.memory:MemoryFS",
                group=group,
            )
        ]

    monkeypatch.setattr(registry_mod, "entry_points", fake_entry_points)
    reg = ProtocolRegistry(fsspec_fallback=False)
    assert calls == 0  # lazy: not loaded at construction

    assert "epmem" in reg.protocols()
    assert calls == 1

    # Second call reuses cached entry points without re-scanning metadata
    factory = reg.get_factory("epmem")
    assert factory is MemoryFS
    assert calls == 1

    # Explicit registration overrides entry point
    store = MemoryStore()
    reg.register("epmem", lambda **kw: MemoryFS(store=store, **kw))
    fs = reg.filesystem("epmem")
    assert isinstance(fs, MemoryFS)
    assert fs.store is store


async def test_url_to_fs_resolution_kwargs_and_chained_error(tmp_path: Path) -> None:
    reg = ProtocolRegistry(entry_point_group=None, fsspec_fallback=True)
    reg.register("memory", MemoryFS)
    reg.register("urlkw", _UrlKwargsFS)

    # Core protocol URL
    fs_mem, path_mem = reg.url_to_fs("memory:///a/b.txt")
    assert isinstance(fs_mem, MemoryFS)
    assert path_mem == "/a/b.txt"

    # URL kwargs extraction + explicit override
    fs_kw, _ = reg.url_to_fs("urlkw://fromurl/file.txt")
    assert isinstance(fs_kw, _UrlKwargsFS)
    assert fs_kw.tag == "fromurl"

    fs_override, _ = reg.url_to_fs("urlkw://fromurl/file.txt", tag="explicit")
    assert isinstance(fs_override, _UrlKwargsFS)
    assert fs_override.tag == "explicit"

    # Local path without scheme -> file:// via fsspec fallback
    sample = tmp_path / "sample.txt"
    sample.write_bytes(b"local-bytes")
    fs_local, local_path = reg.url_to_fs(sample)
    assert isinstance(fs_local, FsspecAdapter)
    assert await fs_local.read_bytes(local_path) == b"local-bytes"

    # Chained URL raises NotImplementedError
    with pytest.raises(NotImplementedError, match="Chained URLs"):
        reg.url_to_fs("zip://inner.txt::file:///archive.zip")


def test_registry_thread_safety() -> None:
    reg = ProtocolRegistry(entry_point_group=None, fsspec_fallback=False)

    def worker(idx: int) -> None:
        proto = f"p{idx}"
        reg.register(proto, MemoryFS)
        assert reg.get_factory(proto) is MemoryFS
        assert isinstance(reg.filesystem(proto), MemoryFS)
        assert proto in reg.protocols()

    total_protos = 32
    with ThreadPoolExecutor(max_workers=8) as ex:
        list(ex.map(worker, range(total_protos)))
    assert len(reg.protocols()) == total_protos
