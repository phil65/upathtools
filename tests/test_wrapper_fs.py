"""WrapperFileSystem: callbacks on find/glob details, mounts in find/glob and writes."""

from __future__ import annotations

from typing import Any

import fsspec
import pytest

from upathtools.filesystems.base import WrapperFileSystem


def _tagging_callback(info: dict[str, Any], fs: Any) -> dict[str, Any]:
    return {**info, "tagged": True}


@pytest.fixture
def mem():
    fs = fsspec.filesystem("memory", skip_instance_cache=True)
    fs.store = {}  # private store, not the global one
    fs.pseudo_dirs = [""]
    fs.pipe({"/root/a.txt": b"a", "/root/sub/b.txt": b"b"})
    return fs


async def test_find_and_glob_detail_with_callback(mem) -> None:
    w = WrapperFileSystem(mem, info_callback=_tagging_callback, skip_instance_cache=True)
    found = await w._find("/root", detail=True)
    assert set(found) == {"/root/a.txt", "/root/sub/b.txt"}
    assert all(info["tagged"] for info in found.values())
    globbed = await w._glob("/root/**/*.txt", detail=True)
    assert set(globbed) == {"/root/a.txt", "/root/sub/b.txt"}
    assert all(info["tagged"] for info in globbed.values())


async def test_mounts_visible_to_glob_and_find(mem) -> None:
    other = fsspec.filesystem("memory", skip_instance_cache=True)
    other.store = {}
    other.pseudo_dirs = [""]
    other.pipe("/data/x.txt", b"x")
    w = WrapperFileSystem(mem, skip_instance_cache=True)
    w.mount("/root/virtual.txt", content=b"v")
    w.mount("/root/mnt", fs=other, root="/data")
    assert await w._glob("/root/virt*") == ["/root/virtual.txt"]
    found = await w._find("/root")
    assert {"/root/virtual.txt", "/root/mnt/x.txt", "/root/a.txt"} <= set(found)


async def test_writes_respect_mounts(mem) -> None:
    other = fsspec.filesystem("memory", skip_instance_cache=True)
    other.store = {}
    other.pseudo_dirs = [""]
    w = WrapperFileSystem(mem, skip_instance_cache=True)
    w.mount("/root/virtual.txt", content=b"v")
    w.mount("/mnt", fs=other)
    await w._pipe_file("/mnt/new.txt", b"n")
    assert other.cat("/new.txt") == b"n"
    assert not mem.exists("/mnt/new.txt")
    await w._rm_file("/mnt/new.txt")
    assert not other.exists("/new.txt")
    with pytest.raises(PermissionError):
        await w._pipe_file("/root/virtual.txt", b"overwrite")
    with pytest.raises(PermissionError):
        await w._rm_file("/root/virtual.txt")


async def test_copy_from_content_mount_into_fs(mem) -> None:
    w = WrapperFileSystem(mem, skip_instance_cache=True)
    w.mount("/root/virtual.txt", content=b"v")
    await w._cp_file("/root/virtual.txt", "/root/copy.txt")
    await w._cp_file("/root/a.txt", "/root/a2.txt")
    assert mem.cat("/root/copy.txt") == b"v"
    assert mem.cat("/root/a2.txt") == b"a"


def test_first_access_waits_for_initialization(mem) -> None:
    import threading
    import time

    seen: list[bool] = []
    started = threading.Event()

    def init(fs: WrapperFileSystem) -> None:
        started.set()
        time.sleep(0.2)
        mem.pipe("/root/init.txt", b"i")

    w = WrapperFileSystem(mem, on_first_access=init, skip_instance_cache=True)
    t = threading.Thread(target=w._ensure_initialized)
    t.start()
    started.wait(5)
    w._ensure_initialized()  # must block until init() finished
    seen.append(mem.exists("/root/init.txt"))
    t.join()
    assert seen == [True]
