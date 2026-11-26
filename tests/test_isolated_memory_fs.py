"""IsolatedMemoryFileSystem leaves fsspec's memory:// alone and drops anonymous stores."""

import fsspec
from fsspec.implementations.memory import MemoryFileSystem

from upathtools.filesystems import IsolatedMemoryFileSystem


def test_memory_protocol_is_fsspecs() -> None:
    import upathtools  # noqa: F401  # entry points registered

    assert fsspec.get_filesystem_class("memory") is MemoryFileSystem
    fsspec.filesystem("memory").pipe("/upathtools-test/x.txt", b"hello")
    assert fsspec.filesystem("memory").cat("/upathtools-test/x.txt") == b"hello"
    fsspec.filesystem("memory").rm("/upathtools-test", recursive=True)


def test_anonymous_isolated_store_is_dropped() -> None:
    import gc

    fs = IsolatedMemoryFileSystem()
    key = fs.key
    assert key in IsolatedMemoryFileSystem.list_keys()
    del fs
    gc.collect()
    assert key not in IsolatedMemoryFileSystem.list_keys()
