"""The filesystem barrels load their submodules on first access."""

import importlib.metadata
import subprocess
import sys

import pytest

import upathtools.filesystems
from upathtools.filesystems import (
    combining_filesystems,
    file_filesystems,
    remote_filesystems,
    sandbox_filesystems,
)


BARRELS = [
    upathtools.filesystems,
    sandbox_filesystems,
    file_filesystems,
    remote_filesystems,
    combining_filesystems,
]


@pytest.mark.parametrize("module", BARRELS, ids=lambda m: m.__name__)
def test_all_exports_resolve(module) -> None:
    for name in module.__all__:
        assert getattr(module, name) is not None, f"{module.__name__}.{name}"
    assert set(module.__all__) <= set(dir(module))


def test_unknown_name_raises_attribute_error() -> None:
    with pytest.raises(AttributeError, match="no attribute 'Nope'"):
        _ = upathtools.filesystems.Nope


def test_union_aliases() -> None:
    from upathtools.filesystems.sandbox_filesystems import DaytonaFS, SandboxFilesystem

    assert DaytonaFS in SandboxFilesystem.__args__
    from upathtools.filesystems.file_filesystems import FileFileSystem, SqliteFileSystem

    assert SqliteFileSystem in FileFileSystem.__args__


@pytest.mark.parametrize("group", ["fsspec.specs", "universal_pathlib.implementations"])
def test_entry_points_load(group: str) -> None:
    eps = [
        ep
        for ep in importlib.metadata.entry_points(group=group)
        if ep.value.startswith("upathtools")
    ]
    assert eps
    for ep in eps:
        assert ep.load() is not None, ep.name


def test_import_does_not_load_sandbox_filesystems() -> None:
    code = (
        "import sys, upathtools, fsspec\n"
        "fsspec.filesystem('asynclocal')\n"
        "prefix = 'upathtools.filesystems.sandbox_filesystems.'\n"
        "loaded = [m for m in sys.modules if m.startswith(prefix)]\n"
        "assert not loaded, loaded\n"
    )
    result = subprocess.run(
        [sys.executable, "-c", code], capture_output=True, text=True, check=False
    )
    assert result.returncode == 0, result.stderr
