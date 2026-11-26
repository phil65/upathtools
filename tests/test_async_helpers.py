"""Tests for async_ops module."""

from __future__ import annotations

import os
from pathlib import Path
import tempfile
from typing import TYPE_CHECKING

import pytest
from upath import UPath

from upathtools import async_ops


if TYPE_CHECKING:
    from collections.abc import Iterator


@pytest.fixture(name="test_dir")
def fixture_test_dir() -> Iterator[Path]:
    """Create a temporary directory with test files."""
    with tempfile.TemporaryDirectory() as temp_dir:
        # Create test directory structure
        test_dir = Path(temp_dir)

        # Create some test files with content
        (test_dir / "file1.txt").write_text("content1")
        (test_dir / "file2.txt").write_text("content2")

        # Create a subdirectory with files
        subdir = test_dir / "subdir"
        subdir.mkdir()
        (subdir / "file3.txt").write_text("content3")
        (subdir / "file4.py").write_text("print('hello')")

        yield test_dir

        # Cleanup happens automatically via context manager


async def test_read_path(test_dir: Path) -> None:
    """Test reading a single file."""
    file_path = test_dir / "file1.txt"
    content = await async_ops.read_path(file_path)
    assert content == "content1"

    # Test binary mode
    content_bytes = await async_ops.read_path(file_path, mode="rb")
    assert content_bytes == b"content1"


async def test_read_path_nonexistent() -> None:
    """Test reading a non-existent file raises error."""
    with pytest.raises(FileNotFoundError):
        await async_ops.read_path("nonexistent.txt")


async def test_list_files(test_dir: Path) -> None:
    """Test listing files with various patterns."""
    # Test basic file listing
    files = await async_ops.list_files(test_dir)
    assert len(files) == 4  # All files  # noqa: PLR2004

    # Test pattern matching
    txt_files = await async_ops.list_files(test_dir, pattern="*.txt")
    assert len(txt_files) == 2  # Only txt files in root dir  # noqa: PLR2004

    # Test recursive pattern matching
    all_txt_files = await async_ops.list_files(test_dir, pattern="**/*.txt")
    assert len(all_txt_files) == 3  # All txt files including subdirs  # noqa: PLR2004

    # Test Python files
    py_files = await async_ops.list_files(test_dir, pattern="**/*.py")
    assert len(py_files) == 1  # One Python file

    # Test with exclude pattern
    filtered_files = await async_ops.list_files(
        test_dir,
        pattern="**/*.*",
        exclude=["*.py"],
    )
    assert len(filtered_files) == 3  # All files except .py  # noqa: PLR2004

    # Verify all returned paths are UPath instances
    assert all(isinstance(f, UPath) for f in files)


async def test_read_folder(test_dir: Path) -> None:
    """Test reading entire folders."""
    # Test reading all text files
    content_map = await async_ops.read_folder(
        test_dir,
        pattern="**/*.txt",
        recursive=True,
    )

    assert len(content_map) == 3  # noqa: PLR2004
    assert "file1.txt" in content_map
    assert "file2.txt" in content_map
    assert os.path.join("subdir", "file3.txt") in content_map  # noqa: PTH118

    # Test content is correct
    assert content_map["file1.txt"] == "content1"

    # Test binary mode
    binary_map = await async_ops.read_folder(
        test_dir,
        pattern="**/*.txt",
        mode="rb",
        recursive=True,
    )
    assert binary_map["file1.txt"] == b"content1"

    # Test parallel loading
    parallel_map = await async_ops.read_folder(
        test_dir,
        pattern="**/*.txt",
        load_parallel=True,
        chunk_size=2,
    )
    assert len(parallel_map) == 3  # noqa: PLR2004
    assert parallel_map["file1.txt"] == "content1"


async def test_read_folder_empty(test_dir: Path) -> None:
    """Test reading an empty folder or with no matching files."""
    # Create empty subdirectory
    empty_dir = test_dir / "empty"
    empty_dir.mkdir()

    # Test reading empty directory
    content_map = await async_ops.read_folder(empty_dir)
    assert len(content_map) == 0

    # Test with non-matching pattern
    content_map = await async_ops.read_folder(test_dir, pattern="*.nonexistent")
    assert len(content_map) == 0


async def test_read_folder_nonexistent() -> None:
    """Test reading a non-existent folder raises error."""
    with pytest.raises(FileNotFoundError):
        await async_ops.read_folder("nonexistent_folder")


if __name__ == "__main__":
    import pytest

    pytest.main(["-v", __file__])


def test_upath_to_fs_sync_dirfs(tmp_path):
    from upathtools.helpers import upath_to_fs

    (tmp_path / "f.txt").write_bytes(b"x")
    fs = upath_to_fs(tmp_path, asynchronous=False)
    assert fs.ls("/", detail=False) == ["f.txt"]


@pytest.fixture
def memory_tree():
    import fsspec

    fs = fsspec.filesystem("memory")
    fs.pipe({
        "/lf-proj/a.txt": b"A",
        "/lf-proj/sub/b.txt": b"B",
        "/lf-proj/sub/deep/c.txt": b"C",
    })
    yield "memory:///lf-proj"
    fs.rm("/lf-proj", recursive=True)


async def test_list_files_keeps_protocol(memory_tree: str) -> None:
    files = await async_ops.list_files(memory_tree, pattern="**/*.txt")
    assert all(f.protocol == "memory" for f in files)
    assert sorted(f.path for f in files) == [
        "/lf-proj/a.txt",
        "/lf-proj/sub/b.txt",
        "/lf-proj/sub/deep/c.txt",
    ]
    content = await async_ops.read_folder(memory_tree, pattern="**/*.txt")
    assert content == {"a.txt": "A", "sub/b.txt": "B", "sub/deep/c.txt": "C"}


async def test_list_files_include_dirs_and_recursive(memory_tree: str) -> None:
    with_dirs = await async_ops.list_files(memory_tree, pattern="**/*", include_dirs=True)
    assert {"/lf-proj/sub", "/lf-proj/sub/deep"} <= {f.path for f in with_dirs}
    top = await async_ops.list_files(memory_tree, pattern="**/*.txt", recursive=False)
    assert [f.path for f in top] == ["/lf-proj/a.txt"]
    shallow = await async_ops.list_files(memory_tree, pattern="**/*.txt", max_depth=1)
    assert [f.path for f in shallow] == ["/lf-proj/a.txt"]


async def test_list_files_detail_has_name_and_path(memory_tree: str) -> None:
    infos = await async_ops.list_files(memory_tree, pattern="*.txt", detail=True)
    assert [(i["name"], i["path"], i["type"]) for i in infos] == [
        ("a.txt", "/lf-proj/a.txt", "file")
    ]


async def test_read_path_keeps_storage_options(tmp_path: Path) -> None:
    import zipfile

    archive = tmp_path / "a.zip"
    with zipfile.ZipFile(archive, "w") as zf:
        zf.writestr("inner.txt", "zipped")
    assert await async_ops.read_path(UPath("zip://inner.txt", fo=str(archive))) == "zipped"


async def test_read_path_uses_bound_filesystem() -> None:
    from upathtools.filesystems import IsolatedMemoryFileSystem

    fs = IsolatedMemoryFileSystem()
    fs.pipe("/only-here.txt", b"isolated")
    assert await async_ops.read_path(fs.get_upath("/only-here.txt")) == "isolated"


async def test_read_path_encoding(tmp_path: Path) -> None:
    p = tmp_path / "latin1.txt"
    p.write_bytes("caf\xe9".encode("latin-1"))
    assert await async_ops.read_path(p, encoding="latin-1") == "caf\xe9"


async def test_read_folder_parallel_skips_only_the_bad_file(tmp_path: Path) -> None:
    for i in range(5):
        (tmp_path / f"good{i}.txt").write_text(f"ok{i}")
    (tmp_path / "bad.txt").write_bytes(b"\xff\xfe\xfa")
    result = await async_ops.read_folder(tmp_path, pattern="*.txt", load_parallel=True)
    assert sorted(result) == [f"good{i}.txt" for i in range(5)]
