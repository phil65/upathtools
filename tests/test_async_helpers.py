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


@pytest.mark.asyncio
async def test_read_path(test_dir: Path) -> None:
    """Test reading a single file."""
    file_path = test_dir / "file1.txt"
    content = await async_ops.read_path(file_path)
    assert content == "content1"

    # Test binary mode
    content_bytes = await async_ops.read_path(file_path, mode="rb")
    assert content_bytes == b"content1"


@pytest.mark.asyncio
async def test_read_path_nonexistent() -> None:
    """Test reading a non-existent file raises error."""
    with pytest.raises(FileNotFoundError):
        await async_ops.read_path("nonexistent.txt")


@pytest.mark.asyncio
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


@pytest.mark.asyncio
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


@pytest.mark.asyncio
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


@pytest.mark.asyncio
async def test_read_folder_nonexistent() -> None:
    """Test reading a non-existent folder raises error."""
    with pytest.raises(FileNotFoundError):
        await async_ops.read_folder("nonexistent_folder")


if __name__ == "__main__":
    import pytest

    pytest.main(["-v", __file__])
