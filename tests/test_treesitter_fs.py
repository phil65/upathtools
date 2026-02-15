"""Tests for TreeSitterFileSystem."""

from __future__ import annotations

from typing import TYPE_CHECKING

import fsspec
import pytest

from upathtools import core
from upathtools.filesystems.file_filesystems.treesitter_fs import TreeSitterFileSystem


if TYPE_CHECKING:
    from pathlib import Path

EXAMPLE_PY = """
def test_func():
    '''Test function'''
    pass

class TestClass:
    '''Test class'''

    def method_one(self):
        '''Method one'''
        return 1

    def method_two(self):
        '''Method two'''
        return 2
"""


@pytest.fixture
def example_py(tmp_path: Path) -> Path:
    """Create a temporary Python file with example content."""
    path = tmp_path / "example.py"
    path.write_text(EXAMPLE_PY)
    return path


def test_static_module_direct_file(example_py: Path) -> None:
    """Test direct file access."""
    fs = core.filesystem("ts", source_file=str(example_py))

    # Test listing
    members = fs.ls("/", detail=True)
    assert len(members) >= 2  # noqa: PLR2004
    assert any(
        m["name"] == "test_func" and m["node_type"] == "function_definition" for m in members
    )
    assert any(m["name"] == "TestClass" and m["node_type"] == "class_definition" for m in members)
    # Test source extraction
    func_source = fs.cat("test_func").decode()
    assert "Test function" in func_source
    assert "pass" in func_source


def test_hierarchical_structure(example_py: Path) -> None:
    """Test that TreeSitter supports nested structure (methods in classes)."""
    fs = core.filesystem("ts", source_file=str(example_py))
    # List class members
    class_members = fs.ls("/TestClass", detail=True)
    assert len(class_members) == 2  # noqa: PLR2004
    assert any(m["name"] == "method_one" for m in class_members)
    assert any(m["name"] == "method_two" for m in class_members)
    # Access nested method
    method_source = fs.cat("/TestClass/method_one").decode()
    assert "Method one" in method_source
    assert "return 1" in method_source


def test_static_module_without_extension(example_py: Path) -> None:
    """Test access without extension."""
    fs = core.filesystem("ts", source_file=str(example_py))
    members = fs.ls("/", detail=False)
    assert len(members) >= 2  # noqa: PLR2004
    assert "test_func" in members
    assert "TestClass" in members


def test_chained_access(example_py: Path) -> None:
    """Test chaining with local files."""
    assert example_py.exists()
    assert example_py.read_text("utf-8")
    url = f"ts::file://{example_py.as_posix()}"
    # Verify with explicit filesystem first
    fs = core.filesystem("ts", source_file=str(example_py))
    content = fs.cat("/").decode()
    assert "test_func" in content
    # Test chained version
    with fsspec.open(url, mode="rb") as f:
        content = f.read().decode()  # pyright: ignore[reportAttributeAccessIssue]
    assert "test_func" in content


def test_member_not_found(example_py: Path) -> None:
    """Test error when requesting non-existent member."""
    fs = core.filesystem("ts", source_file=str(example_py))
    with pytest.raises(FileNotFoundError):
        fs.cat("non_existent")


def test_ts_fs_init_requires_path() -> None:
    """Test that source_file is required."""
    with pytest.raises(ValueError, match="Source file path required"):
        core.filesystem("ts", source_file="")


def test_lazy_loading(example_py: Path) -> None:
    """Test that file is only loaded when needed."""
    fs = core.filesystem("ts", source_file=str(example_py))
    assert isinstance(fs, TreeSitterFileSystem)
    assert fs._source is None
    # Access triggers loading
    fs.ls("/")
    assert fs._source is not None


def test_info_with_metadata(example_py: Path) -> None:
    """Test that info includes TreeSitter-specific metadata."""
    fs = core.filesystem("ts", source_file=str(example_py))
    info = fs.info("/test_func")
    assert info["name"] == "test_func"
    assert info["node_type"] == "function_definition"
    assert "start_line" in info
    assert "end_line" in info
    assert "start_byte" in info
    assert "end_byte" in info
    assert info["start_line"] > 0
    assert info["end_line"] >= info["start_line"]


def test_docstring_extraction(example_py: Path) -> None:
    """Test that docstrings are extracted correctly."""
    fs = core.filesystem("ts", source_file=str(example_py))
    func_info = fs.info("/test_func")
    assert func_info.get("doc") == "Test function"
    class_info = fs.info("/TestClass")
    assert class_info.get("doc") == "Test class"


def test_isdir_behavior(example_py: Path) -> None:
    """Test directory behavior for nodes with children."""
    fs = core.filesystem("ts", source_file=str(example_py))
    # Root is a directory
    assert fs.isdir("/")
    # Class with methods is a directory
    assert fs.isdir("/TestClass")
    # Function without children is not a directory
    assert not fs.isdir("/test_func")
    # Non-existent path is not a directory
    assert not fs.isdir("/non_existent")


if __name__ == "__main__":
    pytest.main([__file__])
