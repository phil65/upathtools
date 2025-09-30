"""Tests for ModuleFS."""

from __future__ import annotations

import os
from textwrap import dedent
from typing import TYPE_CHECKING

import fsspec
import pytest


if TYPE_CHECKING:
    from pathlib import Path


@pytest.fixture
def example_py(tmp_path: Path) -> Path:
    """Create a temporary Python file with example content."""
    path = tmp_path / "example.py"
    path.write_text(
        dedent("""
        def test_func():
            '''Test function'''
            pass

        class TestClass:
            '''Test class'''
            pass
    """)
    )
    return path


def test_init_requires_path() -> None:
    """Test that path is required."""
    with pytest.raises(ValueError, match="Path to Python file required"):
        fsspec.filesystem("mod", fo="")


def test_list_module_contents(example_py: Path) -> None:
    """Test listing module contents."""
    fs = fsspec.filesystem("mod", fo=str(example_py))

    # Test detailed listing
    members = fs.ls("/", detail=True)
    assert len(members) == 2  # noqa: PLR2004

    func = next(m for m in members if m["name"] == "test_func")
    assert func["type"] == "function"
    assert func["doc"] == "Test function"

    cls = next(m for m in members if m["name"] == "TestClass")
    assert cls["type"] == "class"
    assert cls["doc"] == "Test class"

    # Test simple listing
    names = fs.ls("/", detail=False)
    assert set(names) == {"test_func", "TestClass"}


@pytest.mark.skipif(
    os.environ.get("CI") == "true", reason="TODO: can only load source locally"
)
def test_get_module_source(example_py: Path) -> None:
    """Test getting module source code."""
    fs = fsspec.filesystem("mod", fo=str(example_py))

    # Get whole module
    source = fs.cat().decode()
    assert "def test_func():" in source
    assert "class TestClass:" in source

    # Get specific function
    func_source = fs.cat("test_func").decode()
    assert "def test_func():" in func_source
    assert "'''Test function'''" in func_source
    assert "class TestClass:" not in func_source

    # Get specific class
    class_source = fs.cat("TestClass").decode()
    assert "class TestClass:" in class_source
    assert "'''Test class'''" in class_source
    assert "def test_func():" not in class_source


def test_member_not_found(example_py: Path) -> None:
    """Test error when requesting non-existent member."""
    fs = fsspec.filesystem("mod", fo=str(example_py))

    with pytest.raises(FileNotFoundError, match="Member non_existent not found"):
        fs.cat("non_existent")


def test_file_not_found() -> None:
    """Test error when file doesn't exist."""
    fs = fsspec.filesystem("mod", fo="non_existent.py")

    with pytest.raises(FileNotFoundError):
        fs.ls("/")


def test_chained_access(example_py: Path) -> None:
    """Test chaining with file protocol."""
    url = f"mod::file://{example_py}"

    with fsspec.open(url, mode="rb") as f:
        source = f.read().decode()  # type: ignore

    assert "def test_func():" in source
    assert "class TestClass:" in source


def test_path_with_without_py(example_py: Path) -> None:
    """Test that paths with and without .py work."""
    fs1 = fsspec.filesystem("mod", fo=str(example_py))
    fs2 = fsspec.filesystem("mod", fo=str(example_py.with_suffix("")))

    assert fs1.ls("/", detail=False) == fs2.ls("/", detail=False)
    assert fs1.cat().decode() == fs2.cat().decode()


if __name__ == "__main__":
    pytest.main(["-vv", __file__])
