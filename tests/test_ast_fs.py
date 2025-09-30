"""Tests for PythonAstFS."""

from __future__ import annotations

from typing import TYPE_CHECKING

import fsspec
import pytest


if TYPE_CHECKING:
    from pathlib import Path


@pytest.fixture
def example_py(tmp_path: Path) -> Path:
    """Create a temporary Python file with example content."""
    path = tmp_path / "example.py"
    path.write_text("""
def test_func():
    '''Test function'''
    pass

class TestClass:
    '''Test class'''
    pass
""")
    return path


def test_static_module_direct_file(example_py: Path) -> None:
    """Test direct file access."""
    fs = fsspec.filesystem("ast", fo=str(example_py))

    # Test listing
    members = fs.ls("/", detail=True)
    assert len(members) == 2  # noqa: PLR2004
    assert any(m["name"] == "test_func" and m["type"] == "function" for m in members)
    assert any(m["name"] == "TestClass" and m["type"] == "class" for m in members)

    # Test source extraction
    func_source = fs.cat("test_func").decode()
    assert "Test function" in func_source
    assert "pass" in func_source


def test_static_module_without_py_extension(example_py: Path) -> None:
    """Test access without .py extension."""
    fs = fsspec.filesystem("ast", fo=str(example_py.with_suffix("")))

    members = fs.ls("/", detail=False)
    assert len(members) == 2  # noqa: PLR2004
    assert "test_func" in members


def test_chained_access(example_py: Path) -> None:
    """Test chaining with local files."""
    # Let's first verify the file exists and has content
    assert example_py.exists()
    assert example_py.read_text()

    # Use forward slashes and normalize the path
    url = f"ast::file://{example_py.as_posix()}"
    print(f"Debug: URL = {url}")

    # Try with explicit filesystem first
    fs = fsspec.filesystem("ast", fo=str(example_py))
    content = fs.cat().decode()
    assert "test_func" in content

    # Then try the chained version
    with fsspec.open(url, mode="rb") as f:
        content = f.read().decode()  # type: ignore
    assert "test_func" in content


def test_member_not_found(example_py: Path) -> None:
    """Test error when requesting non-existent member."""
    fs = fsspec.filesystem("ast", fo=str(example_py))

    with pytest.raises(FileNotFoundError):
        fs.cat("non_existent")


def test_lazy_loading(example_py: Path) -> None:
    """Test that file is only loaded when needed."""
    fs = fsspec.filesystem("ast", fo=str(example_py))
    assert fs._source is None

    # Access triggers loading
    fs.ls("/")
    assert fs._source is not None


if __name__ == "__main__":
    pytest.main([__file__])
