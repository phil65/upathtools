"""Tests for PackageFS."""

from __future__ import annotations

import sys
from typing import TYPE_CHECKING

import fsspec
import pytest


if TYPE_CHECKING:
    from pathlib import Path


@pytest.fixture
def example_package(tmp_path: Path) -> str:
    """Create a temporary package with example content."""
    pkg_dir = tmp_path / "example_pkg"
    pkg_dir.mkdir()

    # Create __init__.py
    init_file = pkg_dir / "__init__.py"
    init_file.write_text("""
\"\"\"Example package.\"\"\"
from .core import example_func
__version__ = "0.1.0"
    """)

    # Create core.py
    core_file = pkg_dir / "core.py"
    core_file.write_text("""
\"\"\"Core functionality.\"\"\"
def example_func():
    \"\"\"Example function.\"\"\"
    return "Hello"

class ExampleClass:
    \"\"\"Example class.\"\"\"
    def method(self):
        return "World"
    """)

    # Create subpackage
    sub_pkg = pkg_dir / "subpkg"
    sub_pkg.mkdir()
    (sub_pkg / "__init__.py").write_text('"""Subpackage."""')

    sys.path.insert(0, str(tmp_path))

    return "example_pkg"


def test_package_listing(example_package: str) -> None:
    """Test listing package contents."""
    fs = fsspec.filesystem("pkg", package=example_package)

    # Test detailed listing
    contents = fs.ls("/", detail=True)
    assert len(contents) == 2  # core.py and subpkg  # noqa: PLR2004

    # Verify core module
    core = next(m for m in contents if m["name"] == "core")
    assert core["type"] == "module"
    assert "Core functionality" in (core["doc"] or "")

    # Verify subpackage
    subpkg = next(m for m in contents if m["name"] == "subpkg")
    assert subpkg["type"] == "package"
    assert "Subpackage" in (subpkg["doc"] or "")

    # Test simple listing
    names = fs.ls("/", detail=False)
    assert set(names) == {"core", "subpkg"}


def test_module_source(example_package: str) -> None:
    """Test reading module source code."""
    fs = fsspec.filesystem("pkg", package=example_package)

    # Read core module
    source = fs.cat("core").decode()
    assert "def example_func():" in source
    assert "class ExampleClass:" in source


def test_module_not_found(example_package: str) -> None:
    """Test error when requesting non-existent module."""
    fs = fsspec.filesystem("pkg", package=example_package)

    with pytest.raises(FileNotFoundError):
        fs.cat("nonexistent")


def test_chained_ast_access(example_package: str, tmp_path: Path) -> None:
    """Test chaining PackageFS with PythonAstFS."""
    # First get the module path through PackageFS
    pkg_fs = fsspec.filesystem("pkg", package=example_package)
    module_content = pkg_fs.cat("core")

    # Write content to temporary file
    temp_file = tmp_path / "temp_module.py"
    temp_file.write_bytes(module_content)

    # Then analyze it with PythonAstFS
    ast_fs = fsspec.filesystem("ast", fo=str(temp_file))

    # Check if we can list the members
    members = ast_fs.ls("/", detail=True)
    assert len(members) == 2  # example_func and ExampleClass  # noqa: PLR2004

    # Verify function details
    func = next(m for m in members if m["name"] == "example_func")
    assert func["type"] == "function"
    assert "Example function" in (func["doc"] or "")

    # Verify class details
    cls = next(m for m in members if m["name"] == "ExampleClass")
    assert cls["type"] == "class"
    assert "Example class" in (cls["doc"] or "")

    # Get function source
    func_source = ast_fs.cat("example_func").decode()
    assert "def example_func():" in func_source
    assert 'return "Hello"' in func_source


def test_package_validation() -> None:
    """Test package name validation."""
    with pytest.raises(ValueError, match="Package name required"):
        fsspec.filesystem("pkg", package="")


def test_info_retrieval(example_package: str) -> None:
    """Test getting info about package components."""
    fs = fsspec.filesystem("pkg", package=example_package)

    # Get info about core module
    core_info = fs.info("core")
    assert core_info["name"] == "core"
    assert core_info["type"] == "module"
    assert "Core functionality" in (core_info["doc"] or "")

    # Get info about subpackage
    subpkg_info = fs.info("subpkg")
    assert subpkg_info["name"] == "subpkg"
    assert subpkg_info["type"] == "package"
    assert "Subpackage" in (subpkg_info["doc"] or "")


if __name__ == "__main__":
    pytest.main(["-v", __file__])
