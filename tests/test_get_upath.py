"""Regression tests for UPath access to upathtools filesystems."""

from __future__ import annotations

import subprocess
import sys
import textwrap

from upathtools.filesystems import PackageFileSystem
from upathtools.filesystems.delegating_fs import DelegatingFileSystem


def test_get_upath_in_fresh_process():
    """get_upath must work before upath's registry has loaded any entry point."""
    code = textwrap.dedent("""
        from upathtools.filesystems import MarkdownFileSystem, PackageFileSystem

        path = PackageFileSystem("upathtools").get_upath("helpers")
        assert type(path).__name__ == "PackagePath", type(path)
        assert MarkdownFileSystem("README.md").get_upath("x").protocol == "md"
    """)
    result = subprocess.run(
        [sys.executable, "-W", "ignore", "-c", code], capture_output=True, text=True, check=False
    )
    assert result.returncode == 0, result.stderr


def test_package_path_read_text():
    """Module files report their real size, so reading via open() is complete."""
    fs = PackageFileSystem("upathtools")
    data = fs.get_upath("helpers").read_bytes()
    assert data == fs.cat_file("helpers")
    assert fs.info("helpers")["size"] == len(data)


def test_delegating_fs_sync_access(tmp_path):
    """Sync access to async file filesystems must not call sync() inside the loop."""
    md_file = tmp_path / "doc.md"
    md_file.write_bytes(b"# Intro\nhello\n# Usage\nworld\n")
    md = md_file.as_posix()
    fs = DelegatingFileSystem(target_protocol="file")
    assert fs.cat_file(f"{md}::Intro") == b"# Intro\nhello"
    assert fs.ls(f"{md}::", detail=False) == [f"{md}::Intro", f"{md}::Usage"]
