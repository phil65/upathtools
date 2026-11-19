"""Tests for the Microsandbox filesystem against a real microVM.

Requires the microsandbox runtime and access to /dev/kvm (Linux) or
Hypervisor.framework (macOS); skipped otherwise.
"""

from __future__ import annotations

import os
import sys
import time
from typing import TYPE_CHECKING

import pytest

from upathtools.filesystems import MicrosandboxFS


pytest.importorskip("microsandbox")

from microsandbox import is_runtime_installed


if TYPE_CHECKING:
    from collections.abc import AsyncIterator
    from pathlib import Path


def _vm_available() -> bool:
    if sys.platform == "linux" and not os.access("/dev/kvm", os.R_OK | os.W_OK):
        return False
    return sys.platform in ("linux", "darwin") and is_runtime_installed()


pytestmark = pytest.mark.skipif(not _vm_available(), reason="microsandbox runtime / KVM missing")

BINARY = bytes(range(256)) + b"'\"$(rm -rf /)`\n"


@pytest.fixture(scope="session")
async def msb_fs() -> AsyncIterator[MicrosandboxFS]:
    fs = MicrosandboxFS()
    await fs.set_session()
    yield fs
    await fs.close_session()


async def test_binary_roundtrip_and_metadata(msb_fs: MicrosandboxFS) -> None:
    path = "/tmp/upt/nested/data.bin"
    await msb_fs._pipe_file(path, BINARY)
    assert await msb_fs._cat_file(path) == BINARY
    assert await msb_fs._cat_file(path, start=2, end=5) == BINARY[2:5]
    assert await msb_fs._exists(path)
    assert await msb_fs._isfile(path)
    assert not await msb_fs._isdir(path)
    assert await msb_fs._size(path) == len(BINARY)
    modified = await msb_fs._modified(path)
    # Seconds, not milliseconds.
    assert abs(modified - time.time()) < 24 * 3600
    info = await msb_fs._info(path)
    assert info["type"] == "file"
    assert info["size"] == len(BINARY)


async def test_listing(msb_fs: MicrosandboxFS) -> None:
    await msb_fs._pipe_file("/tmp/upt-ls/a.txt", b"a")
    await msb_fs._mkdir("/tmp/upt-ls/sub")
    entries = {entry["name"]: entry for entry in await msb_fs._ls("/tmp/upt-ls")}
    assert entries["/tmp/upt-ls/a.txt"]["type"] == "file"
    assert entries["/tmp/upt-ls/sub"]["type"] == "directory"
    assert sorted(await msb_fs._ls("/tmp/upt-ls", detail=False)) == [
        "/tmp/upt-ls/a.txt",
        "/tmp/upt-ls/sub",
    ]


async def test_errors_map_to_python_exceptions(msb_fs: MicrosandboxFS) -> None:
    with pytest.raises(FileNotFoundError):
        await msb_fs._cat_file("/tmp/does-not-exist")
    with pytest.raises(FileNotFoundError):
        await msb_fs._info("/tmp/does-not-exist")
    assert not await msb_fs._exists("/tmp/does-not-exist")
    await msb_fs._mkdir("/tmp/upt-err")
    with pytest.raises(IsADirectoryError):
        await msb_fs._rm_file("/tmp/upt-err")
    await msb_fs._pipe_file("/tmp/upt-err/f", b"x")
    with pytest.raises(FileExistsError):
        await msb_fs._pipe_file("/tmp/upt-err/f", b"y", mode="create")


async def test_rmdir_only_removes_empty_directories(msb_fs: MicrosandboxFS) -> None:
    await msb_fs._pipe_file("/tmp/upt-rmdir/keep.txt", b"keep")
    with pytest.raises(OSError, match="not empty"):
        await msb_fs._rmdir("/tmp/upt-rmdir")
    assert await msb_fs._cat_file("/tmp/upt-rmdir/keep.txt") == b"keep"
    await msb_fs._rm_file("/tmp/upt-rmdir/keep.txt")
    await msb_fs._rmdir("/tmp/upt-rmdir")
    assert not await msb_fs._exists("/tmp/upt-rmdir")


async def test_mkdir_without_parents(msb_fs: MicrosandboxFS) -> None:
    with pytest.raises(FileNotFoundError):
        await msb_fs._mkdir("/tmp/upt-missing-parent/child", create_parents=False)


async def test_move_and_copy(msb_fs: MicrosandboxFS) -> None:
    await msb_fs._pipe_file("/tmp/upt-mv/src.txt", b"content")
    await msb_fs._cp_file("/tmp/upt-mv/src.txt", "/tmp/upt-mv/copy.txt")
    await msb_fs._mv("/tmp/upt-mv/src.txt", "/tmp/upt-mv/moved.txt")
    assert not await msb_fs._exists("/tmp/upt-mv/src.txt")
    assert await msb_fs._cat_file("/tmp/upt-mv/moved.txt") == b"content"
    assert await msb_fs._cat_file("/tmp/upt-mv/copy.txt") == b"content"


async def test_host_transfer(msb_fs: MicrosandboxFS, tmp_path: Path) -> None:
    local = tmp_path / "upload.bin"
    local.write_bytes(BINARY)
    await msb_fs._put_file(str(local), "/tmp/upt-host/upload.bin")
    assert await msb_fs._cat_file("/tmp/upt-host/upload.bin") == BINARY
    target = tmp_path / "download.bin"
    await msb_fs._get_file("/tmp/upt-host/upload.bin", str(target))
    assert target.read_bytes() == BINARY


async def test_existing_sandbox_is_used_and_not_stopped() -> None:
    import uuid

    from microsandbox import Sandbox

    sandbox = await Sandbox.create(
        f"upt-existing-{uuid.uuid4().hex[:8]}", image="python:3.13-slim", ephemeral=True
    )
    try:
        fs = MicrosandboxFS(sandbox=sandbox)
        await fs._pipe_file("/tmp/marker", b"same vm")
        assert await sandbox.fs.read("/tmp/marker") == b"same vm"
        await fs.close_session()
        # Not owned by the filesystem, so still usable.
        assert await sandbox.fs.exists("/tmp/marker")
    finally:
        await sandbox.stop()
