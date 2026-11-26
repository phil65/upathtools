"""Tests for :class:`upathtools.fs.DirFS`."""

from typing import TYPE_CHECKING, Any

import pytest

from tests.fs.contract import AsyncFSContract
from upathtools.fs import (
    FSSPEC_SYNC_VERSION,
    AsyncFS,
    DirFS,
    FileInfo,
    FilesystemClosedError,
    LocalFS,
    LocalFSInfo,
    MemoryFS,
    PathEscapeError,
    UnsupportedOperation,
)
from upathtools.fs.implementations import dirfs as dirfs_mod


if TYPE_CHECKING:
    from collections.abc import AsyncIterator
    from pathlib import Path


class _ReadOnlyMemFS(AsyncFS[FileInfo]):
    """Minimal read-only AsyncFS to verify DirFS capability mirroring."""

    protocol = "romem"

    def __init__(self, inner: MemoryFS) -> None:
        super().__init__()
        self._inner = inner

    async def info(self, path: str) -> FileInfo:
        return await self._inner.info(path)

    async def ls(self, path: str) -> list[FileInfo]:
        return await self._inner.ls(path)

    async def read_bytes(
        self,
        path: str,
        *,
        start: int | None = None,
        end: int | None = None,
    ) -> bytes:
        return await self._inner.read_bytes(path, start=start, end=end)


class TestDirFSMemoryContract(AsyncFSContract):
    """Run the full AsyncFS behavioral contract against ``DirFS(MemoryFS(), '/sub')``."""

    @pytest.fixture
    async def fs(self) -> AsyncIterator[DirFS[FileInfo]]:
        mem = MemoryFS()
        await mem.mkdir("/sub", exist_ok=True)
        dfs = DirFS(mem, "/sub", close_wrapped=True)
        yield dfs
        await dfs.aclose()

    @pytest.fixture
    async def root(self, fs: DirFS[FileInfo]) -> str:
        await fs.mkdir("/work", exist_ok=True)
        return "/work"


class TestDirFSLocalContract(AsyncFSContract):
    """Run the full AsyncFS behavioral contract against ``DirFS(LocalFS(), tmp_path)``."""

    @pytest.fixture
    async def fs(self, tmp_path: Path) -> AsyncIterator[DirFS[LocalFSInfo]]:
        local = LocalFS()
        jail = tmp_path / "jail"
        jail.mkdir()
        dfs = DirFS(local, jail, close_wrapped=True)
        yield dfs
        await dfs.aclose()

    @pytest.fixture
    async def root(self, fs: DirFS[LocalFSInfo]) -> str:
        await fs.mkdir("/work", exist_ok=True)
        return "/work"


@pytest.mark.parametrize(
    "escape_path",
    [
        "..",
        "/..",
        "/../secret.txt",
        "../secret.txt",
        "a/../../secret.txt",
        "/a/b/../../../secret.txt",
        "dir://../secret.txt",
        "dir:///../secret.txt",
        "//..//secret.txt",
        "./../secret.txt",
        "/./a/../../secret.txt",
    ],
)
async def test_dirfs_escape_attempts_raise_before_io_and_touch_nothing(
    tmp_path: Path,
    escape_path: str,
) -> None:
    # Test against both MemoryFS and LocalFS with a sibling secret outside root.
    mem = MemoryFS()
    await mem.write_bytes("/secret.txt", b"top-secret")
    await mem.mkdir("/jail")
    mem_dfs = DirFS(mem, "/jail")

    local = LocalFS()
    secret_local = tmp_path / "secret.txt"
    secret_local.write_bytes(b"top-secret")
    jail_local = tmp_path / "jail"
    jail_local.mkdir()
    local_dfs = DirFS(local, jail_local)

    for dfs in (mem_dfs, local_dfs):
        with pytest.raises(PathEscapeError):
            await dfs.info(escape_path)
        with pytest.raises(PathEscapeError):
            await dfs.ls(escape_path)
        with pytest.raises(PathEscapeError):
            await dfs.read_bytes(escape_path)
        with pytest.raises(PathEscapeError):
            await dfs.write_bytes(escape_path, b"hacked")
        with pytest.raises(PathEscapeError):
            await dfs.mkdir(escape_path)
        with pytest.raises(PathEscapeError):
            await dfs.rmdir(escape_path)
        with pytest.raises(PathEscapeError):
            await dfs.remove_file(escape_path)
        with pytest.raises(PathEscapeError):
            await dfs.move(escape_path, "/dst.txt")
        with pytest.raises(PathEscapeError):
            await dfs.move("/src.txt", escape_path)
        with pytest.raises(PathEscapeError):
            await dfs.find(escape_path)
        with pytest.raises(PathEscapeError):
            await dfs.glob(escape_path)
        with pytest.raises(PathEscapeError):
            await dfs.grep(escape_path, "top")

    # Wrapped filesystems were never touched outside the jail.
    assert await mem.read_bytes("/secret.txt") == b"top-secret"
    assert secret_local.read_bytes() == b"top-secret"
    assert await mem.ls("/jail") == []
    assert list(jail_local.iterdir()) == []


async def test_dirfs_preserves_tinfo_subclass_and_rewrites_paths(tmp_path: Path) -> None:
    local = LocalFS()
    jail = tmp_path / "jail"
    jail.mkdir()
    dfs = DirFS(local, jail)

    await dfs.write_bytes("/a/b.txt", b"hello")
    (jail / "link.txt").symlink_to(jail / "a" / "b.txt")

    root_info = await dfs.info("/")
    assert isinstance(root_info, LocalFSInfo)
    assert root_info.path == "/"
    assert root_info.is_dir
    assert root_info.ino is not None

    file_info = await dfs.info("/a/b.txt")
    assert isinstance(file_info, LocalFSInfo)
    assert file_info.path == "/a/b.txt"
    assert file_info.uid is not None
    assert file_info.gid is not None
    assert file_info.ino is not None
    assert file_info.nlink is not None

    # Relative and absolute paths map identically under DirFS root.
    assert await dfs.info("a/b.txt") == file_info
    assert await dfs.info("dir:///a/b.txt") == file_info

    listed = await dfs.ls("/")
    assert all(isinstance(e, LocalFSInfo) for e in listed)
    assert [e.path for e in listed] == ["/a", "/link.txt"]

    found = await dfs.find("/", withdirs=True)
    assert all(isinstance(e, LocalFSInfo) for e in found)
    assert [e.path for e in found] == ["/", "/a", "/a/b.txt", "/link.txt"]

    globbed = await dfs.glob("/**/*.txt")
    assert all(isinstance(e, LocalFSInfo) for e in globbed)
    assert [e.path for e in globbed] == ["/a/b.txt", "/link.txt"]


async def test_dirfs_delegates_native_find_grep_and_move_to_wrapped_local(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    local = LocalFS()
    jail = tmp_path / "jail"
    jail.mkdir()
    dfs = DirFS(local, jail)

    await dfs.write_text("/sub/hello.txt", "hello world\n")

    calls: list[tuple[str, str]] = []
    real_find = local.find
    real_grep = local.grep
    real_move = local.move

    async def spy_find(path: str, **kwargs: Any) -> list[LocalFSInfo]:
        calls.append(("find", path))
        return await real_find(path, **kwargs)

    async def spy_grep(path: str, pattern: str, **kwargs: Any) -> list[Any]:
        calls.append(("grep", path))
        return await real_grep(path, pattern, **kwargs)

    async def spy_move(src: str, dst: str, **kwargs: Any) -> None:
        calls.append(("move", f"{src}->{dst}"))
        await real_move(src, dst, **kwargs)

    monkeypatch.setattr(local, "find", spy_find)
    monkeypatch.setattr(local, "grep", spy_grep)
    monkeypatch.setattr(local, "move", spy_move)

    found = await dfs.find("/")
    assert [e.path for e in found] == ["/sub/hello.txt"]
    assert ("find", jail.as_posix()) in calls

    matches = await dfs.grep("/", "hello")
    assert len(matches) == 1
    assert matches[0].path == "/sub/hello.txt"
    assert ("grep", jail.as_posix()) in calls

    await dfs.move("/sub/hello.txt", "/sub/moved.txt")
    assert await dfs.read_text("/sub/moved.txt") == "hello world\n"
    assert (
        "move",
        f"{(jail / 'sub' / 'hello.txt').as_posix()}->{(jail / 'sub' / 'moved.txt').as_posix()}",
    ) in calls


async def test_dirfs_nested_memory_and_local(tmp_path: Path) -> None:
    # Nested over MemoryFS
    mem = MemoryFS()
    await mem.write_bytes("/outer/sibling.txt", b"outer-only")
    inner_mem = DirFS(mem, "/outer")
    nested_mem = DirFS(inner_mem, "/inner")

    await nested_mem.write_bytes("/deep/file.txt", b"nested-mem")
    assert await mem.read_bytes("/outer/inner/deep/file.txt") == b"nested-mem"
    assert (await nested_mem.info("/deep/file.txt")).path == "/deep/file.txt"
    assert await nested_mem.find_paths("/") == ["/deep/file.txt"]
    assert [m.path for m in await nested_mem.grep("/", "nested")] == ["/deep/file.txt"]

    with pytest.raises(PathEscapeError):
        await nested_mem.read_bytes("../sibling.txt")

    # Nested over LocalFS
    local = LocalFS()
    outer_dir = tmp_path / "outer"
    outer_dir.mkdir()
    (outer_dir / "sibling.txt").write_bytes(b"outer-only")
    inner_local = DirFS(local, outer_dir)
    nested_local = DirFS(inner_local, "/inner")

    await nested_local.write_bytes("/a/b.txt", b"nested-local")
    info = await nested_local.info("/a/b.txt")
    assert isinstance(info, LocalFSInfo)
    assert info.path == "/a/b.txt"
    assert (outer_dir / "inner" / "a" / "b.txt").read_bytes() == b"nested-local"
    assert [m.path for m in await nested_local.grep("/", "nested")] == ["/a/b.txt"]

    with pytest.raises(PathEscapeError):
        await nested_local.read_bytes("../sibling.txt")


async def test_dirfs_lazy_root_capabilities_and_close_wrapped() -> None:
    mem = MemoryFS()
    # Root does not exist yet at construction time.
    dfs = DirFS(mem, "/not_created_yet")
    assert dfs.root == "/not_created_yet"
    assert dfs.fs is mem
    assert dfs.close_wrapped is False
    assert not hasattr(dfs, "asynchronous")
    assert dfs.capabilities == mem.capabilities
    assert dirfs_mod.__fsspec_sync__.version == FSSPEC_SYNC_VERSION

    with pytest.raises(FileNotFoundError):
        await dfs.ls("/")

    # Writing lazily creates the root via the wrapped fs.
    await dfs.write_bytes("/hello.txt", b"lazy")
    assert await mem.read_bytes("/not_created_yet/hello.txt") == b"lazy"

    # Cannot rmdir or move the DirFS root itself.
    with pytest.raises(ValueError, match="root"):
        await dfs.rmdir("/")
    with pytest.raises(ValueError, match="root"):
        await dfs.move("/", "/elsewhere")

    # Default close_wrapped=False leaves wrapped fs open.
    await dfs.aclose()
    assert dfs.closed
    assert not mem.closed
    with pytest.raises(FilesystemClosedError):
        await dfs.info("/hello.txt")
    assert await mem.read_bytes("/not_created_yet/hello.txt") == b"lazy"

    # close_wrapped=True closes wrapped fs on aclose().
    dfs_owning = DirFS(mem, "/not_created_yet", close_wrapped=True)
    await dfs_owning.aclose()
    assert dfs_owning.closed
    assert mem.closed

    # Read-only wrapped fs mirrors readonly and empty write capabilities.
    ro_mem = MemoryFS()
    await ro_mem.write_bytes("/sub/item.txt", b"ro")
    ro_dfs = DirFS(_ReadOnlyMemFS(ro_mem), "/sub")
    assert ro_dfs.readonly is True
    assert ro_dfs.capabilities == frozenset()
    assert await ro_dfs.read_bytes("/item.txt") == b"ro"
    with pytest.raises(UnsupportedOperation):
        await ro_dfs.write_bytes("/new.txt", b"x")
