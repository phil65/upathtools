"""Unit tests for :class:`upathtools.fs.AsyncFS` base class and stream edge cases."""

import asyncio
import errno
from typing import TYPE_CHECKING, ClassVar

import pytest

from upathtools.fs import (
    FSSPEC_SYNC_VERSION,
    AsyncFS,
    FileInfo,
    FilesystemClosedError,
    FSTimeoutError,
    MemoryFS,
    PathEscapeError,
    UnsupportedOperation,
    base as base_mod,
)


if TYPE_CHECKING:
    from pathlib import Path

    from upathtools.fs import WriteMode


class _DictReadOnlyFS(AsyncFS[FileInfo]):
    """Minimal read-only AsyncFS implementing ONLY info, ls, and read_bytes."""

    protocol: ClassVar[str | tuple[str, ...]] = "dictro"

    def __init__(self, files: dict[str, bytes], **kwargs: object) -> None:
        super().__init__(**kwargs)  # type: ignore[arg-type]
        self._files = {self._norm(k): v for k, v in files.items()}
        self._dirs: set[str] = {"/"}
        for path in self._files:
            parts = path.strip("/").split("/")
            cur = ""
            for seg in parts[:-1]:
                cur = f"{cur}/{seg}"
                self._dirs.add(cur)
        self.read_calls: list[tuple[str, int | None, int | None]] = []

    async def info(self, path: str) -> FileInfo:
        self._check_open()
        norm = self._norm(path)
        if norm in self._files:
            return FileInfo(path=norm, type="file", size=len(self._files[norm]))
        if norm in self._dirs:
            return FileInfo(path=norm, type="directory", size=0)
        raise FileNotFoundError(errno.ENOENT, "No such file", norm)

    async def ls(self, path: str) -> list[FileInfo]:
        self._check_open()
        norm = self._norm(path)
        if norm in self._files:
            raise NotADirectoryError(errno.ENOTDIR, "Not a directory", norm)
        if norm not in self._dirs:
            raise FileNotFoundError(errno.ENOENT, "No such directory", norm)
        prefix = "/" if norm == "/" else f"{norm}/"
        plen = len(prefix)
        out: list[FileInfo] = [
            FileInfo(path=f, type="file", size=len(data))
            for f, data in self._files.items()
            if f.startswith(prefix) and "/" not in f[plen:]
        ]
        out.extend(
            FileInfo(path=d, type="directory", size=0)
            for d in self._dirs
            if d != norm and d.startswith(prefix) and "/" not in d[plen:]
        )
        out.sort(key=lambda e: e.path)
        return out

    async def read_bytes(
        self,
        path: str,
        *,
        start: int | None = None,
        end: int | None = None,
    ) -> bytes:
        self._check_open()
        norm = self._norm(path)
        self.read_calls.append((norm, start, end))
        if norm in self._dirs:
            raise IsADirectoryError(errno.EISDIR, "Is a directory", norm)
        if norm not in self._files:
            raise FileNotFoundError(errno.ENOENT, "No such file", norm)
        return self._files[norm][start:end]


async def test_minimal_readonly_fs_derived_ops_and_capabilities(tmp_path: Path) -> None:
    fs = _DictReadOnlyFS({
        "/a.txt": b"alpha\nBeta\n",
        "/sub/b.txt": b"alpha\nGamma\n",
        "/sub/bin.dat": b"\x00\x01alpha",
    })

    assert fs.capabilities == frozenset()
    assert fs.readonly is True

    # Derived read operations all work from info/ls/read_bytes alone
    assert await fs.ls_names("/") == ["/a.txt", "/sub"]
    assert await fs.exists("/a.txt")
    assert await fs.is_file("/a.txt")
    assert await fs.is_dir("/sub")
    assert not await fs.exists("/missing")
    assert await fs.size("/a.txt") == len(b"alpha\nBeta\n")
    assert await fs.read_text("/a.txt") == "alpha\nBeta\n"
    assert await fs.read_many(["/a.txt", "/sub/b.txt"]) == {
        "/a.txt": b"alpha\nBeta\n",
        "/sub/b.txt": b"alpha\nGamma\n",
    }
    assert await fs.find_paths("/") == ["/a.txt", "/sub/b.txt", "/sub/bin.dat"]
    assert await fs.glob_paths("/**/*.txt") == ["/a.txt", "/sub/b.txt"]
    assert await fs.du("/") == {"/a.txt": 11, "/sub/b.txt": 12, "/sub/bin.dat": 7}

    walked = [dp async for dp, _, _ in fs.walk("/")]
    assert walked == ["/", "/sub"]

    matches = await fs.grep("/", "alpha")
    assert [m.path for m in matches] == ["/a.txt", "/sub/b.txt"]

    diff_text = await fs.diff("/a.txt", "/sub/b.txt")
    assert "-Beta" in diff_text
    assert "+Gamma" in diff_text

    # Streaming reader uses block_size chunks
    fs.read_calls.clear()
    async with fs.open("/a.txt", "rb", block_size=4) as reader:
        data = await reader.read()
    assert data == b"alpha\nBeta\n"
    assert fs.read_calls == [("/a.txt", 0, 4), ("/a.txt", 4, 8), ("/a.txt", 8, 11)]

    # get_file works on read-only FS
    dst = tmp_path / "out.txt"
    await fs.get_file("/a.txt", dst)
    assert dst.read_bytes() == b"alpha\nBeta\n"

    # All mutations raise UnsupportedOperation
    with pytest.raises(UnsupportedOperation):
        await fs.write_bytes("/new.txt", b"x")
    with pytest.raises(UnsupportedOperation):
        await fs.write_text("/new.txt", "x")
    with pytest.raises(UnsupportedOperation):
        await fs.mkdir("/newdir")
    with pytest.raises(UnsupportedOperation):
        await fs.rmdir("/sub")
    with pytest.raises(UnsupportedOperation):
        await fs.remove_file("/a.txt")
    with pytest.raises(UnsupportedOperation):
        await fs.remove("/a.txt")
    with pytest.raises(UnsupportedOperation):
        await fs.touch("/a.txt", truncate=False)
    with pytest.raises(UnsupportedOperation):
        await fs.copy_file("/a.txt", "/c.txt")
    with pytest.raises(UnsupportedOperation):
        await fs.copy("/a.txt", "/c.txt")
    with pytest.raises(UnsupportedOperation):
        await fs.move("/a.txt", "/c.txt")
    with pytest.raises(UnsupportedOperation):
        await fs.put_file(dst, "/up.txt")
    with pytest.raises(UnsupportedOperation):
        async with fs.open("/new.txt", "wb") as writer:
            await writer.write(b"x")


async def test_path_helpers_cache_and_lifecycle() -> None:
    assert _DictReadOnlyFS.strip_protocol("dictro://a/b/") == "/a/b"
    assert _DictReadOnlyFS.strip_protocol("a/b/../c") == "/a/c"
    assert _DictReadOnlyFS.strip_protocol("") == "/"
    assert _DictReadOnlyFS.parent("/a/b") == "/a"
    assert _DictReadOnlyFS.parent("/a") == "/"
    assert _DictReadOnlyFS.parent("/") == "/"
    assert _DictReadOnlyFS.kwargs_from_url("dictro://a/b") == {}

    with pytest.raises(PathEscapeError):
        _DictReadOnlyFS.strip_protocol("/../escape")

    with pytest.raises(ValueError, match="batch_size"):
        _DictReadOnlyFS({}, batch_size=0)

    fs = _DictReadOnlyFS({}, use_listings_cache=True, listings_expiry_time=60.0, max_cached_paths=4)
    assert fs.dircache is not None
    fs.dircache.set("/a", [FileInfo(path="/a/x", type="file", size=1)])
    assert "/a" in fs.dircache
    fs.invalidate_cache("dictro://a/")
    assert "/a" not in fs.dircache

    async with fs as entered:
        assert entered is fs
        assert not fs.closed
    assert fs.closed

    with pytest.raises(FilesystemClosedError):
        async with fs:
            pass  # pragma: no cover

    assert base_mod.__fsspec_sync__.version == FSSPEC_SYNC_VERSION


@pytest.mark.parametrize("error_cls", [PermissionError, FSTimeoutError])
async def test_exists_is_file_is_dir_propagate_non_missing_errors(
    error_cls: type[Exception],
) -> None:
    class _FailingInfoFS(AsyncFS[FileInfo]):
        async def info(self, path: str) -> FileInfo:
            raise error_cls(f"boom on {path}")

        async def ls(self, path: str) -> list[FileInfo]:
            raise error_cls(f"boom on {path}")

        async def read_bytes(
            self,
            path: str,
            *,
            start: int | None = None,
            end: int | None = None,
        ) -> bytes:
            raise error_cls(f"boom on {path}")

    fs = _FailingInfoFS()
    with pytest.raises(error_cls):
        await fs.exists("/secret")
    with pytest.raises(error_cls):
        await fs.is_file("/secret")
    with pytest.raises(error_cls):
        await fs.is_dir("/secret")
    with pytest.raises(error_cls):
        await fs.find("/secret")
    with pytest.raises(error_cls):
        await fs.glob("/secret")


async def test_cancellation_of_find_and_copy_cancels_child_tasks() -> None:
    class _SlowFS(MemoryFS):
        def __init__(self) -> None:
            super().__init__()
            self.active_ls = 0
            self.active_reads = 0
            self.ls_started = asyncio.Event()
            self.read_started = asyncio.Event()
            self.cancelled_ls = 0
            self.cancelled_reads = 0

        async def ls(self, path: str) -> list[FileInfo]:
            norm = self._norm(path)
            if norm != "/":
                self.active_ls += 1
                self.ls_started.set()
                try:
                    await asyncio.sleep(10)
                except asyncio.CancelledError:
                    self.cancelled_ls += 1
                    raise
                finally:
                    self.active_ls -= 1
            return await super().ls(path)

        async def read_bytes(
            self,
            path: str,
            *,
            start: int | None = None,
            end: int | None = None,
        ) -> bytes:
            self.active_reads += 1
            self.read_started.set()
            try:
                await asyncio.sleep(10)
            except asyncio.CancelledError:
                self.cancelled_reads += 1
                raise
            finally:
                self.active_reads -= 1
            return await super().read_bytes(path, start=start, end=end)

    fs = _SlowFS()
    for i in range(4):
        await fs.write_bytes(f"/dir_{i}/file.txt", b"payload")

    # 1) Cancel find() while sibling ls() tasks are running
    baseline_tasks = set(asyncio.all_tasks())
    find_task = asyncio.create_task(fs.find("/"))
    await fs.ls_started.wait()
    await asyncio.sleep(0)
    find_task.cancel()
    with pytest.raises(asyncio.CancelledError):
        await find_task
    await asyncio.sleep(0)
    assert fs.active_ls == 0
    assert fs.cancelled_ls >= 1
    assert set(asyncio.all_tasks()) <= baseline_tasks

    # 2) Cancel copy() while concurrent read_bytes() tasks are running
    await fs.write_bytes("/src/a.txt", b"A")
    await fs.write_bytes("/src/b.txt", b"B")
    # Use a helper subclass where ls is fast and read_bytes is slow for copy()
    copy_fs = _SlowFS()
    await copy_fs.write_bytes("/f1.txt", b"1")
    await copy_fs.write_bytes("/f2.txt", b"2")

    # Make ls fast for "/" and slow only on read_bytes
    async def fast_ls(path: str) -> list[FileInfo]:
        return await MemoryFS.ls(copy_fs, path)

    copy_fs.ls = fast_ls  # type: ignore[method-assign]
    await copy_fs.write_bytes("/tree/a.txt", b"A")
    await copy_fs.write_bytes("/tree/b.txt", b"B")

    baseline_tasks = set(asyncio.all_tasks())
    copy_task = asyncio.create_task(copy_fs.copy("/tree", "/tree_copy"))
    await copy_fs.read_started.wait()
    await asyncio.sleep(0)
    copy_task.cancel()
    with pytest.raises(asyncio.CancelledError):
        await copy_task
    await asyncio.sleep(0)
    assert copy_fs.active_reads == 0
    assert copy_fs.cancelled_reads >= 1
    assert set(asyncio.all_tasks()) <= baseline_tasks


async def test_default_base_move_leaves_source_intact_on_mid_copy_failure() -> None:
    """Test AsyncFS.move (not MemoryFS's native move) when copy fails mid-flight."""

    class _BaseMoveFS(AsyncFS[FileInfo]):
        def __init__(self, fail_on: str | None = None) -> None:
            super().__init__()
            self._inner = MemoryFS()
            self._fail_on = fail_on

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

        async def write_bytes(
            self,
            path: str,
            data: bytes | bytearray | memoryview,
            *,
            mode: WriteMode = "overwrite",
        ) -> None:
            if self._fail_on is not None and self._norm(path) == self._fail_on:
                raise OSError("injected write failure")
            await self._inner.write_bytes(path, data, mode=mode)

        async def mkdir(self, path: str, *, parents: bool = True, exist_ok: bool = False) -> None:
            await self._inner.mkdir(path, parents=parents, exist_ok=exist_ok)

        async def rmdir(self, path: str) -> None:
            await self._inner.rmdir(path)

        async def remove_file(self, path: str) -> None:
            await self._inner.remove_file(path)

    fs = _BaseMoveFS(fail_on="/dst/b.txt")
    assert "move" not in fs.capabilities
    assert {"write_bytes", "mkdir", "rmdir", "remove_file"} == fs.capabilities

    await fs.write_bytes("/src/a.txt", b"A")
    await fs.write_bytes("/src/b.txt", b"B")

    with pytest.raises(OSError, match="injected write failure"):
        await fs.move("/src", "/dst")

    # Source tree must remain completely untouched when copy fails.
    assert await fs.read_bytes("/src/a.txt") == b"A"
    assert await fs.read_bytes("/src/b.txt") == b"B"

    # Once failure is cleared, default base move succeeds and removes source.
    fs._fail_on = None
    await fs.move("/src", "/dst", overwrite=True)
    assert not await fs.exists("/src")
    assert await fs.read_bytes("/dst/a.txt") == b"A"
    assert await fs.read_bytes("/dst/b.txt") == b"B"
