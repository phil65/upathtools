"""Backend-independent behavioral contract suite for :class:`upathtools.fs.AsyncFS`.

Subclass :class:`AsyncFSContract` in a test module and provide the ``fs`` and ``root``
fixtures to verify a concrete writable :class:`AsyncFS` implementation.
"""

from abc import ABC, abstractmethod
import asyncio
import errno
import io
from typing import TYPE_CHECKING, Any

from fsspec.tests.abstract.common import GLOB_EDGE_CASES_TESTS
import pytest

from upathtools.fs import (
    AsyncBufferedReader,
    AsyncBufferedWriter,
    AsyncReadStream,
    AsyncSeekableStream,
    AsyncWriteStream,
    FileInfo,
    join_path,
)


if TYPE_CHECKING:
    from collections.abc import AsyncIterator
    from pathlib import Path

    from upathtools.fs import AsyncFS


_GLOB_NON_RECURSIVE_CASES = [
    (pat, maxdepth, expected)
    for pat, recursive, maxdepth, expected in GLOB_EDGE_CASES_TESTS["argvalues"]
    if not recursive
]


class AsyncFSContract(ABC):
    """Reusable pytest contract suite for writable :class:`AsyncFS` backends."""

    @abstractmethod
    @pytest.fixture
    async def fs(self) -> AsyncIterator[AsyncFS[Any]]:
        """Yield a fresh writable AsyncFS instance."""
        raise NotImplementedError  # pragma: no cover

    @abstractmethod
    @pytest.fixture
    async def root(self, fs: AsyncFS[Any]) -> str:
        """Return an existing working directory path inside ``fs``."""
        raise NotImplementedError  # pragma: no cover

    def _p(self, root: str, *parts: str) -> str:
        return join_path(root, *parts)

    # --- Basic read / write / info / ls / slicing ---

    async def test_write_read_info_ls_and_size(self, fs: AsyncFS[Any], root: str) -> None:
        file_a = self._p(root, "b.txt")
        file_b = self._p(root, "a.txt")
        sub = self._p(root, "sub")

        await fs.mkdir(sub)
        await fs.write_bytes(file_a, b"hello")
        await fs.write_text(file_b, "world!")

        info_a = await fs.info(file_a)
        assert isinstance(info_a, FileInfo)
        assert info_a.path == file_a
        assert info_a.type == "file"
        assert info_a.is_file
        assert not info_a.is_dir
        assert info_a.size == len(b"hello")
        assert await fs.size(file_a) == len(b"hello")

        info_sub = await fs.info(sub)
        assert info_sub.path == sub
        assert info_sub.type == "directory"
        assert info_sub.is_dir
        assert not info_sub.is_file

        entries = await fs.ls(root)
        assert [e.path for e in entries] == sorted([file_a, file_b, sub])
        assert await fs.ls_names(root) == sorted([file_a, file_b, sub])

        assert await fs.read_bytes(file_a) == b"hello"
        assert await fs.read_text(file_b) == "world!"

    async def test_read_bytes_slice_semantics(self, fs: AsyncFS[Any], root: str) -> None:
        path = self._p(root, "data.bin")
        payload = b"0123456789"
        await fs.write_bytes(path, payload)

        assert await fs.read_bytes(path) == payload
        assert await fs.read_bytes(path, start=2, end=6) == b"2345"
        assert await fs.read_bytes(path, start=5) == b"56789"
        assert await fs.read_bytes(path, end=4) == b"0123"
        assert await fs.read_bytes(path, start=-3) == b"789"
        assert await fs.read_bytes(path, end=-2) == b"01234567"
        assert await fs.read_bytes(path, start=-5, end=-1) == b"5678"
        assert await fs.read_bytes(path, start=-100, end=3) == b"012"
        assert await fs.read_bytes(path, start=2, end=100) == b"23456789"
        assert await fs.read_bytes(path, start=5, end=5) == b""
        assert await fs.read_bytes(path, start=7, end=3) == b""
        assert await fs.read_bytes(path, start=50, end=60) == b""
        assert await fs.read_bytes(path, start=-2, end=-5) == b""

        empty = self._p(root, "empty.bin")
        await fs.write_bytes(empty, b"")
        assert await fs.read_bytes(empty) == b""
        assert await fs.read_bytes(empty, start=0, end=10) == b""

    async def test_write_bytes_modes_and_buffer_types(self, fs: AsyncFS[Any], root: str) -> None:
        path = self._p(root, "buf.bin")
        await fs.write_bytes(path, bytearray(b"abc"), mode="create")
        assert await fs.read_bytes(path) == b"abc"

        with pytest.raises(FileExistsError):
            await fs.write_bytes(path, b"dup", mode="create")

        await fs.write_bytes(path, memoryview(b"xyz"), mode="overwrite")
        assert await fs.read_bytes(path) == b"xyz"

    async def test_exists_is_file_is_dir(self, fs: AsyncFS[Any], root: str) -> None:
        file_p = self._p(root, "f.txt")
        dir_p = self._p(root, "d")
        missing_p = self._p(root, "missing")
        nested_under_file = self._p(root, "f.txt", "child")

        await fs.write_bytes(file_p, b"x")
        await fs.mkdir(dir_p)

        assert await fs.exists(file_p)
        assert await fs.is_file(file_p)
        assert not await fs.is_dir(file_p)

        assert await fs.exists(dir_p)
        assert await fs.is_dir(dir_p)
        assert not await fs.is_file(dir_p)

        assert not await fs.exists(missing_p)
        assert not await fs.is_file(missing_p)
        assert not await fs.is_dir(missing_p)

        assert not await fs.exists(nested_under_file)
        assert not await fs.is_file(nested_under_file)
        assert not await fs.is_dir(nested_under_file)

    async def test_error_types(self, fs: AsyncFS[Any], root: str) -> None:
        file_p = self._p(root, "file.txt")
        dir_p = self._p(root, "dir")
        missing_p = self._p(root, "nope")

        await fs.write_bytes(file_p, b"abc")
        await fs.mkdir(dir_p)
        await fs.write_bytes(self._p(dir_p, "child.txt"), b"1")

        # FileNotFoundError
        with pytest.raises(FileNotFoundError):
            await fs.info(missing_p)
        with pytest.raises(FileNotFoundError):
            await fs.ls(missing_p)
        with pytest.raises(FileNotFoundError):
            await fs.read_bytes(missing_p)
        with pytest.raises(FileNotFoundError):
            await fs.remove_file(missing_p)
        with pytest.raises(FileNotFoundError):
            await fs.rmdir(missing_p)

        # NotADirectoryError
        with pytest.raises(NotADirectoryError):
            await fs.ls(file_p)
        with pytest.raises(NotADirectoryError):
            await fs.rmdir(file_p)
        with pytest.raises(NotADirectoryError):
            await fs.write_bytes(self._p(file_p, "sub"), b"x")
        with pytest.raises(NotADirectoryError):
            await fs.mkdir(self._p(file_p, "sub"))

        # IsADirectoryError
        with pytest.raises(IsADirectoryError):
            await fs.read_bytes(dir_p)
        with pytest.raises(IsADirectoryError):
            await fs.write_bytes(dir_p, b"x")
        with pytest.raises(IsADirectoryError):
            await fs.remove_file(dir_p)

        # FileExistsError
        with pytest.raises(FileExistsError):
            await fs.mkdir(dir_p, exist_ok=False)
        with pytest.raises(FileExistsError):
            await fs.mkdir(file_p, exist_ok=True)
        with pytest.raises(FileExistsError):
            await fs.write_bytes(file_p, b"x", mode="create")

        # ENOTEMPTY
        with pytest.raises(OSError, match="not empty") as exc_info:
            await fs.rmdir(dir_p)
        assert exc_info.value.errno == errno.ENOTEMPTY

    # --- mkdir / rmdir / touch ---

    async def test_mkdir_and_rmdir(self, fs: AsyncFS[Any], root: str) -> None:
        deep = self._p(root, "a", "b", "c")
        with pytest.raises(FileNotFoundError):
            await fs.mkdir(deep, parents=False)

        await fs.mkdir(deep, parents=True)
        assert await fs.is_dir(deep)

        # exist_ok=True succeeds on existing dir; exist_ok=False raises.
        await fs.mkdir(deep, parents=True, exist_ok=True)
        with pytest.raises(FileExistsError):
            await fs.mkdir(deep, parents=True, exist_ok=False)

        # Cannot rmdir non-empty parent.
        with pytest.raises(OSError, match="not empty") as exc_info:
            await fs.rmdir(self._p(root, "a"))
        assert exc_info.value.errno == errno.ENOTEMPTY

        await fs.rmdir(deep)
        assert not await fs.exists(deep)
        await fs.rmdir(self._p(root, "a", "b"))
        await fs.rmdir(self._p(root, "a"))
        assert not await fs.exists(self._p(root, "a"))

    async def test_touch(self, fs: AsyncFS[Any], root: str) -> None:
        p = self._p(root, "touched.txt")
        await fs.touch(p)
        assert await fs.read_bytes(p) == b""

        await fs.write_bytes(p, b"content")
        await fs.touch(p, truncate=False)
        assert await fs.read_bytes(p) == b"content"

        await fs.touch(p, truncate=True)
        assert await fs.read_bytes(p) == b""

        d = self._p(root, "adir")
        await fs.mkdir(d)
        with pytest.raises(IsADirectoryError):
            await fs.touch(d, truncate=True)
        with pytest.raises(IsADirectoryError):
            await fs.touch(d, truncate=False)

    # --- walk / find / glob / du ---

    async def test_walk(self, fs: AsyncFS[Any], root: str) -> None:
        await fs.mkdir(self._p(root, "a", "x"))
        await fs.mkdir(self._p(root, "b"))
        await fs.write_bytes(self._p(root, "top.txt"), b"1")
        await fs.write_bytes(self._p(root, "a", "in_a.txt"), b"2")
        await fs.write_bytes(self._p(root, "a", "x", "deep.txt"), b"3")
        await fs.write_bytes(self._p(root, "b", "in_b.txt"), b"4")

        # topdown=True
        topdown = [item async for item in fs.walk(root, topdown=True)]
        top_dirs = [dp for dp, _, _ in topdown]
        assert top_dirs[0] == root
        assert top_dirs.index(self._p(root, "a")) < top_dirs.index(self._p(root, "a", "x"))

        # topdown=False: children strictly before parents
        bottomup = [item async for item in fs.walk(root, topdown=False)]
        bot_dirs = [dp for dp, _, _ in bottomup]
        assert bot_dirs[-1] == root
        assert bot_dirs.index(self._p(root, "a", "x")) < bot_dirs.index(self._p(root, "a"))

        # maxdepth=1 -> only root itself
        depth1 = [item async for item in fs.walk(root, maxdepth=1)]
        assert len(depth1) == 1
        assert depth1[0][0] == root
        assert [d.path for d in depth1[0][1]] == [self._p(root, "a"), self._p(root, "b")]
        assert [f.path for f in depth1[0][2]] == [self._p(root, "top.txt")]

        # maxdepth=2 -> root, a, b (not a/x)
        depth2 = [dp async for dp, _, _ in fs.walk(root, maxdepth=2)]
        assert set(depth2) == {root, self._p(root, "a"), self._p(root, "b")}

        # Invalid maxdepth
        with pytest.raises(ValueError, match="maxdepth"):
            _ = [item async for item in fs.walk(root, maxdepth=0)]

        # on_error modes on missing path
        missing = self._p(root, "does_not_exist")
        assert [item async for item in fs.walk(missing, on_error="omit")] == []

        with pytest.raises(FileNotFoundError):
            _ = [item async for item in fs.walk(missing, on_error="raise")]

        seen_errors: list[OSError] = []
        assert [item async for item in fs.walk(missing, on_error=seen_errors.append)] == []
        assert len(seen_errors) == 1
        assert isinstance(seen_errors[0], FileNotFoundError)

    async def test_find_and_find_paths(self, fs: AsyncFS[Any], root: str) -> None:
        f1 = self._p(root, "f1.txt")
        sub = self._p(root, "sub")
        f2 = self._p(sub, "f2.txt")
        deep = self._p(sub, "deep")
        f3 = self._p(deep, "f3.txt")

        await fs.write_bytes(f1, b"1")
        await fs.write_bytes(f2, b"22")
        await fs.write_bytes(f3, b"333")

        assert await fs.find_paths(root) == sorted([f1, f2, f3])
        assert await fs.find_paths(root, withdirs=True) == sorted([root, f1, sub, f2, deep, f3])
        assert await fs.find_paths(root, maxdepth=1) == [f1]
        assert await fs.find_paths(root, maxdepth=1, withdirs=True) == sorted([root, f1, sub])

        # Single file returns [full FileInfo]
        single = await fs.find(f2)
        assert len(single) == 1
        assert single[0].path == f2
        assert single[0].is_file
        assert single[0].size == len(b"22")

        # Missing returns []
        assert await fs.find(self._p(root, "missing")) == []

    @pytest.mark.parametrize(("pattern", "maxdepth", "expected"), _GLOB_NON_RECURSIVE_CASES)
    async def test_glob_fsspec_edge_cases(
        self,
        fs: AsyncFS[Any],
        root: str,
        pattern: str,
        maxdepth: int | None,
        expected: list[str],
    ) -> None:
        source = self._p(root, "source")
        await fs.mkdir(source)
        await fs.touch(self._p(source, "file1"))
        await fs.touch(self._p(source, "file2"))
        for idx in range(2):
            subdir = self._p(source, f"subdir{idx}")
            nested = self._p(subdir, "nesteddir")
            await fs.mkdir(nested, parents=True)
            await fs.touch(self._p(subdir, "subfile1"))
            await fs.touch(self._p(subdir, "subfile2"))
            await fs.touch(self._p(nested, "nestedfile"))

        matched = await fs.glob(f"{source}/{pattern}", maxdepth=maxdepth)
        matched_files = sorted(entry.path for entry in matched if entry.is_file)
        expected_abs = sorted(self._p(source, rel) for rel in expected)
        assert matched_files == expected_abs

    async def test_glob_trailing_slash_and_literals(self, fs: AsyncFS[Any], root: str) -> None:
        f = self._p(root, "file.txt")
        d = self._p(root, "dir1")
        nested_f = self._p(d, "sub", "target.txt")
        direct_f = self._p(d, "target.txt")
        await fs.write_bytes(f, b"a")
        await fs.write_bytes(nested_f, b"b")
        await fs.write_bytes(direct_f, b"c")

        # Literal existing & missing
        assert await fs.glob_paths(f) == [f]
        assert await fs.glob_paths(d) == [d]
        assert await fs.glob_paths(self._p(root, "absent")) == []

        # Trailing slash matches only directories (both wildcard and literal)
        assert await fs.glob_paths(f"{root}/*/") == [d]
        assert await fs.glob_paths(f"{d}/") == [d]
        assert await fs.glob_paths(f"{f}/") == []

        # ** matches zero or more levels
        assert await fs.glob_paths(f"{d}/**/target.txt") == sorted([direct_f, nested_f])

        # Invalid patterns / maxdepth
        with pytest.raises(ValueError, match=r"\*\*"):
            await fs.glob(f"{root}/a**b")
        with pytest.raises(ValueError, match="maxdepth"):
            await fs.glob(f"{root}/**", maxdepth=0)

    async def test_du(self, fs: AsyncFS[Any], root: str) -> None:
        f1 = self._p(root, "a.bin")
        f2 = self._p(root, "sub", "b.bin")
        await fs.write_bytes(f1, b"1234")
        await fs.write_bytes(f2, b"123456")

        assert await fs.du(root) == {f1: 4, f2: 6}
        assert await fs.du(root, maxdepth=1) == {f1: 4}
        du_dirs = await fs.du(root, withdirs=True)
        assert du_dirs[f1] == len(b"1234")
        assert du_dirs[f2] == len(b"123456")
        assert self._p(root, "sub") in du_dirs
        assert await fs.du(self._p(root, "missing")) == {}

    # --- copy / move / remove ---

    async def test_copy_file_and_tree(self, fs: AsyncFS[Any], root: str) -> None:
        # Single file copy
        src_f = self._p(root, "src.txt")
        dst_f = self._p(root, "dst.txt")
        existing_dir = self._p(root, "adir")
        await fs.write_bytes(src_f, b"hello")
        await fs.mkdir(existing_dir)

        await fs.copy(src_f, dst_f)
        assert await fs.read_bytes(dst_f) == b"hello"

        with pytest.raises(FileExistsError):
            await fs.copy(src_f, dst_f, overwrite=False)

        await fs.write_bytes(src_f, b"updated")
        await fs.copy(src_f, dst_f, overwrite=True)
        assert await fs.read_bytes(dst_f) == b"updated"

        # File onto existing directory -> IsADirectoryError (no "copy into")
        with pytest.raises(IsADirectoryError):
            await fs.copy(src_f, existing_dir)

        # Tree copy onto new target
        tree_src = self._p(root, "tree_src")
        tree_dst = self._p(root, "tree_dst")
        await fs.write_bytes(self._p(tree_src, "a.txt"), b"A")
        await fs.write_bytes(self._p(tree_src, "sub", "b.txt"), b"B")
        await fs.mkdir(self._p(tree_src, "empty"))

        await fs.copy(tree_src, tree_dst)
        assert await fs.read_bytes(self._p(tree_dst, "a.txt")) == b"A"
        assert await fs.read_bytes(self._p(tree_dst, "sub", "b.txt")) == b"B"
        assert await fs.is_dir(self._p(tree_dst, "empty"))

        # Tree copy merge + overwrite=False pre-flight (nothing written on conflict!)
        merge_dst = self._p(root, "merge_dst")
        await fs.write_bytes(self._p(merge_dst, "sub", "b.txt"), b"OLD")
        await fs.write_bytes(self._p(merge_dst, "keep.txt"), b"KEEP")

        with pytest.raises(FileExistsError):
            await fs.copy(tree_src, merge_dst, overwrite=False)

        assert not await fs.exists(self._p(merge_dst, "a.txt"))
        assert await fs.read_bytes(self._p(merge_dst, "sub", "b.txt")) == b"OLD"

        await fs.copy(tree_src, merge_dst, overwrite=True)
        assert await fs.read_bytes(self._p(merge_dst, "a.txt")) == b"A"
        assert await fs.read_bytes(self._p(merge_dst, "sub", "b.txt")) == b"B"
        assert await fs.read_bytes(self._p(merge_dst, "keep.txt")) == b"KEEP"

        # Copying directory onto existing file -> FileExistsError
        with pytest.raises(FileExistsError):
            await fs.copy(tree_src, dst_f)

        # Copying directory into itself -> ValueError
        with pytest.raises(ValueError, match="into itself"):
            await fs.copy(tree_src, self._p(tree_src, "inside"))

    async def test_move_file_and_tree(self, fs: AsyncFS[Any], root: str) -> None:
        # Move single file
        src_f = self._p(root, "m_src.txt")
        dst_f = self._p(root, "m_dst.txt")
        await fs.write_bytes(src_f, b"move-me")

        await fs.move(src_f, dst_f)
        assert not await fs.exists(src_f)
        assert await fs.read_bytes(dst_f) == b"move-me"

        # overwrite=False leaves source intact
        other_f = self._p(root, "m_other.txt")
        await fs.write_bytes(other_f, b"stay")
        with pytest.raises(FileExistsError):
            await fs.move(other_f, dst_f, overwrite=False)
        assert await fs.read_bytes(other_f) == b"stay"
        assert await fs.read_bytes(dst_f) == b"move-me"

        # Move tree + conflict leaves source intact
        t_src = self._p(root, "mt_src")
        t_dst = self._p(root, "mt_dst")
        await fs.write_bytes(self._p(t_src, "x.txt"), b"X")
        await fs.write_bytes(self._p(t_src, "sub", "y.txt"), b"Y")
        await fs.write_bytes(self._p(t_dst, "sub", "y.txt"), b"EXISTING")

        with pytest.raises(FileExistsError):
            await fs.move(t_src, t_dst, overwrite=False)
        assert await fs.read_bytes(self._p(t_src, "x.txt")) == b"X"
        assert await fs.read_bytes(self._p(t_src, "sub", "y.txt")) == b"Y"
        assert not await fs.exists(self._p(t_dst, "x.txt"))

        await fs.move(t_src, t_dst, overwrite=True)
        assert not await fs.exists(t_src)
        assert await fs.read_bytes(self._p(t_dst, "x.txt")) == b"X"
        assert await fs.read_bytes(self._p(t_dst, "sub", "y.txt")) == b"Y"

        # Root cannot be moved
        with pytest.raises(ValueError, match="root"):
            await fs.move("/", self._p(root, "nowhere"))

    async def test_remove(self, fs: AsyncFS[Any], root: str) -> None:
        f = self._p(root, "rem.txt")
        await fs.write_bytes(f, b"bye")
        await fs.remove(f)
        assert not await fs.exists(f)

        empty_d = self._p(root, "empty_d")
        await fs.mkdir(empty_d)
        await fs.remove(empty_d, recursive=False)
        assert not await fs.exists(empty_d)

        non_empty = self._p(root, "tree")
        await fs.write_bytes(self._p(non_empty, "a", "f.txt"), b"1")
        with pytest.raises(OSError, match="not empty") as exc_info:
            await fs.remove(non_empty, recursive=False)
        assert exc_info.value.errno == errno.ENOTEMPTY

        await fs.remove(non_empty, recursive=True)
        assert not await fs.exists(non_empty)

        with pytest.raises(ValueError, match="root"):
            await fs.remove("/", recursive=True)

    # --- get_file / put_file ---

    async def test_get_and_put_file(self, fs: AsyncFS[Any], root: str, tmp_path: Path) -> None:
        local_src = tmp_path / "upload.bin"
        local_src.write_bytes(b"local-payload")
        remote = self._p(root, "remote.bin")

        await fs.put_file(local_src, remote)
        assert await fs.read_bytes(remote) == b"local-payload"

        with pytest.raises(FileExistsError):
            await fs.put_file(local_src, remote, mode="create")

        local_dst = tmp_path / "download.bin"
        await fs.get_file(remote, local_dst)
        assert local_dst.read_bytes() == b"local-payload"

    # --- grep & diff ---

    async def test_grep(self, fs: AsyncFS[Any], root: str) -> None:
        f1 = self._p(root, "alpha.py")
        f2 = self._p(root, "beta.txt")
        f3 = self._p(root, "binary.bin")
        await fs.write_text(f1, "line1\nHello World\nhello again\nline4\n")
        await fs.write_text(f2, "start\nhello beta\nend\n")
        await fs.write_bytes(f3, b"hello\x00binary\x00data")

        # Smart case: lowercase -> case-insensitive; skips binary file
        matches = await fs.grep(root, "hello")
        assert [(m.path, m.line_number) for m in matches] == [
            (f1, 2),
            (f1, 3),
            (f2, 2),
        ]

        # Smart case: uppercase -> case-sensitive
        upper_matches = await fs.grep(root, "Hello")
        assert [(m.path, m.line_number) for m in upper_matches] == [(f1, 2)]
        assert upper_matches[0].submatches == ((0, 5),)

        # max_count
        expected_max = 2
        limited = await fs.grep(root, "hello", max_count=expected_max)
        assert len(limited) == expected_max

        # Context lines
        ctx = await fs.grep(f1, "Hello", context_before=1, context_after=1)
        assert len(ctx) == 1
        assert ctx[0].context_before == ("line1",)
        assert ctx[0].context_after == ("hello again",)

        # Globs filter
        only_py = await fs.grep(root, "hello", globs=["*.py"])
        assert all(m.path == f1 for m in only_py)
        exclude_py = await fs.grep(root, "hello", globs=["!*.py"])
        assert all(m.path == f2 for m in exclude_py)

        # Multiline
        ml = await fs.grep(f1, r"World\nhello", multiline=True)
        expected_line = 2
        assert len(ml) == 1
        assert ml[0].line_number == expected_line

        # Missing path raises FileNotFoundError
        with pytest.raises(FileNotFoundError):
            await fs.grep(self._p(root, "nope"), "hello")

    async def test_diff(self, fs: AsyncFS[Any], root: str) -> None:
        p1 = self._p(root, "v1.txt")
        p2 = self._p(root, "v2.txt")
        await fs.write_text(p1, "a\nb\nc\n")
        await fs.write_text(p2, "a\nB\nc\n")

        diff_out = await fs.diff(p1, p2)
        assert f"--- {p1}" in diff_out
        assert f"+++ {p2}" in diff_out
        assert "-b" in diff_out
        assert "+B" in diff_out

        assert await fs.diff(p1, p1) == ""

    # --- open streams ---

    async def test_open_read_stream(self, fs: AsyncFS[Any], root: str) -> None:
        p = self._p(root, "lines.txt")
        content = b"first line\nsecond line\nthird"
        await fs.write_bytes(p, content)

        async with fs.open(p, "rb", block_size=7) as reader:
            assert isinstance(reader, AsyncBufferedReader)
            assert isinstance(reader, AsyncReadStream)
            assert isinstance(reader, AsyncSeekableStream)
            assert reader.size == len(content)
            assert reader.tell() == 0

            assert await reader.read(5) == b"first"
            assert reader.tell() == len(b"first")
            assert await reader.readline() == b" line\n"
            assert reader.seek(0) == 0
            assert await reader.read() == content
            assert await reader.read() == b""

            # Seek variants
            assert reader.seek(-5, io.SEEK_END) == len(content) - 5
            assert await reader.read() == b"third"
            second_line_pos = len(b"first line\n")
            assert reader.seek(second_line_pos, io.SEEK_SET) == second_line_pos
            word_pos = second_line_pos + len(b"second ")
            assert reader.seek(len(b"second "), io.SEEK_CUR) == word_pos
            assert await reader.read(4) == b"line"

            # Async iteration by line
            reader.seek(0)
            lines = [line async for line in reader]
            assert lines == [b"first line\n", b"second line\n", b"third"]

        # Closed checks + idempotent aclose
        assert reader.closed
        await reader.aclose()
        with pytest.raises(ValueError, match="closed"):
            await reader.read()
        with pytest.raises(ValueError, match="closed"):
            reader.tell()
        with pytest.raises(ValueError, match="closed"):
            reader.seek(0)

    async def test_open_write_modes_and_exception_discard(
        self, fs: AsyncFS[Any], root: str
    ) -> None:
        p = self._p(root, "stream.bin")

        # wb
        async with fs.open(p, "wb") as writer:
            assert isinstance(writer, AsyncBufferedWriter)
            assert isinstance(writer, AsyncWriteStream)
            assert await writer.write(b"hel") == len(b"hel")
            assert await writer.write(bytearray(b"lo")) == len(b"lo")
            await writer.flush()
        assert await fs.read_bytes(p) == b"hello"
        assert writer.closed
        await writer.aclose()  # idempotent
        with pytest.raises(ValueError, match="closed"):
            await writer.write(b"nope")

        # wb exception discards buffer and leaves existing file untouched
        async def _aborted_write() -> None:
            async with fs.open(p, "wb") as w:
                await w.write(b"corrupted")
                raise RuntimeError("boom")

        with pytest.raises(RuntimeError, match="boom"):
            await _aborted_write()
        assert await fs.read_bytes(p) == b"hello"

        # ab appends to existing and creates if missing
        async with fs.open(p, "ab") as writer:
            await writer.write(b" world")
        assert await fs.read_bytes(p) == b"hello world"

        ab_new = self._p(root, "ab_new.bin")
        async with fs.open(ab_new, "ab") as writer:
            await writer.write(b"fresh")
        assert await fs.read_bytes(ab_new) == b"fresh"

        # xb succeeds on new, fails at open time on existing, fails at close on race
        xb_path = self._p(root, "exclusive.bin")
        async with fs.open(xb_path, "xb") as writer:
            await writer.write(b"only-once")
        assert await fs.read_bytes(xb_path) == b"only-once"

        with pytest.raises(FileExistsError):
            async with fs.open(xb_path, "xb"):
                pass  # pragma: no cover

        xb_race = self._p(root, "exclusive_race.bin")

        async def _raced_exclusive_write() -> None:
            async with fs.open(xb_race, "xb") as w:
                await w.write(b"writer-data")
                # Concurrent creator wins before close.
                await fs.write_bytes(xb_race, b"racer-data")

        with pytest.raises(FileExistsError):
            await _raced_exclusive_write()
        assert await fs.read_bytes(xb_race) == b"racer-data"

        # Invalid mode
        with pytest.raises(ValueError, match="Unsupported mode"):
            fs.open(p, "r")  # type: ignore[call-overload]  # ty: ignore[no-matching-overload]

    # --- read_many & concurrency ---

    async def test_read_many_on_error_modes(self, fs: AsyncFS[Any], root: str) -> None:
        p1 = self._p(root, "r1.bin")
        p2 = self._p(root, "r2.bin")
        missing = self._p(root, "missing.bin")
        await fs.write_bytes(p1, b"one")
        await fs.write_bytes(p2, b"two")

        assert await fs.read_many([p1, p2], limit=1) == {p1: b"one", p2: b"two"}
        assert await fs.read_many([]) == {}

        with pytest.raises(FileNotFoundError):
            await fs.read_many([p1, missing, p2], on_error="raise")

        omitted = await fs.read_many([p1, missing, p2], on_error="omit")
        assert omitted == {p1: b"one", p2: b"two"}

        returned = await fs.read_many([p1, missing, p2], on_error="return")
        assert returned[p1] == b"one"
        assert returned[p2] == b"two"
        assert isinstance(returned[missing], FileNotFoundError)

    async def test_concurrent_writes(self, fs: AsyncFS[Any], root: str) -> None:
        paths = [self._p(root, f"concurrent_{i}.bin") for i in range(25)]
        await asyncio.gather(*(fs.write_bytes(p, f"val-{i}".encode()) for i, p in enumerate(paths)))
        results = await fs.read_many(paths)
        assert results == {p: f"val-{i}".encode() for i, p in enumerate(paths)}
