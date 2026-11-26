"""Tests for :class:`upathtools.fs.LocalFS` and :class:`upathtools.fs.LocalFSInfo`."""

import asyncio
from concurrent.futures import ThreadPoolExecutor
import contextlib
import errno
import os
from pathlib import Path
import stat
from typing import TYPE_CHECKING, Any, Self

import pytest

from tests.fs.contract import AsyncFSContract
from upathtools.fs import (
    FSSPEC_SYNC_VERSION,
    AsyncFS,
    FilesystemClosedError,
    LocalFS,
    LocalFSInfo,
    PathEscapeError,
)
from upathtools.fs.implementations import local as local_mod


if TYPE_CHECKING:
    from collections.abc import AsyncIterator


class TestLocalFSContract(AsyncFSContract):
    """Run the full AsyncFS behavioral contract (including GLOB_EDGE_CASES) against LocalFS."""

    @pytest.fixture
    async def fs(self) -> AsyncIterator[LocalFS]:
        local = LocalFS()
        yield local
        await local.aclose()

    @pytest.fixture
    async def root(self, fs: LocalFS, tmp_path: Path) -> str:
        work = tmp_path / "work"
        await fs.mkdir(work.as_posix(), exist_ok=True)
        return work.as_posix()


async def test_local_strip_protocol_relative_and_windows_drives(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.chdir(tmp_path)
    cwd_posix = tmp_path.as_posix()

    assert LocalFS.strip_protocol("a/b.txt") == f"{cwd_posix}/a/b.txt"
    assert LocalFS.strip_protocol("./a/b.txt") == f"{cwd_posix}/a/b.txt"
    assert LocalFS.strip_protocol(".") == cwd_posix
    assert LocalFS.strip_protocol("") == cwd_posix
    assert LocalFS.strip_protocol("file:///home/user/doc.txt") == "/home/user/doc.txt"
    assert LocalFS.strip_protocol("local:///home/user/doc.txt/") == "/home/user/doc.txt"
    assert LocalFS.strip_protocol("file:/home/user/doc.txt") == "/home/user/doc.txt"
    assert LocalFS.strip_protocol("/") == "/"

    # Windows drive handling works even on Linux without hard-coding "/" as sole root.
    assert LocalFS.strip_protocol("C:/Users/alice/file.txt") == "C:/Users/alice/file.txt"
    assert LocalFS.strip_protocol(r"C:\Users\alice\file.txt") == "C:/Users/alice/file.txt"
    assert LocalFS.strip_protocol("file:///C:/Users/alice/") == "C:/Users/alice"
    assert LocalFS.strip_protocol("C:/") == "C:/"
    assert LocalFS.strip_protocol("C:") == "C:/"
    assert LocalFS.parent("C:/Users/alice") == "C:/Users"
    assert LocalFS.parent("C:/Users") == "C:/"
    assert LocalFS.parent("C:/") == "C:/"
    assert LocalFS.parent("/home/alice") == "/home"
    assert LocalFS.parent("/") == "/"

    with pytest.raises(PathEscapeError):
        LocalFS.strip_protocol("/../outside")
    with pytest.raises(PathEscapeError):
        LocalFS.strip_protocol("C:/../outside")

    # Relative path resolution works against cwd at call time.
    fs = LocalFS()
    await fs.write_bytes("rel_file.txt", b"relative")
    assert await fs.read_bytes("rel_file.txt") == b"relative"
    assert (tmp_path / "rel_file.txt").read_bytes() == b"relative"


async def test_local_symlinks_info_and_no_loop_in_walk_find(tmp_path: Path) -> None:
    fs = LocalFS()
    target_file = tmp_path / "target.txt"
    target_file.write_bytes(b"0123456789")
    sub_dir = tmp_path / "sub"
    sub_dir.mkdir()
    (sub_dir / "inside.txt").write_bytes(b"inside")

    file_link = tmp_path / "link_to_file"
    dir_link = tmp_path / "link_to_dir"
    loop_link = sub_dir / "loop_to_root"
    dangling_link = tmp_path / "dangling"

    file_link.symlink_to(target_file)
    dir_link.symlink_to(sub_dir)
    loop_link.symlink_to(tmp_path)
    dangling_link.symlink_to(tmp_path / "does_not_exist")

    # File symlink info
    f_info = await fs.info(file_link.as_posix())
    assert isinstance(f_info, LocalFSInfo)
    assert f_info.is_symlink is True
    assert f_info.symlink_target == str(target_file)
    assert f_info.type == "file"
    assert f_info.is_file
    assert f_info.size == len(b"0123456789")
    assert f_info.mode is not None
    assert f_info.uid is not None
    assert f_info.gid is not None
    assert f_info.ino is not None
    assert f_info.nlink is not None
    assert f_info.mtime is not None
    assert f_info.ctime is not None
    fsspec_dict = f_info.to_fsspec_dict()
    assert fsspec_dict["islink"] is True
    assert fsspec_dict["destination"] == str(target_file)
    assert fsspec_dict["uid"] == f_info.uid
    assert fsspec_dict["ino"] == f_info.ino

    # Directory symlink info
    d_info = await fs.info(dir_link.as_posix())
    assert isinstance(d_info, LocalFSInfo)
    assert d_info.is_symlink is True
    assert d_info.symlink_target == str(sub_dir)
    assert d_info.type == "directory"
    assert d_info.is_dir

    # Dangling symlink info
    dang_info = await fs.info(dangling_link.as_posix())
    assert isinstance(dang_info, LocalFSInfo)
    assert dang_info.is_symlink is True
    assert dang_info.symlink_target == str(tmp_path / "does_not_exist")
    assert dang_info.type == "other"
    assert not dang_info.is_file
    assert not dang_info.is_dir

    # walk() must NOT follow directory symlinks into infinite loops
    walked_dirs = [dirpath async for dirpath, _, _ in fs.walk(tmp_path.as_posix())]
    assert walked_dirs == [tmp_path.as_posix(), sub_dir.as_posix()]

    # find() must NOT follow directory symlinks into infinite loops
    found_files = await fs.find_paths(tmp_path.as_posix(), withdirs=False)
    assert found_files == sorted([
        dangling_link.as_posix(),
        file_link.as_posix(),
        (sub_dir / "inside.txt").as_posix(),
        target_file.as_posix(),
    ])
    found_all = await fs.find_paths(tmp_path.as_posix(), withdirs=True)
    assert found_all == sorted([
        tmp_path.as_posix(),
        dangling_link.as_posix(),
        dir_link.as_posix(),
        file_link.as_posix(),
        sub_dir.as_posix(),
        (sub_dir / "inside.txt").as_posix(),
        loop_link.as_posix(),
        target_file.as_posix(),
    ])


async def test_local_auto_mkdir_per_instance_isolation(tmp_path: Path) -> None:
    fs_auto = LocalFS(auto_mkdir=True)
    fs_no_auto = LocalFS(auto_mkdir=False)

    assert fs_auto.auto_mkdir is True
    assert fs_no_auto.auto_mkdir is False

    # Missing parent fails on auto_mkdir=False, succeeds on auto_mkdir=True.
    p1 = (tmp_path / "missing_a" / "f.txt").as_posix()
    with pytest.raises(FileNotFoundError):
        await fs_no_auto.write_bytes(p1, b"nope")
    assert not (tmp_path / "missing_a").exists()

    p2 = (tmp_path / "created_b" / "nested" / "f.txt").as_posix()
    await fs_auto.write_bytes(p2, b"created")
    assert await fs_no_auto.read_bytes(p2) == b"created"

    # Second write on fs_no_auto still fails for a different missing parent (no shared state).
    p3 = (tmp_path / "still_missing" / "f.txt").as_posix()
    with pytest.raises(FileNotFoundError):
        await fs_no_auto.write_bytes(p3, b"nope")


async def test_local_atomic_write_no_temp_files_and_no_partial_reads(tmp_path: Path) -> None:
    fs = LocalFS()
    target = tmp_path / "atomic.bin"
    payload_a = b"A" * (256 * 1024)
    payload_b = b"B" * (256 * 1024)
    await fs.write_bytes(target.as_posix(), payload_a)

    stop = asyncio.Event()
    reads_seen = 0

    async def reader() -> None:
        nonlocal reads_seen
        while not stop.is_set():
            data = await fs.read_bytes(target.as_posix())
            assert data in (payload_a, payload_b), f"Observed partial write of length {len(data)}"
            reads_seen += 1

    async def writer() -> None:
        for i in range(25):
            await fs.write_bytes(target.as_posix(), payload_b if i % 2 == 0 else payload_a)
        stop.set()

    await asyncio.gather(reader(), writer())
    assert reads_seen > 0

    # No temporary files left in directory after writes or failed write onto directory.
    dir_path = tmp_path / "adir"
    dir_path.mkdir()
    with pytest.raises(IsADirectoryError):
        await fs.write_bytes(dir_path.as_posix(), b"should-fail")

    leftovers = [p.name for p in tmp_path.iterdir() if p.name.endswith(".upathtools-tmp")]
    assert leftovers == []


async def test_local_create_mode_race_exactly_one_winner(tmp_path: Path) -> None:
    fs = LocalFS()
    target = (tmp_path / "race.bin").as_posix()
    contenders = 8

    results = await asyncio.gather(
        *(fs.write_bytes(target, f"winner-{i}".encode(), mode="create") for i in range(contenders)),
        return_exceptions=True,
    )
    successes = [r for r in results if r is None]
    errors = [r for r in results if isinstance(r, FileExistsError)]
    assert len(successes) == 1
    assert len(errors) == contenders - 1

    winner_idx = results.index(None)
    assert await fs.read_bytes(target) == f"winner-{winner_idx}".encode()


async def test_local_read_bytes_range_does_not_read_whole_file(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    fs = LocalFS()
    target = tmp_path / "large.bin"
    total_size = 64 * 1024
    content = bytes(i % 251 for i in range(total_size))
    target.write_bytes(content)

    read_sizes: list[int] = []
    real_open = Path.open

    class _SpyFile:
        def __init__(self, inner: Any) -> None:
            self._inner = inner

        def __enter__(self) -> Self:
            self._inner.__enter__()
            return self

        def __exit__(self, *args: object) -> None:
            self._inner.__exit__(*args)

        def fileno(self) -> int:
            return self._inner.fileno()

        def seek(self, offset: int, whence: int = 0) -> int:
            return self._inner.seek(offset, whence)

        def read(self, size: int = -1) -> bytes:
            read_sizes.append(size)
            return self._inner.read(size)

    def spy_open(self_path: Path, mode: str = "r", *args: Any, **kwargs: Any) -> Any:
        fh = real_open(self_path, mode, *args, **kwargs)
        if self_path == target and mode == "rb":
            return _SpyFile(fh)
        return fh

    monkeypatch.setattr(Path, "open", spy_open)

    chunk = await fs.read_bytes(target.as_posix(), start=1000, end=1064)
    assert chunk == content[1000:1064]
    assert read_sizes == [64]

    # Empty / out-of-range slice performs zero read() calls.
    read_sizes.clear()
    assert await fs.read_bytes(target.as_posix(), start=500, end=200) == b""
    assert read_sizes == []


async def test_local_ripgrep_parity_find_grep_and_glob(tmp_path: Path) -> None:
    fs = LocalFS()
    tree = tmp_path / "tree"
    tree.mkdir()

    # Build rich tree: git repo marker (.git), hidden file, .gitignore + ignored file,
    # nested dirs, binary file, non-ASCII text, and symlinks (file, dir, dangling).
    (tree / ".git").mkdir()
    (tree / ".hidden.txt").write_text("hello from hidden\n", encoding="utf-8")
    (tree / ".gitignore").write_text("ignored.txt\nignored_dir/\n", encoding="utf-8")
    (tree / "ignored.txt").write_text("hello from ignored\n", encoding="utf-8")
    (tree / "alpha.py").write_text(
        "line1\nHello World\nhello again\nhéllo world\nline5\n",
        encoding="utf-8",
    )
    (tree / "binary.bin").write_bytes(b"hello\x00binary\x00data\n")

    sub = tree / "sub"
    sub.mkdir()
    (sub / "beta.txt").write_text("hello beta\nsecond hello\n", encoding="utf-8")
    deep = sub / "deep"
    deep.mkdir()
    (deep / "gamma.py").write_text("HELLO GAMMA\nhello gamma\n", encoding="utf-8")

    (tree / "ignored_dir").mkdir()
    (tree / "ignored_dir" / "secret.txt").write_text("hello ignored dir\n", encoding="utf-8")

    (tree / "link_alpha.py").symlink_to(tree / "alpha.py")
    (tree / "link_sub").symlink_to(sub)
    (tree / "dangling_link").symlink_to(tree / "no_such_target")

    tree_str = tree.as_posix()

    # 1. find() parity with AsyncFS.find across maxdepth x withdirs
    for maxdepth in (None, 1, 2):
        for withdirs in (False, True):
            native = await fs.find(tree_str, maxdepth=maxdepth, withdirs=withdirs)
            base = await AsyncFS.find(fs, tree_str, maxdepth=maxdepth, withdirs=withdirs)
            assert native == base, f"find mismatch for maxdepth={maxdepth}, withdirs={withdirs}"

    # Also exercise ripgrep_rs.files_with_info filtering options directly
    no_hidden_paths = {e.path for e in await fs.find(tree_str, hidden=False, no_ignore=True)}
    assert (tree / ".hidden.txt").as_posix() not in no_hidden_paths
    assert (tree / "alpha.py").as_posix() in no_hidden_paths

    with_ignore_paths = {e.path for e in await fs.find(tree_str, hidden=True, no_ignore=False)}
    assert (tree / "ignored.txt").as_posix() not in with_ignore_paths
    assert (tree / "ignored_dir" / "secret.txt").as_posix() not in with_ignore_paths
    assert (tree / "alpha.py").as_posix() in with_ignore_paths

    # 2. grep() parity with AsyncFS.grep across options
    grep_cases: list[dict[str, Any]] = [
        {"pattern": "hello"},
        {"pattern": "Hello"},
        {"pattern": "hello", "case_sensitive": True},
        {"pattern": "Hello", "case_sensitive": False},
        {"pattern": "hello", "max_count": 3},
        {"pattern": "hello", "context_before": 1, "context_after": 1},
        {"pattern": "world"},  # exercises non-ASCII line ("héllo world") char offsets
        {"pattern": "hello", "globs": ["*.py"]},
        {"pattern": "hello", "globs": ["!*.py"]},
        {"pattern": "hello", "globs": ["sub/*.txt"]},
        {"pattern": r"World\nhello", "multiline": True},
        {"pattern": r"(?=hello)hello"},  # Python lookahead fallback
    ]
    for kwargs in grep_cases:
        native_matches = await fs.grep(tree_str, **kwargs)
        base_matches = await AsyncFS.grep(fs, tree_str, **kwargs)
        assert native_matches == base_matches, f"grep mismatch for {kwargs}"

    # Single-file grep parity (including binary file and glob exclusion)
    for target_file in (tree / "alpha.py", tree / "binary.bin"):
        for kwargs in ({"pattern": "hello"}, {"pattern": "hello", "globs": ["!*.py"]}):
            assert await fs.grep(target_file.as_posix(), **kwargs) == await AsyncFS.grep(
                fs, target_file.as_posix(), **kwargs
            )

    # ripgrep-specific hidden=False / no_ignore=False options on grep()
    no_hidden_grep = await fs.grep(tree_str, "hello", hidden=False, no_ignore=True)
    assert all(not m.path.endswith(".hidden.txt") for m in no_hidden_grep)
    respect_ignore_grep = await fs.grep(tree_str, "hello", hidden=True, no_ignore=False)
    assert all("ignored" not in m.path for m in respect_ignore_grep)

    # 3. glob() on top of find() matches base
    for pat in (f"{tree_str}/*.py", f"{tree_str}/**/*.py", f"{tree_str}/*/"):
        assert await fs.glob(pat) == await AsyncFS.glob(fs, pat)


async def test_local_move_cross_device_fallback(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    fs = LocalFS()
    src_file = tmp_path / "src.txt"
    dst_file = tmp_path / "dst.txt"
    src_file.write_bytes(b"cross-device-file")

    src_dir = tmp_path / "src_dir"
    (src_dir / "nested").mkdir(parents=True)
    (src_dir / "nested" / "a.txt").write_bytes(b"cross-device-dir")
    dst_dir = tmp_path / "dst_dir"

    real_replace = Path.replace

    def fake_exdev(self_path: Path, target: Path | str) -> Path:
        if self_path in (src_file, src_dir):
            raise OSError(errno.EXDEV, "Invalid cross-device link")
        return real_replace(self_path, target)

    monkeypatch.setattr(Path, "replace", fake_exdev)

    await fs.move(src_file.as_posix(), dst_file.as_posix())
    assert not src_file.exists()
    assert dst_file.read_bytes() == b"cross-device-file"

    await fs.move(src_dir.as_posix(), dst_dir.as_posix())
    assert not src_dir.exists()
    assert (dst_dir / "nested" / "a.txt").read_bytes() == b"cross-device-dir"


async def test_local_closed_capabilities_and_sync_note(tmp_path: Path) -> None:
    fs = LocalFS()
    assert not fs.readonly
    assert fs.capabilities == frozenset({
        "write_bytes",
        "mkdir",
        "rmdir",
        "remove_file",
        "move",
        "grep",
    })
    assert local_mod.__fsspec_sync__.version == FSSPEC_SYNC_VERSION

    await fs.aclose()
    assert fs.closed
    await fs.aclose()  # idempotent

    p = (tmp_path / "a.txt").as_posix()
    with pytest.raises(FilesystemClosedError):
        await fs.info(p)
    with pytest.raises(FilesystemClosedError):
        await fs.ls(tmp_path.as_posix())
    with pytest.raises(FilesystemClosedError):
        await fs.read_bytes(p)
    with pytest.raises(FilesystemClosedError):
        await fs.write_bytes(p, b"x")
    with pytest.raises(FilesystemClosedError):
        await fs.mkdir(p)
    with pytest.raises(FilesystemClosedError):
        await fs.rmdir(p)
    with pytest.raises(FilesystemClosedError):
        await fs.remove_file(p)
    with pytest.raises(FilesystemClosedError):
        await fs.move(p, f"{p}.dst")
    with pytest.raises(FilesystemClosedError):
        await fs.find(tmp_path.as_posix())
    with pytest.raises(FilesystemClosedError):
        await fs.grep(tmp_path.as_posix(), "x")


def test_local_eight_thread_stress(tmp_path: Path) -> None:
    """Eight threads each running asyncio.run() against ONE shared LocalFS."""
    fs = LocalFS()
    shared_dir = (tmp_path / "shared").as_posix()
    asyncio.run(fs.mkdir(shared_dir))

    num_threads = 8
    iterations = 80
    slot_count = 6

    def worker(tid: int) -> None:
        async def run() -> None:
            for i in range(iterations):
                shared_path = f"{shared_dir}/slot_{(tid + i) % slot_count}.bin"
                own_path = f"{shared_dir}/thread_{tid}.bin"
                payload = f"t{tid}-i{i}-".encode() * 32

                await fs.write_bytes(shared_path, payload)
                await fs.write_bytes(own_path, payload)

                try:
                    data = await fs.read_bytes(shared_path)
                    assert data.startswith(b"t")
                except FileNotFoundError:
                    pass

                entries = await fs.ls(shared_dir)
                assert all(e.path.startswith(f"{shared_dir}/") for e in entries)

                if i % 3 == 0:
                    with contextlib.suppress(FileNotFoundError):
                        await fs.remove_file(shared_path)

            await fs.write_bytes(f"{shared_dir}/final_{tid}.bin", f"done-{tid}".encode())

        asyncio.run(run())

    with ThreadPoolExecutor(max_workers=num_threads) as pool:
        futures = [pool.submit(worker, tid) for tid in range(num_threads)]
        for fut in futures:
            fut.result()

    async def verify() -> None:
        for tid in range(num_threads):
            assert await fs.read_bytes(f"{shared_dir}/final_{tid}.bin") == f"done-{tid}".encode()
        entries = await fs.ls(shared_dir)
        assert not any(e.name.endswith(".upathtools-tmp") for e in entries)
        paths = [e.path for e in entries]
        assert paths == sorted(paths)
        for entry in entries:
            data = await fs.read_bytes(entry.path)
            assert entry.size == len(data)
        await fs.aclose()

    asyncio.run(verify())


@pytest.mark.skipif(os.name == "nt", reason="POSIX permission bits")
async def test_local_write_keeps_umask_and_existing_permissions(tmp_path: Path) -> None:
    fs = LocalFS()
    fresh = tmp_path / "fresh.txt"
    await fs.write_bytes(str(fresh), b"x")
    umask = os.umask(0)
    os.umask(umask)
    assert stat.S_IMODE(fresh.stat().st_mode) == 0o666 & ~umask

    script = tmp_path / "run.sh"
    script.write_bytes(b"old")
    mode = 0o750
    script.chmod(mode)
    await fs.write_bytes(str(script), b"new")
    assert stat.S_IMODE(script.stat().st_mode) == mode
    assert script.read_bytes() == b"new"


async def test_local_write_through_symlink_keeps_the_link(tmp_path: Path) -> None:
    fs = LocalFS()
    target = tmp_path / "target.txt"
    target.write_bytes(b"old")
    link = tmp_path / "link.txt"
    link.symlink_to(target)
    await fs.write_bytes(str(link), b"new")
    assert link.is_symlink()
    assert target.read_bytes() == b"new"
