"""AsyncLocalFileSystem's ripgrep-backed _find/_glob follow fsspec semantics."""

import os
import subprocess
from typing import TYPE_CHECKING

import fsspec
import pytest

from upathtools.filesystems import AsyncLocalFileSystem


if TYPE_CHECKING:
    from pathlib import Path


@pytest.fixture
def tree(tmp_path: Path) -> str:
    """A git repo with nested, hidden and ignored files, and a dir named like a file."""
    for rel in [
        "a/one.txt",
        "a/b/two.txt",
        "a/b/c/three.txt",
        "x/y/b.txt",
        ".hid/h.txt",
        "ignored.txt",
        "top.txt",
        "d.txt/inner.txt",
    ]:
        p = tmp_path / rel
        p.parent.mkdir(parents=True, exist_ok=True)
        p.write_text(rel)
    (tmp_path / ".gitignore").write_text("ignored.txt\n")
    # Without the GIT_* variables a commit hook sets (they point at the outer repo)
    env = {k: v for k, v in os.environ.items() if not k.startswith("GIT_")}
    subprocess.run(["git", "init", "-q"], cwd=tmp_path, check=True, env=env)
    return tmp_path.as_posix()


def _rel(root: str, paths) -> list[str]:
    return sorted(p.removeprefix(root) for p in paths)


@pytest.mark.parametrize(
    ("pattern", "maxdepth"),
    [
        ("/*", None),
        ("/*.txt", None),
        ("/**/*.txt", None),
        ("/**/*.txt", 1),
        ("/**/*", 2),
        ("/*/y/b.txt", None),
        ("/a/*/two.txt", None),
        ("/a/**", None),
    ],
)
async def test_glob_matches_fsspec(tree: str, pattern: str, maxdepth: int | None) -> None:
    fs = AsyncLocalFileSystem()
    ref = fsspec.filesystem("file", skip_instance_cache=True)
    expected = _rel(tree, ref.glob(tree + pattern, maxdepth=maxdepth))
    got = await fs._glob(tree + pattern, maxdepth=maxdepth)
    assert _rel(tree, got) == expected
    detailed = await fs._glob(tree + pattern, maxdepth=maxdepth, detail=True)
    assert _rel(tree, detailed) == expected


@pytest.mark.parametrize("maxdepth", [None, 1, 2])
@pytest.mark.parametrize("withdirs", [False, True])
async def test_find_matches_fsspec(tree: str, maxdepth: int | None, withdirs: bool) -> None:
    fs = AsyncLocalFileSystem()
    ref = fsspec.filesystem("file", skip_instance_cache=True)
    expected = _rel(tree, ref.find(tree, maxdepth=maxdepth, withdirs=withdirs))
    assert _rel(tree, await fs._find(tree, maxdepth=maxdepth, withdirs=withdirs)) == expected
    detailed = await fs._find(tree, maxdepth=maxdepth, withdirs=withdirs, detail=True)
    assert _rel(tree, detailed) == expected
    assert all(info["type"] in {"file", "directory"} for info in detailed.values())


async def test_find_on_a_file(tree: str) -> None:
    fs = AsyncLocalFileSystem()
    assert await fs._find(tree + "/top.txt") == [tree + "/top.txt"]


async def test_find_can_still_skip_ignored(tree: str) -> None:
    fs = AsyncLocalFileSystem()
    found = _rel(tree, await fs._find(tree, hidden=False, no_ignore=False))
    assert "/ignored.txt" not in found
    assert "/.hid/h.txt" not in found
    assert "/top.txt" in found
