"""DelegatingFileSystem drops parsed views when the underlying file changes."""

import fsspec

from upathtools.filesystems.delegating_fs import DelegatingFileSystem


def _memory():
    fs = fsspec.filesystem("memory", skip_instance_cache=True)
    fs.store = {}
    fs.pseudo_dirs = [""]
    return fs


async def _sections(fs: DelegatingFileSystem, path: str) -> list[str]:
    return sorted(e.rsplit("::", 1)[-1] for e in await fs._ls(path + "::", detail=False))


async def test_rewrite_invalidates_parsed_view() -> None:
    fs = DelegatingFileSystem(_memory())
    await fs._pipe_file("/doc.md", b"# Intro\nold text\n")
    assert await _sections(fs, "/doc.md") == ["Intro"]
    await fs._pipe_file("/doc.md", b"# Intro\nnew text\n\n# Added\nmore\n")
    assert await _sections(fs, "/doc.md") == ["Added", "Intro"]


async def test_copy_target_and_recursive_rm_invalidate() -> None:
    fs = DelegatingFileSystem(_memory())
    await fs._pipe_file("/a.md", b"# A\n")
    await fs._pipe_file("/dir/b.md", b"# B\n")
    assert await _sections(fs, "/dir/b.md") == ["B"]
    await fs._cp_file("/a.md", "/dir/b.md")
    assert await _sections(fs, "/dir/b.md") == ["A"]
    await fs._rm("/dir", recursive=True)
    await fs._pipe_file("/dir/b.md", b"# C\n")
    assert await _sections(fs, "/dir/b.md") == ["C"]
