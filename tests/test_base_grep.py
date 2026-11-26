"""BaseAsyncFileSystem._grep reads files concurrently, keeps file order and limits."""

from __future__ import annotations

import asyncio
from typing import Any

from upathtools.filesystems.base import BaseAsyncFileSystem


class SlowFS(BaseAsyncFileSystem):
    protocol = "slowgreptest"

    def __init__(self, files: dict[str, bytes], **kwargs: Any) -> None:
        super().__init__(**kwargs)
        self.files = files
        self.active = 0
        self.peak = 0

    async def _find(self, path: str, maxdepth: Any = None, withdirs: bool = False, **kw: Any):
        return sorted(self.files)

    async def _cat_file(self, path: str, start: Any = None, end: Any = None, **kw: Any) -> bytes:
        self.active += 1
        self.peak = max(self.peak, self.active)
        await asyncio.sleep(0.01)
        self.active -= 1
        if self.files[path] is None:
            raise PermissionError(path)
        return self.files[path]


async def test_grep_reads_concurrently_in_order() -> None:
    files: dict[str, Any] = {f"/d/f{i:02}.txt": f"hit {i}\nmiss\n".encode() for i in range(20)}
    files["/d/f05.txt"] = None  # unreadable: skipped
    fs = SlowFS(files, skip_instance_cache=True)
    matches = await fs._grep("/d", "hit")
    assert [m.path for m in matches] == [f"f{i:02}.txt" for i in range(20) if i != 5]  # noqa: PLR2004
    assert fs.peak > 1


async def test_grep_max_count() -> None:
    files = {f"/d/f{i:02}.txt": b"hit\nhit\n" for i in range(10)}
    fs = SlowFS(files, skip_instance_cache=True)
    matches = await fs._grep("/d", "hit", max_count=3)
    assert [(m.path, m.line_number) for m in matches] == [
        ("f00.txt", 1),
        ("f00.txt", 2),
        ("f01.txt", 1),
    ]


async def test_fsspec_grep_concurrent_and_ordered() -> None:
    from upathtools.async_ops import fsspec_grep

    files: dict[str, Any] = {f"/d/f{i:02}.txt": f"hit {i}\n".encode() for i in range(20)}
    files["/d/f03.txt"] = None

    class GlobFS(SlowFS):
        async def _glob(self, path: str, **kw: Any):
            return {p: {"name": p, "type": "file"} for p in sorted(self.files)}

    fs = GlobFS(files, skip_instance_cache=True)
    result = await fsspec_grep(fs, "hit", "/d")
    assert list(result["matches"]) == [f"/d/f{i:02}.txt" for i in range(20) if i != 3]  # noqa: PLR2004
    assert fs.peak > 1
