"""Codex filesystem listing with a fake client (no codex app-server)."""

from __future__ import annotations

import asyncio
from types import SimpleNamespace
from typing import Any

from upathtools.filesystems.sandbox_filesystems import codex_fs
from upathtools.filesystems.sandbox_filesystems.codex_fs import CodexFS


BOUND = 4
MTIME = 2.0


class _FakeFs:
    def __init__(self, n: int) -> None:
        self.entries = [SimpleNamespace(file_name=f"f{i}", is_directory=i == 0) for i in range(n)]
        self.active = 0
        self.peak = 0

    async def read_directory(self, path: str) -> list[Any]:
        return self.entries

    async def get_metadata(self, path: str) -> Any:
        self.active += 1
        self.peak = max(self.peak, self.active)
        await asyncio.sleep(0.01)
        self.active -= 1
        if path.endswith("f3"):
            raise RuntimeError("gone")
        return SimpleNamespace(created_at_ms=1000, modified_at_ms=2000)


async def test_ls_fetches_metadata_concurrently_and_bounded(monkeypatch) -> None:
    monkeypatch.setattr(codex_fs, "METADATA_CONCURRENCY", BOUND)
    fake = _FakeFs(10)
    fs = CodexFS(client=SimpleNamespace(fs=fake), asynchronous=True, skip_instance_cache=True)
    infos = await fs._ls("/work", detail=True)
    assert [i["name"] for i in infos] == [f"/work/f{i}" for i in range(10)]
    assert infos[0]["type"] == "directory"
    assert infos[1]["mtime"] == MTIME
    assert infos[3]["mtime"] == 0.0  # a failed metadata call falls back to zeros
    assert fake.peak == BOUND
