"""Wiki filesystem: how often it pulls (no network: git is faked)."""

import pathlib
import subprocess
from typing import TYPE_CHECKING, Any

from upathtools.filesystems.remote_filesystems.wiki_fs import WikiFileSystem


if TYPE_CHECKING:
    import pytest

TWO_PULLS = 2


def _fake_wiki(monkeypatch: pytest.MonkeyPatch, **kwargs: Any) -> tuple[WikiFileSystem, list]:
    fs = WikiFileSystem(owner="o", repo="r", asynchronous=True, skip_instance_cache=True, **kwargs)
    fs._initialized = True
    (pathlib.Path(fs.temp_dir) / "Home.md").write_text("# Home")
    calls: list[list[str]] = []

    async def run_git(cmd: list[str], check: bool = True) -> subprocess.CompletedProcess:
        calls.append(cmd)
        stdout = "master\n" if cmd[1] == "rev-parse" else ""
        return subprocess.CompletedProcess(cmd, 0, stdout, "")

    monkeypatch.setattr(fs, "_run_git", run_git)
    return fs, calls


def _pulls(calls: list[list[str]]) -> int:
    return sum(1 for cmd in calls if cmd[1] in ("pull", "fetch"))


async def test_two_listings_pull_once(monkeypatch: pytest.MonkeyPatch) -> None:
    fs, calls = _fake_wiki(monkeypatch)
    try:
        assert await fs._ls("", detail=False) == [str(pathlib.Path(fs.temp_dir) / "Home.md")]
        await fs._ls("", detail=False)
        await fs._cat_file("Home.md")
        assert _pulls(calls) == 1
    finally:
        fs.close()


async def test_refresh_pulls_again(monkeypatch: pytest.MonkeyPatch) -> None:
    fs, calls = _fake_wiki(monkeypatch)
    try:
        await fs._ls("", detail=False)
        await fs.refresh()
        assert _pulls(calls) == TWO_PULLS
        await fs._ls("", detail=False)  # refresh() restarted the interval
        assert _pulls(calls) == TWO_PULLS
    finally:
        fs.close()


async def test_pull_interval_zero_pulls_every_time(monkeypatch: pytest.MonkeyPatch) -> None:
    fs, calls = _fake_wiki(monkeypatch, pull_interval=0)
    try:
        await fs._ls("", detail=False)
        await fs._ls("", detail=False)
        assert _pulls(calls) == TWO_PULLS
    finally:
        fs.close()
