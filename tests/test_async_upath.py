"""Tests for AsyncUPath."""

import pytest

from upathtools import AsyncUPath


async def test_atouch_keeps_existing_content(tmp_path):
    path = AsyncUPath(tmp_path / "file.txt")
    await path.awrite_text("hello")
    await path.atouch()
    assert await path.aread_text() == "hello"
    with pytest.raises(FileExistsError):
        await path.atouch(exist_ok=False)


async def test_atouch_creates_file(tmp_path):
    path = AsyncUPath(tmp_path / "new.txt")
    await path.atouch()
    assert await path.ais_file()


async def test_aunlink_missing_ok(tmp_path):
    path = AsyncUPath(tmp_path / "missing.txt")
    await path.aunlink(missing_ok=True)
    with pytest.raises(FileNotFoundError):
        await path.aunlink()
