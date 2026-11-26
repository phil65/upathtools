"""helpers.copy / clean_directory / to_upath / upath_to_fs on common inputs."""

from __future__ import annotations

from typing import TYPE_CHECKING
import zipfile

import pytest
from upath import UPath

from upathtools.helpers import clean_directory, copy, to_upath, upath_to_fs


if TYPE_CHECKING:
    from pathlib import Path


@pytest.fixture
def src(tmp_path: Path) -> Path:
    (tmp_path / "src" / "sub").mkdir(parents=True)
    (tmp_path / "src" / "a.txt").write_text("a")
    (tmp_path / "src" / "sub" / "b.txt").write_text("b")
    return tmp_path / "src"


def test_copy_folder_into_existing_folder(src: Path, tmp_path: Path) -> None:
    dst = tmp_path / "dst"
    dst.mkdir()
    copy(src, dst)
    assert (dst / "a.txt").read_text() == "a"
    assert (dst / "sub" / "b.txt").read_text() == "b"


def test_copy_folder_onto_file_fails(src: Path, tmp_path: Path) -> None:
    target = tmp_path / "file.txt"
    target.write_text("x")
    with pytest.raises(RuntimeError, match="folder to file"):
        copy(src, target)


def test_copy_file_to_new_name_with_existing_parent(src: Path, tmp_path: Path) -> None:
    (tmp_path / "dst").mkdir()
    copy(src / "a.txt", tmp_path / "dst" / "new.txt", exist_ok=False)
    assert (tmp_path / "dst" / "new.txt").read_text() == "a"
    with pytest.raises(FileExistsError):
        copy(src / "a.txt", tmp_path / "dst" / "new.txt", exist_ok=False)


def test_clean_relative_directory(src: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.chdir(src.parent)
    (src / ".keep").write_text("")
    clean_directory("src")
    assert sorted(p.name for p in src.iterdir()) == [".keep"]
    clean_directory("src", remove_hidden=True)
    assert list(src.iterdir()) == []


def test_to_upath_keeps_storage_options_for_strings() -> None:
    assert to_upath("memory:///x", foo="bar").storage_options["foo"] == "bar"


def test_upath_to_fs_keeps_storage_options(tmp_path: Path) -> None:
    archive = tmp_path / "a.zip"
    with zipfile.ZipFile(archive, "w") as zf:
        zf.writestr("inner/f.txt", "zipped")
    fs = upath_to_fs(UPath("zip://inner", fo=str(archive)), asynchronous=False)
    assert fs.cat("f.txt") == b"zipped"
