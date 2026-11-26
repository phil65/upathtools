import importlib.util
from typing import TYPE_CHECKING
import zipfile

from fsspec import filesystem
import pytest

import upathtools
from upathtools.filesystems.auto_expand_fs import AutoExpandFS, main_protocol


if TYPE_CHECKING:
    from pathlib import Path


@pytest.fixture
def workspace(tmp_path: Path) -> Path:
    (tmp_path / "a.txt").write_text("x")
    (tmp_path / "README.md").write_text(
        "# Install\n\npip install x\n\n# Usage\n\nRun it.\n\n## Options\n\n--fast\n"
    )
    (tmp_path / "app.py").write_text(
        "class App:\n    def run(self):\n        pass\n\n\ndef main():\n    App().run()\n"
    )
    with zipfile.ZipFile(tmp_path / "pack.zip", "w") as archive:
        archive.writestr("inner/b.txt", "hello")
    return tmp_path


def names(paths: list[str], root: Path) -> list[str]:
    # fsspec writes paths with forward slashes, also on Windows
    return sorted(path.replace("\\", "/").removeprefix(f"{root.as_posix()}/") for path in paths)


def test_sync_use_over_a_local_directory(workspace: Path) -> None:
    """The wrapped local fs is async inside; blocking calls must still work."""
    fs = AutoExpandFS(fs=filesystem("file"), asynchronous=False)
    assert names(fs.ls(str(workspace), detail=False), workspace) == [
        "README.md",
        "a.txt",
        "app.py",
        "pack.zip",
    ]
    assert fs.cat_file(f"{workspace}/a.txt") == b"x"


def test_archive_entries_keep_their_outer_path(workspace: Path) -> None:
    fs = AutoExpandFS(fs=filesystem("file"), asynchronous=False)
    assert fs.isdir(f"{workspace}/pack.zip")
    listed = fs.ls(f"{workspace}/pack.zip", detail=False)
    assert names(listed, workspace) == ["pack.zip/inner"]
    assert names(fs.ls(listed[0], detail=False), workspace) == ["pack.zip/inner/b.txt"]
    assert fs.cat_file(f"{workspace}/pack.zip/inner/b.txt") == b"hello"


def test_markdown_expander(workspace: Path) -> None:
    upathtools.register_all_filesystems()
    expanders = {".md": ("md", lambda fs, path: {"fo": path, "target_protocol": main_protocol(fs)})}
    fs = AutoExpandFS(fs=filesystem("file"), expanders=expanders, asynchronous=False)
    sections = fs.ls(f"{workspace}/README.md", detail=False)
    assert names(sections, workspace) == ["README.md/Install", "README.md/Usage"]
    assert fs.cat_file(f"{workspace}/README.md/Usage").startswith(b"# Usage")
    # subsections list under their section, and open from there
    nested = fs.ls(f"{workspace}/README.md/Usage", detail=False)
    assert names(nested, workspace) == ["README.md/Usage/Options"]
    assert fs.cat_file(nested[0]).startswith(b"## Options")


@pytest.mark.skipif(
    importlib.util.find_spec("tree_sitter_python") is None, reason="needs upathtools[tree-sitter]"
)
def test_code_expander(workspace: Path) -> None:
    upathtools.register_all_filesystems()
    expanders = {
        ".py": ("ts", lambda fs, path: {"source_file": path, "target_protocol": main_protocol(fs)})
    }
    fs = AutoExpandFS(fs=filesystem("file"), expanders=expanders, asynchronous=False)
    assert names(fs.ls(f"{workspace}/app.py", detail=False), workspace) == [
        "app.py/App",
        "app.py/main",
    ]
    assert names(fs.ls(f"{workspace}/app.py/App", detail=False), workspace) == ["app.py/App/run"]
    assert fs.cat_file(f"{workspace}/app.py/App").startswith(b"class App")


async def test_async_use(workspace: Path) -> None:
    fs = AutoExpandFS(fs=filesystem("file"), asynchronous=True)
    assert await fs._isdir(f"{workspace}/pack.zip")
    assert await fs._cat_file(f"{workspace}/pack.zip/inner/b.txt") == b"hello"
