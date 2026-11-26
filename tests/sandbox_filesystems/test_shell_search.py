"""Tests for the sandbox shell search helpers, run against local rg/grep/find."""

import os
from pathlib import Path
import shutil
import subprocess
import sys

import pytest

from upathtools.filesystems.sandbox_filesystems._shell_search import (
    build_find_script,
    build_grep_script,
    parse_find_output,
    parse_grep_output,
)


pytestmark = pytest.mark.skipif(
    sys.platform != "linux",
    reason="the helpers target Linux sandboxes (GNU grep/find); tests use host tools",
)


def run(script: str, *, without_rg: bool = False, tool_dir: Path | None = None) -> str:
    env = dict(os.environ)
    if without_rg:
        assert tool_dir is not None
        env["PATH"] = str(tool_dir)
    result = subprocess.run(["sh", "-c", script], capture_output=True, env=env, check=False)
    return result.stdout.decode()


@pytest.fixture
def tool_dir(tmp_path_factory: pytest.TempPathFactory) -> Path:
    """A PATH containing only the tools the grep fallback needs (no rg)."""
    directory = tmp_path_factory.mktemp("bin")
    for tool in ("sh", "grep", "head"):
        target = shutil.which(tool)
        assert target is not None
        (directory / tool).symlink_to(target)
    return directory


@pytest.fixture
def tree(tmp_path: Path) -> Path:
    (tmp_path / "src").mkdir()
    (tmp_path / "src" / "a.py").write_text("import os\nTODO: fix\nvalue = 'it''s'\n")
    (tmp_path / "src" / "b.txt").write_text("todo lowercase\nnothing\n")
    (tmp_path / "src" / "with space.py").write_text("x = 1  # TODO later\n")
    (tmp_path / ".hidden").mkdir()
    (tmp_path / ".hidden" / "c.py").write_text("TODO hidden\n")
    (tmp_path / "bin.dat").write_bytes(b"\x00\x01TODO\x00")
    return tmp_path


TOOLS = [
    pytest.param(
        False, id="rg", marks=pytest.mark.skipif(shutil.which("rg") is None, reason="no rg")
    ),
    pytest.param(True, id="grep"),
]


@pytest.mark.parametrize("without_rg", TOOLS)
def test_grep_case_insensitive_by_default(tree: Path, tool_dir: Path, without_rg: bool) -> None:
    script = build_grep_script(str(tree), "todo")
    matches = parse_grep_output(run(script, without_rg=without_rg, tool_dir=tool_dir))
    found = {(Path(m.path).name, m.line_number) for m in matches}
    assert found == {("a.py", 2), ("b.txt", 1), ("with space.py", 1)}
    assert all(m.text.lower().count("todo") for m in matches)


@pytest.mark.parametrize("without_rg", TOOLS)
def test_grep_case_sensitive_globs_and_hidden(tree: Path, tool_dir: Path, without_rg: bool) -> None:
    script = build_grep_script(str(tree), "TODO", case_sensitive=True, globs=["*.py"])
    names = {
        Path(m.path).name
        for m in parse_grep_output(run(script, without_rg=without_rg, tool_dir=tool_dir))
    }
    assert names == {"a.py", "with space.py"}
    script = build_grep_script(str(tree), "TODO", case_sensitive=True, hidden=True, globs=["*.py"])
    names = {
        Path(m.path).name
        for m in parse_grep_output(run(script, without_rg=without_rg, tool_dir=tool_dir))
    }
    assert names == {"a.py", "with space.py", "c.py"}
    script = build_grep_script(str(tree), "TODO", globs=["!*.py"])
    names = {
        Path(m.path).name
        for m in parse_grep_output(run(script, without_rg=without_rg, tool_dir=tool_dir))
    }
    assert names == {"b.txt"}


@pytest.mark.parametrize("without_rg", TOOLS)
def test_grep_limit_and_quoting(tree: Path, tool_dir: Path, without_rg: bool) -> None:
    script = build_grep_script(str(tree), "it''s|import", max_count=1)
    matches = parse_grep_output(run(script, without_rg=without_rg, tool_dir=tool_dir), max_count=1)
    assert len(matches) == 1
    assert Path(matches[0].path).name == "a.py"


def test_grep_rg_submatches(tree: Path) -> None:
    if shutil.which("rg") is None:
        pytest.skip("no rg")
    matches = parse_grep_output(run(build_grep_script(str(tree / "src" / "a.py"), "fix")))
    assert matches[0].submatches == [(6, 9)]


def test_grep_missing_path_returns_no_matches(tmp_path: Path) -> None:
    assert parse_grep_output(run(build_grep_script(str(tmp_path / "missing"), "x"))) == []


def test_find_lists_files_with_metadata(tree: Path) -> None:
    entries = parse_find_output(run(build_find_script(str(tree), maxdepth=None, withdirs=False)))
    by_name = {Path(e.path).name: e for e in entries}
    assert set(by_name) == {"a.py", "b.txt", "with space.py", "c.py", "bin.dat"}
    assert by_name["b.txt"].size == len("todo lowercase\nnothing\n")
    assert by_name["b.txt"].type == "file"
    assert by_name["b.txt"].mtime > 0
    assert by_name["b.txt"].mode & 0o400


def test_find_depth_and_dirs(tree: Path) -> None:
    entries = parse_find_output(run(build_find_script(str(tree), maxdepth=1, withdirs=True)))
    by_name = {Path(e.path).name: e.type for e in entries}
    assert by_name == {"src": "directory", ".hidden": "directory", "bin.dat": "file"}
