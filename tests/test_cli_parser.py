"""Tests for the CLI-style command interface."""

from __future__ import annotations

from upath import UPath

from upathtools.cli_parser import execute_cli


def _setup(tmp_path):
    (tmp_path / "a.py").write_bytes(b"x = 1  # TODO fix\ny = 2\n")
    (tmp_path / "b.txt").write_bytes(b"y = 2\n")
    return UPath(tmp_path)


def test_grep_uses_pattern_argument(tmp_path):
    base = _setup(tmp_path)
    results = list(execute_cli("grep TODO . -r", base))
    matches = [m for r in results for m in r.matches]
    assert [m.line_number for m in matches] == [1]


def test_find_single_dash_options(tmp_path):
    base = _setup(tmp_path)
    found = list(execute_cli("find . -name '*.py' -type f", base))
    assert [UPath(f.path).name for f in found] == ["a.py"]


def test_head_tail_wc_ls(tmp_path):
    base = _setup(tmp_path)
    assert str(execute_cli("head a.py -n 1", base)) == "x = 1  # TODO fix"
    assert str(execute_cli("tail a.py -n 1", base)) == "y = 2"
    assert execute_cli("wc a.py", base).data.lines == 2  # noqa: PLR2004
    assert sorted(e.name for e in execute_cli("ls", base)) == ["a.py", "b.txt"]
