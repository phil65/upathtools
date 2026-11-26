"""Search inside sandboxes with the sandbox's own command-line tools.

Grepping or walking a remote filesystem through file APIs means one request per
file or directory. Running `rg`/`grep`/`find` inside the sandbox does the work
next to the data and returns only the results. These helpers build the shell
scripts and parse their output; each filesystem supplies a way to run a script.

- grep: ripgrep (`rg --json`) when installed, else `grep -rnHIZ -E`
- find: GNU `find -printf` (Debian/Ubuntu based images)
"""

import base64
from dataclasses import dataclass
import json
import shlex
from typing import Any, Literal

from upathtools.filesystems.base import GrepMatch


DEFAULT_GREP_LIMIT = 100
MAX_OUTPUT_BYTES = 10 * 1024 * 1024
"""Upper bound for transferred search output."""

_RG_MARKER = "__upathtools_rg__"
_GREP_MARKER = "__upathtools_grep__"


def build_grep_script(
    path: str,
    pattern: str,
    *,
    max_count: int | None = None,
    case_sensitive: bool | None = None,
    hidden: bool = False,
    no_ignore: bool = False,
    globs: list[str] | None = None,
    multiline: bool = False,
) -> str:
    """Build a POSIX shell script that searches `path` for `pattern`.

    Semantics follow `BaseAsyncFileSystem._grep`: `case_sensitive=None` searches
    case-insensitively, `globs` entries starting with `!` exclude.

    Without ripgrep, `.gitignore` files are not respected (`no_ignore` has no effect)
    and `multiline` is not supported.

    Returns:
        Script printing a marker line, then the search tool's output
    """
    limit = max_count or DEFAULT_GREP_LIMIT
    quoted_pattern = shlex.quote(pattern)
    quoted_path = shlex.quote(path)

    rg_flags = ["--json", "-s" if case_sensitive else "-i"]
    if hidden:
        rg_flags.append("--hidden")
    if no_ignore:
        rg_flags.append("--no-ignore")
    if multiline:
        rg_flags.extend(["-U", "--multiline-dotall"])
    rg_flags.extend(f"-g {shlex.quote(glob)}" for glob in globs or [])
    # rg --json prints begin/end records per file besides each match.
    rg_lines = 3 * limit + 16

    grep_flags = ["-rnHIZ", "-E"]
    if not case_sensitive:
        grep_flags.append("-i")
    if not hidden:
        grep_flags.extend(["--exclude='.*'", "--exclude-dir='.*'"])
    for glob in globs or []:
        if glob.startswith("!"):
            grep_flags.append(f"--exclude={shlex.quote(glob[1:])}")
        else:
            grep_flags.append(f"--include={shlex.quote(glob)}")

    return (
        "if command -v rg >/dev/null 2>&1; then "
        f"echo {_RG_MARKER}; "
        f"rg {' '.join(rg_flags)} -e {quoted_pattern} -- {quoted_path} "
        f"| head -n {rg_lines} | head -c {MAX_OUTPUT_BYTES}; "
        "else "
        f"echo {_GREP_MARKER}; "
        f"grep {' '.join(grep_flags)} -e {quoted_pattern} -- {quoted_path} "
        f"| head -n {limit} | head -c {MAX_OUTPUT_BYTES}; "
        "fi"
    )


def _rg_text(value: dict[str, Any]) -> str:
    if "text" in value:
        return str(value["text"])
    return base64.b64decode(value.get("bytes", "")).decode(errors="replace")


def parse_grep_output(output: str, *, max_count: int | None = None) -> list[GrepMatch]:
    """Parse the output of a script built by `build_grep_script`."""
    limit = max_count or DEFAULT_GREP_LIMIT
    marker, _, body = output.partition("\n")
    matches: list[GrepMatch] = []
    if marker.strip() == _RG_MARKER:
        for line in body.splitlines():
            if not line.startswith("{"):
                continue
            try:
                record = json.loads(line)
            except json.JSONDecodeError:
                continue  # truncated last line
            if record.get("type") != "match":
                continue
            data = record["data"]
            matches.append(
                GrepMatch(
                    path=_rg_text(data["path"]),
                    line_number=int(data["line_number"]),
                    text=_rg_text(data["lines"]).rstrip("\r\n"),
                    submatches=[(int(s["start"]), int(s["end"])) for s in data["submatches"]],
                    absolute_offset=int(data.get("absolute_offset", 0)),
                )
            )
            if len(matches) >= limit:
                break
    elif marker.strip() == _GREP_MARKER:
        for line in body.splitlines():
            # -Z: "<path>\0<line>:<text>"
            file_path, sep, rest = line.partition("\0")
            if not sep:
                continue
            number, _, text = rest.partition(":")
            if not number.isdigit():
                continue
            matches.append(GrepMatch(path=file_path, line_number=int(number), text=text))
            if len(matches) >= limit:
                break
    return matches


@dataclass(frozen=True)
class FoundEntry:
    """A path reported by `find`."""

    path: str
    type: Literal["file", "directory", "link", "other"]
    size: int
    mtime: float
    mode: int


_FIND_TYPES: dict[str, Literal["file", "directory", "link", "other"]] = {
    "f": "file",
    "d": "directory",
    "l": "link",
}


def build_find_script(path: str, *, maxdepth: int | None, withdirs: bool) -> str:
    """Build a script listing everything below `path` (GNU find).

    Output: one NUL-terminated record per entry: type, size, mtime, mode, path,
    separated by tabs.
    """
    args = [shlex.quote(path), "-mindepth", "1"]
    if maxdepth is not None:
        args.extend(["-maxdepth", str(maxdepth)])
    if not withdirs:
        args.extend(["!", "-type", "d"])
    return f"find {' '.join(args)} -printf '%y\\t%s\\t%T@\\t%m\\t%p\\0'"


def parse_find_output(output: str) -> list[FoundEntry]:
    """Parse the output of a script built by `build_find_script`."""
    entries: list[FoundEntry] = []
    for record in output.split("\0"):
        if not record:
            continue
        parts = record.split("\t", 4)
        if len(parts) != 5:  # noqa: PLR2004
            continue
        kind, size, mtime, mode, entry_path = parts
        entries.append(
            FoundEntry(
                path=entry_path.lstrip("\n"),
                type=_FIND_TYPES.get(kind, "other"),
                size=int(size) if size.isdigit() else 0,
                mtime=float(mtime) if mtime else 0.0,
                mode=int(mode, 8) if mode else 0,
            )
        )
    return entries
