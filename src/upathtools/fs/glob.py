"""Glob translation, regex caching, and bulk destination path mapping."""

from functools import lru_cache
import re
from typing import TYPE_CHECKING

from upathtools.fs._sync import FSSPEC_SYNC_VERSION, FsspecSyncNote


if TYPE_CHECKING:
    from collections.abc import Iterable, Sequence


__fsspec_sync__ = FsspecSyncNote(
    version=FSSPEC_SYNC_VERSION,
    upstream=(
        "fsspec/utils.py:_translate",
        "fsspec/utils.py:glob_translate",
        "fsspec/utils.py:common_prefix",
        "fsspec/utils.py:other_paths",
    ),
    notes=(
        "Adds has_magic and thread-safe LRU-cached compile_glob. "
        "common_prefix handles empty iterables by returning ''. "
        "other_paths accepts any Sequence[str] and raises ValueError instead of "
        "AssertionError when destination sequence length mismatches source length."
    ),
)

_MAGIC_RE = re.compile(r"[*?[]")
_SET_OP_RE = re.compile(r"([&~|])")


def has_magic(pattern: str) -> bool:
    """Return True if ``pattern`` contains glob wildcard characters (``*``, ``?``, ``[``)."""
    return _MAGIC_RE.search(pattern) is not None


def _translate(pat: str, star: str, question_mark: str) -> list[str]:
    """Translate a single glob path component into regex fragments."""
    res: list[str] = []
    add = res.append
    i, n = 0, len(pat)
    while i < n:
        c = pat[i]
        i += 1
        if c == "*":
            # Compress consecutive '*' into one.
            if not res or res[-1] is not star:
                add(star)
        elif c == "?":
            add(question_mark)
        elif c == "[":
            j = i
            if j < n and pat[j] == "!":
                j += 1
            if j < n and pat[j] == "]":
                j += 1
            while j < n and pat[j] != "]":
                j += 1
            if j >= n:
                add(r"\[")
            else:
                stuff = pat[i:j]
                if "-" not in stuff:
                    stuff = stuff.replace("\\", r"\\")
                else:
                    chunks: list[str] = []
                    k = i + 2 if pat[i] == "!" else i + 1
                    while True:
                        k = pat.find("-", k, j)
                        if k < 0:
                            break
                        chunks.append(pat[i:k])
                        i = k + 1
                        k += 3
                    chunk = pat[i:j]
                    if chunk:
                        chunks.append(chunk)
                    else:
                        chunks[-1] += "-"
                    # Remove empty ranges -- invalid in RE.
                    for k in range(len(chunks) - 1, 0, -1):
                        if chunks[k - 1][-1] > chunks[k][0]:
                            chunks[k - 1] = chunks[k - 1][:-1] + chunks[k][1:]
                            del chunks[k]
                    # Escape backslashes and hyphens for set difference (--).
                    # Hyphens that create ranges shouldn't be escaped.
                    stuff = "-".join(s.replace("\\", r"\\").replace("-", r"\-") for s in chunks)
                # Escape set operations (&&, ~~ and ||).
                stuff = _SET_OP_RE.sub(r"\\\1", stuff)
                i = j + 1
                if not stuff:
                    # Empty range: never match.
                    add("(?!)")
                elif stuff == "!":
                    # Negated empty range: match any character.
                    add(".")
                else:
                    if stuff[0] == "!":
                        stuff = "^" + stuff[1:]
                    elif stuff[0] in ("^", "["):
                        stuff = "\\" + stuff
                    add(f"[{stuff}]")
        else:
            add(re.escape(c))
    return res


def glob_translate(pattern: str) -> str:
    r"""Translate a POSIX-style glob pattern to a regular expression string.

    Exact port of ``fsspec.utils.glob_translate`` (fixed to ``recursive=True``,
    ``include_hidden=True``, and separator ``"/"`` on all platforms).

    Args:
        pattern: Glob pattern using ``/`` as path separator.

    Returns:
        Regular expression string anchored at the end with ``\Z``.

    Raises:
        ValueError: If ``"**"`` appears inside a path component rather than as the
            entire component.
    """
    any_sep = "/"
    not_sep = "[^/]"
    one_last_segment = f"{not_sep}+"
    one_segment = f"{one_last_segment}{any_sep}"
    any_segments = f"(?:.+{any_sep})?"
    any_last_segments = ".*"
    results: list[str] = []
    parts = pattern.split(any_sep)
    last_part_idx = len(parts) - 1
    for idx, part in enumerate(parts):
        if part == "*":
            results.append(one_segment if idx < last_part_idx else one_last_segment)
            continue
        if part == "**":
            results.append(any_segments if idx < last_part_idx else any_last_segments)
            continue
        if "**" in part:
            raise ValueError("Invalid pattern: '**' can only be an entire path component")
        if part:
            results.extend(_translate(part, f"{not_sep}*", not_sep))
        if idx < last_part_idx:
            results.append(any_sep)
    res = "".join(results)
    return rf"(?s:{res})\Z"


@lru_cache(maxsize=512)
def compile_glob(pattern: str) -> re.Pattern[str]:
    """Translate and compile a glob pattern into a cached regular expression.

    Backed by ``functools.lru_cache`` which is thread-safe in CPython 3.14+.

    Args:
        pattern: Glob pattern using ``/`` as path separator.

    Returns:
        Compiled :class:`re.Pattern` for matching full POSIX paths via ``match()``.
    """
    return re.compile(glob_translate(pattern))


def common_prefix(paths: Iterable[str]) -> str:
    """Return the longest slash-delimited path prefix common to all ``paths``.

    Args:
        paths: Iterable of POSIX-style path strings.

    Returns:
        Longest common directory/component prefix, or ``""`` if none.
    """
    parts = [p.split("/") for p in paths]
    if not parts:
        return ""
    lmax = min(len(p) for p in parts)
    first = parts[0]
    common_len = 0
    for idx in range(lmax):
        if all(p[idx] == first[idx] for p in parts):
            common_len = idx + 1
        else:
            break
    return "/".join(first[:common_len])


def other_paths(
    paths: Sequence[str],
    path2: str | Sequence[str],
    exists: bool = False,
    flatten: bool = False,
) -> list[str]:
    """Map source paths onto destination paths for bulk copy/transfer operations.

    Port of ``fsspec.utils.other_paths``.

    Args:
        paths: Input source paths.
        path2: Destination root path string, or an explicit sequence of destination
            paths with the same length as ``paths``.
        exists: When ``path2`` is a string representing an existing directory,
            preserve the trailing component of the common prefix inside ``path2``.
        flatten: When ``True`` and ``path2`` is a string, place all source basenames
            directly under ``path2``.

    Returns:
        List of destination paths aligned 1:1 with ``paths``.

    Raises:
        ValueError: If ``path2`` is a sequence whose length differs from ``paths``.
    """
    if isinstance(path2, str):
        dest = path2.rstrip("/")
        if flatten:
            return [f"{dest}/{p.split('/')[-1]}" for p in paths]
        if not paths:
            return []
        cp = common_prefix(paths)
        if exists:
            cp = cp.rsplit("/", 1)[0]
        if not cp and all(not s.startswith("/") for s in paths):
            return [f"{dest}/{p}" for p in paths]
        return [p.replace(cp, dest, 1) for p in paths]

    if len(paths) != len(path2):
        raise ValueError(
            f"Destination path count ({len(path2)}) does not match source count ({len(paths)})"
        )
    return list(path2)
