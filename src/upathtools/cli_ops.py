"""CLI-like filesystem operations.

This module provides async-first functions that emulate common CLI commands.
All async functions have sync wrappers for convenience.
"""

from __future__ import annotations

import asyncio
from dataclasses import dataclass, field
import fnmatch
import re
import stat
from typing import TYPE_CHECKING, Any, Literal, overload


if TYPE_CHECKING:
    from collections.abc import AsyncIterator, Iterator
    from re import Pattern

    from upath import UPath


__all__ = [
    "DuEntry",
    "FindResult",
    "GrepMatch",
    "GrepResult",
    "LsEntry",
    "WcResult",
    "acat",
    "acat_bytes",
    "acp",
    "adiff",
    "adu",
    "afind",
    "agrep",
    "ahead",
    "als",
    "amkdir",
    "amv",
    "arm",
    "atail",
    "atouch",
    "awc",
    "cat",
    "cat_bytes",
    "cp",
    "diff",
    "du",
    "find",
    "grep",
    "head",
    "ls",
    "mkdir",
    "mv",
    "rm",
    "tail",
    "touch",
    "wc",
]


@dataclass
class GrepMatch:
    """A single grep match result."""

    path: str
    line_number: int
    line: str
    match_start: int
    match_end: int

    def __str__(self) -> str:
        return f"{self.path}:{self.line_number}:{self.line}"


@dataclass
class GrepResult:
    """Result of a grep operation on a single file."""

    path: str
    matches: list[GrepMatch] = field(default_factory=list)
    error: str | None = None

    @property
    def match_count(self) -> int:
        return len(self.matches)

    def __bool__(self) -> bool:
        return bool(self.matches)


@dataclass
class FindResult:
    """Result of a find operation."""

    path: str
    type: Literal["file", "directory", "unknown"]
    size: int | None = None
    mtime: float | None = None


@dataclass
class WcResult:
    """Result of a word count operation."""

    path: str
    lines: int
    words: int
    chars: int
    bytes_: int

    def __str__(self) -> str:
        return f"{self.lines:8d} {self.words:8d} {self.chars:8d} {self.path}"


@dataclass
class LsEntry:
    """Entry from ls operation."""

    name: str
    path: str
    type: Literal["file", "directory", "unknown"]
    size: int | None = None
    mtime: float | None = None
    mode: int | None = None
    size_human: str | None = None


@dataclass
class DuEntry:
    """Entry from du operation."""

    path: str
    size: int
    size_human: str | None = None


def _resolve_path(path: str, base: UPath) -> UPath:
    """Resolve a string path relative to base."""
    if not path or path == ".":
        return base
    if path.startswith("/"):
        return type(base)(path, protocol=base.protocol, **base.storage_options)
    return base / path


# ==================== Async implementations ====================


async def agrep(
    pattern: str,
    path: str,
    base: UPath,
    *,
    recursive: bool = False,
    ignore_case: bool = False,
    invert_match: bool = False,
    whole_word: bool = False,
    fixed_string: bool = False,
    max_count: int | None = None,
    include: str | None = None,
    exclude: str | None = None,
    context_before: int = 0,
    context_after: int = 0,
    binary_files: Literal["binary", "text", "skip"] = "skip",
    encoding: str = "utf-8",
    errors: str = "replace",
) -> AsyncIterator[GrepResult]:
    """Search for pattern in files asynchronously."""
    regex_pattern = re.escape(pattern) if fixed_string else pattern

    if whole_word:
        regex_pattern = rf"\b{regex_pattern}\b"

    flags = re.IGNORECASE if ignore_case else 0
    compiled = re.compile(regex_pattern, flags)

    resolved = _resolve_path(path, base)

    async for file_path in _aget_files_to_search(resolved, recursive, include, exclude):
        yield await _agrep_file(
            compiled,
            file_path,
            invert_match=invert_match,
            max_count=max_count,
            context_before=context_before,
            context_after=context_after,
            binary_files=binary_files,
            encoding=encoding,
            errors=errors,
        )


async def _aget_files_to_search(
    path: UPath,
    recursive: bool,
    include: str | None,
    exclude: str | None,
) -> AsyncIterator[UPath]:
    """Get list of files to search based on options."""
    try:
        stat_result = await asyncio.to_thread(path.stat)
    except (OSError, ValueError):
        return

    mode = stat_result.st_mode
    if stat.S_ISREG(mode):
        if _matches_filters(path, include, exclude):
            yield path
        return

    if not stat.S_ISDIR(mode):
        return

    glob_pattern = "**/*" if recursive else "*"
    items = await asyncio.to_thread(list, path.glob(glob_pattern))
    for item in items:
        try:
            item_stat = await asyncio.to_thread(item.stat)
        except (OSError, ValueError):
            continue
        if stat.S_ISREG(item_stat.st_mode) and _matches_filters(item, include, exclude):
            yield item


def _matches_filters(path: UPath, include: str | None, exclude: str | None) -> bool:
    """Check if path matches include/exclude filters."""
    name = path.name
    if include and not fnmatch.fnmatch(name, include):
        return False
    return not (exclude and fnmatch.fnmatch(name, exclude))


def _is_binary(data: bytes) -> bool:
    """Check if data appears to be binary."""
    if b"\x00" in data[:8192]:
        return True
    text_chars = set(bytes(range(32, 127)) + b"\n\r\t\f\b")
    non_text = sum(1 for byte in data[:8192] if byte not in text_chars)
    return non_text > len(data[:8192]) * 0.3


async def _agrep_file(
    pattern: Pattern[str],
    path: UPath,
    *,
    invert_match: bool,
    max_count: int | None,
    context_before: int,
    context_after: int,
    binary_files: Literal["binary", "text", "skip"],
    encoding: str,
    errors: str,
) -> GrepResult:
    """Search a single file for pattern matches."""
    path_str = str(path)
    result = GrepResult(path=path_str)

    try:
        data = await asyncio.to_thread(path.read_bytes)

        if _is_binary(data):
            match binary_files:
                case "skip":
                    return result
                case "binary":
                    result.error = "Binary file matches"
                    return result
                case "text":
                    pass

        text = data.decode(encoding, errors=errors)
        lines = text.splitlines()

        context_buffer: list[tuple[int, str]] = []
        pending_after = 0
        match_count = 0

        for line_num, line in enumerate(lines, 1):
            match = pattern.search(line)
            is_match = bool(match) != invert_match

            if is_match:
                if context_before > 0:
                    for ctx_num, ctx_line in context_buffer:
                        if not any(m.line_number == ctx_num for m in result.matches):
                            result.matches.append(
                                GrepMatch(
                                    path=path_str,
                                    line_number=ctx_num,
                                    line=ctx_line,
                                    match_start=-1,
                                    match_end=-1,
                                )
                            )

                result.matches.append(
                    GrepMatch(
                        path=path_str,
                        line_number=line_num,
                        line=line,
                        match_start=match.start() if match else 0,
                        match_end=match.end() if match else len(line),
                    )
                )

                match_count += 1
                pending_after = context_after

                if max_count and match_count >= max_count:
                    break
            else:
                if pending_after > 0:
                    result.matches.append(
                        GrepMatch(
                            path=path_str,
                            line_number=line_num,
                            line=line,
                            match_start=-1,
                            match_end=-1,
                        )
                    )
                    pending_after -= 1

                if context_before > 0:
                    context_buffer.append((line_num, line))
                    if len(context_buffer) > context_before:
                        context_buffer.pop(0)

    except Exception as e:  # noqa: BLE001
        result.error = str(e)

    return result


async def afind(
    path: str,
    base: UPath,
    *,
    name: str | None = None,
    iname: str | None = None,
    type_: Literal["f", "d", "file", "directory"] | None = None,
    maxdepth: int | None = None,
    mindepth: int | None = None,
    size_min: int | None = None,
    size_max: int | None = None,
    regex: str | None = None,
    newer_than: float | None = None,
    older_than: float | None = None,
) -> AsyncIterator[FindResult]:
    """Find files matching criteria asynchronously."""
    compiled_regex = re.compile(regex) if regex else None
    resolved = _resolve_path(path, base)

    async for item, depth in _awalk_with_depth(resolved, maxdepth):
        if mindepth is not None and depth < mindepth:
            continue

        # Single stat call to get all file info
        try:
            stat_result = await asyncio.to_thread(item.stat)
        except (OSError, ValueError):
            continue

        mode = stat_result.st_mode
        is_file = stat.S_ISREG(mode)
        is_dir = stat.S_ISDIR(mode)
        size = stat_result.st_size
        mtime = stat_result.st_mtime

        item_type: Literal["file", "directory", "unknown"] = (
            "file" if is_file else "directory" if is_dir else "unknown"
        )

        if type_ is not None:
            want_file = type_ in ("f", "file")
            want_dir = type_ in ("d", "directory")
            if want_file and not is_file:
                continue
            if want_dir and not is_dir:
                continue

        item_name = item.name
        if name and not fnmatch.fnmatch(item_name, name):
            continue
        if iname and not fnmatch.fnmatch(item_name.lower(), iname.lower()):
            continue

        if compiled_regex and not compiled_regex.search(str(item)):
            continue

        if size_min is not None and size < size_min:
            continue
        if size_max is not None and size > size_max:
            continue
        if newer_than is not None and mtime < newer_than:
            continue
        if older_than is not None and mtime > older_than:
            continue

        yield FindResult(path=str(item), type=item_type, size=size, mtime=mtime)


async def _awalk_with_depth(
    path: UPath,
    maxdepth: int | None = None,
    current_depth: int = 0,
) -> AsyncIterator[tuple[UPath, int]]:
    """Walk directory tree asynchronously."""
    yield path, current_depth

    if maxdepth is not None and current_depth >= maxdepth:
        return

    try:
        stat_result = await asyncio.to_thread(path.stat)
    except (OSError, ValueError):
        return

    if stat.S_ISDIR(stat_result.st_mode):
        try:
            children = await asyncio.to_thread(list, path.iterdir())
            for child in children:
                async for item in _awalk_with_depth(child, maxdepth, current_depth + 1):
                    yield item
        except PermissionError:
            pass


@overload
async def ahead(
    path: str,
    base: UPath,
    n: int = 10,
    *,
    encoding: str = "utf-8",
    errors: str = "replace",
    as_bytes: Literal[False] = False,
) -> str: ...


@overload
async def ahead(
    path: str,
    base: UPath,
    n: int = 10,
    *,
    encoding: str = "utf-8",
    errors: str = "replace",
    as_bytes: Literal[True],
) -> bytes: ...


async def ahead(
    path: str,
    base: UPath,
    n: int = 10,
    *,
    encoding: str = "utf-8",
    errors: str = "replace",
    as_bytes: bool = False,
) -> str | bytes:
    """Get first n lines of a file asynchronously."""
    resolved = _resolve_path(path, base)
    data = await asyncio.to_thread(resolved.read_bytes)

    if as_bytes:
        byte_lines = data.split(b"\n")[:n]
        return b"\n".join(byte_lines)

    text = data.decode(encoding, errors=errors)
    str_lines = text.splitlines()[:n]
    return "\n".join(str_lines)


@overload
async def atail(
    path: str,
    base: UPath,
    n: int = 10,
    *,
    encoding: str = "utf-8",
    errors: str = "replace",
    as_bytes: Literal[False] = False,
) -> str: ...


@overload
async def atail(
    path: str,
    base: UPath,
    n: int = 10,
    *,
    encoding: str = "utf-8",
    errors: str = "replace",
    as_bytes: Literal[True],
) -> bytes: ...


async def atail(
    path: str,
    base: UPath,
    n: int = 10,
    *,
    encoding: str = "utf-8",
    errors: str = "replace",
    as_bytes: bool = False,
) -> str | bytes:
    """Get last n lines of a file asynchronously."""
    resolved = _resolve_path(path, base)
    data = await asyncio.to_thread(resolved.read_bytes)

    if as_bytes:
        byte_lines = data.split(b"\n")[-n:]
        return b"\n".join(byte_lines)

    text = data.decode(encoding, errors=errors)
    str_lines = text.splitlines()[-n:]
    return "\n".join(str_lines)


async def acat(
    *paths: str,
    base: UPath,
    encoding: str = "utf-8",
    errors: str = "replace",
) -> str:
    """Concatenate and return file contents asynchronously."""
    parts: list[str] = []
    for p in paths:
        resolved = _resolve_path(p, base)
        data = await asyncio.to_thread(resolved.read_bytes)
        parts.append(data.decode(encoding, errors=errors))
    return "".join(parts)


async def acat_bytes(*paths: str, base: UPath) -> bytes:
    """Concatenate and return file contents as bytes asynchronously."""
    parts: list[bytes] = []
    for p in paths:
        resolved = _resolve_path(p, base)
        data = await asyncio.to_thread(resolved.read_bytes)
        parts.append(data)
    return b"".join(parts)


async def awc(
    path: str,
    base: UPath,
    *,
    encoding: str = "utf-8",
    errors: str = "replace",
) -> WcResult:
    """Count lines, words, and characters asynchronously."""
    resolved = _resolve_path(path, base)
    data = await asyncio.to_thread(resolved.read_bytes)
    text = data.decode(encoding, errors=errors)

    lines = text.count("\n")
    if text and not text.endswith("\n"):
        lines += 1

    words = len(text.split())
    chars = len(text)
    bytes_ = len(data)

    return WcResult(path=str(resolved), lines=lines, words=words, chars=chars, bytes_=bytes_)


async def als(
    path: str,
    base: UPath,
    *,
    all_: bool = False,
    long: bool = False,
    human_readable: bool = False,
    recursive: bool = False,
    sort_by: Literal["name", "size", "mtime"] = "name",
    reverse: bool = False,
) -> list[LsEntry]:
    """List directory contents asynchronously."""
    resolved = _resolve_path(path, base)
    items = await _als_collect(resolved, all_, recursive, long)

    match sort_by:
        case "name":
            items.sort(key=lambda x: x.name, reverse=reverse)
        case "size":
            items.sort(key=lambda x: x.size or 0, reverse=reverse)
        case "mtime":
            items.sort(key=lambda x: x.mtime or 0, reverse=reverse)

    if human_readable:
        for item in items:
            if item.size is not None:
                item.size_human = _human_readable_size(item.size)

    return items


async def _als_collect(
    path: UPath,
    all_: bool,
    recursive: bool,
    detailed: bool,
) -> list[LsEntry]:
    """Collect items for ls asynchronously."""
    items: list[LsEntry] = []

    try:
        stat_result = await asyncio.to_thread(path.stat)
    except (OSError, ValueError):
        return items

    if not stat.S_ISDIR(stat_result.st_mode):
        entry = await _amake_ls_entry(path, detailed)
        if entry:
            items.append(entry)
        return items

    try:
        children = await asyncio.to_thread(list, path.iterdir())
        for child in children:
            name = child.name
            if not all_ and name.startswith("."):
                continue

            entry = await _amake_ls_entry(child, detailed)
            if entry:
                items.append(entry)

                if recursive and entry.type == "directory":
                    items.extend(await _als_collect(child, all_, recursive, detailed))

    except PermissionError:
        pass

    return items


async def _amake_ls_entry(path: UPath, detailed: bool) -> LsEntry | None:
    """Create an LsEntry from a path asynchronously."""
    try:
        stat_result = await asyncio.to_thread(path.stat)
    except (OSError, ValueError):
        return None

    file_mode = stat_result.st_mode
    is_dir = stat.S_ISDIR(file_mode)
    is_file = stat.S_ISREG(file_mode)
    item_type: Literal["file", "directory", "unknown"] = (
        "directory" if is_dir else "file" if is_file else "unknown"
    )

    size: int | None = None
    mtime: float | None = None
    mode: int | None = None

    if detailed:
        size = stat_result.st_size
        mtime = stat_result.st_mtime
        mode = file_mode

    return LsEntry(
        name=path.name,
        path=str(path),
        type=item_type,
        size=size,
        mtime=mtime,
        mode=mode,
    )


def _human_readable_size(size: int) -> str:
    """Convert bytes to human readable format."""
    for unit in ("B", "K", "M", "G", "T", "P"):
        if abs(size) < 1024:  # noqa: PLR2004
            return f"{size:3.1f}{unit}" if unit != "B" else f"{size}{unit}"
        size = int(size / 1024)
    return f"{size}E"


async def adiff(
    path1: str,
    path2: str,
    base: UPath,
    *,
    encoding: str = "utf-8",
    errors: str = "replace",
    context_lines: int = 3,
) -> str:
    """Compare two files asynchronously."""
    import difflib

    resolved1 = _resolve_path(path1, base)
    resolved2 = _resolve_path(path2, base)

    text1 = (await asyncio.to_thread(resolved1.read_bytes)).decode(encoding, errors=errors)
    text2 = (await asyncio.to_thread(resolved2.read_bytes)).decode(encoding, errors=errors)

    lines1 = text1.splitlines(keepends=True)
    lines2 = text2.splitlines(keepends=True)

    diff_lines = difflib.unified_diff(
        lines1,
        lines2,
        fromfile=str(resolved1),
        tofile=str(resolved2),
        n=context_lines,
    )

    return "".join(diff_lines)


async def atouch(
    path: str,
    base: UPath,
    *,
    exist_ok: bool = True,
    parents: bool = False,
) -> None:
    """Create empty file or update timestamp asynchronously."""
    resolved = _resolve_path(path, base)

    if parents:
        await asyncio.to_thread(resolved.parent.mkdir, parents=True, exist_ok=True)

    exists = await asyncio.to_thread(resolved.exists)
    if exists:
        if not exist_ok:
            msg = f"File exists: {resolved}"
            raise FileExistsError(msg)
        try:
            await asyncio.to_thread(resolved.touch, exist_ok=True)
        except (NotImplementedError, AttributeError):
            data = await asyncio.to_thread(resolved.read_bytes)
            await asyncio.to_thread(resolved.write_bytes, data)
    else:
        await asyncio.to_thread(resolved.write_bytes, b"")


async def amkdir(
    path: str,
    base: UPath,
    *,
    parents: bool = False,
    exist_ok: bool = False,
    mode: int = 0o755,
) -> None:
    """Create directory asynchronously."""
    resolved = _resolve_path(path, base)
    await asyncio.to_thread(resolved.mkdir, parents=parents, exist_ok=exist_ok, mode=mode)


async def arm(
    path: str,
    base: UPath,
    *,
    recursive: bool = False,
    force: bool = False,
) -> None:
    """Remove file or directory asynchronously."""
    resolved = _resolve_path(path, base)

    try:
        stat_result = await asyncio.to_thread(resolved.stat)
    except (OSError, ValueError):
        if force:
            return
        msg = f"Path not found: {resolved}"
        raise FileNotFoundError(msg) from None

    if stat.S_ISDIR(stat_result.st_mode):
        if not recursive:
            msg = f"Cannot remove directory without recursive=True: {resolved}"
            raise IsADirectoryError(msg)
        children = await asyncio.to_thread(list, resolved.iterdir())
        for child in children:
            await arm(
                str(child),
                base=type(resolved)("/", protocol=resolved.protocol, **resolved.storage_options),
                recursive=True,
                force=force,
            )
        await asyncio.to_thread(resolved.rmdir)
    else:
        await asyncio.to_thread(resolved.unlink)


async def acp(
    src: str,
    dst: str,
    base: UPath,
    *,
    recursive: bool = False,
    force: bool = False,
) -> None:
    """Copy file or directory asynchronously."""
    src_resolved = _resolve_path(src, base)
    dst_resolved = _resolve_path(dst, base)

    try:
        src_stat = await asyncio.to_thread(src_resolved.stat)
    except (OSError, ValueError):
        msg = f"Source not found: {src_resolved}"
        raise FileNotFoundError(msg) from None

    try:
        await asyncio.to_thread(dst_resolved.stat)
        # Destination exists
        if not force:
            msg = f"Destination exists: {dst_resolved}"
            raise FileExistsError(msg)
    except (OSError, ValueError):
        pass  # Destination doesn't exist, that's fine

    if stat.S_ISDIR(src_stat.st_mode):
        if not recursive:
            msg = f"Cannot copy directory without recursive=True: {src_resolved}"
            raise IsADirectoryError(msg)

        await asyncio.to_thread(dst_resolved.mkdir, parents=True, exist_ok=True)
        children = await asyncio.to_thread(list, src_resolved.iterdir())
        for child in children:
            await acp(
                str(child),
                str(dst_resolved / child.name),
                base=type(base)("/", protocol=base.protocol, **base.storage_options),
                recursive=True,
                force=force,
            )
    else:
        await asyncio.to_thread(dst_resolved.parent.mkdir, parents=True, exist_ok=True)
        data = await asyncio.to_thread(src_resolved.read_bytes)
        await asyncio.to_thread(dst_resolved.write_bytes, data)


async def amv(
    src: str,
    dst: str,
    base: UPath,
    *,
    force: bool = False,
) -> None:
    """Move file or directory asynchronously."""
    src_resolved = _resolve_path(src, base)
    dst_resolved = _resolve_path(dst, base)

    try:
        src_stat = await asyncio.to_thread(src_resolved.stat)
    except (OSError, ValueError):
        msg = f"Source not found: {src_resolved}"
        raise FileNotFoundError(msg) from None

    try:
        await asyncio.to_thread(dst_resolved.stat)
        # Destination exists
        if not force:
            msg = f"Destination exists: {dst_resolved}"
            raise FileExistsError(msg)
    except (OSError, ValueError):
        pass  # Destination doesn't exist, that's fine

    try:
        await asyncio.to_thread(src_resolved.rename, dst_resolved)
    except (OSError, NotImplementedError):
        src_is_dir = stat.S_ISDIR(src_stat.st_mode)
        await acp(src, dst, base, recursive=src_is_dir, force=force)
        await arm(src, base, recursive=True, force=True)


async def adu(
    path: str,
    base: UPath,
    *,
    human_readable: bool = False,
    summarize: bool = False,
    max_depth: int | None = None,
) -> list[DuEntry]:
    """Estimate file space usage asynchronously."""
    resolved = _resolve_path(path, base)
    sizes: dict[str, int] = {}

    async def _calc_size(p: UPath, depth: int) -> int:
        total = 0
        try:
            stat_result = await asyncio.to_thread(p.stat)
        except (OSError, ValueError):
            return 0

        mode = stat_result.st_mode
        if stat.S_ISREG(mode):
            total = stat_result.st_size
        elif stat.S_ISDIR(mode):
            try:
                children = await asyncio.to_thread(list, p.iterdir())
                for child in children:
                    total += await _calc_size(child, depth + 1)
            except PermissionError:
                pass

        if not summarize and (max_depth is None or depth <= max_depth):
            sizes[str(p)] = total
        return total

    total = await _calc_size(resolved, 0)

    results: list[DuEntry] = []
    if summarize:
        human = _human_readable_size(total) if human_readable else None
        results.append(DuEntry(path=str(resolved), size=total, size_human=human))
    else:
        for path_str, size in sorted(sizes.items()):
            human = _human_readable_size(size) if human_readable else None
            results.append(DuEntry(path=path_str, size=size, size_human=human))

    return results


# ==================== Sync wrappers ====================


def grep(
    pattern: str,
    path: str,
    base: UPath,
    **kwargs: Any,
) -> Iterator[GrepResult]:
    """Search for pattern in files (sync wrapper)."""

    async def _run():
        return [result async for result in agrep(pattern, path, base, **kwargs)]

    results = asyncio.run(_run())
    yield from results


def find(path: str, base: UPath, **kwargs: Any) -> Iterator[FindResult]:
    """Find files matching criteria (sync wrapper)."""

    async def _run():
        return [result async for result in afind(path, base, **kwargs)]

    results = asyncio.run(_run())
    yield from results


def head(path: str, base: UPath, **kwargs: Any) -> str:
    """Get first n lines (sync wrapper)."""
    return asyncio.run(ahead(path, base, **kwargs))


def tail(path: str, base: UPath, **kwargs: Any) -> str:
    """Get last n lines (sync wrapper)."""
    return asyncio.run(atail(path, base, **kwargs))


def cat(*paths: str, base: UPath, **kwargs: Any) -> str:
    """Concatenate files (sync wrapper)."""
    return asyncio.run(acat(*paths, base=base, **kwargs))


def cat_bytes(*paths: str, base: UPath) -> bytes:
    """Concatenate files as bytes (sync wrapper)."""
    return asyncio.run(acat_bytes(*paths, base=base))


def wc(path: str, base: UPath, **kwargs: Any) -> WcResult:
    """Count lines, words, chars (sync wrapper)."""
    return asyncio.run(awc(path, base, **kwargs))


def ls(path: str, base: UPath, **kwargs: Any) -> list[LsEntry]:
    """List directory (sync wrapper)."""
    return asyncio.run(als(path, base, **kwargs))


def diff(path1: str, path2: str, base: UPath, **kwargs: Any) -> str:
    """Compare files (sync wrapper)."""
    return asyncio.run(adiff(path1, path2, base, **kwargs))


def touch(path: str, base: UPath, **kwargs: Any) -> None:
    """Create/touch file (sync wrapper)."""
    return asyncio.run(atouch(path, base, **kwargs))


def mkdir(path: str, base: UPath, **kwargs: Any) -> None:
    """Create directory (sync wrapper)."""
    return asyncio.run(amkdir(path, base, **kwargs))


def rm(path: str, base: UPath, **kwargs: Any) -> None:
    """Remove file/directory (sync wrapper)."""
    return asyncio.run(arm(path, base, **kwargs))


def cp(src: str, dst: str, base: UPath, **kwargs: Any) -> None:
    """Copy file/directory (sync wrapper)."""
    return asyncio.run(acp(src, dst, base, **kwargs))


def mv(src: str, dst: str, base: UPath, **kwargs: Any) -> None:
    """Move file/directory (sync wrapper)."""
    return asyncio.run(amv(src, dst, base, **kwargs))


def du(path: str, base: UPath, **kwargs: Any) -> list[DuEntry]:
    """Disk usage (sync wrapper)."""
    return asyncio.run(adu(path, base, **kwargs))
