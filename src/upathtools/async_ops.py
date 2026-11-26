"""Helpers for async filesystem operations."""

from __future__ import annotations

import asyncio
import logging
import os
import posixpath
import re
import threading
from typing import TYPE_CHECKING, Any, Literal, overload

from fsspec.asyn import AsyncFileSystem
from fsspec.implementations.local import LocalFileSystem


if TYPE_CHECKING:
    from collections.abc import Mapping

    import fsspec
    from upath import UPath
    from upath.types import JoinablePathLike


logger = logging.getLogger(__name__)
DEFAULT_MAX_SIZE = 64_000


# Async filesystems created for a protocol, per event loop: async filesystems (HTTP
# sessions, ...) only work on the loop they were created on. Closed loops are dropped.
_loop_fs_cache: dict[int, tuple[asyncio.AbstractEventLoop, dict[str, AsyncFileSystem]]] = {}
_loop_fs_cache_lock = threading.Lock()


def _fs_for_protocol(protocol: str, storage_options: Mapping[str, Any]) -> AsyncFileSystem:
    """Async filesystem for a protocol, cached per running event loop."""
    from fsspec.utils import tokenize

    loop = asyncio.get_running_loop()
    key = tokenize(protocol, dict(storage_options))
    with _loop_fs_cache_lock:
        for loop_id, (cached_loop, _) in list(_loop_fs_cache.items()):
            if cached_loop.is_closed():
                del _loop_fs_cache[loop_id]
        entry = _loop_fs_cache.get(id(loop))
        if entry is None or entry[0] is not loop:
            entry = _loop_fs_cache[id(loop)] = (loop, {})
        if (fs := entry[1].get(key)) is not None:
            return fs
    fs = _create_async_fs(protocol, storage_options)
    with _loop_fs_cache_lock:
        return entry[1].setdefault(key, fs)


def _create_async_fs(protocol: str, storage_options: Mapping[str, Any]) -> AsyncFileSystem:
    from fsspec.registry import get_filesystem_class

    from upathtools.filesystems import AsyncLocalFileSystem

    if protocol in ("", "file", "local") and not storage_options:
        return AsyncLocalFileSystem()
    cls = get_filesystem_class(protocol)
    if issubclass(cls, AsyncFileSystem):
        # fsspec's instance cache is per thread, not per loop: skip it
        return cls(asynchronous=True, skip_instance_cache=True, **storage_options)
    return to_async_fs(cls(**storage_options))


async def get_async_fs(
    path_or_fs: JoinablePathLike | fsspec.AbstractFileSystem,
) -> AsyncFileSystem:
    """Get appropriate async filesystem for path.

    A filesystem instance is used itself (sync ones get an async wrapper, kept on the
    instance), never one that merely compares equal to it. For a path, the filesystem
    comes from its protocol and storage options, one per event loop.
    """
    import fsspec

    from upathtools.helpers import to_upath

    if isinstance(path_or_fs, fsspec.AbstractFileSystem):
        return to_async_fs(path_or_fs)

    path_obj = to_upath(path_or_fs)
    # A UPath bound to a sync filesystem instance (fs.get_upath(), zip with fo, ...):
    # use that instance; async filesystems are made per loop from protocol + options.
    bound = getattr(path_obj, "_fs_cached", None)
    if bound is not None and not isinstance(bound, AsyncFileSystem):
        return to_async_fs(bound)
    return _fs_for_protocol(path_obj.protocol, path_obj.storage_options)


@overload
def to_async_fs[T: AsyncFileSystem](fs: T, asynchronous: bool = True) -> T: ...


@overload
def to_async_fs(fs: fsspec.AbstractFileSystem, asynchronous: bool = True) -> AsyncFileSystem: ...


def to_async_fs(fs: fsspec.AbstractFileSystem, asynchronous: bool = True) -> AsyncFileSystem:
    """Convert a sync filesystem to async if needed.

    If the filesystem is already async, it is returned unchanged.
    Otherwise, it is wrapped in a WrapperFileSystem (one per instance: fsspec's instance
    cache would hand out one wrapper for all instances that compare equal).

    Args:
        fs: The filesystem to convert
        asynchronous: Value for the asynchronous flag (needed for DirFileSystem compatibility)
    """
    from upathtools.filesystems.base import WrapperFileSystem

    if isinstance(fs, AsyncFileSystem):
        return fs
    attr = "_upathtools_async_wrapper" if asynchronous else "_upathtools_sync_wrapper"
    wrapper = fs.__dict__.get(attr)
    if wrapper is None:
        wrapper = WrapperFileSystem(fs, asynchronous=asynchronous, skip_instance_cache=True)
        wrapper = fs.__dict__.setdefault(attr, wrapper)
    return wrapper


async def is_directory(
    fs: AsyncFileSystem,
    path: str | dict[str, Any],
    *,
    entry_type: str | None = None,
) -> bool:
    """Smart directory detection that trusts filesystem metadata when available.

    Args:
        fs: Async filesystem instance
        path: Path string or fsspec info dict (from fs.info() or fs.ls(detail=True))
        entry_type: Optional type info (deprecated, pass dict instead)

    Returns:
        True if path is a directory, False otherwise
    """
    if isinstance(path, dict):
        entry_type = path.get("type")
        path = path["name"]
    if entry_type == "directory":
        return True
    if entry_type == "file":
        return False
    return await fs._isdir(path)


def is_directory_sync(
    fs: fsspec.AbstractFileSystem,
    path: str | dict[str, Any],
    *,
    entry_type: str | None = None,
) -> bool:
    """Smart directory detection that trusts filesystem metadata when available (sync version).

    Args:
        fs: Filesystem instance
        path: Path string or fsspec info dict (from fs.info() or fs.ls(detail=True))
        entry_type: Optional type info (deprecated, pass dict instead)

    Returns:
        True if path is a directory, False otherwise
    """
    if isinstance(path, dict):
        entry_type = path.get("type")
        path = path["name"]
    if entry_type == "directory":
        return True
    if entry_type == "file":
        return False
    return fs.isdir(path)


@overload
async def read_path(
    path: JoinablePathLike,
    mode: Literal["rt"] = "rt",
    encoding: str = ...,
) -> str: ...


@overload
async def read_path(path: JoinablePathLike, mode: Literal["rb"], encoding: str = ...) -> bytes: ...


async def read_path(
    path: JoinablePathLike,
    mode: Literal["rt", "rb"] = "rt",
    encoding: str = "utf-8",
) -> str | bytes:
    """Read file content asynchronously when possible.

    Args:
        path: Path to read
        mode: Read mode ("rt" for text, "rb" for binary)
        encoding: File encoding for text files

    Returns:
        File content as string or bytes depending on mode
    """
    from upathtools.helpers import to_upath

    path_obj = to_upath(path)
    fs = await get_async_fs(path_obj)
    data = await fs._cat_file(path_obj.path)
    return data if mode == "rb" else data.decode(encoding)


@overload
async def read_folder(
    path: JoinablePathLike,
    *,
    pattern: str = "**/*",
    recursive: bool = True,
    include_dirs: bool = False,
    exclude: list[str] | None = None,
    max_depth: int | None = None,
    mode: Literal["rt"] = "rt",
    encoding: str = "utf-8",
    load_parallel: bool = False,
    chunk_size: int = 50,
) -> Mapping[str, str]: ...


@overload
async def read_folder(
    path: JoinablePathLike,
    *,
    pattern: str = "**/*",
    recursive: bool = True,
    include_dirs: bool = False,
    exclude: list[str] | None = None,
    max_depth: int | None = None,
    mode: Literal["rb"],
    encoding: str = "utf-8",
    load_parallel: bool = False,
    chunk_size: int = 50,
) -> Mapping[str, bytes]: ...


async def read_folder(
    path: JoinablePathLike,
    *,
    pattern: str = "**/*",
    recursive: bool = True,
    include_dirs: bool = False,
    exclude: list[str] | None = None,
    max_depth: int | None = None,
    mode: Literal["rt", "rb"] = "rt",
    encoding: str = "utf-8",
    load_parallel: bool = True,
    chunk_size: int = 50,
) -> Mapping[str, str | bytes]:
    """Asynchronously read files in a folder matching a pattern.

    Args:
        path: Base directory to read from
        pattern: Glob pattern to match files against (e.g. "**/*.py" for Python files)
        recursive: Whether to search subdirectories
        include_dirs: Whether to include directories in results
        exclude: List of patterns to exclude (uses fnmatch against relative paths)
        max_depth: Maximum directory depth for recursive search
        mode: Read mode ("rt" for text, "rb" for binary)
        encoding: File encoding for text mode
        load_parallel: Whether to load files concurrently
        chunk_size: Number of files to load in parallel when load_parallel=True

    Returns:
        Mapping of relative paths to file contents

    Raises:
        FileNotFoundError: If base path doesn't exist
    """
    fs, root, matches = await _glob_matches(
        path,
        pattern=pattern,
        recursive=recursive,
        include_dirs=include_dirs,
        exclude=exclude,
        max_depth=max_depth,
    )
    is_local = isinstance(fs, LocalFileSystem)
    files = [(p, _relative(p, root, is_local)) for p, info in matches if not _is_dir(info)]

    async def read_one(file_path: str) -> str | bytes:
        data = await fs._cat_file(file_path)
        return data if mode == "rb" else data.decode(encoding)

    result: dict[str, str | bytes] = {}
    if load_parallel:
        # At most chunk_size reads at a time; one failing file doesn't drop others
        semaphore = asyncio.Semaphore(max(chunk_size, 1))

        async def bounded(file_path: str) -> str | bytes:
            async with semaphore:
                return await read_one(file_path)

        contents = await asyncio.gather(*(bounded(p) for p, _ in files), return_exceptions=True)
        for (_, rel_path), content in zip(files, contents, strict=True):
            if isinstance(content, BaseException):
                if not isinstance(content, Exception):
                    raise content
                logger.warning("Failed to read %s: %s", rel_path, content)
            else:
                result[rel_path] = content
    else:
        for file_path, rel_path in files:
            try:
                result[rel_path] = await read_one(file_path)
            except Exception as e:  # noqa: BLE001
                logger.warning("Failed to read %s: %s", rel_path, e)
    return result


def _is_dir(info: Mapping[str, Any]) -> bool:
    return info.get("type") == "directory"


def _relative(path: str, root: str, is_local: bool) -> str:
    """Path relative to root (OS separators for local paths, like os.path.relpath)."""
    prefix = root.rstrip("/") + "/"
    rel = path[len(prefix) :] if path.startswith(prefix) else path.lstrip("/")
    return os.path.normpath(rel) if is_local else rel


async def _glob_matches(
    path: JoinablePathLike,
    *,
    pattern: str,
    recursive: bool,
    include_dirs: bool,
    exclude: list[str] | None,
    max_depth: int | None,
) -> tuple[AsyncFileSystem, str, list[tuple[str, dict[str, Any]]]]:
    """Glob below a path on its own filesystem (protocol and storage options kept)."""
    from fnmatch import fnmatch

    from upathtools.helpers import to_upath

    base_path = to_upath(path)
    fs = await get_async_fs(base_path)
    stripped = fs._strip_protocol(base_path.path)
    root = stripped if isinstance(stripped, str) else stripped[0]
    if not await fs._exists(root):
        msg = f"Path does not exist: {path}"
        raise FileNotFoundError(msg)
    if not recursive:
        # Only the folder itself: drop recursive wildcards from the pattern
        pattern = "/".join(part for part in pattern.split("/") if part != "**") or "*"
        max_depth = None
    glob_path = root.rstrip("/") + "/" + pattern
    found = await fs._glob(glob_path, maxdepth=max_depth, detail=True)
    matches: list[tuple[str, dict[str, Any]]] = []
    for file_path, info in found.items():
        if file_path.rstrip("/") == root.rstrip("/"):
            continue
        rel_path = _relative(file_path, root, is_local=False)
        if exclude and any(fnmatch(rel_path, pat) for pat in exclude):
            continue
        if _is_dir(info) and not include_dirs:
            continue
        matches.append((file_path, dict(info)))
    return fs, root, matches


@overload
async def list_files(
    path: JoinablePathLike,
    *,
    pattern: str = "**/*",
    recursive: bool = True,
    include_dirs: bool = False,
    exclude: list[str] | None = None,
    max_depth: int | None = None,
    detail: Literal[False] = False,
) -> list[UPath]: ...


@overload
async def list_files(
    path: JoinablePathLike,
    *,
    pattern: str = "**/*",
    recursive: bool = True,
    include_dirs: bool = False,
    exclude: list[str] | None = None,
    max_depth: int | None = None,
    detail: Literal[True] = ...,
) -> list[dict[str, Any]]: ...


async def list_files(
    path: JoinablePathLike,
    *,
    pattern: str = "**/*",
    recursive: bool = True,
    include_dirs: bool = False,
    exclude: list[str] | None = None,
    max_depth: int | None = None,
    detail: bool = False,
) -> list[UPath] | list[dict[str, Any]]:
    """List files in a folder matching a pattern.

    Args:
        path: Base directory to read from
        pattern: Glob pattern to match files against (e.g. "**/*.py" for Python files)
        recursive: Whether to search subdirectories
        include_dirs: Whether to include directories in results
        exclude: List of patterns to exclude (uses fnmatch against relative paths)
        max_depth: Maximum directory depth for recursive search
        detail: Include file details in the result

    Returns:
        List of UPath objects for matching files

    Raises:
        FileNotFoundError: If base path doesn't exist
    """
    from upathtools.helpers import to_upath

    _, _, matches = await _glob_matches(
        path,
        pattern=pattern,
        recursive=recursive,
        include_dirs=include_dirs,
        exclude=exclude,
        max_depth=max_depth,
    )
    if detail:
        return [
            {**info, "name": posixpath.basename(file_path.rstrip("/")), "path": file_path}
            for file_path, info in matches
        ]
    base_path = to_upath(path)
    return [base_path.with_segments(file_path) for file_path, _ in matches]


async def read_folder_as_text(
    path: JoinablePathLike,
    *,
    pattern: str = "**/*",
    recursive: bool = True,
    include_dirs: bool = False,
    exclude: list[str] | None = None,
    max_depth: int | None = None,
    encoding: str = "utf-8",
    load_parallel: bool = True,
    chunk_size: int = 50,
) -> str:
    """Read files in a folder and combine them into a single text document.

    The output format is a markdown-style document where each file's content
    is preceded by a header containing the filename.

    Args:
        path: Base directory to read from
        pattern: Glob pattern to match files against (e.g. "**/*.py" for Python files)
        recursive: Whether to search subdirectories
        include_dirs: Whether to include directories in results
        exclude: List of patterns to exclude (uses fnmatch against relative paths)
        max_depth: Maximum directory depth for recursive search
        encoding: File encoding for text files
        load_parallel: Whether to load files concurrently
        chunk_size: Number of files to load in parallel when load_parallel=True

    Returns:
        A text document containing all file contents with headers

    Example:
        ```python
        text = await read_folder_as_text("src", pattern="**/*.py")
        # Returns:
        # # Content of src/main.py
        # def main():
        #     ...
        #
        # # Content of src/utils.py
        # def helper():
        #     ...
        # ```
    """
    file_contents = await read_folder(
        path,
        pattern=pattern,
        recursive=recursive,
        include_dirs=include_dirs,
        exclude=exclude,
        max_depth=max_depth,
        mode="rt",
        encoding=encoding,
        load_parallel=load_parallel,
        chunk_size=chunk_size,
    )

    result_parts: list[str] = []
    for rel_path, content in sorted(file_contents.items()):
        assert isinstance(content, str), "Expected string content in text mode"
        result_parts.extend([
            f"# Content of {rel_path}",
            "",
            content.rstrip(),
            "",
            "",  # Extra newline for separation
        ])

    return "\n".join(result_parts).rstrip() + "\n"


async def fsspec_grep(
    fs: AsyncFileSystem,
    pattern: str,
    path: str,
    *,
    file_pattern: str = "**/*",
    case_sensitive: bool = False,
    max_matches: int = 100,
    max_output_bytes: int = DEFAULT_MAX_SIZE,
    context_lines: int = 0,
) -> dict[str, Any]:
    """Execute grep using fsspec filesystem (Python implementation).

    Args:
        fs: FSSpec filesystem instance
        pattern: Regex pattern to search for
        path: Base directory to search in
        file_pattern: Glob pattern to filter files
        case_sensitive: Whether search is case-sensitive
        max_matches: Maximum total matches across all files
        max_output_bytes: Maximum total output bytes
        context_lines: Number of context lines before/after match

    Returns:
        Dictionary with matches grouped by file
    """
    try:
        flags = 0 if case_sensitive else re.IGNORECASE
        regex = re.compile(pattern, flags)
    except re.error as e:
        return {"error": f"Invalid regex pattern: {e}"}

    try:
        glob_path = f"{path.rstrip('/')}/{file_pattern}"

        matches: dict[str, list[dict[str, Any]]] = {}
        total_matches = 0
        total_bytes = 0

        file_infos = await fs._glob(glob_path, detail=True)
        for file_path, info in file_infos.items():  # pyright: ignore[reportAttributeAccessIssue]
            if total_matches >= max_matches or total_bytes >= max_output_bytes:
                break

            # Skip directories
            assert isinstance(file_path, str)
            if await is_directory(fs, file_path, entry_type=info.get("type")):
                continue

            try:
                content = await fs._cat_file(file_path)
                # Skip binary files
                if b"\x00" in content[:8192]:
                    continue

                text = content.decode("utf-8", errors="replace")
                lines = text.splitlines()

                file_matches: list[dict[str, Any]] = []
                for line_num, line in enumerate(lines, 1):
                    if total_matches >= max_matches or total_bytes >= max_output_bytes:
                        break

                    if regex.search(line):
                        file_info: dict[str, Any] = {
                            "line_number": line_num,
                            "content": line.rstrip(),
                        }
                        if context_lines > 0:
                            start = max(0, line_num - 1 - context_lines)
                            end = min(len(lines), line_num + context_lines)
                            file_info["context_before"] = lines[start : line_num - 1]
                            file_info["context_after"] = lines[line_num:end]

                        file_matches.append(file_info)
                        total_matches += 1
                        total_bytes += len(line.encode("utf-8"))

                if file_matches:
                    matches[file_path] = file_matches  # pyright: ignore[reportArgumentType]

            except Exception as e:  # noqa: BLE001
                logger.debug("Error reading file %r during grep: %s", file_path, str(e))
                continue

        was_truncated = total_matches >= max_matches or total_bytes >= max_output_bytes
    except Exception as e:
        logger.exception("Error in fsspec grep")
        return {"error": f"Grep failed: {e}"}
    else:
        return {
            "matches": matches,
            "match_count": total_matches,
            "was_truncated": was_truncated,
        }


if __name__ == "__main__":
    from pprint import pprint

    async def main() -> None:
        # Test with current directory
        files = await read_folder(
            ".",
            pattern="**/*.py",
            recursive=True,
            exclude=["__pycache__/*", "*.pyc"],
            max_depth=4,
            load_parallel=True,
        )
        print("\nFound files:")
        pprint(list(files.keys()))

        print("\nFirst file content:")
        first_file = next(iter(files))
        print(f"\n{first_file}:")
        print(files[first_file][:500] + "...")  # Show first 500 chars

    asyncio.run(main())
