"""Helpers for async filesystem operations."""

from __future__ import annotations

import asyncio
from functools import lru_cache
from itertools import batched
import logging
import os
from typing import TYPE_CHECKING, Any, Literal, overload

from upath import UPath


if TYPE_CHECKING:
    from collections.abc import Mapping, Sequence

    import fsspec
    from fsspec.asyn import AsyncFileSystem
    from upath.types import JoinablePathLike


logger = logging.getLogger(__name__)


@lru_cache(maxsize=32)
def _get_cached_fs(protocol_or_fs: str | fsspec.AbstractFileSystem) -> AsyncFileSystem:
    """Cached filesystem creation."""
    import fsspec
    from fsspec.asyn import AsyncFileSystem
    from fsspec.implementations.asyn_wrapper import AsyncFileSystemWrapper
    from morefs.asyn_local import AsyncLocalFileSystem

    if isinstance(protocol_or_fs, str):
        if protocol_or_fs in ("", "file"):
            return AsyncLocalFileSystem()

        fs = fsspec.filesystem(protocol_or_fs, asynchronous=True)
    else:
        fs = protocol_or_fs
    if not isinstance(fs, AsyncFileSystem):
        fs = AsyncFileSystemWrapper(fs)
    return fs


async def get_async_fs(
    path_or_fs: JoinablePathLike | fsspec.AbstractFileSystem,
) -> AsyncFileSystem:
    """Get appropriate async filesystem for path."""
    import fsspec

    from upathtools.helpers import to_upath

    if isinstance(path_or_fs, fsspec.AbstractFileSystem):
        return _get_cached_fs(path_or_fs)

    path_obj = to_upath(path_or_fs)
    return _get_cached_fs(path_obj.protocol)


@overload
async def read_path(
    path: JoinablePathLike,
    mode: Literal["rt"] = "rt",
    encoding: str = ...,
) -> str: ...


@overload
async def read_path(
    path: JoinablePathLike, mode: Literal["rb"], encoding: str = ...
) -> bytes: ...


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
    f = await fs.open_async(path_obj.path, mode=mode)
    async with f:
        return await f.read()


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
    from upathtools.helpers import to_upath

    base_path = to_upath(path)
    matching_files = await list_files(
        path,
        pattern=pattern,
        recursive=recursive,
        include_dirs=include_dirs,
        exclude=exclude,
        max_depth=max_depth,
    )

    result: dict[str, str | bytes] = {}

    if load_parallel:
        # Process files in chunks
        for chunk in batched(matching_files, chunk_size, strict=False):
            # Create tasks for this chunk
            tasks = [read_path(p, mode=mode, encoding=encoding) for p in chunk]
            # Execute chunk in parallel
            try:
                contents: Sequence[str | bytes] = await asyncio.gather(*tasks)
                # Map results back to relative paths
                for file_path, content in zip(chunk, contents, strict=True):
                    rel_path = os.path.relpath(str(file_path), str(base_path))
                    result[rel_path] = content
            except Exception as e:  # noqa: BLE001
                msg = "Failed to read chunk starting at %s: %s"
                logger.warning(msg, os.path.relpath(str(chunk[0]), str(base_path)), e)
    else:
        # Sequential reading
        for file_path in matching_files:
            try:
                content = await read_path(file_path, mode=mode, encoding=encoding)
                rel_path = os.path.relpath(str(file_path), str(base_path))
                result[rel_path] = content
            except Exception as e:  # noqa: BLE001
                rel_path = os.path.relpath(str(file_path), str(base_path))
                logger.warning("Failed to read %s: %s", rel_path, e)

    return result


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
    detail: Literal[True],
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
    from fnmatch import fnmatch

    from upathtools.helpers import to_upath

    base_path = to_upath(path)
    if not base_path.exists():
        msg = f"Path does not exist: {path}"
        raise FileNotFoundError(msg)

    fs = await get_async_fs(base_path)
    matching_files: list[UPath | dict[str, Any]] = []

    # Get all matching paths
    if recursive:
        glob_path = str(base_path / pattern)
        paths = await fs._glob(glob_path, maxdepth=max_depth, detail=detail)
    else:
        paths = await fs._glob(str(base_path / pattern), detail=detail)

    # Filter and collect paths
    if detail:
        assert isinstance(paths, dict)
        for file_path, file_info in paths.items():
            assert isinstance(file_path, str)
            rel_path = os.path.relpath(file_path, str(base_path))
            if exclude and any(fnmatch(rel_path, pat) for pat in exclude):
                continue

            # Skip directories unless explicitly included
            is_dir = await fs._isdir(file_path)
            if is_dir and not include_dirs:
                continue

            if not is_dir:
                name = os.path.basename(file_path)  # noqa: PTH119
                dct = {**file_info, "name": name, "path": file_path}
                matching_files.append(dct)
    else:
        for file_path in paths:
            assert isinstance(file_path, str)

            path_obj = UPath(file_path)
            rel_path = os.path.relpath(file_path, str(base_path))

            # Skip excluded patterns
            if exclude and any(fnmatch(rel_path, pat) for pat in exclude):
                continue

            # Skip directories unless explicitly included
            is_dir = await fs._isdir(file_path)
            if is_dir and not include_dirs:
                continue

            if not is_dir:
                matching_files.append(path_obj)

    return matching_files  # type: ignore


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


if __name__ == "__main__":
    from pprint import pprint

    async def main():
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
