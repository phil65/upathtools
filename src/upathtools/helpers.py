from __future__ import annotations

import logging
import os
from typing import TYPE_CHECKING, overload

import fsspec
from upath import UPath

from upathtools import core


if TYPE_CHECKING:
    from typing import Any, Literal

    from fsspec.asyn import AsyncFileSystem
    from upath.types import JoinablePathLike

    from upathtools.async_upath import AsyncUPath


logger = logging.getLogger(__name__)


@overload
def to_upath(path: JoinablePathLike | str, as_async: Literal[True]) -> AsyncUPath: ...


@overload
def to_upath(path: JoinablePathLike | str, as_async: Literal[False]) -> UPath: ...


@overload
def to_upath(path: JoinablePathLike | str, as_async: bool) -> UPath | AsyncUPath: ...


@overload
def to_upath(path: JoinablePathLike | str) -> UPath: ...


def to_upath(
    path: JoinablePathLike | str, as_async: bool = False, **storage_options: Any
) -> UPath | AsyncUPath:
    from upathtools.async_upath import AsyncUPath

    if isinstance(path, UPath):
        path_obj = path
    else:
        raw = os.fspath(path) if isinstance(path, os.PathLike) else path
        path_obj = UPath(raw, **storage_options)
    return AsyncUPath._from_upath(path_obj) if as_async else path_obj


def fsspec_copy(
    source_path: JoinablePathLike,
    output_path: JoinablePathLike,
    exist_ok: bool = True,
) -> None:
    """Copy source_path to output_path, making sure any parent directories exist.

    The output_path may be a directory.

    Args:
        source_path: File to copy
        output_path: path where file should get copied to.
        exist_ok: Whether exception should be raised in case stuff would get overwritten
    """
    if isinstance(source_path, UPath):
        src = fsspec.FSMap(source_path.path, source_path.fs)
    else:
        src = fsspec.get_mapper(str(source_path))
    if isinstance(output_path, UPath):
        target = fsspec.FSMap(output_path.path, output_path.fs)
    else:
        target = fsspec.get_mapper(str(output_path))
    if not exist_ok and any(key in target for key in src):
        msg = "cannot overwrite if exist_ok is set to False"
        raise RuntimeError(msg)
    for k in src:
        target[k] = src[k]


def copy(
    source_path: JoinablePathLike,
    output_path: JoinablePathLike,
    exist_ok: bool = True,
) -> None:
    """Copy source_path to output_path, making sure any parent directories exist.

    The output_path may be a directory.

    Args:
        source_path: File to copy
        output_path: path where file should get copied to.
        exist_ok: Whether exception should be raised in case stuff would get overwritten
    """
    output_p = to_upath(output_path)
    source_p = to_upath(source_path)
    if source_p.is_dir():
        if output_p.exists() and not output_p.is_dir():
            msg = "Cannot copy folder to file!"
            raise RuntimeError(msg)
        # Copy the folder's content into output_path (created if missing)
        for src_file in source_p.rglob("*"):
            if src_file.is_dir():
                continue
            _copy_file(src_file, output_p / src_file.relative_to(source_p).as_posix(), exist_ok)
        output_p.mkdir(parents=True, exist_ok=True)
    else:
        if output_p.is_dir():
            output_p /= source_p.name
        _copy_file(source_p, output_p, exist_ok)


def _copy_file(source: UPath, target: UPath, exist_ok: bool) -> None:
    if not exist_ok and target.exists():
        msg = f"Target exists: {target}"
        raise FileExistsError(msg)
    target.parent.mkdir(parents=True, exist_ok=True)
    source.copy(target)


def clean_directory(directory: JoinablePathLike, remove_hidden: bool = False) -> None:
    """Remove the content of a directory recursively but not the directory itself."""
    folder = to_upath(directory)
    folder_to_remove = UPath(folder)
    if not folder_to_remove.exists():
        return
    for entry in folder_to_remove.iterdir():
        if entry.name.startswith(".") and not remove_hidden:
            continue
        if entry.is_dir():
            entry.fs.rm(entry.path, recursive=True)
        else:
            entry.unlink()


def write_file(
    content: str | bytes,
    output_path: JoinablePathLike,
    errors: str | None = None,
    **kwargs: Any,
) -> None:
    """Write content to output_path, making sure any parent directories exist.

    Encoding will be chosen automatically based on type of content

    Args:
        content: Content to write
        output_path: path where file should get written to.
        errors: how to handle errors. Possible options:
                "strict", "ignore", "replace", "surrogateescape",
                "xmlcharrefreplace", "backslashreplace", "namereplace"
        kwargs: Additional keyword arguments passed to open
    """
    output_p = to_upath(output_path)
    output_p.parent.mkdir(parents=True, exist_ok=True)
    mode = "wb" if isinstance(content, bytes) else "w"
    kwargs["encoding"] = None if "b" in mode else "utf-8"
    if errors:
        kwargs["errors"] = errors
    with output_p.open(mode=mode, **kwargs) as f:  # type: ignore[call-overload]
        f.write(content)


def multi_glob(
    directory: str | None = None,
    keep_globs: list[str] | None = None,
    drop_globs: list[str] | None = None,
) -> list[UPath]:
    """Return a list of all files matching multiple globs.

    Return a list of all files in the given directory that match the
    patterns in keep_globs and do not match the patterns in drop_globs.
    The patterns are defined using glob syntax.

    Args:
        directory: The directory to search in. If not provided, the current
            working directory is used.
        keep_globs: A list of glob patterns to keep.
        drop_globs: A list of glob patterns to drop.

    Returns:
        A list of Path objects representing the files that match the given
        patterns.

    Raises:
        ValueError: If the directory does not exist.

    Example:
        files = multi_glob(keep_globs=["**/*.py"])
        files = multi_glob(drop_globs=["**/__pycache__/**/*"])
        ```
    """
    keep_globs = keep_globs or ["**/*"]
    drop_globs = drop_globs or [".git/**/*"]
    directory_path = UPath(directory) if directory else UPath.cwd()

    if not directory_path.is_dir():
        msg = f"{directory!r} is not a directory."
        raise ValueError(msg)

    def files_from_globs(globs: list[str]) -> set[UPath]:
        return {p for pattern in globs for p in directory_path.glob(pattern) if p.is_file()}

    matching_files = files_from_globs(keep_globs) - files_from_globs(drop_globs)
    return [file.relative_to(to_upath(directory_path)) for file in matching_files]


@overload
def upath_to_fs(
    path: JoinablePathLike,
    asynchronous: Literal[True] = True,
    **storage_options: Any,
) -> AsyncFileSystem: ...


@overload
def upath_to_fs(
    path: JoinablePathLike,
    asynchronous: bool,
    **storage_options: Any,
) -> fsspec.AbstractFileSystem: ...


def upath_to_fs(
    path: JoinablePathLike,
    asynchronous: bool = True,
    **storage_options: Any,
) -> fsspec.AbstractFileSystem:
    """Convert a UPath to its underlying filesystem, using the path as root.

    Uses DirFileSystem to wrap the filesystem with the path as the root,
    making the filesystem appear to be rooted at that specific directory.

    Args:
        path: UPath object to extract filesystem from
        asynchronous: Whether to return an async filesystem wrapper if needed
        storage_options: Additional storage options to pass to filesystem creation

    Returns:
        The filesystem instance, wrapped with DirFileSystem if path has a directory,
        and optionally wrapped for async operations

    Example:
        ```python
        from upath import UPath

        # Get an (async) filesystem rooted at the given path
        path = UPath("s3://bucket/folder")
        async_fs = upath_to_fs(path)  # Rooted at "bucket/folder"

        # Get a sync filesystem
        fs = upath_to_fs(path, asynchronous=False)
        ```
    """
    from fsspec.asyn import AsyncFileSystem
    from fsspec.implementations.asyn_wrapper import AsyncFileSystemWrapper
    from fsspec.implementations.dirfs import DirFileSystem

    if isinstance(path, UPath) and not storage_options:
        # The path's own filesystem keeps its storage options (zip fo=..., s3 keys)
        fs, parsed_path = path.fs, path.path
    else:
        fs, parsed_path = core.url_to_fs(str(path), **storage_options)
    if asynchronous and not isinstance(fs, AsyncFileSystem):
        fs = AsyncFileSystemWrapper(fs, asynchronous=True)
    if parsed_path not in ("", "/", "."):
        # DirFileSystem must match the sync/async mode of an async-capable inner fs
        dir_async = fs.asynchronous if isinstance(fs, AsyncFileSystem) else asynchronous
        fs = DirFileSystem(path=parsed_path, fs=fs, asynchronous=dir_async)
    if asynchronous and not isinstance(fs, AsyncFileSystem):
        fs = AsyncFileSystemWrapper(fs, asynchronous=True)
    return fs


if __name__ == "__main__":
    import fsspec

    # Test cases showing different path types and behaviors
    test_paths = [
        "file:///tmp",  # Directory - no wrapping
        "file:///tmp/test.txt",  # File - wrapped at parent dir
        "/tmp/test.txt",  # Local file - wrapped at parent dir
        "memory://folder/test.txt",  # Memory file - wrapped at parent dir
        "memory://",  # Memory root - no wrapping
    ]

    print("Testing upath_to_fs function:")
    print("=" * 50)
    for path in test_paths:
        try:
            print(f"\nTesting: {path}")
            upath = to_upath(path)
            _, parsed_path = core.url_to_fs(str(upath))
            print(f"  Parsed path: {parsed_path}")
            # Show our result
            result_fs = upath_to_fs(upath)
            print(f"  Result: {result_fs}")
            # Test async version
            async_fs = upath_to_fs(upath, asynchronous=True)
            print(f"  Async: {type(async_fs).__name__}")
        except (ImportError, AttributeError, ValueError, FileNotFoundError) as e:
            print(f"  ERROR: {e}")
