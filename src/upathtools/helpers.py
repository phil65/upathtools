from __future__ import annotations

import logging
import os
from typing import TYPE_CHECKING

import upath


if TYPE_CHECKING:
    from typing import Any

    from upath.types import JoinablePathLike


logger = logging.getLogger(__name__)


def to_upath(path: JoinablePathLike | str) -> upath.UPath:
    return (
        upath.UPath(os.fspath(path))
        if isinstance(path, os.PathLike)
        else upath.UPath(path)
    )


def fsspec_copy(
    source_path: JoinablePathLike,
    output_path: JoinablePathLike,
    exist_ok: bool = True,
):
    """Copy source_path to output_path, making sure any parent directories exist.

    The output_path may be a directory.

    Args:
        source_path: File to copy
        output_path: path where file should get copied to.
        exist_ok: Whether exception should be raised in case stuff would get overwritten
    """
    import fsspec

    if isinstance(source_path, upath.UPath):
        src = fsspec.FSMap(source_path.path, source_path.fs)
    else:
        src = fsspec.get_mapper(str(source_path))
    if isinstance(output_path, upath.UPath):
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
):
    """Copy source_path to output_path, making sure any parent directories exist.

    The output_path may be a directory.

    Args:
        source_path: File to copy
        output_path: path where file should get copied to.
        exist_ok: Whether exception should be raised in case stuff would get overwritten
    """
    output_p = to_upath(output_path)
    source_p = to_upath(source_path)
    output_p.parent.mkdir(parents=True, exist_ok=exist_ok)
    if source_p.is_dir():
        if output_p.is_dir():
            msg = "Cannot copy folder to file!"
            raise RuntimeError(msg)
        source_p.copy(output_p, exist_ok=exist_ok)
    else:
        if output_p.is_dir():
            output_p /= source_p.name
        source_p.copy(output_p)


def clean_directory(directory: JoinablePathLike, remove_hidden: bool = False) -> None:
    """Remove the content of a directory recursively but not the directory itself."""
    folder = to_upath(directory)
    folder_to_remove = upath.UPath(folder)
    if not folder_to_remove.exists():
        return
    for entry in folder_to_remove.iterdir():
        if entry.name.startswith(".") and not remove_hidden:
            continue
        path = folder_to_remove / entry
        if path.is_dir():
            path.rmdir(True)
        else:
            path.unlink()


def write_file(
    content: str | bytes,
    output_path: JoinablePathLike,
    errors: str | None = None,
    **kwargs: Any,
):
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
        f.write(content)  # type: ignore


def multi_glob(
    directory: str | None = None,
    keep_globs: list[str] | None = None,
    drop_globs: list[str] | None = None,
) -> list[upath.UPath]:
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
    directory_path = upath.UPath(directory) if directory else upath.UPath.cwd()

    if not directory_path.is_dir():
        msg = f"{directory!r} is not a directory."
        raise ValueError(msg)

    def files_from_globs(globs: list[str]) -> set[upath.UPath]:
        return {
            file
            for pattern in globs
            for file in directory_path.glob(pattern)
            if file.is_file()
        }

    matching_files = files_from_globs(keep_globs) - files_from_globs(drop_globs)
    return [file.relative_to(to_upath(directory_path)) for file in matching_files]
