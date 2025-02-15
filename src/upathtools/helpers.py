from __future__ import annotations

import logging
import shutil
from typing import TYPE_CHECKING

import upath


if TYPE_CHECKING:
    import os
    from typing import Any


logger = logging.getLogger(__name__)


def fsspec_copy(
    source_path: str | os.PathLike[str],
    output_path: str | os.PathLike[str],
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
    source_path: str | os.PathLike[str],
    output_path: str | os.PathLike[str],
    exist_ok: bool = True,
):
    """Copy source_path to output_path, making sure any parent directories exist.

    The output_path may be a directory.

    Args:
        source_path: File to copy
        output_path: path where file should get copied to.
        exist_ok: Whether exception should be raised in case stuff would get overwritten
    """
    output_p = upath.UPath(output_path)
    source_p = upath.UPath(source_path)
    output_p.parent.mkdir(parents=True, exist_ok=exist_ok)
    if source_p.is_dir():
        if output_p.is_dir():
            msg = "Cannot copy folder to file!"
            raise RuntimeError(msg)
        shutil.copytree(source_p, output_p, dirs_exist_ok=exist_ok)
    else:
        if output_p.is_dir():
            output_p /= source_p.name
        shutil.copyfile(source_p, output_p)


def clean_directory(
    directory: str | os.PathLike[str], remove_hidden: bool = False
) -> None:
    """Remove the content of a directory recursively but not the directory itself."""
    folder_to_remove = upath.UPath(directory)
    if not folder_to_remove.exists():
        return
    for entry in folder_to_remove.iterdir():
        if entry.name.startswith(".") and not remove_hidden:
            continue
        path = folder_to_remove / entry
        if path.is_dir():
            shutil.rmtree(path, True)
        else:
            path.unlink()


def write_file(
    content: str | bytes,
    output_path: str | os.PathLike[str],
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
    output_p = upath.UPath(output_path)
    output_p.parent.mkdir(parents=True, exist_ok=True)
    mode = "wb" if isinstance(content, bytes) else "w"
    kwargs["encoding"] = None if "b" in mode else "utf-8"
    if errors:
        kwargs["errors"] = errors
    with output_p.open(mode=mode, **kwargs) as f:  # type: ignore[call-overload]
        f.write(content)  # type: ignore
