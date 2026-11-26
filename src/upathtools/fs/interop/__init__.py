"""Bidirectional interop between ``upathtools.fs`` and ``fsspec`` / ``upath``."""

from upathtools.fs.interop.from_fsspec import FsspecAdapter, ensure_async_fs, from_fsspec
from upathtools.fs.interop.to_fsspec import (
    AsFsspecFileSystem,
    fsspec_class_for,
    register_fsspec_bridge,
    register_upath_bridge,
    to_fsspec,
)


__all__ = [
    "AsFsspecFileSystem",
    "FsspecAdapter",
    "ensure_async_fs",
    "from_fsspec",
    "fsspec_class_for",
    "register_fsspec_bridge",
    "register_upath_bridge",
    "to_fsspec",
]
