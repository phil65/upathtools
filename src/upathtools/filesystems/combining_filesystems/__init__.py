"""Combining filesystems that are composed of multiple filesystems."""

from __future__ import annotations

from .flat_union_fs import FlatUnionFileSystem, FlatUnionInfo, FlatUnionPath
from .union_fs import UnionFileSystem, UnionInfo, UnionPath
from .overlay_fs import OverlayFileSystem, OverlayInfo, OverlayPath

__all__ = [
    "FlatUnionFileSystem",
    "FlatUnionInfo",
    "FlatUnionPath",
    "OverlayFileSystem",
    "OverlayInfo",
    "OverlayPath",
    "UnionFileSystem",
    "UnionInfo",
    "UnionPath",
]
