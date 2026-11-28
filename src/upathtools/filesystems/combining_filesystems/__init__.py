"""Combining filesystems that are composed of multiple filesystems."""

from typing import TYPE_CHECKING

from upathtools._lazy import lazy_exports


if TYPE_CHECKING:
    from .flat_union_fs import FlatUnionFileSystem, FlatUnionInfo, FlatUnionPath
    from .union_fs import UnionFileSystem, UnionInfo, UnionPath
    from .overlay_fs import OverlayFileSystem, OverlayInfo, OverlayPath

__getattr__, __dir__ = lazy_exports(
    __name__,
    {
        "FlatUnionFileSystem": ".flat_union_fs",
        "FlatUnionInfo": ".flat_union_fs",
        "FlatUnionPath": ".flat_union_fs",
        "UnionFileSystem": ".union_fs",
        "UnionInfo": ".union_fs",
        "UnionPath": ".union_fs",
        "OverlayFileSystem": ".overlay_fs",
        "OverlayInfo": ".overlay_fs",
        "OverlayPath": ".overlay_fs",
    },
)

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
