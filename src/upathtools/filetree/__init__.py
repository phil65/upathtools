"""Tree visualization package."""

from __future__ import annotations

from upathtools.filetree.filetree import (
    get_directory_tree,
    SortCriteria,
    DirectoryTree,
    TreeOptions,
)
from typing import TYPE_CHECKING

if TYPE_CHECKING:
    import jinja2


def setup_jinjarope_filters(env: jinja2.Environment):
    """Setup JinjaRope filters for filetree."""
    env.filters["get_directory_tree"] = get_directory_tree


__all__ = ["DirectoryTree", "SortCriteria", "TreeOptions", "get_directory_tree"]
