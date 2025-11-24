"""Mixins for adding UPath integration to fsspec filesystems."""

from __future__ import annotations

from typing import TYPE_CHECKING


if TYPE_CHECKING:
    from upath import UPath


class UPathFileSystemMixin[TPath: UPath]:
    """Mixin to add UPath integration to any fsspec filesystem.

    This mixin adds the ability to create UPath objects that are pre-configured
    with the filesystem instance, allowing seamless integration between fsspec
    filesystems and UPath.

    Example:
        ```python
        from fsspec.implementations.local import LocalFileSystem
        from upath import UPath

        class LocalWithUPath(UPathFileSystemMixin[UPath], LocalFileSystem):
            upath_cls = UPath

        fs = LocalWithUPath()
        path = fs.get_upath("/some/path")  # Returns UPath with fs cached
        ```
    """

    upath_cls: type[TPath]

    def get_upath(self, path: str | None = None) -> TPath:
        """Get a UPath object for the given path.

        The returned UPath object will have this filesystem instance cached,
        avoiding the need to recreate the filesystem for subsequent operations.

        Args:
            path: The path to create a UPath for. If None, uses the filesystem's
                root_marker if available, otherwise defaults to "/".
        """
        if path is None:
            path = getattr(self, "root_marker", "/")

        path_obj = self.upath_cls(path)
        path_obj._fs_cached = self  # pyright: ignore[reportAttributeAccessIssue]
        return path_obj
