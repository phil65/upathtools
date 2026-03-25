from __future__ import annotations

from abc import abstractmethod
import operator
from typing import Any

from fsspec import AbstractFileSystem
from fsspec.utils import tokenize


class AbstractArchiveFileSystem(AbstractFileSystem):
    """A generic superclass for implementing Archive-based filesystems.

    Currently, it is shared amongst
    :class:`~fsspec.implementations.zip.ZipFileSystem`,
    :class:`~fsspec.implementations.libarchive.LibArchiveFileSystem` and
    :class:`~fsspec.implementations.tar.TarFileSystem`.
    """

    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.dir_cache: dict[str, Any] | None = None

    def __str__(self):
        return f"<Archive-like object {type(self).__name__} at {id(self)}>"

    __repr__ = __str__

    def ukey(self, path):
        return tokenize(path, self.fo, self.protocol)

    @abstractmethod
    def _get_dirs(self) -> dict[str, Any]:
        """Populate the directory cache with all directory names."""

    def _all_dirnames(self, paths: set[str] | list[str]) -> set[str]:
        """Returns *all* directory names for each path in paths, including intermediate ones."""
        if len(paths) == 0:
            return set()

        dirnames = {self._parent(path) for path in paths} - {self.root_marker}
        return dirnames | self._all_dirnames(dirnames)

    def info(self, path: str, **kwargs: Any) -> dict[str, Any]:
        dir_cache = self._get_dirs()
        path = self._strip_protocol(path)  # pyright: ignore[reportAssignmentType]
        if path in {"", "/"} and self.dir_cache:
            return {"name": "", "type": "directory", "size": 0}
        if path in dir_cache:
            return dir_cache[path]
        if path + "/" in dir_cache:
            return dir_cache[path + "/"]
        raise FileNotFoundError(path)

    def ls(self, path, detail=True, **kwargs):
        dir_cache = self._get_dirs()
        paths = {}
        for p, f in dir_cache.items():
            p = p.rstrip("/")
            root = p.rsplit("/", 1)[0] if "/" in p else ""
            if root == path.rstrip("/"):
                paths[p] = f
            elif all((a == b) for a, b in zip(path.split("/"), ["", *p.strip("/").split("/")])):
                # root directory entry
                ppath = p.rstrip("/").split("/", 1)[0]
                if ppath not in paths:
                    out = {"name": ppath, "size": 0, "type": "directory"}
                    paths[ppath] = out
        if detail:
            return sorted(paths.values(), key=operator.itemgetter("name"))
        return sorted(paths)
