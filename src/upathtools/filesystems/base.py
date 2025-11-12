"""Filesystem implementation for browsing Pydantic BaseModel schemas."""

from __future__ import annotations

from fsspec.asyn import AsyncFileSystem
from upath import UPath


class BaseUPath(UPath):
    """UPath implementation for browsing Pydantic BaseModel schemas."""


class BaseAsyncFileSystem[TPath: UPath](AsyncFileSystem):
    """Filesystem for browsing Pydantic BaseModel schemas and field definitions."""

    upath_cls: type[TPath]

    def get_upath(self, path: str) -> TPath:
        path_obj = self.upath_cls(path or "")
        path_obj._fs_cached = self  # pyright: ignore[reportAttributeAccessIssue]
        return path_obj
