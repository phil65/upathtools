"""Filesystem implementation for browsing Pydantic BaseModel schemas."""

from __future__ import annotations

from fsspec.asyn import AsyncFileSystem
from upath import UPath


class BaseUPath(UPath):
    """UPath implementation for browsing Pydantic BaseModel schemas."""

    @classmethod
    def _fs_factory(
        cls,
        urlpath: str,
        protocol: str,
        storage_options,
    ):
        """Override upath's _fs_factory.

        Fix the bug where _get_kwargs_from_urls result is ignored.
        """
        from fsspec.registry import get_filesystem_class

        fs_cls = get_filesystem_class(protocol)
        so_dct = fs_cls._get_kwargs_from_urls(urlpath)
        so_dct.update(storage_options)
        return fs_cls(**so_dct)  # Use so_dct instead of storage_options


class BaseAsyncFileSystem[TPath: UPath](AsyncFileSystem):
    """Filesystem for browsing Pydantic BaseModel schemas and field definitions."""

    upath_cls: type[TPath]

    def get_upath(self, path: str) -> TPath:
        path_obj = self.upath_cls(path or "")
        path_obj._fs_cached = self  # pyright: ignore[reportAttributeAccessIssue]
        return path_obj
