"""Azure Blob Storage filesystem with UPath integration (requires adlfs)."""

from __future__ import annotations

from adlfs.spec import AzureBlobFileSystem as _AzureBlobFileSystem
from upath.implementations.cloud import AzurePath

from upathtools.filesystems.base import BaseAsyncFileFileSystem


class AzureBlobFileSystem(BaseAsyncFileFileSystem[AzurePath], _AzureBlobFileSystem):
    """Azure Blob Storage filesystem with UPath integration."""

    upath_cls = AzurePath
