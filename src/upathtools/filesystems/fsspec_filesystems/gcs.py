"""Google Cloud Storage filesystem with UPath integration (requires gcsfs)."""

from __future__ import annotations

from gcsfs.core import GCSFileSystem as _GCSFileSystem
from upath.implementations.cloud import GCSPath

from upathtools.filesystems.base import BaseAsyncFileSystem


class GCSFileSystem(BaseAsyncFileSystem[GCSPath], _GCSFileSystem):
    """Google Cloud Storage filesystem with UPath integration."""

    upath_cls = GCSPath
