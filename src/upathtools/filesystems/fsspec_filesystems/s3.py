"""S3 filesystem with UPath integration (requires s3fs)."""

from __future__ import annotations

from s3fs.core import S3FileSystem as _S3FileSystem
from upath.implementations.cloud import S3Path

from upathtools.filesystems.base import BaseFileSystem


class S3FileSystem(BaseFileSystem[S3Path], _S3FileSystem):
    """AWS S3 filesystem with UPath integration."""

    upath_cls = S3Path
