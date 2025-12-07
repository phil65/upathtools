"""SFTP filesystem with UPath integration (requires paramiko)."""

from __future__ import annotations

from fsspec.implementations.sftp import SFTPFileSystem as _SFTPFileSystem
from upath.implementations.sftp import SFTPPath

from upathtools.filesystems.base import BaseFileSystem


class SFTPFileSystem(BaseFileSystem[SFTPPath], _SFTPFileSystem):
    """SFTP filesystem with UPath integration."""

    upath_cls = SFTPPath
