"""SMB filesystem with UPath integration (requires smbprotocol)."""

from __future__ import annotations

from fsspec.implementations.smb import SMBFileSystem as _SMBFileSystem
from upath.implementations.smb import SMBPath

from upathtools.filesystems.base import BaseFileSystem


class SMBFileSystem(BaseFileSystem[SMBPath], _SMBFileSystem):
    """SMB filesystem with UPath integration."""

    upath_cls = SMBPath
