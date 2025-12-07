"""WebDAV filesystem with UPath integration (requires webdav4)."""

from __future__ import annotations

from upath.implementations.webdav import WebdavPath
from webdav4.fsspec import WebdavFileSystem as _WebdavFileSystem

from upathtools.filesystems.base import BaseFileSystem


class WebdavFileSystem(BaseFileSystem[WebdavPath], _WebdavFileSystem):
    """WebDAV filesystem with UPath integration."""

    upath_cls = WebdavPath
