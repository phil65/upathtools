"""HuggingFace Hub filesystem with UPath integration (requires huggingface_hub)."""

from __future__ import annotations

from huggingface_hub.hf_file_system import (  # type: ignore[import-not-found]
    HfFileSystem as _HfFileSystem,
)
from upath.implementations.cloud import HfPath

from upathtools.filesystems.base import BaseFileSystem


class HfFileSystem(BaseFileSystem[HfPath], _HfFileSystem):
    """HuggingFace Hub filesystem with UPath integration."""

    upath_cls = HfPath
