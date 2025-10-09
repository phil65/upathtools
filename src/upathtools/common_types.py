"""Common types for upathtools."""

from __future__ import annotations

import os
from typing import Protocol, runtime_checkable


@runtime_checkable
class VFSPathLike(Protocol):
    def __vfspath__(self) -> str: ...


AnyPathLike = VFSPathLike | os.PathLike[str]
AnyPath = AnyPathLike | str
