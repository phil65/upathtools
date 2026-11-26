"""Common types for upathtools."""

import os
from typing import Protocol, runtime_checkable


@runtime_checkable
class VFSPathLike(Protocol):
    def __vfspath__(self) -> str: ...


AnyPathLike = VFSPathLike | os.PathLike[str]
AnyPath = AnyPathLike | str
