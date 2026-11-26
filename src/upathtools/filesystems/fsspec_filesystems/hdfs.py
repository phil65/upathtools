"""HDFS filesystem with UPath integration (requires pyarrow)."""

from fsspec.implementations.arrow import HadoopFileSystem as _HadoopFileSystem
from upath.implementations.hdfs import HDFSPath

from upathtools.filesystems.base import BaseFileSystem


class HadoopFileSystem(BaseFileSystem[HDFSPath], _HadoopFileSystem):
    """Hadoop HDFS filesystem with UPath integration."""

    upath_cls = HDFSPath
