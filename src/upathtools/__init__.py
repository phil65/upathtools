__version__ = "0.6.0"

from upathtools.async_ops import read_path, read_folder, list_files, read_folder_as_text
from upathtools.httpx_fs import HttpPath, HTTPFileSystem


def register_http_filesystems():
    from fsspec import register_implementation
    from upath import registry

    register_implementation("http", HTTPFileSystem, clobber=True)
    registry.register_implementation("http", HttpPath, clobber=True)
    register_implementation("https", HTTPFileSystem, clobber=True)
    registry.register_implementation("https", HttpPath, clobber=True)


__all__ = [
    "HTTPFileSystem",
    "HttpPath",
    "list_files",
    "read_folder",
    "read_folder_as_text",
    "read_path",
    "register_http_filesystems",
]
