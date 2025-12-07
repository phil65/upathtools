"""Remote filesystems."""

from upathtools.filesystems.remote_filesystems.gitlab_fs import (
    GitLabFileSystem,
    GitLabInfo,
    GitLabPath,
)

__all__ = [
    "GitLabFileSystem",
    "GitLabInfo",
    "GitLabPath",
]
