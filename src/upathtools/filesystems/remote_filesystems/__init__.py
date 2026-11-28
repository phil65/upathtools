"""Remote filesystems."""

from typing import TYPE_CHECKING

from upathtools._lazy import lazy_exports


if TYPE_CHECKING:
    from upathtools.filesystems.remote_filesystems.github_fs import (
        GithubFileSystem,
        GithubInfo,
        GithubPath,
    )
    from upathtools.filesystems.remote_filesystems.gitlab_fs import (
        GitLabFileSystem,
        GitLabInfo,
        GitLabPath,
    )
    from upathtools.filesystems.remote_filesystems.linear_fs import (
        LinearCommentInfo,
        LinearIssueFileSystem,
        LinearIssueInfo,
        LinearIssuePath,
        LinearProjectInfo,
    )

__getattr__, __dir__ = lazy_exports(
    __name__,
    {
        "GithubFileSystem": "upathtools.filesystems.remote_filesystems.github_fs",
        "GithubInfo": "upathtools.filesystems.remote_filesystems.github_fs",
        "GithubPath": "upathtools.filesystems.remote_filesystems.github_fs",
        "GitLabFileSystem": "upathtools.filesystems.remote_filesystems.gitlab_fs",
        "GitLabInfo": "upathtools.filesystems.remote_filesystems.gitlab_fs",
        "GitLabPath": "upathtools.filesystems.remote_filesystems.gitlab_fs",
        "LinearCommentInfo": "upathtools.filesystems.remote_filesystems.linear_fs",
        "LinearIssueFileSystem": "upathtools.filesystems.remote_filesystems.linear_fs",
        "LinearIssueInfo": "upathtools.filesystems.remote_filesystems.linear_fs",
        "LinearIssuePath": "upathtools.filesystems.remote_filesystems.linear_fs",
        "LinearProjectInfo": "upathtools.filesystems.remote_filesystems.linear_fs",
    },
)

__all__ = [
    "GitLabFileSystem",
    "GitLabInfo",
    "GitLabPath",
    "GithubFileSystem",
    "GithubInfo",
    "GithubPath",
    "LinearCommentInfo",
    "LinearIssueFileSystem",
    "LinearIssueInfo",
    "LinearIssuePath",
    "LinearProjectInfo",
]
