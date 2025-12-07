"""GitLab filesystem implementation using python-gitlab client."""

from __future__ import annotations

import io
import os
from typing import TYPE_CHECKING, Any, Literal, overload

from upathtools.filesystems.base import BaseFileSystem, BaseUPath, FileInfo


if TYPE_CHECKING:
    import gitlab
    from gitlab.v4.objects import Project


class GitLabInfo(FileInfo, total=False):
    """Info dict for GitLab filesystem paths."""

    size: int
    mode: str
    sha: str


class GitLabPath(BaseUPath[GitLabInfo]):
    """GitLab-specific UPath implementation."""

    __slots__ = ()


class GitLabFileSystem(BaseFileSystem[GitLabPath, GitLabInfo]):
    """Filesystem for accessing GitLab repository contents.

    Uses python-gitlab client for API access. Supports browsing repository
    files and directories at a specific ref (branch, tag, or commit).

    URL format: gitlab://[host/]namespace/project[@ref]/path
    Examples:
        - gitlab://mygroup/myproject/src/main.py
        - gitlab://mygroup/myproject@main/src/main.py
        - gitlab://gitlab.example.com/mygroup/myproject@v1.0.0/README.md
    """

    protocol = "gitlab"
    upath_cls = GitLabPath
    root_marker = ""

    def __init__(
        self,
        project_id: str | int | None = None,
        ref: str | None = None,
        url: str = "https://gitlab.com",
        private_token: str | None = None,
        oauth_token: str | None = None,
        job_token: str | None = None,
        **kwargs: Any,
    ) -> None:
        """Initialize GitLab filesystem.

        Args:
            project_id: GitLab project ID or path (e.g., 'namespace/project')
            ref: Git ref (branch, tag, commit SHA). If None, uses default branch
            url: GitLab instance URL
            private_token: GitLab private/personal access token
            oauth_token: GitLab OAuth token
            job_token: GitLab CI job token
            **kwargs: Additional filesystem options
        """
        super().__init__(**kwargs)
        self._project_id = project_id
        self._ref = ref
        self._url = url
        self._private_token = private_token or os.environ.get("GITLAB_TOKEN")
        self._oauth_token = oauth_token
        self._job_token = job_token
        self._client: gitlab.Gitlab | None = None
        self._project: Project | None = None
        self._default_branch: str | None = None

    @staticmethod
    def _get_kwargs_from_urls(path: str) -> dict[str, Any]:
        """Parse GitLab URL into constructor kwargs.

        Supports formats:
            - gitlab://namespace/project/path
            - gitlab://namespace/project@ref/path
            - gitlab://gitlab.example.com/namespace/project@ref/path
        """
        path = path.removeprefix("gitlab://")
        if not path:
            return {}

        out: dict[str, Any] = {}
        ref: str | None = None

        # Check if custom host is specified (contains more than 2 slashes before @)
        parts = path.split("/")
        if len(parts) >= 3 and "." in parts[0]:  # noqa: PLR2004
            # Custom GitLab instance
            out["url"] = f"https://{parts[0]}"
            parts = parts[1:]
            path = "/".join(parts)

        # Handle ref in project path (namespace/project@ref)
        if "@" in path:
            path_before_at, rest = path.split("@", 1)
            if "/" in rest:
                ref, _ = rest.split("/", 1)
            else:
                ref = rest
            # Reconstruct path without ref
            path = path_before_at
            if "/" in rest:
                path = path_before_at

        # Extract namespace/project (first two path components)
        parts = path.split("/")
        if len(parts) >= 2:  # noqa: PLR2004
            out["project_id"] = f"{parts[0]}/{parts[1]}"

        if ref:
            out["ref"] = ref

        return out

    @classmethod
    def _strip_protocol(cls, path: str) -> str:  # noqa: PLR0911
        """Strip protocol and extract file path."""
        path = path.removeprefix("gitlab://")
        if not path:
            return ""

        # Remove host if present
        parts = path.split("/")
        if len(parts) >= 1 and "." in parts[0]:
            parts = parts[1:]

        # Handle ref in project part (namespace/project@ref/path)
        if len(parts) >= 2:  # noqa: PLR2004
            project_part = parts[1]
            if "@" in project_part:
                # Format: namespace/project@ref/path/to/file
                _project_name, ref_and_path = project_part.split("@", 1)
                if "/" in ref_and_path:
                    # ref/path/to/file -> extract path after ref
                    _, file_path = ref_and_path.split("/", 1)
                    # Combine with remaining parts
                    remaining = parts[2:]
                    if remaining:
                        return file_path + "/" + "/".join(remaining)
                    return file_path
                # Just ref, no file path after it
                if len(parts) > 2:  # noqa: PLR2004
                    return "/".join(parts[2:])
                return ""

        # No ref specified, skip namespace/project
        if len(parts) > 2:  # noqa: PLR2004
            return "/".join(parts[2:])
        return ""

    def _get_client(self) -> gitlab.Gitlab:
        """Get or create GitLab client."""
        if self._client is None:
            try:
                import gitlab
            except ImportError as exc:
                msg = "python-gitlab package is required for GitLabFileSystem"
                raise ImportError(msg) from exc

            self._client = gitlab.Gitlab(
                url=self._url,
                private_token=self._private_token,
                oauth_token=self._oauth_token,
                job_token=self._job_token,
            )
        return self._client

    def _get_project(self) -> Project:
        """Get GitLab project object."""
        if self._project is None:
            if not self._project_id:
                msg = "project_id is required"
                raise ValueError(msg)
            client = self._get_client()
            self._project = client.projects.get(self._project_id)
        return self._project

    @property
    def ref(self) -> str:
        """Get the current ref, defaulting to project's default branch."""
        if self._ref is not None:
            return self._ref
        if self._default_branch is None:
            project = self._get_project()
            self._default_branch = project.default_branch
        return self._default_branch or "main"

    @overload
    def ls(self, path: str, detail: Literal[True] = ..., **kwargs: Any) -> list[GitLabInfo]: ...

    @overload
    def ls(self, path: str, detail: Literal[False], **kwargs: Any) -> list[str]: ...

    def ls(self, path: str, detail: bool = True, **kwargs: Any) -> list[GitLabInfo] | list[str]:
        """List directory contents.

        Args:
            path: Path relative to repository root
            detail: If True, return detailed info dicts
            **kwargs: Additional arguments (recursive supported)

        Returns:
            List of file paths or detailed info dicts
        """
        path = self._strip_protocol(path).strip("/")
        project = self._get_project()
        recursive = kwargs.get("recursive", False)

        try:
            items = project.repository_tree(
                path=path or "",
                ref=self.ref,
                recursive=recursive,
                get_all=True,
            )
        except Exception as exc:
            if "not found" in str(exc).lower() or "404" in str(exc):
                raise FileNotFoundError(path) from exc
            raise

        if not detail:
            return [item["path"] for item in items]

        return [
            GitLabInfo(
                name=item["path"],
                type="directory" if item["type"] == "tree" else "file",
                mode=item.get("mode", ""),
                sha=item.get("id", ""),
            )
            for item in items
        ]

    def cat(self, path: str, **kwargs: Any) -> bytes:
        """Read file contents.

        Args:
            path: Path to file
            **kwargs: Additional keyword arguments

        Returns:
            File contents as bytes
        """
        return self._cat_file(path, **kwargs)

    def _cat_file(
        self,
        path: str,
        start: int | None = None,
        end: int | None = None,
        **kwargs: Any,
    ) -> bytes:
        """Read file contents.

        Args:
            path: Path to file
            start: Start byte offset
            end: End byte offset
            **kwargs: Additional keyword arguments

        Returns:
            File contents as bytes
        """
        path = self._strip_protocol(path).strip("/")
        project = self._get_project()

        try:
            # Use raw endpoint for direct content
            content = project.files.raw(file_path=path, ref=self.ref)
        except Exception as exc:
            if "not found" in str(exc).lower() or "404" in str(exc):
                raise FileNotFoundError(path) from exc
            raise

        if isinstance(content, str):
            content = content.encode()

        if start is not None or end is not None:
            content = content[start:end]

        return content

    def info(self, path: str, **kwargs: Any) -> GitLabInfo:
        """Get info about a path.

        Args:
            path: Path to file or directory
            **kwargs: Additional keyword arguments

        Returns:
            Info dict for the path
        """
        path = self._strip_protocol(path).strip("/")

        if not path:
            # Root directory
            return GitLabInfo(name="", type="directory")

        project = self._get_project()

        # Try to get file info first
        try:
            f = project.files.get(file_path=path, ref=self.ref)
            return GitLabInfo(
                name=path,
                type="file",
                size=f.size,
                sha=f.blob_id,
            )
        except Exception:  # noqa: BLE001
            pass

        # Check if it's a directory by listing it
        try:
            items = project.repository_tree(path=path, ref=self.ref)
            if items is not None:
                return GitLabInfo(name=path, type="directory")
        except Exception:  # noqa: BLE001
            pass

        raise FileNotFoundError(path)

    def exists(self, path: str, **kwargs: Any) -> bool:
        """Check if path exists."""
        try:
            self.info(path)
        except FileNotFoundError:
            return False
        else:
            return True

    def isfile(self, path: str) -> bool:
        """Check if path is a file."""
        try:
            info = self.info(path)
            return info["type"] == "file"
        except FileNotFoundError:
            return False

    def isdir(self, path: str) -> bool:
        """Check if path is a directory."""
        try:
            info = self.info(path)
            return info["type"] == "directory"
        except FileNotFoundError:
            return False

    def _open(
        self,
        path: str,
        mode: str = "rb",
        **kwargs: Any,
    ) -> io.BytesIO:
        """Open a file for reading.

        Args:
            path: Path to file
            mode: File mode (only 'rb' supported)
            **kwargs: Additional keyword arguments

        Returns:
            BytesIO object with file contents
        """
        if mode != "rb":
            msg = f"Mode {mode!r} not supported, only 'rb' is available"
            raise NotImplementedError(msg)

        content = self._cat_file(path)
        return io.BytesIO(content)

    @property
    def tags(self) -> list[str]:
        """Get list of tag names in the repository."""
        project = self._get_project()
        return [tag.name for tag in project.tags.list(get_all=True)]

    @property
    def branches(self) -> list[str]:
        """Get list of branch names in the repository."""
        project = self._get_project()
        return [branch.name for branch in project.branches.list(get_all=True)]

    @property
    def refs(self) -> dict[str, list[str]]:
        """Get all named references (tags and branches)."""
        return {"tags": self.tags, "branches": self.branches}

    def invalidate_cache(self, path: str | None = None) -> None:
        """Invalidate any cached directory listings."""
        self.dircache.clear()


if __name__ == "__main__":
    fs = GitLabFileSystem("phil65/test")
    print(fs.tags)
    print(fs.branches)
    print(fs.refs)
