"""GitHub repository filesystem implementation with async support using httpx."""

from __future__ import annotations

import base64
import contextlib
import logging
import os
from typing import TYPE_CHECKING, Any, Literal, Self, Unpack, overload
import weakref

from fsspec.utils import infer_storage_options

from upathtools.async_helpers import sync, sync_wrapper
from upathtools.filesystems.base import BaseAsyncFileSystem, BaseUPath, FileInfo


if TYPE_CHECKING:
    from collections.abc import Iterator, Sequence

    import httpx
    from upath._chain import FSSpecChainParser
    from upath.types import JoinablePathLike
    from upath.types.storage_options import GitHubStorageOptions


class GithubInfo(FileInfo):
    """Info dict for GitHub filesystem paths."""

    size: int
    mode: str
    sha: str


logger = logging.getLogger(__name__)


class GithubPath(BaseUPath[GithubInfo]):
    """GitHubPath supporting the fsspec.GitHubFileSystem."""

    __slots__ = ()

    if TYPE_CHECKING:

        def __init__(
            self,
            *args: JoinablePathLike,
            protocol: Literal["github"] | None = ...,
            chain_parser: FSSpecChainParser = ...,
            **storage_options: Unpack[GitHubStorageOptions],
        ) -> None: ...

    @property
    def path(self) -> str:
        pth = super().path
        if pth == ".":
            return ""
        return pth

    def iterdir(self) -> Iterator[Self]:
        if self.is_file():
            raise NotADirectoryError(str(self))
        yield from super().iterdir()

    @property
    def parts(self) -> Sequence[str]:
        parts = super().parts
        if parts and parts[0] == "/":
            return parts[1:]
        return parts


class GithubFileSystem(BaseAsyncFileSystem[GithubPath, GithubInfo]):
    """Filesystem for accessing GitHub repository files.

    Supports accessing files in a GitHub repository at a specific point in history
    (commit SHA, branch, or tag). Uses httpx for async operations with GitHub API.

    For files less than 1 MB, content is returned directly from the API.
    For larger files or git-lfs tracked files, content is fetched from download_url.
    """

    protocol = "github"
    upath_cls = GithubPath
    tree_url = "https://api.github.com/repos/{org}/{repo}/git/trees/{sha}"
    content_url = "https://api.github.com/repos/{org}/{repo}/contents/{path}?ref={sha}"
    repo_url = "https://api.github.com/repos/{org}/{repo}"
    branches_url = "https://api.github.com/repos/{org}/{repo}/branches"
    tags_url = "https://api.github.com/repos/{org}/{repo}/tags"

    def __init__(
        self,
        org: str,
        repo: str,
        sha: str | None = None,
        username: str | None = None,
        token: str | None = None,
        timeout: float | None = None,
        asynchronous: bool = False,
        loop: Any = None,
        client_kwargs: dict[str, Any] | None = None,
        **kwargs: Any,
    ) -> None:
        """Initialize the filesystem.

        Args:
            org: GitHub organization or user
            repo: Repository name
            sha: Commit SHA, branch, or tag (defaults to default branch)
            username: GitHub username for authentication
            token: GitHub personal access token
            timeout: Connection timeout in seconds
            asynchronous: Whether to use async operations
            loop: Event loop for async operations
            client_kwargs: Additional arguments for httpx client
            **kwargs: Additional filesystem options
        """
        super().__init__(asynchronous=asynchronous, loop=loop, **kwargs)

        if (username is None) ^ (token is None):
            msg = "Auth requires both username and token"
            raise ValueError(msg)

        self.org = org
        self.repo = repo
        self.username = username
        self.token = token or os.environ.get("GH_TOKEN") or os.environ.get("GITHUB_TOKEN")
        self.timeout = timeout if timeout is not None else 60.0
        self.client_kwargs = client_kwargs or {}
        self._session: httpx.AsyncClient | None = None
        self.dircache: dict[str, list[GithubInfo]] = {}

        # Build headers for authentication
        self.headers: dict[str, str] = {}
        if self.token:
            self.headers["Authorization"] = f"token {self.token}"
        if self.username:
            self.headers["Authorization"] = f"token {self.token}"

        self._sha = sha
        self.root = ""  # Will be set after determining default branch

    @property
    def fsid(self) -> str:
        """Filesystem ID."""
        return f"github-{self.org}-{self.repo}"

    async def set_session(self) -> httpx.AsyncClient:
        """Set up and return the httpx async client."""
        if self._session is None:
            import httpx

            self._session = httpx.AsyncClient(
                follow_redirects=True,
                timeout=self.timeout,
                headers=self.headers,
                **self.client_kwargs,
            )

            # Determine SHA if not provided
            if self._sha is None:
                r = await self._session.get(self.repo_url.format(org=self.org, repo=self.repo))
                r.raise_for_status()
                self._sha = r.json()["default_branch"]

            self.root = self._sha

            if not self.asynchronous:
                weakref.finalize(self, self.close_session, self.loop, self._session)

        return self._session

    @staticmethod
    def close_session(loop: Any, session: httpx.AsyncClient) -> None:
        """Close the httpx session."""
        if loop is not None and loop.is_running():
            with contextlib.suppress(TimeoutError, RuntimeError):
                sync(loop, session.aclose, timeout=0.1)

    @classmethod
    def _strip_protocol(cls, path: str) -> str:
        """Strip protocol prefix from path."""
        opts = infer_storage_options(path)
        if "username" not in opts:
            # Simple path without org:repo format
            path = opts.get("path", path)
            return path.lstrip("/")
        # Extract path from github://org:repo@sha/path format
        return opts["path"].lstrip("/")

    @staticmethod
    def _get_kwargs_from_urls(path: str) -> dict[str, Any]:
        """Extract org, repo, and sha from URL."""
        opts = infer_storage_options(path)
        if "username" not in opts:
            return {}
        out = {"org": opts["username"], "repo": opts["password"]}
        if opts["host"]:
            out["sha"] = opts["host"]
        return out

    async def _fetch_branches(self) -> list[str]:
        """Fetch branch names."""
        session = await self.set_session()
        r = await session.get(self.branches_url.format(org=self.org, repo=self.repo))
        r.raise_for_status()
        return [b["name"] for b in r.json()]

    async def _fetch_tags(self) -> list[str]:
        """Fetch tag names."""
        session = await self.set_session()
        r = await session.get(self.tags_url.format(org=self.org, repo=self.repo))
        r.raise_for_status()
        return [t["name"] for t in r.json()]

    @property
    def branches(self) -> list[str]:
        """Get branch names (sync wrapper)."""
        result = sync(self.loop, self._fetch_branches)
        return result if result is not None else []

    @property
    def tags(self) -> list[str]:
        """Get tag names (sync wrapper)."""
        result = sync(self.loop, self._fetch_tags)
        return result if result is not None else []

    @overload
    async def _ls(self, path: str, detail: Literal[False] = ..., **kwargs: Any) -> list[str]: ...

    @overload
    async def _ls(self, path: str, detail: Literal[True], **kwargs: Any) -> list[GithubInfo]: ...

    async def _ls(
        self, path: str, detail: bool = False, **kwargs: Any
    ) -> list[str] | list[GithubInfo]:
        """List files at given path.

        Args:
            path: Path relative to repository root
            detail: If True, return detailed info dicts
            **kwargs: Additional arguments (sha can override default)

        Returns:
            List of filenames or info dicts
        """
        session = await self.set_session()
        path = self._strip_protocol(path).rstrip("/")
        sha = kwargs.get("sha", self.root)

        # Handle root or navigate to path
        if path == "":
            _sha = sha
        else:
            # Navigate through path components to find the tree SHA
            parts = path.split("/")
            so_far = ""
            _sha = sha

            for part in parts:
                out: list[GithubInfo] = await self._ls(so_far, detail=True, sha=sha, _sha=_sha)
                so_far += "/" + part if so_far else part
                matching = [o for o in out if o["name"] == so_far]
                if not matching:
                    raise FileNotFoundError(path)
                entry = matching[0]
                if entry["type"] == "file":
                    return [entry] if detail else [path]  # type: ignore[list-item]
                _sha = entry.get("sha", sha)

        # Check cache
        cache_key = f"{path}@{sha}"
        if cache_key not in self.dircache or "_sha" in kwargs:
            _sha = kwargs.get("_sha", _sha)
            r = await session.get(self.tree_url.format(org=self.org, repo=self.repo, sha=_sha))
            if r.status_code == 404:  # noqa: PLR2004
                raise FileNotFoundError(path)
            r.raise_for_status()

            types = {"blob": "file", "tree": "directory"}
            results: list[GithubInfo] = []
            for f in r.json()["tree"]:
                if f["type"] not in types:
                    continue
                ftype = types[f["type"]]
                info: GithubInfo = {
                    "name": f"{path}/{f['path']}" if path else f["path"],
                    "mode": f["mode"],
                    "type": ftype,  # type: ignore[typeddict-item]
                    "size": f.get("size", 0),
                    "sha": f["sha"],
                }
                results.append(info)
            if "_sha" not in kwargs:
                self.dircache[cache_key] = results
        else:
            results = self.dircache[cache_key]

        if detail:
            return results
        return sorted([f["name"] for f in results])

    ls = sync_wrapper(_ls)

    async def _cat_file(self, path: str, **kwargs: Any) -> bytes:
        """Read file contents.

        Args:
            path: Path to file
            **kwargs: Additional arguments (sha can override default)

        Returns:
            File contents as bytes
        """
        session = await self.set_session()
        path = self._strip_protocol(path)
        sha = kwargs.get("sha", self.root)

        url = self.content_url.format(org=self.org, repo=self.repo, path=path, sha=sha)
        r = await session.get(url)
        if r.status_code == 404:  # noqa: PLR2004
            raise FileNotFoundError(path)
        r.raise_for_status()

        content_json = r.json()

        # If content is directly available (< 1MB)
        if content_json.get("content"):
            content = base64.b64decode(content_json["content"])

            # Check if it's a git-lfs pointer
            if not content.startswith(b"version https://git-lfs.github.com/"):
                return content

        # Download from download_url for large files or git-lfs
        download_url = content_json.get("download_url")
        if not download_url:
            msg = f"No download URL available for {path}"
            raise ValueError(msg)

        r = await session.get(download_url)
        r.raise_for_status()
        return r.content

    cat_file = sync_wrapper(_cat_file)  # pyright: ignore[reportAssignmentType]

    async def _pipe_file(self, path: str, value: bytes, **kwargs: Any) -> None:
        """Write file contents (not implemented for GitHub).

        Args:
            path: Path to file
            value: Content to write
            **kwargs: Additional arguments
        """
        msg = "Writing to GitHub repositories is not implemented"
        raise NotImplementedError(msg)

    pipe_file = sync_wrapper(_pipe_file)

    async def _info(self, path: str, **kwargs: Any) -> GithubInfo:
        """Get file info.

        Args:
            path: Path to file or directory
            **kwargs: Additional arguments (sha can override default)

        Returns:
            Info dict for the path
        """
        await self.set_session()
        path = self._strip_protocol(path)
        sha = kwargs.get("sha", self.root)

        if path == "":
            # Root directory
            return {
                "name": "",
                "type": "directory",
                "size": 0,
                "sha": sha,
                "mode": "040000",
            }

        # Get parent directory listing
        parent = path.rsplit("/", 1)[0] if "/" in path else ""
        entries = await self._ls(parent, detail=True, sha=sha)

        # Find matching entry
        for entry in entries:
            if entry["name"] == path:
                return entry

        raise FileNotFoundError(path)

    info = sync_wrapper(_info)

    async def _exists(self, path: str, **kwargs: Any) -> bool:
        """Check if path exists.

        Args:
            path: Path to check
            **kwargs: Additional arguments

        Returns:
            True if path exists
        """
        try:
            await self._info(path, **kwargs)
        except FileNotFoundError:
            return False
        else:
            return True

    exists = sync_wrapper(_exists)  # pyright: ignore[reportAssignmentType]

    async def _isdir(self, path: str, **kwargs: Any) -> bool:
        """Check if path is a directory.

        Args:
            path: Path to check
            **kwargs: Additional arguments

        Returns:
            True if path is a directory
        """
        try:
            info = await self._info(path, **kwargs)
            return info["type"] == "directory"
        except FileNotFoundError:
            return False

    isdir = sync_wrapper(_isdir)

    async def _isfile(self, path: str, **kwargs: Any) -> bool:
        """Check if path is a file.

        Args:
            path: Path to check
            **kwargs: Additional arguments

        Returns:
            True if path is a file
        """
        try:
            info = await self._info(path, **kwargs)
            return info["type"] == "file"
        except FileNotFoundError:
            return False

    isfile = sync_wrapper(_isfile)

    def invalidate_cache(self, path: str | None = None) -> None:
        """Invalidate directory cache.

        Args:
            path: Path to invalidate (None for all)
        """
        if path is None:
            self.dircache.clear()
        else:
            # Remove all cache entries for this path and descendants
            path = self._strip_protocol(path)
            keys_to_remove = [k for k in self.dircache if k.startswith(path)]
            for key in keys_to_remove:
                del self.dircache[key]

    @classmethod
    def repos(
        cls,
        org_or_user: str,
        is_org: bool = True,
        token: str | None = None,
        timeout: float = 60.0,
    ) -> list[str]:
        """List repository names for given org or user.

        Args:
            org_or_user: GitHub organization or username
            is_org: True if organization, False if user
            token: GitHub token for authentication
            timeout: Request timeout

        Returns:
            List of repository names
        """
        import httpx

        url = f"https://api.github.com/{'orgs' if is_org else 'users'}/{org_or_user}/repos"
        headers = {}
        if token:
            headers["Authorization"] = f"token {token}"

        with httpx.Client(timeout=timeout, headers=headers) as client:
            r = client.get(url)
            r.raise_for_status()
            return [repo["name"] for repo in r.json()]


if __name__ == "__main__":
    import asyncio

    async def main() -> None:
        fs = GithubFileSystem("python", "cpython", asynchronous=True)
        files = await fs._ls("Doc")
        print(files[:10])
        content = await fs._cat_file("README.md")
        print(content[:200])

    asyncio.run(main())
