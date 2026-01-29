"""GitHub Issues filesystem implementation with async support using httpx."""

from __future__ import annotations

import contextlib
import logging
import os
from typing import TYPE_CHECKING, Any, Literal, overload
import weakref

from fsspec.utils import infer_storage_options

from upathtools.async_helpers import sync, sync_wrapper
from upathtools.filesystems.base import BaseAsyncFileSystem, BaseUPath, FileInfo, GrepMatch


if TYPE_CHECKING:
    import httpx


class IssueInfo(FileInfo, total=False):
    """Info dict for GitHub Issues filesystem paths."""

    size: int
    issue_number: int
    title: str | None
    state: str | None
    body: str | None
    html_url: str | None
    created_at: str | None
    updated_at: str | None
    closed_at: str | None
    author: str | None
    labels: list[str] | None
    assignees: list[str] | None
    comments_count: int | None
    milestone: str | None
    locked: bool | None


logger = logging.getLogger(__name__)


class IssuePath(BaseUPath[IssueInfo]):
    """UPath implementation for GitHub Issues filesystem."""

    __slots__ = ()


class IssueFileSystem(BaseAsyncFileSystem[IssuePath, IssueInfo]):
    """Filesystem for accessing GitHub Issues.

    Provides read-only access to GitHub issues as files.
    Each issue is represented as a markdown file containing the issue body.

    URL format: githubissues://org/repo/issue_number
    """

    protocol = "githubissues"
    upath_cls = IssuePath
    issues_url = "https://api.github.com/repos/{org}/{repo}/issues"
    issue_url = "https://api.github.com/repos/{org}/{repo}/issues/{issue_number}"

    def __init__(
        self,
        org: str | None = None,
        repo: str | None = None,
        sha: str | None = None,
        token: str | None = None,
        timeout: float | None = None,
        loop: Any = None,
        client_kwargs: dict[str, Any] | None = None,
        **kwargs: Any,
    ) -> None:
        """Initialize the filesystem.

        Args:
            org: GitHub organization/user name
            repo: GitHub repository name
            sha: Not used, kept for API compatibility with github fs
            token: GitHub personal access token for authentication
            timeout: Connection timeout in seconds
            loop: Event loop for async operations
            client_kwargs: Additional arguments for httpx client
            **kwargs: Additional filesystem options
        """
        super().__init__(loop=loop, **kwargs)

        self.org = org
        self.repo = repo
        self.sha = sha  # Unused but kept for compatibility
        self.token = token or os.environ.get("GITHUB_TOKEN") or os.environ.get("GH_TOKEN")
        self.timeout = timeout if timeout is not None else 60.0
        self.client_kwargs = client_kwargs or {}
        self._session: httpx.AsyncClient | None = None

        if not org or not repo:
            msg = "Both org and repo must be provided"
            raise ValueError(msg)

        self.headers: dict[str, str] = {
            "Accept": "application/vnd.github+json",
            "X-GitHub-Api-Version": "2022-11-28",
        }
        if self.token:
            self.headers["Authorization"] = f"Bearer {self.token}"

        self.dircache: dict[str, Any] = {}

    @property
    def fsid(self) -> str:
        """Filesystem ID."""
        return f"githubissues-{self.org}-{self.repo}"

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
        path = infer_storage_options(path).get("path", path)
        return path.lstrip("/")

    @classmethod
    def _get_kwargs_from_urls(cls, path: str) -> dict[str, Any]:
        """Parse URL into constructor kwargs.

        URL format: githubissues://org:repo@sha/path
        or: githubissues://org/repo/issue_number
        """
        so = infer_storage_options(path)
        out: dict[str, Any] = {}

        if so.get("username"):
            out["org"] = so["username"]
        if so.get("password"):
            out["repo"] = so["password"]
        if so.get("host"):
            out["sha"] = so["host"]

        return out

    async def _fetch_issue(self, issue_number: int) -> dict[str, Any]:
        """Fetch a specific issue.

        Args:
            issue_number: Issue number to fetch

        Returns:
            Dictionary containing issue data

        Raises:
            FileNotFoundError: If issue is not found
        """
        session = await self.set_session()
        url = self.issue_url.format(org=self.org, repo=self.repo, issue_number=issue_number)

        logger.debug("Fetching issue: %s", url)
        response = await session.get(url)

        if response.status_code == 404:  # noqa: PLR2004
            msg = f"Issue not found: {self.org}/{self.repo}#{issue_number}"
            raise FileNotFoundError(msg)

        response.raise_for_status()
        return response.json()

    async def _fetch_issues(
        self,
        state: str = "all",
        page: int = 1,
        per_page: int = 100,
    ) -> list[dict[str, Any]]:
        """Fetch issues for the repository.

        Args:
            state: Issue state filter ('open', 'closed', 'all')
            page: Page number for pagination
            per_page: Number of issues per page

        Returns:
            List of issue data dictionaries
        """
        session = await self.set_session()
        url = self.issues_url.format(org=self.org, repo=self.repo)
        params: dict[str, Any] = {"state": state, "page": page, "per_page": per_page}

        logger.debug("Fetching issues: %s", url)
        response = await session.get(url, params=params)
        response.raise_for_status()
        return response.json()

    async def _get_all_issues(self) -> list[IssueInfo]:
        """Get all issues as IssueInfo objects."""
        if "" in self.dircache:
            return self.dircache[""]

        issues = await self._fetch_issues(page=1, per_page=100)
        all_issues = issues.copy()
        page = 2
        while len(issues) == 100:  # noqa: PLR2004
            issues = await self._fetch_issues(page=page, per_page=100)
            all_issues.extend(issues)
            page += 1

        # Filter out pull requests (they appear in the issues endpoint)
        all_issues = [i for i in all_issues if "pull_request" not in i]

        out = [_issue_to_info(issue) for issue in all_issues]
        self.dircache[""] = out
        return out

    @overload
    async def _ls(
        self, path: str, detail: Literal[True] = ..., **kwargs: Any
    ) -> list[IssueInfo]: ...

    @overload
    async def _ls(self, path: str, detail: Literal[False], **kwargs: Any) -> list[str]: ...

    async def _ls(
        self,
        path: str,
        detail: bool = True,
        **kwargs: Any,
    ) -> list[IssueInfo] | list[str]:
        """List contents of path.

        Args:
            path: Path to list (empty for root, issue number for specific issue)
            detail: Whether to include detailed information
            **kwargs: Additional arguments

        Returns:
            List of issue information or names
        """
        path = self._strip_protocol(path or "")
        logger.debug("Listing path: %s", path)

        if not path:
            # Root - list all issues
            results = await self._get_all_issues()
        else:
            # Specific issue
            parts = path.rstrip("/").split("/")
            issue_part = parts[0].removesuffix(".md")

            try:
                issue_number = int(issue_part)
            except ValueError:
                msg = f"Invalid issue path: {path}"
                raise FileNotFoundError(msg) from None

            try:
                issue = await self._fetch_issue(issue_number)
                results = [_issue_to_info(issue)]
            except FileNotFoundError:
                msg = f"Issue not found: {path}"
                raise FileNotFoundError(msg) from None

        if detail:
            return results
        return [f["name"] for f in results]

    ls = sync_wrapper(_ls)  # pyright: ignore[reportAssignmentType]

    async def _cat_file(
        self,
        path: str,
        start: int | None = None,
        end: int | None = None,
        **kwargs: Any,
    ) -> bytes:
        """Get contents of an issue as markdown."""
        path = self._strip_protocol(path)
        issue_part = path.rstrip("/").split("/")[0].removesuffix(".md")

        try:
            issue_number = int(issue_part)
        except ValueError:
            msg = f"Invalid issue path: {path}"
            raise FileNotFoundError(msg) from None

        issue = await self._fetch_issue(issue_number)
        title = issue.get("title", "")
        body = issue.get("body") or ""
        state = issue.get("state", "")
        labels = [lbl["name"] for lbl in issue.get("labels", [])]
        author = issue.get("user", {}).get("login", "") if issue.get("user") else ""

        # Format as markdown with frontmatter-like header
        lines = [
            f"# {title}",
            "",
            f"**Issue #{issue_number}** | **State:** {state} | **Author:** @{author}",
        ]
        if labels:
            lines.append(f"**Labels:** {', '.join(labels)}")
        lines.extend(["", "---", "", body])

        content = "\n".join(lines).encode()

        if start is not None or end is not None:
            start = start or 0
            end = min(end or len(content), len(content))
            content = content[start:end]

        return content

    cat_file = sync_wrapper(_cat_file)  # type: ignore

    async def _pipe_file(
        self,
        path: str,
        value: bytes,
        *,
        title: str | None = None,
        labels: list[str] | None = None,
        assignees: list[str] | None = None,
        milestone: int | None = None,
        **kwargs: Any,
    ) -> None:
        """Create or update an issue.

        For new issues, the path is ignored (issue number assigned by GitHub).
        For updates, path should be the issue number.

        The content (value) becomes the issue body.
        Title can be passed as kwarg or extracted from first markdown heading.

        Args:
            path: Issue number to update, or any path for new issue
            value: Issue body content (markdown)
            title: Issue title (if not provided, extracted from first # heading)
            labels: List of label names to apply
            assignees: List of usernames to assign
            milestone: Milestone ID to associate
            **kwargs: Additional arguments
        """
        if not self.token:
            msg = "GitHub token is required for write operations"
            raise ValueError(msg)

        session = await self.set_session()
        path = self._strip_protocol(path)

        # Decode content
        try:
            body = value.decode()
        except UnicodeDecodeError:
            msg = "Issue body must be valid UTF-8 text"
            raise ValueError(msg) from None

        # Extract title from first heading if not provided
        if title is None:
            for line in body.split("\n"):
                line = line.strip()
                if line.startswith("# "):
                    title = line[2:].strip()
                    # Remove the title line from body
                    body = body.replace(line, "", 1).strip()
                    break

        if not title:
            msg = "Issue title is required (pass title= or start body with '# Title')"
            raise ValueError(msg)

        # Check if updating existing issue
        issue_number: int | None = None
        if path:
            issue_part = path.rstrip("/").split("/")[0].removesuffix(".md")
            with contextlib.suppress(ValueError):
                issue_number = int(issue_part)
                # Verify issue exists
                await self._fetch_issue(issue_number)

        data: dict[str, Any] = {"title": title, "body": body}
        if labels is not None:
            data["labels"] = labels
        if assignees is not None:
            data["assignees"] = assignees
        if milestone is not None:
            data["milestone"] = milestone

        if issue_number is not None:
            # Update existing issue
            url = self.issue_url.format(org=self.org, repo=self.repo, issue_number=issue_number)
            logger.debug("Updating issue #%d: %s", issue_number, url)
            response = await session.patch(url, json=data)
        else:
            # Create new issue
            url = self.issues_url.format(org=self.org, repo=self.repo)
            logger.debug("Creating new issue: %s", url)
            response = await session.post(url, json=data)

        if response.status_code >= 400:  # noqa: PLR2004
            logger.error("API error: %s %s", response.status_code, response.text)
            response.raise_for_status()

        self.invalidate_cache()

    pipe_file = sync_wrapper(_pipe_file)  # pyright: ignore[reportAssignmentType]

    async def _info(self, path: str, **kwargs: Any) -> IssueInfo:
        """Get info for a path."""
        path = self._strip_protocol(path)

        if not path:
            return IssueInfo(name="", type="directory", size=0)

        issue_part = path.rstrip("/").split("/")[0].removesuffix(".md")

        try:
            issue_number = int(issue_part)
        except ValueError:
            msg = f"Invalid issue path: {path}"
            raise FileNotFoundError(msg) from None

        issue = await self._fetch_issue(issue_number)
        return _issue_to_info(issue)

    info = sync_wrapper(_info)  # type: ignore

    async def _exists(self, path: str, **kwargs: Any) -> bool:
        """Check if path exists."""
        try:
            await self._info(path)
        except FileNotFoundError:
            return False
        else:
            return True

    exists = sync_wrapper(_exists)  # type: ignore

    async def _isdir(self, path: str) -> bool:
        """Check if path is a directory."""
        path = self._strip_protocol(path)
        return not path  # Only root is a directory

    isdir = sync_wrapper(_isdir)

    async def _isfile(self, path: str) -> bool:
        """Check if path is a file."""
        path = self._strip_protocol(path)
        if not path:
            return False
        return await self._exists(path)

    isfile = sync_wrapper(_isfile)

    async def _grep(
        self,
        path: str,
        pattern: str,
        *,
        max_count: int | None = None,
        case_sensitive: bool | None = None,
        hidden: bool = False,
        no_ignore: bool = False,
        globs: list[str] | None = None,
        context_before: int | None = None,
        context_after: int | None = None,
        multiline: bool = False,
    ) -> list[GrepMatch]:
        """Search issues using GitHub's search API.

        Uses the GitHub search API which is much faster than fetching
        all issues and searching locally.

        Args:
            path: Ignored (searches all issues in repo)
            pattern: Search query string
            max_count: Maximum total number of matches to return.
            case_sensitive: Not used (GitHub API handles search).
            hidden: Not used.
            no_ignore: Not used.
            globs: Not used.
            context_before: Not used.
            context_after: Not used.
            multiline: Not used.

        Returns:
            List of GrepMatch objects with issue file, number, and content.
        """
        session = await self.set_session()

        # Build GitHub search query
        query = f"repo:{self.org}/{self.repo} is:issue {pattern}"
        url = "https://api.github.com/search/issues"
        params: dict[str, Any] = {"q": query, "per_page": 100}

        # Request text match metadata
        headers = {**self.headers, "Accept": "application/vnd.github.text-match+json"}

        logger.debug("Searching issues: %s", query)
        response = await session.get(url, params=params, headers=headers)
        response.raise_for_status()
        data = response.json()

        matches: list[GrepMatch] = []
        for item in data.get("items", []):
            issue_number = item.get("number")
            title = item.get("title", "")

            # Extract text matches if available
            text_matches = item.get("text_matches", [])
            if text_matches:
                matches.extend(
                    GrepMatch(
                        path=f"{issue_number}.md",
                        line_number=issue_number,
                        text=tm.get("fragment", ""),
                    )
                    for tm in text_matches
                )
            else:
                # No text match detail, just include the issue
                matches.append(
                    GrepMatch(
                        path=f"{issue_number}.md",
                        line_number=issue_number,
                        text=title,
                    )
                )

        if max_count is not None:
            return matches[:max_count]
        return matches

    grep = sync_wrapper(_grep)

    def invalidate_cache(self, path: str | None = None) -> None:
        """Clear the directory cache."""
        if path is None:
            self.dircache.clear()
        else:
            path = self._strip_protocol(path or "")
            self.dircache.pop(path, None)
            self.dircache.pop("", None)


def _issue_to_info(issue: dict[str, Any]) -> IssueInfo:
    """Convert GitHub API issue response to IssueInfo."""
    body = issue.get("body") or ""
    return IssueInfo(
        name=f"{issue['number']}.md",
        type="file",
        size=len(body.encode()),
        issue_number=issue["number"],
        title=issue.get("title"),
        state=issue.get("state"),
        body=body,
        html_url=issue.get("html_url"),
        created_at=issue.get("created_at"),
        updated_at=issue.get("updated_at"),
        closed_at=issue.get("closed_at"),
        author=issue.get("user", {}).get("login") if issue.get("user") else None,
        labels=[lbl["name"] for lbl in issue.get("labels", [])],
        assignees=[a["login"] for a in issue.get("assignees", [])],
        comments_count=issue.get("comments"),
        milestone=issue.get("milestone", {}).get("title") if issue.get("milestone") else None,
        locked=issue.get("locked"),
    )


if __name__ == "__main__":
    import asyncio

    logging.basicConfig(level=logging.INFO)
    print(f"Environment GITHUB_TOKEN set: {'GITHUB_TOKEN' in os.environ}")

    async def main() -> None:
        fs = IssueFileSystem(org="phil65", repo="mknodes")
        print("\nListing issues:")
        issues = await fs._ls("", detail=True)
        for issue in issues[:5]:  # Show first 5
            print(f"  #{issue.get('issue_number')}: {issue.get('title')} [{issue.get('state')}]")

        if issues:
            first_issue = issues[0].get("issue_number")
            print(f"\nReading issue #{first_issue}:")
            content = await fs._cat_file(f"{first_issue}.md")
            print(content.decode()[:500])

        # Example: Creating a new issue (uncomment to test)
        # await fs._pipe_file(
        #     "new.md",
        #     b"# Test Issue\n\nThis is a test issue created via IssueFileSystem.",
        #     labels=["test"],
        # )

    asyncio.run(main())
