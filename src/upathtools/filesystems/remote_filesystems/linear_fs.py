"""Linear Issues filesystem implementation with async support using httpx."""

from __future__ import annotations

import contextlib
import logging
import os
from typing import TYPE_CHECKING, Any, Literal, overload
import weakref

from fsspec.asyn import sync, sync_wrapper
from fsspec.utils import infer_storage_options

from upathtools.filesystems.base import BaseAsyncFileSystem, BaseUPath, FileInfo


if TYPE_CHECKING:
    import httpx


class LinearIssueInfo(FileInfo, total=False):
    """Info dict for Linear Issues filesystem paths."""

    size: int
    issue_id: str
    identifier: str  # e.g., "ENG-123"
    title: str | None
    state: str | None
    description: str | None
    url: str | None
    created_at: str | None
    updated_at: str | None
    due_date: str | None
    priority: int | None
    priority_label: str | None
    labels: list[str] | None
    assignee: str | None
    project: str | None


class LinearCommentInfo(FileInfo, total=False):
    """Info dict for Linear comment files."""

    size: int
    comment_id: str
    issue_identifier: str
    body: str | None
    created_at: str | None
    updated_at: str | None
    author: str | None


logger = logging.getLogger(__name__)


class LinearIssuePath(BaseUPath[LinearIssueInfo]):
    """UPath implementation for Linear Issues filesystem."""

    __slots__ = ()


class LinearIssueFileSystem(BaseAsyncFileSystem[LinearIssuePath, LinearIssueInfo]):
    """Filesystem for accessing Linear Issues.

    Provides read/write access to Linear issues as files.
    Each issue is represented as a markdown file.

    URL format: linear://team_key/TEAM-123.md (flat mode)
    URL format: linear://team_key/TEAM-123/issue.md (extended mode)

    In extended mode, issues are folders containing:
    - issue.md: The main issue content
    - comments/001.md, 002.md, etc.: Comments on the issue
    """

    protocol = "linear"
    upath_cls = LinearIssuePath
    base_url = "https://api.linear.app/graphql"

    def __init__(
        self,
        team_key: str | None = None,
        api_key: str | None = None,
        extended: bool = False,
        timeout: float | None = None,
        loop: Any = None,
        client_kwargs: dict[str, Any] | None = None,
        **kwargs: Any,
    ) -> None:
        """Initialize the filesystem.

        Args:
            team_key: Linear team key (e.g., "ENG")
            api_key: Linear API key for authentication
            extended: If True, issues are folders with comments as sub-files
            timeout: Connection timeout in seconds
            loop: Event loop for async operations
            client_kwargs: Additional arguments for httpx client
            **kwargs: Additional filesystem options
        """
        super().__init__(loop=loop, **kwargs)

        self.team_key = team_key
        self.api_key = api_key or os.environ.get("LINEAR_API_KEY")
        self.extended = extended
        self.timeout = timeout if timeout is not None else 60.0
        self.client_kwargs = client_kwargs or {}
        self._session: httpx.AsyncClient | None = None
        self._team_id: str | None = None

        if not team_key:
            msg = "team_key must be provided"
            raise ValueError(msg)

        if not self.api_key:
            msg = "api_key must be provided or LINEAR_API_KEY environment variable must be set"
            raise ValueError(msg)

        self.headers: dict[str, str] = {
            "Content-Type": "application/json",
            "Authorization": self.api_key,
        }

        self.dircache: dict[str, Any] = {}

    @property
    def fsid(self) -> str:
        """Filesystem ID."""
        return f"linear-{self.team_key}"

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

        URL format: linear://team_key/path
        """
        so = infer_storage_options(path)
        out: dict[str, Any] = {}

        if so.get("host"):
            out["team_key"] = so["host"]

        return out

    async def _graphql_request(
        self,
        query: str,
        variables: dict[str, Any] | None = None,
    ) -> dict[str, Any]:
        """Execute a GraphQL request.

        Args:
            query: GraphQL query string
            variables: Query variables

        Returns:
            Response data

        Raises:
            RuntimeError: If the request fails
        """
        session = await self.set_session()
        payload: dict[str, Any] = {"query": query}
        if variables:
            payload["variables"] = variables

        logger.debug("GraphQL request: %s", query[:100])
        response = await session.post(self.base_url, json=payload)

        if response.status_code != 200:  # noqa: PLR2004
            msg = f"GraphQL request failed: {response.status_code} {response.text}"
            raise RuntimeError(msg)

        result = response.json()
        if "errors" in result:
            msg = f"GraphQL errors: {result['errors']}"
            raise RuntimeError(msg)

        return result.get("data", {})

    async def _get_team_id(self) -> str:
        """Get the team ID from the team key."""
        if self._team_id is not None:
            return self._team_id

        query = """
        query GetTeam($filter: TeamFilter) {
            teams(filter: $filter, first: 1) {
                nodes {
                    id
                    key
                    name
                }
            }
        }
        """
        variables = {"filter": {"key": {"eq": self.team_key}}}
        data = await self._graphql_request(query, variables)

        teams = data.get("teams", {}).get("nodes", [])
        if not teams:
            msg = f"Team not found: {self.team_key}"
            raise FileNotFoundError(msg)

        self._team_id = teams[0]["id"]
        return self._team_id

    async def _fetch_issue(self, identifier: str) -> dict[str, Any]:
        """Fetch a specific issue by identifier.

        Args:
            identifier: Issue identifier (e.g., "ENG-123")

        Returns:
            Dictionary containing issue data

        Raises:
            FileNotFoundError: If issue is not found
        """
        # Extract the number from the identifier (e.g., "PHI-7" -> 7)
        try:
            parts = identifier.split("-")
            if len(parts) != 2:
                raise ValueError("Invalid identifier format")
            issue_number = int(parts[1])
        except (ValueError, IndexError):
            msg = f"Invalid issue identifier format: {identifier}"
            raise FileNotFoundError(msg) from None

        query = """
        query GetIssue($filter: IssueFilter) {
            issues(filter: $filter, first: 1) {
                nodes {
                    id
                    identifier
                    title
                    description
                    url
                    createdAt
                    updatedAt
                    dueDate
                    priority
                    priorityLabel
                    state {
                        name
                    }
                    assignee {
                        name
                        email
                    }
                    labels {
                        nodes {
                            name
                        }
                    }
                    project {
                        name
                    }
                }
            }
        }
        """
        variables = {"filter": {"number": {"eq": issue_number}}}
        data = await self._graphql_request(query, variables)

        issues = data.get("issues", {}).get("nodes", [])
        if not issues:
            msg = f"Issue not found: {identifier}"
            raise FileNotFoundError(msg)

        # Verify the identifier matches (in case of team key mismatch)
        found_issue = issues[0]
        if found_issue.get("identifier") != identifier:
            msg = f"Issue not found: {identifier}"
            raise FileNotFoundError(msg)

        return found_issue

    async def _fetch_issues(self) -> list[dict[str, Any]]:
        """Fetch all issues for the team.

        Returns:
            List of issue data dictionaries
        """
        team_id = await self._get_team_id()

        query = """
        query GetTeamIssues($teamId: String!, $after: String) {
            team(id: $teamId) {
                issues(first: 100, after: $after) {
                    nodes {
                        id
                        identifier
                        title
                        description
                        url
                        createdAt
                        updatedAt
                        dueDate
                        priority
                        priorityLabel
                        state {
                            name
                        }
                        assignee {
                            name
                            email
                        }
                        labels {
                            nodes {
                                name
                            }
                        }
                        project {
                            name
                        }
                    }
                    pageInfo {
                        hasNextPage
                        endCursor
                    }
                }
            }
        }
        """
        all_issues: list[dict[str, Any]] = []
        cursor: str | None = None

        while True:
            variables: dict[str, Any] = {"teamId": team_id}
            if cursor:
                variables["after"] = cursor

            data = await self._graphql_request(query, variables)
            issues_data = data.get("team", {}).get("issues", {})
            issues = issues_data.get("nodes", [])
            all_issues.extend(issues)

            page_info = issues_data.get("pageInfo", {})
            if not page_info.get("hasNextPage"):
                break
            cursor = page_info.get("endCursor")

        return all_issues

    async def _fetch_comments(self, issue_id: str) -> list[dict[str, Any]]:
        """Fetch comments for an issue.

        Args:
            issue_id: Linear issue ID (UUID)

        Returns:
            List of comment data dictionaries
        """
        query = """
        query GetIssueComments($issueId: String!, $after: String) {
            issue(id: $issueId) {
                comments(first: 100, after: $after) {
                    nodes {
                        id
                        body
                        createdAt
                        updatedAt
                        user {
                            name
                            email
                        }
                    }
                    pageInfo {
                        hasNextPage
                        endCursor
                    }
                }
            }
        }
        """
        all_comments: list[dict[str, Any]] = []
        cursor: str | None = None

        while True:
            variables: dict[str, Any] = {"issueId": issue_id}
            if cursor:
                variables["after"] = cursor

            data = await self._graphql_request(query, variables)
            comments_data = data.get("issue", {}).get("comments", {})
            comments = comments_data.get("nodes", [])
            all_comments.extend(comments)

            page_info = comments_data.get("pageInfo", {})
            if not page_info.get("hasNextPage"):
                break
            cursor = page_info.get("endCursor")

        return all_comments

    async def _get_all_issues(self) -> list[LinearIssueInfo]:
        """Get all issues as LinearIssueInfo objects."""
        cache_key = "_issues"
        if cache_key in self.dircache:
            return self.dircache[cache_key]

        issues = await self._fetch_issues()
        out = [_issue_to_info(issue, extended=self.extended) for issue in issues]
        self.dircache[cache_key] = out
        return out

    @overload
    async def _ls(
        self, path: str, detail: Literal[True] = ..., **kwargs: Any
    ) -> list[LinearIssueInfo | LinearCommentInfo]: ...

    @overload
    async def _ls(self, path: str, detail: Literal[False], **kwargs: Any) -> list[str]: ...

    async def _ls(
        self,
        path: str,
        detail: bool = True,
        **kwargs: Any,
    ) -> list[LinearIssueInfo | LinearCommentInfo] | list[str]:
        """List contents of path.

        Args:
            path: Path to list
            detail: Whether to include detailed information
            **kwargs: Additional arguments

        Returns:
            List of issue/comment information or names
        """
        path = self._strip_protocol(path or "")
        logger.debug("Listing path: %s (extended=%s)", path, self.extended)

        if not path:
            # Root - list all issues
            results = await self._get_all_issues()
            if detail:
                return results
            return [f["name"] for f in results]

        parts = path.rstrip("/").split("/")

        if self.extended:
            # Extended mode: issues are directories
            identifier = parts[0]

            if len(parts) == 1:
                # Listing issue directory - show issue.md and comments/
                issue = await self._fetch_issue(identifier)
                results: list[LinearIssueInfo | LinearCommentInfo] = [
                    LinearIssueInfo(
                        name=f"{identifier}/issue.md",
                        type="file",
                        size=len((issue.get("description") or "").encode()),
                    ),
                    LinearIssueInfo(
                        name=f"{identifier}/comments",
                        type="directory",
                        size=0,
                    ),
                ]
                if detail:
                    return results
                return [f["name"] for f in results]

            if len(parts) == 2 and parts[1] == "comments":
                # Listing comments directory
                issue = await self._fetch_issue(identifier)
                comments = await self._fetch_comments(issue["id"])
                results = [
                    _comment_to_info(comment, identifier, idx)
                    for idx, comment in enumerate(comments, 1)
                ]
                if detail:
                    return results
                return [f["name"] for f in results]

        # Flat mode or specific file - check if it's a valid issue
        identifier = parts[0].removesuffix(".md")
        try:
            issue = await self._fetch_issue(identifier)
            results = [_issue_to_info(issue, extended=self.extended)]
            if detail:
                return results
            return [f["name"] for f in results]
        except FileNotFoundError:
            msg = f"Path not found: {path}"
            raise FileNotFoundError(msg) from None

    ls = sync_wrapper(_ls)

    async def _cat_file(
        self,
        path: str,
        start: int | None = None,
        end: int | None = None,
        **kwargs: Any,
    ) -> bytes:
        """Get contents of an issue or comment as markdown."""
        path = self._strip_protocol(path)
        parts = path.rstrip("/").split("/")

        if self.extended:
            identifier = parts[0]

            if len(parts) >= 2 and parts[1] == "issue.md":
                # Reading main issue file
                issue = await self._fetch_issue(identifier)
                content = _format_issue_markdown(issue)
            elif len(parts) >= 3 and parts[1] == "comments":
                # Reading a comment file
                comment_file = parts[2].removesuffix(".md")
                try:
                    comment_idx = int(comment_file) - 1
                except ValueError:
                    msg = f"Invalid comment path: {path}"
                    raise FileNotFoundError(msg) from None

                issue = await self._fetch_issue(identifier)
                comments = await self._fetch_comments(issue["id"])

                if comment_idx < 0 or comment_idx >= len(comments):
                    msg = f"Comment not found: {path}"
                    raise FileNotFoundError(msg)

                content = _format_comment_markdown(comments[comment_idx], identifier)
            else:
                msg = f"Invalid path: {path}"
                raise FileNotFoundError(msg)
        else:
            # Flat mode
            identifier = parts[0].removesuffix(".md")
            issue = await self._fetch_issue(identifier)
            content = _format_issue_markdown(issue)

        content_bytes = content.encode()
        if start is not None or end is not None:
            start = start or 0
            end = min(end or len(content_bytes), len(content_bytes))
            content_bytes = content_bytes[start:end]

        return content_bytes

    cat_file = sync_wrapper(_cat_file)  # type: ignore

    async def _pipe_file(
        self,
        path: str,
        value: bytes,
        *,
        title: str | None = None,
        state_id: str | None = None,
        priority: int | None = None,
        labels: list[str] | None = None,
        assignee_id: str | None = None,
        **kwargs: Any,
    ) -> None:
        """Create or update an issue or comment.

        For issues, the content becomes the description.
        Title can be passed as kwarg or extracted from first markdown heading.

        Args:
            path: Issue identifier to update, or path for new issue
            value: Issue/comment body content (markdown)
            title: Issue title (if not provided, extracted from first # heading)
            state_id: Workflow state ID to set
            priority: Priority level (0-4)
            labels: List of label IDs to apply
            assignee_id: User ID to assign
            **kwargs: Additional arguments
        """
        path = self._strip_protocol(path)
        parts = path.rstrip("/").split("/")

        try:
            body = value.decode()
        except UnicodeDecodeError:
            msg = "Content must be valid UTF-8 text"
            raise ValueError(msg) from None

        if self.extended and len(parts) >= 3 and parts[1] == "comments":
            # Creating/updating a comment
            identifier = parts[0]
            issue = await self._fetch_issue(identifier)
            await self._create_comment(issue["id"], body)
            self.invalidate_cache()
            return

        # Creating/updating an issue
        if self.extended:
            identifier = parts[0] if parts[0] != "issue.md" else None
        else:
            identifier = parts[0].removesuffix(".md") if parts else None

        # Extract title from first heading if not provided
        if title is None:
            for line in body.split("\n"):
                line = line.strip()
                if line.startswith("# "):
                    title = line[2:].strip()
                    body = body.replace(line, "", 1).strip()
                    break

        # Check if updating existing issue
        existing_issue: dict[str, Any] | None = None
        if identifier:
            with contextlib.suppress(FileNotFoundError):
                existing_issue = await self._fetch_issue(identifier)

        if existing_issue:
            await self._update_issue(
                existing_issue["id"],
                description=body,
                title=title,
                state_id=state_id,
                priority=priority,
                assignee_id=assignee_id,
            )
        else:
            if not title:
                msg = "Issue title is required (pass title= or start body with '# Title')"
                raise ValueError(msg)

            team_id = await self._get_team_id()
            await self._create_issue(
                team_id=team_id,
                title=title,
                description=body,
                state_id=state_id,
                priority=priority,
                assignee_id=assignee_id,
            )

        self.invalidate_cache()

    pipe_file = sync_wrapper(_pipe_file)

    async def _create_issue(
        self,
        team_id: str,
        title: str,
        description: str,
        state_id: str | None = None,
        priority: int | None = None,
        assignee_id: str | None = None,
    ) -> dict[str, Any]:
        """Create a new issue."""
        mutation = """
        mutation CreateIssue($input: IssueCreateInput!) {
            issueCreate(input: $input) {
                success
                issue {
                    id
                    identifier
                    title
                    url
                }
            }
        }
        """
        input_data: dict[str, Any] = {
            "teamId": team_id,
            "title": title,
            "description": description,
        }
        if state_id:
            input_data["stateId"] = state_id
        if priority is not None:
            input_data["priority"] = priority
        if assignee_id:
            input_data["assigneeId"] = assignee_id

        data = await self._graphql_request(mutation, {"input": input_data})
        result = data.get("issueCreate", {})

        if not result.get("success"):
            msg = "Failed to create issue"
            raise RuntimeError(msg)

        return result.get("issue", {})

    async def _update_issue(
        self,
        issue_id: str,
        description: str | None = None,
        title: str | None = None,
        state_id: str | None = None,
        priority: int | None = None,
        assignee_id: str | None = None,
    ) -> dict[str, Any]:
        """Update an existing issue."""
        mutation = """
        mutation UpdateIssue($id: String!, $input: IssueUpdateInput!) {
            issueUpdate(id: $id, input: $input) {
                success
                issue {
                    id
                    identifier
                    title
                    url
                }
            }
        }
        """
        input_data: dict[str, Any] = {}
        if description is not None:
            input_data["description"] = description
        if title is not None:
            input_data["title"] = title
        if state_id:
            input_data["stateId"] = state_id
        if priority is not None:
            input_data["priority"] = priority
        if assignee_id:
            input_data["assigneeId"] = assignee_id

        data = await self._graphql_request(mutation, {"id": issue_id, "input": input_data})
        result = data.get("issueUpdate", {})

        if not result.get("success"):
            msg = "Failed to update issue"
            raise RuntimeError(msg)

        return result.get("issue", {})

    async def _create_comment(self, issue_id: str, body: str) -> dict[str, Any]:
        """Create a comment on an issue."""
        mutation = """
        mutation CreateComment($input: CommentCreateInput!) {
            commentCreate(input: $input) {
                success
                comment {
                    id
                    body
                    createdAt
                }
            }
        }
        """
        input_data = {"issueId": issue_id, "body": body}
        data = await self._graphql_request(mutation, {"input": input_data})
        result = data.get("commentCreate", {})

        if not result.get("success"):
            msg = "Failed to create comment"
            raise RuntimeError(msg)

        return result.get("comment", {})

    async def _info(self, path: str, **kwargs: Any) -> LinearIssueInfo | LinearCommentInfo:
        """Get info for a path."""
        path = self._strip_protocol(path)

        if not path:
            return LinearIssueInfo(name="", type="directory", size=0)

        parts = path.rstrip("/").split("/")

        if self.extended:
            identifier = parts[0]

            if len(parts) == 1:
                # Issue directory
                try:
                    await self._fetch_issue(identifier)
                    return LinearIssueInfo(name=identifier, type="directory", size=0)
                except FileNotFoundError:
                    msg = f"Issue not found: {identifier}"
                    raise FileNotFoundError(msg) from None

            if len(parts) == 2:
                if parts[1] == "issue.md":
                    issue = await self._fetch_issue(identifier)
                    return _issue_to_info(issue, extended=True, as_file=True)
                if parts[1] == "comments":
                    return LinearIssueInfo(name=f"{identifier}/comments", type="directory", size=0)

            if len(parts) == 3 and parts[1] == "comments":
                comment_file = parts[2].removesuffix(".md")
                try:
                    comment_idx = int(comment_file) - 1
                except ValueError:
                    msg = f"Invalid comment path: {path}"
                    raise FileNotFoundError(msg) from None

                issue = await self._fetch_issue(identifier)
                comments = await self._fetch_comments(issue["id"])

                if comment_idx < 0 or comment_idx >= len(comments):
                    msg = f"Comment not found: {path}"
                    raise FileNotFoundError(msg)

                return _comment_to_info(comments[comment_idx], identifier, comment_idx + 1)

        # Flat mode
        identifier = parts[0].removesuffix(".md")
        issue = await self._fetch_issue(identifier)
        return _issue_to_info(issue, extended=False)

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

        if not path:
            return True

        if not self.extended:
            return False

        parts = path.rstrip("/").split("/")

        if len(parts) == 1:
            # Issue directory
            try:
                await self._fetch_issue(parts[0])
                return True
            except FileNotFoundError:
                return False

        if len(parts) == 2 and parts[1] == "comments":
            return True

        return False

    isdir = sync_wrapper(_isdir)

    async def _isfile(self, path: str) -> bool:
        """Check if path is a file."""
        path = self._strip_protocol(path)
        if not path:
            return False

        try:
            info = await self._info(path)
            return info.get("type") == "file"
        except FileNotFoundError:
            return False

    isfile = sync_wrapper(_isfile)

    def invalidate_cache(self, path: str | None = None) -> None:
        """Clear the directory cache."""
        if path is None:
            self.dircache.clear()
        else:
            path = self._strip_protocol(path or "")
            self.dircache.pop(path, None)
            self.dircache.pop("_issues", None)


def _issue_to_info(
    issue: dict[str, Any],
    *,
    extended: bool = False,
    as_file: bool = False,
) -> LinearIssueInfo:
    """Convert Linear API issue response to LinearIssueInfo."""
    description = issue.get("description") or ""
    identifier = issue["identifier"]

    if extended and not as_file:
        name = identifier
        file_type = "directory"
    else:
        name = f"{identifier}/issue.md" if extended else f"{identifier}.md"
        file_type = "file"

    return LinearIssueInfo(
        name=name,
        type=file_type,
        size=len(description.encode()) if file_type == "file" else 0,
        issue_id=issue["id"],
        identifier=identifier,
        title=issue.get("title"),
        state=issue.get("state", {}).get("name") if issue.get("state") else None,
        description=description,
        url=issue.get("url"),
        created_at=issue.get("createdAt"),
        updated_at=issue.get("updatedAt"),
        due_date=issue.get("dueDate"),
        priority=issue.get("priority"),
        priority_label=issue.get("priorityLabel"),
        labels=[lbl["name"] for lbl in issue.get("labels", {}).get("nodes", [])],
        assignee=(issue.get("assignee", {}).get("name") if issue.get("assignee") else None),
        project=(issue.get("project", {}).get("name") if issue.get("project") else None),
    )


def _comment_to_info(
    comment: dict[str, Any],
    issue_identifier: str,
    index: int,
) -> LinearCommentInfo:
    """Convert Linear API comment response to LinearCommentInfo."""
    body = comment.get("body") or ""
    return LinearCommentInfo(
        name=f"{issue_identifier}/comments/{index:03d}.md",
        type="file",
        size=len(body.encode()),
        comment_id=comment["id"],
        issue_identifier=issue_identifier,
        body=body,
        created_at=comment.get("createdAt"),
        updated_at=comment.get("updatedAt"),
        author=comment.get("user", {}).get("name") if comment.get("user") else None,
    )


def _format_issue_markdown(issue: dict[str, Any]) -> str:
    """Format an issue as markdown with frontmatter-style header."""
    title = issue.get("title", "")
    identifier = issue["identifier"]
    description = issue.get("description") or ""
    state = issue.get("state", {}).get("name", "") if issue.get("state") else ""
    priority_label = issue.get("priorityLabel", "")
    assignee = issue.get("assignee", {}).get("name", "") if issue.get("assignee") else ""
    labels = [lbl["name"] for lbl in issue.get("labels", {}).get("nodes", [])]
    project = issue.get("project", {}).get("name", "") if issue.get("project") else ""
    due_date = issue.get("dueDate") or ""
    url = issue.get("url") or ""

    lines = [
        f"# {title}",
        "",
        f"**{identifier}** | **State:** {state} | **Priority:** {priority_label}",
    ]

    if assignee:
        lines.append(f"**Assignee:** {assignee}")
    if labels:
        lines.append(f"**Labels:** {', '.join(labels)}")
    if project:
        lines.append(f"**Project:** {project}")
    if due_date:
        lines.append(f"**Due:** {due_date}")
    if url:
        lines.append(f"**URL:** {url}")

    lines.extend(["", "---", "", description])

    return "\n".join(lines)


def _format_comment_markdown(comment: dict[str, Any], issue_identifier: str) -> str:
    """Format a comment as markdown."""
    body = comment.get("body") or ""
    author = comment.get("user", {}).get("name", "Unknown") if comment.get("user") else "Unknown"
    created_at = comment.get("createdAt", "")

    lines = [
        f"**Comment on {issue_identifier}**",
        f"**Author:** {author} | **Created:** {created_at}",
        "",
        "---",
        "",
        body,
    ]

    return "\n".join(lines)


if __name__ == "__main__":
    import asyncio

    logging.basicConfig(level=logging.INFO)
    print(f"Environment LINEAR_API_KEY set: {'LINEAR_API_KEY' in os.environ}")

    async def main() -> None:
        # Example usage - replace with your team key
        fs = LinearIssueFileSystem(team_key="ENG", extended=True)

        print("\nListing issues:")
        issues = await fs._ls("", detail=True)
        for issue in issues[:5]:  # Show first 5
            print(f"  {issue.get('identifier')}: {issue.get('title')} [{issue.get('state')}]")

        if issues:
            first_issue = issues[0].get("identifier")
            print(f"\nReading issue {first_issue}:")
            content = await fs._cat_file(f"{first_issue}/issue.md")
            print(content.decode()[:500])

            print(f"\nListing comments for {first_issue}:")
            comments = await fs._ls(f"{first_issue}/comments", detail=True)
            for comment in comments[:3]:
                print(f"  {comment.get('name')}: by {comment.get('author')}")

    asyncio.run(main())
