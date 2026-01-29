"""Linear Issues filesystem implementation with async support using httpx."""

from __future__ import annotations

import contextlib
import logging
import os
from typing import TYPE_CHECKING, Any, Literal, overload
import weakref

from fsspec.utils import infer_storage_options

from upathtools.async_helpers import sync, sync_wrapper
from upathtools.filesystems.base import BaseAsyncFileSystem, BaseUPath, FileInfo


if TYPE_CHECKING:
    import httpx


GroupBy = Literal["project"] | None

# Constants for identifier parsing and path handling
IDENTIFIER_PARTS_COUNT = 2
MIN_ISSUE_COMMENT_PATH_PARTS = 2
MIN_COMMENT_FILE_PATH_PARTS = 3

ISSUE_QUERY = """
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
                id
                name
            }
            team {
                key
            }
        }
    }
}
"""

ALL_ISSUES_QUERY = """
query GetAllIssues($after: String) {
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
                id
                name
            }
            team {
                key
            }
        }
        pageInfo {
            hasNextPage
            endCursor
        }
    }
}
"""

PROJECTS_QUERY = """
query GetProjects($after: String) {
    projects(first: 100, after: $after) {
        nodes {
            id
            name
            description
            url
            state
            createdAt
            updatedAt
        }
        pageInfo {
            hasNextPage
            endCursor
        }
    }
}
"""

ISSUE_COMMENTS_QUERY = """
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
    project_id: str | None


class LinearCommentInfo(FileInfo, total=False):
    """Info dict for Linear comment files."""

    size: int
    comment_id: str
    issue_identifier: str
    body: str | None
    created_at: str | None
    updated_at: str | None
    author: str | None


class LinearProjectInfo(FileInfo, total=False):
    """Info dict for Linear project directories."""

    size: int
    project_id: str
    project_name: str
    description: str | None
    state: str | None
    url: str | None
    created_at: str | None
    updated_at: str | None


logger = logging.getLogger(__name__)


class LinearIssuePath(BaseUPath[LinearIssueInfo]):
    """UPath implementation for Linear Issues filesystem."""

    __slots__ = ()


class LinearIssueFileSystem(BaseAsyncFileSystem[LinearIssuePath, LinearIssueInfo]):
    """Filesystem for accessing Linear Issues.

    Provides read/write access to Linear issues as files.
    Each issue is represented as a markdown file.

    Structure depends on parameters:
    - extended=False, group_by=None: linear:///PHI-123.md (flat)
    - extended=True, group_by=None: linear:///PHI-123/issue.md + comments/
    - extended=False, group_by="project": linear:///ProjectName/PHI-123.md
    - extended=True, group_by="project": linear:///ProjectName/PHI-123/issue.md + comments/

    Issues without a project appear under "_unassigned/" when group_by="project".

    Deletion examples:
    - fs.rm("PHI-123.md")  # Delete issue in flat mode
    - fs.rm("ProjectName/PHI-123.md")  # Delete issue in project mode
    - fs.rm("PHI-123/issue.md")  # Delete issue in extended mode
    - fs.rm("PHI-123", recursive=True)  # Delete issue directory in extended mode

    Note: Deleting individual comments is not supported.
    """

    protocol = "linear"
    upath_cls = LinearIssuePath
    base_url = "https://api.linear.app/graphql"

    def __init__(
        self,
        api_key: str | None = None,
        extended: bool = False,
        group_by: GroupBy = None,
        timeout: float | None = None,
        loop: Any = None,
        client_kwargs: dict[str, Any] | None = None,
        **kwargs: Any,
    ) -> None:
        """Initialize the filesystem.

        Args:
            api_key: Linear API key for authentication
            extended: If True, issues are folders with comments as sub-files
            group_by: How to group issues. None for flat, "project" for project folders
            timeout: Connection timeout in seconds
            loop: Event loop for async operations
            client_kwargs: Additional arguments for httpx client
            **kwargs: Additional filesystem options
        """
        super().__init__(loop=loop, **kwargs)

        self.api_key = api_key or os.environ.get("LINEAR_API_KEY")
        self.extended = extended
        self.group_by = group_by
        self.timeout = timeout if timeout is not None else 60.0
        self.client_kwargs = client_kwargs or {}
        self._session: httpx.AsyncClient | None = None

        if not self.api_key:
            msg = "api_key must be provided or LINEAR_API_KEY environment variable must be set"
            raise ValueError(msg)

        self.headers = {"Content-Type": "application/json", "Authorization": self.api_key}
        self.dircache: dict[str, Any] = {}

    @property
    def fsid(self) -> str:
        """Filesystem ID."""
        return f"linear-{self.group_by or 'flat'}"

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
        """Parse URL into constructor kwargs."""
        return {}

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

    async def _fetch_issue_by_identifier(self, identifier: str) -> dict[str, Any]:
        """Fetch a specific issue by identifier.

        Args:
            identifier: Issue identifier (e.g., "ENG-123")

        Returns:
            Dictionary containing issue data

        Raises:
            FileNotFoundError: If issue is not found
        """

        # Extract the number from the identifier (e.g., "PHI-7" -> 7)
        def _parse_identifier_or_raise(identifier: str) -> tuple[int, str]:
            parts = identifier.split("-")
            if len(parts) != IDENTIFIER_PARTS_COUNT:
                msg = f"Invalid issue identifier format: {identifier}"
                raise FileNotFoundError(msg)

            try:
                issue_number = int(parts[1])
            except (ValueError, IndexError):
                msg = f"Invalid issue identifier format: {identifier}"
                raise FileNotFoundError(msg) from None
            else:
                team_key = parts[0]
                return issue_number, team_key

        issue_num, team_key = _parse_identifier_or_raise(identifier)
        variables = {"filter": {"number": {"eq": issue_num}, "team": {"key": {"eq": team_key}}}}
        data = await self._graphql_request(ISSUE_QUERY, variables)
        issues = data.get("issues", {}).get("nodes", [])
        if not issues:
            msg = f"Issue not found: {identifier}"
            raise FileNotFoundError(msg)
        found_issue = issues[0]
        if found_issue.get("identifier") != identifier:
            msg = f"Issue not found: {identifier}"
            raise FileNotFoundError(msg)
        return found_issue

    async def _fetch_all_issues(self) -> list[dict[str, Any]]:
        """Fetch all issues accessible to the user.

        Returns:
            List of issue data dictionaries
        """
        all_issues: list[dict[str, Any]] = []
        cursor: str | None = None

        while True:
            variables: dict[str, Any] = {}
            if cursor:
                variables["after"] = cursor

            data = await self._graphql_request(ALL_ISSUES_QUERY, variables)
            issues_data = data.get("issues", {})
            issues = issues_data.get("nodes", [])
            all_issues.extend(issues)

            page_info = issues_data.get("pageInfo", {})
            if not page_info.get("hasNextPage"):
                break
            cursor = page_info.get("endCursor")

        return all_issues

    async def _fetch_projects(self) -> list[dict[str, Any]]:
        """Fetch all projects.

        Returns:
            List of project data dictionaries
        """
        cache_key = "_projects"
        if cache_key in self.dircache:
            return self.dircache[cache_key]

        all_projects: list[dict[str, Any]] = []
        cursor: str | None = None

        while True:
            variables: dict[str, Any] = {}
            if cursor:
                variables["after"] = cursor

            data = await self._graphql_request(PROJECTS_QUERY, variables)
            projects_data = data.get("projects", {})
            projects = projects_data.get("nodes", [])
            all_projects.extend(projects)

            page_info = projects_data.get("pageInfo", {})
            if not page_info.get("hasNextPage"):
                break
            cursor = page_info.get("endCursor")

        self.dircache[cache_key] = all_projects
        return all_projects

    async def _fetch_comments(self, issue_id: str) -> list[dict[str, Any]]:
        """Fetch comments for an issue.

        Args:
            issue_id: Linear issue ID (UUID)

        Returns:
            List of comment data dictionaries
        """
        all_comments: list[dict[str, Any]] = []
        cursor: str | None = None

        while True:
            variables: dict[str, Any] = {"issueId": issue_id}
            if cursor:
                variables["after"] = cursor

            data = await self._graphql_request(ISSUE_COMMENTS_QUERY, variables)
            comments_data = data.get("issue", {}).get("comments", {})
            comments = comments_data.get("nodes", [])
            all_comments.extend(comments)

            page_info = comments_data.get("pageInfo", {})
            if not page_info.get("hasNextPage"):
                break
            cursor = page_info.get("endCursor")

        return all_comments

    async def _get_all_issues_cached(self) -> list[dict[str, Any]]:
        """Get all issues with caching."""
        cache_key = "_issues"
        if cache_key in self.dircache:
            return self.dircache[cache_key]

        issues = await self._fetch_all_issues()
        self.dircache[cache_key] = issues
        return issues

    async def _get_issues_by_project(self) -> dict[str | None, list[dict[str, Any]]]:
        """Get issues grouped by project name.

        Returns:
            Dict mapping project name (or None for unassigned) to list of issues
        """
        issues = await self._get_all_issues_cached()
        grouped: dict[str | None, list[dict[str, Any]]] = {}

        for issue in issues:
            project = issue.get("project")
            project_name = project.get("name") if project else None
            if project_name not in grouped:
                grouped[project_name] = []
            grouped[project_name].append(issue)

        return grouped

    def _get_issue_path(self, issue: dict[str, Any], include_project: bool = False) -> str:
        """Get the path for an issue based on current settings."""
        identifier = issue["identifier"]
        project = issue.get("project")
        project_name = project.get("name") if project else None

        parts: list[str] = []

        if include_project and self.group_by == "project":
            parts.append(project_name or "_unassigned")

        if self.extended:
            parts.append(identifier)
            parts.append("issue.md")
        else:
            parts.append(f"{identifier}.md")

        return "/".join(parts)

    @overload
    async def _ls(
        self, path: str, detail: Literal[True] = ..., **kwargs: Any
    ) -> list[LinearIssueInfo | LinearCommentInfo | LinearProjectInfo]: ...

    @overload
    async def _ls(self, path: str, detail: Literal[False], **kwargs: Any) -> list[str]: ...

    async def _ls(
        self,
        path: str,
        detail: bool = True,
        **kwargs: Any,
    ) -> list[LinearIssueInfo | LinearCommentInfo | LinearProjectInfo] | list[str]:
        """List contents of path."""
        path = self._strip_protocol(path or "")
        logger.debug(
            "Listing path: %s (extended=%s, group_by=%s)", path, self.extended, self.group_by
        )

        parts = path.rstrip("/").split("/") if path else []

        # Root listing
        if not path:
            return await self._ls_root(detail)

        if self.group_by == "project":
            return await self._ls_project_mode(parts, detail)
        return await self._ls_flat_mode(parts, detail)

    async def _ls_root(
        self, detail: bool
    ) -> list[LinearIssueInfo | LinearCommentInfo | LinearProjectInfo] | list[str]:
        """List root directory."""
        if self.group_by == "project":
            # List projects as directories
            grouped = await self._get_issues_by_project()
            results: list[LinearProjectInfo] = []

            # Add project directories
            projects = await self._fetch_projects()
            project_names = {p["name"] for p in projects}

            results.extend([
                _project_to_info(project) for project in projects if project["name"] in grouped
            ])

            # Add _unassigned if there are unassigned issues
            if None in grouped:
                results.append(
                    LinearProjectInfo(
                        name="_unassigned",
                        type="directory",
                        size=0,
                        project_id="",
                        project_name="_unassigned",
                        description="Issues not assigned to any project",
                    )
                )

            # Add projects that have issues but weren't in the projects query
            results.extend([
                LinearProjectInfo(
                    name=project_name,
                    type="directory",
                    size=0,
                    project_id="",
                    project_name=project_name,
                )
                for project_name in grouped
                if project_name is not None and project_name not in project_names
            ])

            if detail:
                return results  # type: ignore[return-value]
            return [r["name"] for r in results]

        # Flat mode - list all issues
        issues = await self._get_all_issues_cached()
        results_flat = [_issue_to_info(issue, extended=self.extended) for issue in issues]
        if detail:
            return results_flat  # type: ignore[return-value]
        return [r["name"] for r in results_flat]

    async def _ls_project_mode(
        self, parts: list[str], detail: bool
    ) -> list[LinearIssueInfo | LinearCommentInfo | LinearProjectInfo] | list[str]:
        """List in project grouping mode."""
        project_name = parts[0]
        actual_project_name: str | None = None if project_name == "_unassigned" else project_name

        grouped = await self._get_issues_by_project()

        if actual_project_name not in grouped and project_name not in grouped:
            msg = f"Project not found: {project_name}"
            raise FileNotFoundError(msg)

        project_issues = grouped.get(actual_project_name, [])

        if len(parts) == 1:
            # List issues in project
            results = [
                _issue_to_info(issue, extended=self.extended, prefix=project_name)
                for issue in project_issues
            ]
            if detail:
                return results  # type: ignore[return-value]
            return [r["name"] for r in results]

        # Deeper path - delegate to issue handling
        parts[1]
        remaining_parts = parts[1:]
        return await self._ls_issue_path(remaining_parts, detail, prefix=project_name)

    async def _ls_flat_mode(
        self, parts: list[str], detail: bool
    ) -> list[LinearIssueInfo | LinearCommentInfo | LinearProjectInfo] | list[str]:
        """List in flat mode."""
        return await self._ls_issue_path(parts, detail)

    async def _ls_issue_path(
        self, parts: list[str], detail: bool, prefix: str | None = None
    ) -> list[LinearIssueInfo | LinearCommentInfo | LinearProjectInfo] | list[str]:
        """List issue-related paths."""
        identifier = parts[0].removesuffix(".md")

        if not self.extended:
            # Flat mode - just return the issue
            issue = await self._fetch_issue_by_identifier(identifier)
            results = [_issue_to_info(issue, extended=False, prefix=prefix)]
            if detail:
                return results  # type: ignore[return-value]
            return [r["name"] for r in results]

        # Extended mode
        if len(parts) == 1:
            # Listing issue directory
            issue = await self._fetch_issue_by_identifier(identifier)
            base = f"{prefix}/{identifier}" if prefix else identifier
            issue_contents: list[LinearIssueInfo | LinearCommentInfo] = [
                LinearIssueInfo(
                    name=f"{base}/issue.md",
                    type="file",
                    size=len((issue.get("description") or "").encode()),
                ),
                LinearIssueInfo(
                    name=f"{base}/comments",
                    type="directory",
                    size=0,
                ),
            ]
            if detail:
                return issue_contents  # type: ignore[return-value]
            return [r["name"] for r in issue_contents]

        if len(parts) == MIN_ISSUE_COMMENT_PATH_PARTS and parts[1] == "comments":
            # Listing comments directory
            issue = await self._fetch_issue_by_identifier(identifier)
            comments = await self._fetch_comments(issue["id"])
            base = f"{prefix}/{identifier}" if prefix else identifier
            comment_results: list[LinearCommentInfo] = [
                _comment_to_info(comment, base, idx) for idx, comment in enumerate(comments, 1)
            ]
            if detail:
                return comment_results  # type: ignore[return-value]
            return [r["name"] for r in comment_results]

        msg = f"Invalid path: {'/'.join(parts)}"
        raise FileNotFoundError(msg)

    ls = sync_wrapper(_ls)  # pyright: ignore[reportAssignmentType]

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

        # Handle project prefix if in project mode
        if self.group_by == "project" and len(parts) > 1:
            parts = parts[1:]  # Skip project name

        if self.extended:
            identifier = parts[0]

            if len(parts) >= MIN_ISSUE_COMMENT_PATH_PARTS and parts[1] == "issue.md":
                issue = await self._fetch_issue_by_identifier(identifier)
                content = _format_issue_markdown(issue)
            elif len(parts) >= MIN_COMMENT_FILE_PATH_PARTS and parts[1] == "comments":
                comment_file = parts[2].removesuffix(".md")
                try:
                    comment_idx = int(comment_file) - 1
                except ValueError:
                    msg = f"Invalid comment path: {path}"
                    raise FileNotFoundError(msg) from None

                issue = await self._fetch_issue_by_identifier(identifier)
                comments = await self._fetch_comments(issue["id"])

                if comment_idx < 0 or comment_idx >= len(comments):
                    msg = f"Comment not found: {path}"
                    raise FileNotFoundError(msg)

                content = _format_comment_markdown(comments[comment_idx], identifier)
            else:
                msg = f"Invalid path: {path}"
                raise FileNotFoundError(msg)
        else:
            identifier = parts[0].removesuffix(".md")
            issue = await self._fetch_issue_by_identifier(identifier)
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
        team_id: str | None = None,
        state_id: str | None = None,
        priority: int | None = None,
        project_id: str | None = None,
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
            team_id: Team ID for new issues (required for creation)
            state_id: Workflow state ID to set
            priority: Priority level (0-4)
            project_id: Project ID to associate
            assignee_id: User ID to assign
            **kwargs: Additional arguments
        """
        path = self._strip_protocol(path)
        parts = path.rstrip("/").split("/")

        # Handle project prefix
        if self.group_by == "project" and len(parts) > 1:
            parts = parts[1:]

        try:
            body = value.decode()
        except UnicodeDecodeError:
            msg = "Content must be valid UTF-8 text"
            raise ValueError(msg) from None

        if self.extended and len(parts) >= MIN_COMMENT_FILE_PATH_PARTS and parts[1] == "comments":
            identifier = parts[0]
            issue = await self._fetch_issue_by_identifier(identifier)
            await self._create_comment(issue["id"], body)
            self.invalidate_cache()
            return

        # Creating/updating an issue
        if self.extended:
            update_identifier = parts[0] if len(parts) > 0 and parts[0] != "issue.md" else None
        else:
            update_identifier = parts[0].removesuffix(".md") if parts else None

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
        if update_identifier and "-" in update_identifier:
            with contextlib.suppress(FileNotFoundError):
                existing_issue = await self._fetch_issue_by_identifier(update_identifier)

        if existing_issue:
            await self._update_issue(
                existing_issue["id"],
                description=body,
                title=title,
                state_id=state_id,
                priority=priority,
                project_id=project_id,
                assignee_id=assignee_id,
            )
        else:
            if not title:
                msg = "Issue title is required (pass title= or start body with '# Title')"
                raise ValueError(msg)
            if not team_id:
                msg = "team_id is required for creating new issues"
                raise ValueError(msg)

            await self._create_issue(
                team_id=team_id,
                title=title,
                description=body,
                state_id=state_id,
                priority=priority,
                project_id=project_id,
                assignee_id=assignee_id,
            )

        self.invalidate_cache()

    pipe_file = sync_wrapper(_pipe_file)  # pyright: ignore[reportAssignmentType]

    async def _create_issue(
        self,
        team_id: str,
        title: str,
        description: str,
        state_id: str | None = None,
        priority: int | None = None,
        project_id: str | None = None,
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
        if project_id:
            input_data["projectId"] = project_id
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
        project_id: str | None = None,
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
        if project_id:
            input_data["projectId"] = project_id
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

    async def _delete_issue(self, issue_id: str) -> dict[str, Any]:
        """Delete an issue."""
        mutation = """
        mutation DeleteIssue($id: String!) {
            issueDelete(id: $id) {
                success
            }
        }
        """
        data = await self._graphql_request(mutation, {"id": issue_id})
        result = data.get("issueDelete", {})
        if not result.get("success"):
            msg = f"Failed to delete issue {issue_id}"
            raise RuntimeError(msg)

        return result

    async def _info(
        self, path: str, **kwargs: Any
    ) -> LinearIssueInfo | LinearCommentInfo | LinearProjectInfo:
        """Get info for a path."""
        path = self._strip_protocol(path)
        if not path:
            return LinearIssueInfo(name="", type="directory", size=0)

        parts = path.rstrip("/").split("/")
        if self.group_by == "project":
            project_name = parts[0]

            if len(parts) == 1:
                # Project directory
                grouped = await self._get_issues_by_project()
                actual_name: str | None = None if project_name == "_unassigned" else project_name
                if actual_name not in grouped and project_name != "_unassigned":
                    msg = f"Project not found: {project_name}"
                    raise FileNotFoundError(msg)
                return LinearProjectInfo(
                    name=project_name,
                    type="directory",
                    size=0,
                    project_id="",
                    project_name=project_name,
                )

            # Strip project prefix for further processing
            parts = parts[1:]

        return await self._info_issue_path(
            parts, prefix=parts[0] if self.group_by == "project" else None
        )

    async def _info_issue_path(
        self, parts: list[str], prefix: str | None = None
    ) -> LinearIssueInfo | LinearCommentInfo:
        """Get info for issue-related paths."""
        identifier = parts[0].removesuffix(".md")

        if not self.extended:
            issue = await self._fetch_issue_by_identifier(identifier)
            return _issue_to_info(issue, extended=False, prefix=prefix)

        if len(parts) == 1:
            # Issue directory
            await self._fetch_issue_by_identifier(identifier)
            base = f"{prefix}/{identifier}" if prefix else identifier
            return LinearIssueInfo(name=base, type="directory", size=0)

        if len(parts) == MIN_ISSUE_COMMENT_PATH_PARTS:
            if parts[1] == "issue.md":
                issue = await self._fetch_issue_by_identifier(identifier)
                return _issue_to_info(issue, extended=True, as_file=True, prefix=prefix)
            if parts[1] == "comments":
                base = f"{prefix}/{identifier}" if prefix else identifier
                return LinearIssueInfo(name=f"{base}/comments", type="directory", size=0)

        if len(parts) == MIN_COMMENT_FILE_PATH_PARTS and parts[1] == "comments":
            comment_file = parts[2].removesuffix(".md")
            try:
                comment_idx = int(comment_file) - 1
            except ValueError:
                msg = f"Invalid comment path: {'/'.join(parts)}"
                raise FileNotFoundError(msg) from None

            issue = await self._fetch_issue_by_identifier(identifier)
            comments = await self._fetch_comments(issue["id"])

            if comment_idx < 0 or comment_idx >= len(comments):
                msg = f"Comment not found: {'/'.join(parts)}"
                raise FileNotFoundError(msg)

            base = f"{prefix}/{identifier}" if prefix else identifier
            return _comment_to_info(comments[comment_idx], base, comment_idx + 1)

        msg = f"Invalid path: {'/'.join(parts)}"
        raise FileNotFoundError(msg)

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

        try:
            info = await self._info(path)
            return info.get("type") == "directory"
        except FileNotFoundError:
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

    async def _rm_file(self, path: str, **kwargs: Any) -> None:
        """Delete a file (issue or comment)."""
        path = self._strip_protocol(path)
        if not path:
            msg = "Cannot delete root path"
            raise ValueError(msg)

        # Parse the path to identify what to delete
        if self.extended:
            # In extended mode: project/issue-id/issue.md or project/issue-id/comments/comment-id.md
            parts = path.split("/")
            if self.group_by == "project":
                if len(parts) < 3:  # noqa: PLR2004
                    msg = f"Invalid path for deletion: {path}"
                    raise ValueError(msg)
                issue_identifier = parts[1]
                remaining_path = "/".join(parts[2:])
            else:
                if len(parts) < 2:  # noqa: PLR2004
                    msg = f"Invalid path for deletion: {path}"
                    raise ValueError(msg)
                issue_identifier = parts[0]
                remaining_path = "/".join(parts[1:])

            if remaining_path == "issue.md":
                # Delete the issue itself
                issue = await self._fetch_issue_by_identifier(issue_identifier)
                await self._delete_issue(issue["id"])
                self.invalidate_cache()
            elif remaining_path.startswith("comments/") and remaining_path.endswith(".md"):
                msg = "Deleting individual comments is not supported"
                raise NotImplementedError(msg)
            else:
                msg = f"Cannot delete file: {path}"
                raise ValueError(msg)
        # In flat mode: issue-id.md or project/issue-id.md
        elif path.endswith(".md"):
            path_without_ext = path[:-3]
            if self.group_by == "project":
                parts = path_without_ext.split("/")
                if len(parts) != 2:  # noqa: PLR2004
                    msg = f"Invalid path for deletion: {path}"
                    raise ValueError(msg)
                issue_identifier = parts[1]
            else:
                issue_identifier = path_without_ext

            # Delete the issue
            issue = await self._fetch_issue_by_identifier(issue_identifier)
            await self._delete_issue(issue["id"])
            self.invalidate_cache()
        else:
            msg = f"Cannot delete file: {path}"
            raise ValueError(msg)

    rm_file = sync_wrapper(_rm_file)

    async def _rm(self, path: str, recursive: bool = False, **kwargs: Any) -> None:
        """Remove a file or directory."""
        path = self._strip_protocol(path)
        if not path:
            msg = "Cannot delete root path"
            raise ValueError(msg)

        # Check if it's a file or directory
        try:
            info = await self._info(path)
        except FileNotFoundError:
            raise FileNotFoundError(f"Path not found: {path}") from None

        is_file = info.get("type") == "file"
        is_dir = info.get("type") == "directory"

        if is_file:
            await self._rm_file(path, **kwargs)
        elif is_dir:
            if not recursive:
                msg = f"Cannot delete directory {path} without recursive=True"
                raise ValueError(msg)

            # In extended mode, deleting a directory means deleting the issue
            if self.extended:
                parts = path.split("/")
                if self.group_by == "project":
                    if len(parts) != 2:  # noqa: PLR2004
                        msg = f"Invalid directory path for deletion: {path}"
                        raise ValueError(msg)
                    issue_identifier = parts[1]
                else:
                    if len(parts) != 1:
                        msg = f"Invalid directory path for deletion: {path}"
                        raise ValueError(msg)
                    issue_identifier = parts[0]

                # Delete the issue
                issue = await self._fetch_issue_by_identifier(issue_identifier)
                await self._delete_issue(issue["id"])
                self.invalidate_cache()
            else:
                msg = f"Cannot delete directory in flat mode: {path}"
                raise ValueError(msg)
        else:
            msg = f"Path not found: {path}"
            raise FileNotFoundError(msg)

    rm = sync_wrapper(_rm)  # pyright: ignore[reportAssignmentType]

    def invalidate_cache(self, path: str | None = None) -> None:
        """Clear the directory cache."""
        if path is None:
            self.dircache.clear()
        else:
            path = self._strip_protocol(path or "")
            self.dircache.pop(path, None)
            self.dircache.pop("_issues", None)
            self.dircache.pop("_projects", None)


def _issue_to_info(
    issue: dict[str, Any],
    *,
    extended: bool = False,
    as_file: bool = False,
    prefix: str | None = None,
) -> LinearIssueInfo:
    """Convert Linear API issue response to LinearIssueInfo."""
    description = issue.get("description") or ""
    identifier = issue["identifier"]

    if extended and not as_file:
        name = f"{prefix}/{identifier}" if prefix else identifier
        file_type = "directory"
    else:
        if extended:
            name = f"{prefix}/{identifier}/issue.md" if prefix else f"{identifier}/issue.md"
        else:
            name = f"{prefix}/{identifier}.md" if prefix else f"{identifier}.md"
        file_type = "file"

    project = issue.get("project")

    return LinearIssueInfo(
        name=name,
        type=file_type,  # type: ignore[typeddict-item]
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
        project=project.get("name") if project else None,
        project_id=project.get("id") if project else None,
    )


def _comment_to_info(
    comment: dict[str, Any],
    issue_base: str,
    index: int,
) -> LinearCommentInfo:
    """Convert Linear API comment response to LinearCommentInfo."""
    body = comment.get("body") or ""
    return LinearCommentInfo(
        name=f"{issue_base}/comments/{index:03d}.md",
        type="file",
        size=len(body.encode()),
        comment_id=comment["id"],
        issue_identifier=issue_base.split("/")[-1] if "/" in issue_base else issue_base,
        body=body,
        created_at=comment.get("createdAt"),
        updated_at=comment.get("updatedAt"),
        author=comment.get("user", {}).get("name") if comment.get("user") else None,
    )


def _project_to_info(project: dict[str, Any]) -> LinearProjectInfo:
    """Convert Linear API project response to LinearProjectInfo."""
    return LinearProjectInfo(
        name=project["name"],
        type="directory",
        size=0,
        project_id=project["id"],
        project_name=project["name"],
        description=project.get("description"),
        state=project.get("state"),
        url=project.get("url"),
        created_at=project.get("createdAt"),
        updated_at=project.get("updatedAt"),
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
        # Example usage - flat mode
        print("\n=== Flat Mode ===")
        fs = LinearIssueFileSystem(extended=False, group_by=None)

        print("\nListing all issues:")
        issues = await fs._ls("", detail=True)
        for issue in issues[:5]:
            print(f"  {issue.get('identifier')}: {issue.get('title')} [{issue.get('project')}]")

        # Example usage - project grouped mode
        print("\n=== Project Grouped Mode ===")
        fs_proj = LinearIssueFileSystem(extended=True, group_by="project")

        print("\nListing root (projects):")
        projects = await fs_proj._ls("", detail=True)
        for proj in projects:
            print(f"  📁 {proj.get('name')}")

        if projects:
            first_proj = projects[0].get("name")
            print(f"\nListing issues in {first_proj}:")
            proj_issues = await fs_proj._ls(first_proj, detail=True)
            for issue in proj_issues[:3]:
                print(f"  {issue.get('name')}")

    asyncio.run(main())
