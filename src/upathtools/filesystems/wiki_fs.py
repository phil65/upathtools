"""GitHub Wiki filesystem implementation using git operations."""

from __future__ import annotations

import io
import logging
import os
import pathlib
import shutil
import subprocess
import tempfile
from typing import TYPE_CHECKING, Any, Literal, overload

from fsspec.asyn import AsyncFileSystem, sync_wrapper
from fsspec.utils import infer_storage_options
from upath import UPath


if TYPE_CHECKING:
    from collections.abc import Buffer


logger = logging.getLogger(__name__)

# Constants
PREVIEW_LENGTH = 200


class WikiPath(UPath):
    """UPath implementation for GitHub Wiki filesystem."""

    __slots__ = ()


class WikiFileSystem(AsyncFileSystem):
    """Filesystem for accessing GitHub Wiki pages using git operations.

    This implementation uses git commands to interact with GitHub Wiki repositories.
    GitHub wikis are actually separate git repositories, so we use sparse checkout
    to efficiently access and modify wiki content.
    """

    protocol = "wiki"

    def __init__(
        self,
        owner: str | None = None,
        repo: str | None = None,
        token: str | None = None,
        asynchronous: bool = False,
        loop: Any = None,
        **kwargs: Any,
    ) -> None:
        """Initialize the filesystem.

        Args:
            owner: GitHub repository owner/organization
            repo: GitHub repository name
            token: GitHub personal access token for authentication
            asynchronous: Whether to use async operations
            loop: Event loop for async operations
            **kwargs: Additional filesystem options
        """
        super().__init__(asynchronous=asynchronous, loop=loop, **kwargs)

        self.owner = owner
        self.repo = repo
        token_env = os.environ.get("GITHUB_TOKEN") or os.environ.get("GH_TOKEN")
        self.token = token or token_env

        # We need both owner and repo to function
        if not owner or not repo:
            msg = "Both owner and repo must be provided"
            raise ValueError(msg)

        # Create wiki URL
        self.wiki_url = f"https://github.com/{owner}/{repo}.wiki.git"
        if self.token:
            # Insert token into URL for auth
            self.auth_url = f"https://{self.token}@github.com/{owner}/{repo}.wiki.git"
        else:
            self.auth_url = self.wiki_url

        # Create a temporary directory for git operations
        self.temp_dir = tempfile.mkdtemp(prefix=f"wiki-{owner}-{repo}-")
        self._setup_git_repo()

        # Initialize cache
        self.dircache: dict[str, Any] = {}

    def _make_path(self, path: str) -> UPath:
        """Create a path object from string."""
        return WikiPath(path)

    @property
    def fsid(self) -> str:
        """Filesystem ID."""
        return f"wiki-{self.owner}-{self.repo}"

    def _pull_latest_changes(self) -> bool:
        """Pull latest changes from remote repository."""
        try:
            # Check if we're on a branch first
            result = subprocess.run(
                ["git", "rev-parse", "--abbrev-ref", "HEAD"],
                cwd=self.temp_dir,
                check=False,  # Don't fail if not on a branch
                capture_output=True,
                text=True,
            )

            if result.stdout.strip() == "HEAD":
                # Not on a branch, just fetch
                subprocess.run(
                    ["git", "fetch", "origin"],
                    cwd=self.temp_dir,
                    check=True,
                    capture_output=True,
                    text=True,
                )
                return True
            # On a branch, do a pull
            subprocess.run(
                ["git", "pull", "--ff-only"],
                cwd=self.temp_dir,
                check=True,
                capture_output=True,
                text=True,
            )
        except subprocess.CalledProcessError as e:
            logger.debug("Git operation failed: %s", e.stderr)
            return False
        else:
            return True

    def _setup_git_repo(self) -> None:
        """Setup a git repository for the wiki with sparse checkout."""
        try:
            # Initialize git repo
            subprocess.run(
                ["git", "init"],
                cwd=self.temp_dir,
                check=True,
                capture_output=True,
                text=True,
            )

            # Add remote
            subprocess.run(
                ["git", "remote", "add", "origin", self.auth_url],
                cwd=self.temp_dir,
                check=True,
                capture_output=True,
                text=True,
            )

            # Try to fetch to see if the wiki exists
            try:
                subprocess.run(
                    ["git", "fetch", "--depth=1"],
                    cwd=self.temp_dir,
                    check=True,
                    capture_output=True,
                    text=True,
                )

                # Checkout the default branch
                subprocess.run(
                    ["git", "checkout", "origin/master", "--", "."],
                    cwd=self.temp_dir,
                    check=True,
                    capture_output=True,
                    text=True,
                )

                logger.debug("Wiki repository initialized successfully")
            except subprocess.CalledProcessError as e:
                if "repository not found" in e.stderr.lower():
                    msg = f"Wiki not found for {self.owner}/{self.repo}"
                    raise FileNotFoundError(msg) from e
                raise

        except subprocess.CalledProcessError as e:
            msg = f"Error setting up git repository: {e.stderr}"
            raise RuntimeError(msg) from e

    def __del__(self) -> None:
        """Clean up the temporary directory when the object is destroyed."""
        self.close()

    def close(self) -> None:
        """Close the filesystem and clean up resources."""
        if hasattr(self, "temp_dir") and pathlib.Path(self.temp_dir).exists():
            try:
                shutil.rmtree(self.temp_dir)
                logger.debug("Temporary directory removed: %s", self.temp_dir)
            except OSError as e:
                logger.warning("Failed to remove temporary directory: %s", e)

    @classmethod
    def _strip_protocol(cls, path: str) -> str:
        """Strip protocol prefix from path."""
        path = infer_storage_options(path).get("path", path)
        return path.lstrip("/")

    @classmethod
    def _get_kwargs_from_urls(cls, path: str) -> dict[str, Any]:
        """Parse URL into constructor kwargs."""
        so = infer_storage_options(path)
        out = {}

        if so.get("username"):
            out["owner"] = so["username"]
        if so.get("password"):
            out["token"] = so["password"]
        if so.get("host"):
            # The host part will be the repo
            out["repo"] = so["host"]

        return out

    @overload
    async def _ls(
        self,
        path: str = "",
        detail: Literal[True] = True,
        **kwargs: Any,
    ) -> list[dict[str, Any]]: ...

    @overload
    async def _ls(
        self,
        path: str = "",
        detail: Literal[False] = False,
        **kwargs: Any,
    ) -> list[str]: ...

    async def _ls(
        self,
        path: str = "",
        detail: bool = True,
        **kwargs: Any,
    ) -> list[dict[str, Any]] | list[str]:
        """List wiki pages.

        Args:
            path: Path to list (empty for all pages)
            detail: Whether to include detailed information
            **kwargs: Additional arguments

        Returns:
            List of pages with metadata or just page names
        """
        path = self._strip_protocol(path or "")

        # Pull latest changes
        self._pull_latest_changes()

        # List files in the directory
        target = pathlib.Path(self.temp_dir) / path
        if not target.exists():
            if path:  # Only raise error if not root path
                msg = f"Path not found: {path}"
                raise FileNotFoundError(msg)
            files = []
        else:
            files = [str(t) for t in target.iterdir()]

        # Filter markdown files
        markdown_files = [f for f in files if f.endswith(".md")]

        if detail:
            result = []
            for filename in markdown_files:
                file_path = target / filename
                stat = file_path.stat()

                # Get git metadata
                try:
                    # Get last commit date
                    git_log = subprocess.run(
                        ["git", "log", "-1", "--format=%at", "--", str(file_path)],
                        cwd=self.temp_dir,
                        check=True,
                        capture_output=True,
                        text=True,
                    )
                    last_modified = (
                        int(git_log.stdout.strip()) if git_log.stdout.strip() else 0
                    )

                    # Get creation date (first commit)
                    git_log_first = subprocess.run(
                        ["git", "log", "--reverse", "--format=%at", "--", str(file_path)],
                        cwd=self.temp_dir,
                        check=True,
                        capture_output=True,
                        text=True,
                    )
                    created_at = (
                        int(git_log_first.stdout.strip().split("\n")[0])
                        if git_log_first.stdout.strip()
                        else 0
                    )

                except (subprocess.CalledProcessError, ValueError, IndexError):
                    last_modified = int(stat.st_mtime)
                    created_at = int(stat.st_ctime)

                # Convert filename to wiki title
                file = pathlib.Path(filename)
                title = file.stem.replace("-", " ")

                result.append({
                    "name": filename,
                    "type": "file",
                    "size": stat.st_size,
                    "title": title,
                    "created_at": created_at,
                    "updated_at": last_modified,
                    "html_url": f"https://github.com/{self.owner}/{self.repo}/wiki/{file.stem}",
                })
            return result

        return markdown_files

    ls = sync_wrapper(_ls)

    async def _cat_file(
        self,
        path: str,
        start: int | None = None,
        end: int | None = None,
        **kwargs: Any,
    ) -> bytes:
        """Get content of a wiki page.

        Args:
            path: Path to the wiki page file
            start: Start byte position
            end: End byte position
            **kwargs: Additional arguments

        Returns:
            Page content as bytes

        Raises:
            FileNotFoundError: If page doesn't exist
        """
        path = self._strip_protocol(path)
        file_path = pathlib.Path(self.temp_dir) / path

        if not file_path.exists():
            msg = f"Wiki page not found: {path}"
            raise FileNotFoundError(msg)

        # Pull latest changes to make sure we have the current version
        self._pull_latest_changes()

        with file_path.open("rb") as f:
            if start is not None or end is not None:
                start = start or 0
                f.seek(start)
                if end is not None:
                    return f.read(end - start)
            return f.read()

    cat_file = sync_wrapper(_cat_file)  # pyright: ignore

    async def _pipe_file(
        self,
        path: str,
        value: bytes,
        **kwargs: Any,
    ) -> None:
        """Write content to a wiki page.

        Args:
            path: Path to the wiki page file
            value: Content to write
            **kwargs: Additional keyword arguments including:
                - message: Commit message for the wiki edit

        Raises:
            ValueError: If token is not provided for write operations
        """
        if not self.token:
            msg = "GitHub token is required for write operations"
            raise ValueError(msg)

        path = self._strip_protocol(path)
        file_path = pathlib.Path(self.temp_dir) / path

        # Make sure directory exists
        file_path.parent.mkdir(parents=True, exist_ok=True)

        # Pull latest changes first
        self._pull_latest_changes()

        # Write content to file
        with file_path.open("wb") as f:
            f.write(value)

        # Commit and push changes
        file = pathlib.Path(path)
        page_title = file.stem.replace("-", " ")
        msg = kwargs.get("message", f"Update {page_title}")

        try:
            # Add the file
            subprocess.run(
                ["git", "add", path],
                cwd=self.temp_dir,
                check=True,
                capture_output=True,
                text=True,
            )

            # Commit
            subprocess.run(
                ["git", "commit", "-m", msg],
                cwd=self.temp_dir,
                check=True,
                capture_output=True,
                text=True,
            )

            # Push
            subprocess.run(
                ["git", "push", "origin", "HEAD:master"],
                cwd=self.temp_dir,
                check=True,
                capture_output=True,
                text=True,
            )
        except subprocess.CalledProcessError as e:
            error_msg = f"Error pushing changes: {e.stderr}"
            raise RuntimeError(error_msg) from e

        # Invalidate cache
        self.dircache.clear()

    pipe_file = sync_wrapper(_pipe_file)

    async def _rm_file(self, path: str, **kwargs: Any) -> None:
        """Delete a wiki page.

        Args:
            path: Path to the wiki page file
            **kwargs: Additional keyword arguments

        Raises:
            ValueError: If token is not provided for delete operations
        """
        if not self.token:
            msg = "GitHub token is required for delete operations"
            raise ValueError(msg)

        path = self._strip_protocol(path)
        file_path = pathlib.Path(self.temp_dir) / path

        if not file_path.exists():
            msg = f"Wiki page not found: {path}"
            raise FileNotFoundError(msg)

        # Pull latest changes first
        self._pull_latest_changes()

        # Remove the file
        try:
            # Delete file
            file_path.unlink()

            # Stage the deletion
            subprocess.run(
                ["git", "rm", path],
                cwd=self.temp_dir,
                check=True,
                capture_output=True,
                text=True,
            )

            # Commit
            file = pathlib.Path(path)
            page_title = file.stem.replace("-", " ")
            msg = kwargs.get("message", f"Delete {page_title}")

            subprocess.run(
                ["git", "commit", "-m", msg],
                cwd=self.temp_dir,
                check=True,
                capture_output=True,
                text=True,
            )

            # Push
            subprocess.run(
                ["git", "push", "origin", "HEAD:master"],
                cwd=self.temp_dir,
                check=True,
                capture_output=True,
                text=True,
            )
        except subprocess.CalledProcessError as e:
            error_msg = f"Error deleting file: {e.stderr}"
            raise RuntimeError(error_msg) from e

        # Invalidate cache
        self.dircache.clear()

    rm_file = sync_wrapper(_rm_file)

    async def _info(self, path: str, **kwargs: Any) -> dict[str, Any]:
        """Get info about a wiki page or root.

        Args:
            path: Path to get info for
            **kwargs: Additional arguments

        Returns:
            Dictionary containing detailed metadata

        Raises:
            FileNotFoundError: If path doesn't exist
        """
        path = self._strip_protocol(path)

        if not path:
            # Root directory
            return {
                "name": "",
                "size": 0,
                "type": "directory",
                "wiki": f"{self.owner}/{self.repo}",
            }

        file_path = pathlib.Path(self.temp_dir) / path
        if not file_path.exists():
            msg = f"Path not found: {path}"
            raise FileNotFoundError(msg)

        if file_path.is_dir():
            return {
                "name": file_path.name or path,
                "size": 0,
                "type": "directory",
            }

        # File info
        stat = file_path.stat()
        file = pathlib.Path(path)

        # Get git metadata
        try:
            # Get last commit date
            git_log = subprocess.run(
                ["git", "log", "-1", "--format=%at", "--", str(file_path)],
                cwd=self.temp_dir,
                check=True,
                capture_output=True,
                text=True,
            )
            last_modified = int(git_log.stdout.strip()) if git_log.stdout.strip() else 0

            # Get creation date (first commit)
            git_log_first = subprocess.run(
                ["git", "log", "--reverse", "--format=%at", "--", str(file_path)],
                cwd=self.temp_dir,
                check=True,
                capture_output=True,
                text=True,
            )
            created_at = (
                int(git_log_first.stdout.strip().split("\n")[0])
                if git_log_first.stdout.strip()
                else 0
            )

        except (subprocess.CalledProcessError, ValueError, IndexError):
            last_modified = int(stat.st_mtime)
            created_at = int(stat.st_ctime)

        # Convert filename to wiki title
        title = file.stem.replace("-", " ")

        return {
            "name": file.name,
            "type": "file",
            "size": stat.st_size,
            "title": title,
            "created_at": created_at,
            "updated_at": last_modified,
            "html_url": f"https://github.com/{self.owner}/{self.repo}/wiki/{file.stem}",
        }

    info = sync_wrapper(_info)

    async def _exists(self, path: str, **kwargs: Any) -> bool:
        """Check if a path exists.

        Args:
            path: Path to check
            **kwargs: Additional arguments

        Returns:
            True if the path exists, False otherwise
        """
        path = self._strip_protocol(path)
        file_path = pathlib.Path(self.temp_dir) / path
        return file_path.exists()

    exists = sync_wrapper(_exists)  # pyright: ignore

    async def _isdir(self, path: str, **kwargs: Any) -> bool:
        """Check if path is a directory.

        Args:
            path: Path to check
            **kwargs: Additional arguments

        Returns:
            True if the path is a directory, False otherwise
        """
        path = self._strip_protocol(path)
        if not path:
            return True

        file_path = pathlib.Path(self.temp_dir) / path
        return file_path.is_dir()

    isdir = sync_wrapper(_isdir)

    async def _isfile(self, path: str, **kwargs: Any) -> bool:
        """Check if path is a file.

        Args:
            path: Path to check
            **kwargs: Additional arguments

        Returns:
            True if the path is a file, False otherwise
        """
        path = self._strip_protocol(path)
        if not path:
            return False

        file_path = pathlib.Path(self.temp_dir) / path
        return file_path.is_file()

    isfile = sync_wrapper(_isfile)

    def _open(
        self,
        path: str,
        mode: str = "rb",
        **kwargs: Any,
    ) -> io.BytesIO | WikiBufferedWriter:
        """Open a wiki page as a file.

        Args:
            path: Path to the wiki page
            mode: File mode ('rb' for reading, 'wb' for writing)
            **kwargs: Additional arguments

        Returns:
            File-like object for reading or writing

        Raises:
            ValueError: If token is not provided for write operations
            NotImplementedError: If mode is not supported
        """
        if "r" in mode:
            content = self.cat_file(path)
            assert isinstance(content, bytes), "Content should be bytes"
            return io.BytesIO(content)

        if "w" in mode:
            if not self.token:
                msg = "GitHub token is required for write operations"
                raise ValueError(msg)

            buffer = io.BytesIO()
            return WikiBufferedWriter(buffer, self, path, **kwargs)

        msg = f"Mode {mode} not supported"
        raise NotImplementedError(msg)

    def invalidate_cache(self, path: str | None = None) -> None:
        """Clear the cache.

        Args:
            path: Optional path to invalidate (currently ignores path)
        """
        # For simplicity, we just clear the entire cache
        self.dircache.clear()

        # Pull latest changes
        self._pull_latest_changes()


class WikiBufferedWriter(io.BufferedIOBase):
    """Buffered writer for wiki pages that writes to the wiki when closed."""

    def __init__(
        self,
        buffer: io.BytesIO,
        fs: WikiFileSystem,
        path: str,
        **kwargs: Any,
    ):
        """Initialize the writer.

        Args:
            buffer: Buffer to store content
            fs: WikiFileSystem instance
            path: Path to write to
            **kwargs: Additional arguments to pass to pipe_file
        """
        super().__init__()
        self.buffer = buffer
        self.fs = fs
        self.path = path
        self.kwargs = kwargs

    def write(self, data: Buffer) -> int:
        """Write data to the buffer.

        Args:
            data: Data to write

        Returns:
            Number of bytes written
        """
        return self.buffer.write(data)

    def close(self) -> None:
        """Close the writer and write content to the wiki."""
        if not self.closed:
            # Get the buffer contents and write to the wiki
            content = self.buffer.getvalue()
            self.fs.pipe_file(self.path, content, **self.kwargs)
            self.buffer.close()
            super().close()

    def readable(self) -> bool:
        """Whether the writer is readable."""
        return False

    def writable(self) -> bool:
        """Whether the writer is writable."""
        return True


if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO)
    token = os.environ.get("GITHUB_TOKEN")

    print("GitHub token found in environment" if token else "No GitHub token found")
    test_repos = [("microsoft", "vscode"), ("python", "cpython")]
    for owner, repo in test_repos:
        print(f"\nTrying wiki for {owner}/{repo}...")
        try:
            fs = WikiFileSystem(owner=owner, repo=repo, token=token)
            pages = fs.ls("/", detail=True)
            print(f"Success! Found {len(pages)} wiki pages")

            if pages:
                print("\nWiki pages:")
                for i, page in enumerate(pages[:5]):  # Show first 5 pages
                    print(f"{i + 1}. {page['name']} ({page['title']})")

                # Read the first page
                first_page = pages[0]["name"]
                print(f"\nReading page: {first_page}")
                content = fs.cat_file(first_page)
                # Fix type error with proper type assertion
                preview = content[:PREVIEW_LENGTH]
                print(preview)
                break
        except FileNotFoundError:
            print(f"No wiki found for {owner}/{repo}")
        except Exception as e:  # noqa: BLE001
            print(f"Error: {e}")
