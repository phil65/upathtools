"""GitHub Wiki filesystem implementation using git operations."""

from __future__ import annotations

import asyncio
import io
import logging
import os
import pathlib
import shutil
import subprocess
import tempfile
from typing import Any, Literal, overload

from fsspec.utils import infer_storage_options

from upathtools.async_helpers import sync_wrapper
from upathtools.filesystems.base import BaseAsyncFileSystem, BaseUPath, BufferedWriter, FileInfo


class WikiInfo(FileInfo, total=False):
    """Info dict for Wiki filesystem paths."""

    size: int
    wiki: str
    title: str
    created_at: int
    updated_at: int
    html_url: str


logger = logging.getLogger(__name__)

# Constants
PREVIEW_LENGTH = 200


class WikiPath(BaseUPath[WikiInfo]):
    """UPath implementation for GitHub Wiki filesystem."""

    __slots__ = ()


class WikiFileSystem(BaseAsyncFileSystem[WikiPath, WikiInfo]):
    """Filesystem for accessing GitHub Wiki pages using git operations.

    This implementation uses git commands to interact with GitHub Wiki repositories.
    GitHub wikis are actually separate git repositories, so we use sparse checkout
    to efficiently access and modify wiki content.
    """

    protocol = "wiki"
    upath_cls = WikiPath

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
        if not owner or not repo:  # We need both owner and repo to function
            msg = "Both owner and repo must be provided"
            raise ValueError(msg)
        self.wiki_url = f"https://github.com/{owner}/{repo}.wiki.git"
        if self.token:
            # Insert token into URL for auth
            self.auth_url = f"https://{self.token}@github.com/{owner}/{repo}.wiki.git"
        else:
            self.auth_url = self.wiki_url
        self.temp_dir = tempfile.mkdtemp(prefix=f"wiki-{owner}-{repo}-")
        self._initialized = False
        self.dircache: dict[str, Any] = {}

    @property
    def fsid(self) -> str:
        """Filesystem ID."""
        return f"wiki-{self.owner}-{self.repo}"

    async def _ensure_initialized(self) -> None:
        """Ensure git repository is initialized."""
        if not self._initialized:
            await self._setup_git_repo()
            self._initialized = True

    async def _run_git(self, cmd: list[str], check: bool = True):
        """Run a git command directly without initialization check."""
        proc = await asyncio.create_subprocess_exec(
            *cmd,
            cwd=self.temp_dir,
            stdout=asyncio.subprocess.PIPE,
            stderr=asyncio.subprocess.PIPE,
        )
        stdout_bytes, stderr_bytes = await proc.communicate()
        stdout = stdout_bytes.decode() if stdout_bytes else ""
        stderr = stderr_bytes.decode() if stderr_bytes else ""

        if check and proc.returncode != 0:
            raise subprocess.CalledProcessError(proc.returncode or -1, cmd, stdout, stderr)

        return subprocess.CompletedProcess(cmd, proc.returncode or 0, stdout, stderr)

    async def run_cmd(self, cmd: list[str], check: bool = True):
        """Run a command in the temp wiki directory."""
        await self._ensure_initialized()
        return await self._run_git(cmd, check)

    async def _pull_latest_changes(self) -> bool:
        """Pull the latest changes from the remote repository."""
        try:
            # Check if we're on a branch first. Don't fail if not on a branch.
            result = await self.run_cmd(["git", "rev-parse", "--abbrev-ref", "HEAD"], check=False)
            if result.stdout.strip() == "HEAD":
                await self.run_cmd(
                    ["git", "fetch", "origin"], check=True
                )  # Not on a branch, just fetch
                return True
            await self.run_cmd(["git", "pull", "--ff-only"])  # On a branch, do a pull
        except subprocess.CalledProcessError as e:
            logger.debug("Git operation failed: %s", e.stderr)
            return False
        else:
            return True

    async def _setup_git_repo(self) -> None:
        """Initialize git repository for the wiki."""
        try:
            await self._run_git(["git", "init"])
            await self._run_git(["git", "remote", "add", "origin", self.auth_url])

            try:  # Try to fetch to see if the wiki exists
                await self._run_git(["git", "fetch", "--depth=1"])
                await self._run_git(["git", "checkout", "origin/master", "--", "."])
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
        if pathlib.Path(self.temp_dir).exists():
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
        await self._pull_latest_changes()
        target = pathlib.Path(self.temp_dir) / path
        if not target.exists():
            if path:  # Only raise error if not root path
                msg = f"Path not found: {path}"
                raise FileNotFoundError(msg)
            files = []
        else:
            files = [str(t) for t in target.iterdir()]

        markdown_files = [f for f in files if f.endswith(".md")]
        if detail:
            result = []
            for filename in markdown_files:
                file_path = target / filename
                stat = file_path.stat()
                # Get git metadata
                try:
                    # Get last commit date
                    log = await self.run_cmd([
                        "git",
                        "log",
                        "-1",
                        "--format=%at",
                        "--",
                        str(file_path),
                    ])
                    last_modified = int(log.stdout.strip()) if log.stdout.strip() else 0
                    # Get creation date (first commit)
                    cmd = ["git", "log", "--reverse", "--format=%at", "--", str(file_path)]
                    stdout_result = await self.run_cmd(cmd)
                    stdout = stdout_result.stdout
                    created_at = int(stdout.strip().split("\n")[0]) if stdout.strip() else 0
                except (subprocess.CalledProcessError, ValueError, IndexError):
                    last_modified = int(stat.st_mtime)
                    created_at = int(stat.st_ctime)
                file = pathlib.Path(filename)  # Convert filename to wiki title
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
        await self._pull_latest_changes()
        with file_path.open("rb") as f:
            if start is not None or end is not None:
                start = start or 0
                f.seek(start)
                if end is not None:
                    return f.read(end - start)
            return f.read()

    cat_file = sync_wrapper(_cat_file)  # pyright: ignore

    async def _pipe_file(self, path: str, value: bytes, **kwargs: Any) -> None:
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
        file_path.parent.mkdir(parents=True, exist_ok=True)
        await self._pull_latest_changes()
        with file_path.open("wb") as f:
            f.write(value)
        file = pathlib.Path(path)
        page_title = file.stem.replace("-", " ")
        msg = kwargs.get("message", f"Update {page_title}")
        try:
            await self.run_cmd(["git", "add", path])
            await self.run_cmd(["git", "commit", "-m", msg])
            await self.run_cmd(["git", "push", "origin", "HEAD:master"])
        except subprocess.CalledProcessError as e:
            error_msg = f"Error pushing changes: {e.stderr}"
            raise RuntimeError(error_msg) from e

        self.dircache.clear()  # Invalidate cache

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

        await self._ensure_initialized()
        await self._pull_latest_changes()
        try:
            file_path.unlink()  # Delete file
            await self.run_cmd(["git", "rm", path])  # Stage the deletion
            file = pathlib.Path(path)
            page_title = file.stem.replace("-", " ")
            msg = kwargs.get("message", f"Delete {page_title}")
            await self.run_cmd(["git", "commit", "-m", msg])
            await self.run_cmd(["git", "push", "origin", "HEAD:master"])
        except subprocess.CalledProcessError as e:
            error_msg = f"Error deleting file: {e.stderr}"
            raise RuntimeError(error_msg) from e

        self.dircache.clear()  # Invalidate cache

    rm_file = sync_wrapper(_rm_file)

    async def _info(self, path: str, **kwargs: Any) -> WikiInfo:
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

        if not path:  # Root directory
            info = f"{self.owner}/{self.repo}"
            return WikiInfo(name="", size=0, type="directory", wiki=info)

        file_path = pathlib.Path(self.temp_dir) / path
        if not file_path.exists():
            msg = f"Path not found: {path}"
            raise FileNotFoundError(msg)

        if file_path.is_dir():
            return WikiInfo(name=file_path.name or path, size=0, type="directory")

        stat = file_path.stat()  # File info
        file = pathlib.Path(path)
        try:
            # Get last commit date
            log = await self.run_cmd(["git", "log", "-1", "--format=%at", "--", str(file_path)])
            last_modified = int(log.stdout.strip()) if log.stdout.strip() else 0
            # Get creation date (first commit)
            first = await self.run_cmd([
                "git",
                "log",
                "--reverse",
                "--format=%at",
                "--",
                str(file_path),
            ])
            created_at = int(first.stdout.strip().split("\n")[0]) if first.stdout.strip() else 0
        except (subprocess.CalledProcessError, ValueError, IndexError):
            last_modified = int(stat.st_mtime)
            created_at = int(stat.st_ctime)

        title = file.stem.replace("-", " ")  # Convert filename to wiki title
        return WikiInfo(
            name=file.name,
            type="file",
            size=stat.st_size,
            title=title,
            created_at=created_at,
            updated_at=last_modified,
            html_url=f"https://github.com/{self.owner}/{self.repo}/wiki/{file.stem}",
        )

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
        block_size=None,
        autocommit=True,
        cache_options=None,
        **kwargs: Any,
    ) -> io.BytesIO | BufferedWriter:
        """Open a wiki page as a file.

        Args:
            path: Path to the wiki page
            mode: File mode ('rb' for reading, 'wb' for writing)
            block_size: Block size for reading or writing
            autocommit: Whether to automatically commit changes
            cache_options: Cache options for reading or writing
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
            return BufferedWriter(buffer, self, path, **kwargs)

        msg = f"Mode {mode} not supported"
        raise NotImplementedError(msg)

    async def invalidate_cache(self, path: str | None = None) -> None:
        """Clear the cache.

        Args:
            path: Optional path to invalidate (currently ignores path)
        """
        self.dircache.clear()  # For simplicity, we just clear the entire cache
        await self._pull_latest_changes()


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
