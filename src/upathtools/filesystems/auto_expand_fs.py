"""Filesystem that automatically expands archive/structured files into virtual directories.

This filesystem wraps another filesystem and automatically treats certain file types
(like .zip, .tar.gz, .md) as directories that can be browsed into.

Example:
    ```python
    from fsspec import filesystem
    from upathtools.filesystems.auto_expand_fs import AutoExpandFS

    # Wrap a local filesystem
    local_fs = filesystem("file")
    auto_fs = AutoExpandFS(fs=local_fs)

    # Now .zip files appear as directories
    auto_fs.ls("/path/to/archive.zip")  # Lists contents of the zip
    auto_fs.cat("/path/to/archive.zip/inner/file.txt")  # Reads file inside zip

    # Structured files need an expander (and upathtools' protocols registered):
    upathtools.register_all_filesystems()
    def markdown(fs, path):
        return {"fo": path, "target_protocol": main_protocol(fs)}

    auto_fs = AutoExpandFS(fs=local_fs, expanders={".md": ("md", markdown)})
    auto_fs.ls("/path/to/readme.md")  # Lists markdown sections
    ```
"""

from __future__ import annotations

from typing import TYPE_CHECKING, Any, Literal, overload

import fsspec
from fsspec.asyn import AsyncFileSystem

from upathtools.async_ops import to_async_fs
from upathtools.filesystems.base import BaseAsyncFileSystem, BaseUPath, FileInfo


if TYPE_CHECKING:
    from collections.abc import Callable

    from fsspec.spec import AbstractFileSystem


class AutoExpandInfo(FileInfo, total=False):
    """Info dict for auto-expand filesystem paths."""

    size: int
    expanded: bool
    original_type: str | None


class AutoExpandPath(BaseUPath[AutoExpandInfo]):
    """UPath implementation for AutoExpandFS."""

    __slots__ = ()


# Default handlers mapping extension to (protocol, fs_kwargs_factory)
# The factory takes the parent fs and file path, returns kwargs for the expansion fs
type ExpanderFactory = Callable[[AsyncFileSystem, str], dict[str, Any]]


def main_protocol(fs: AbstractFileSystem) -> str:
    """A filesystem's protocol as one string (fsspec allows a tuple of aliases)."""
    protocol = fs.protocol
    return protocol if isinstance(protocol, str) else protocol[0]


DEFAULT_EXPANDERS: dict[str, tuple[str, ExpanderFactory]] = {
    ".zip": ("zip", lambda fs, path: {"fo": path, "target_protocol": main_protocol(fs)}),
    ".tar": ("tar", lambda fs, path: {"fo": path, "target_protocol": main_protocol(fs)}),
    ".tar.gz": ("tar", lambda fs, path: {"fo": path, "target_protocol": main_protocol(fs)}),
    ".tgz": ("tar", lambda fs, path: {"fo": path, "target_protocol": main_protocol(fs)}),
    ".tar.bz2": ("tar", lambda fs, path: {"fo": path, "target_protocol": main_protocol(fs)}),
    ".tar.xz": ("tar", lambda fs, path: {"fo": path, "target_protocol": main_protocol(fs)}),
}


class AutoExpandFS(BaseAsyncFileSystem[AutoExpandPath, AutoExpandInfo]):
    """Filesystem that automatically expands archive/structured files as directories.

    When accessing a path like `/data/archive.zip/inner/file.txt`, this filesystem:
    1. Detects that `archive.zip` is an expandable file type
    2. Creates an appropriate sub-filesystem (e.g., ZipFileSystem)
    3. Delegates the remaining path (`inner/file.txt`) to that sub-filesystem

    Supports:
    - .zip files (via fsspec's zip protocol)
    - .tar, .tar.gz, .tgz, .tar.bz2, .tar.xz (via fsspec's tar protocol)
    - Custom expanders can be registered
    """

    protocol = "autoexpand"
    root_marker = "/"
    upath_cls = AutoExpandPath

    def __init__(
        self,
        fs: AbstractFileSystem | None = None,
        target_protocol: str | None = None,
        target_options: dict[str, Any] | None = None,
        expanders: dict[str, tuple[str, ExpanderFactory]] | None = None,
        **storage_options: Any,
    ) -> None:
        """Initialize auto-expand filesystem.

        Args:
            fs: Filesystem to wrap. If None, created from target_protocol.
            target_protocol: Protocol for target filesystem if fs not provided.
            target_options: Options for target filesystem if fs not provided.
            expanders: Custom expanders mapping extension to (protocol, kwargs_factory).
            **storage_options: Additional storage options.
        """
        super().__init__(**storage_options)

        if fs is None:
            target_protocol = target_protocol or "file"
            fs = fsspec.filesystem(target_protocol, **(target_options or {}))
        assert fs
        # Wrap non-async filesystems
        self.fs = to_async_fs(fs)
        self.expanders = {**DEFAULT_EXPANDERS, **(expanders or {})}
        self._expansion_cache: dict[str, AsyncFileSystem] = {}

    def _inner_path(self, path: str) -> str:
        """``path`` as the wrapped filesystem spells it (without protocol, its own root)."""
        return self.fs._strip_protocol(path)

    def _get_expansion_ext(self, path: str) -> str | None:
        """Check if path has an expandable extension.

        Returns the matching extension or None.
        """
        path_lower = path.lower()
        # Check longer extensions first (e.g., .tar.gz before .gz)
        for ext in sorted(self.expanders.keys(), key=len, reverse=True):
            if path_lower.endswith(ext):
                return ext
        return None

    async def _parse_path(self, path: str) -> tuple[AsyncFileSystem, str, str]:
        """Parse path and return (filesystem, remaining_path, expanded_file).

        If path contains an expandable file, returns the expansion filesystem, the
        path within it, and the expandable file's own path (to prefix inner names
        with); otherwise the wrapped filesystem, the path, and "".
        """
        if not path or path == self.root_marker:
            return self.fs, self.root_marker, ""

        # Normalize the way the wrapped filesystem does: local paths keep their
        # leading "/", root-less ones (archives, GitHub, ...) don't have one
        path = self._inner_path(path)
        parts = path.split("/")

        # Walk through path parts looking for expandable files
        for i in range(len(parts)):
            current_path = "/".join(parts[: i + 1])
            if not current_path:  # the empty first part of an absolute path
                continue

            ext = self._get_expansion_ext(current_path)
            if ext is None:
                continue

            try:  # Check if this is actually a file on the underlying fs
                info = await self.fs._info(current_path)
                if info.get("type") == "file":
                    # Get or create expansion filesystem
                    expansion_fs = self._get_expansion_fs(current_path, ext)
                    # Remaining path is everything after this part
                    remaining = "/".join(parts[i + 1 :])
                    return expansion_fs, remaining or "/", current_path
            except FileNotFoundError:
                # Not a file, continue checking
                pass

        # No expansion needed
        return self.fs, path, ""

    def _get_expansion_fs(self, file_path: str, ext: str) -> AsyncFileSystem:
        """Get or create filesystem for expanding a file."""
        from upathtools import core

        if file_path in self._expansion_cache:
            return self._expansion_cache[file_path]

        protocol, kwargs_factory = self.expanders[ext]
        kwargs = kwargs_factory(self.fs, file_path)
        expansion_fs = core.filesystem(protocol, **kwargs)
        # Wrap non-async filesystems
        expansion_fs = to_async_fs(expansion_fs)

        self._expansion_cache[file_path] = expansion_fs
        return expansion_fs

    async def _info(self, path: str, **kwargs: Any) -> AutoExpandInfo:
        """Get info about a path."""
        fs, inner_path, _expanded_file = await self._parse_path(path)

        if fs is self.fs:
            # Check if this is an expandable file
            info = await self.fs._info(inner_path)
            ext = self._get_expansion_ext(inner_path)
            if ext and info.get("type") == "file":
                # Expandable file appears as directory
                return AutoExpandInfo(
                    name=inner_path,
                    size=info.get("size", 0),
                    type="directory",
                    expanded=True,
                    original_type="file",
                )
            return AutoExpandInfo(
                name=info.get("name", inner_path),
                size=info.get("size", 0),
                type=info.get("type", "file"),
                expanded=False,
            )

        # Delegating to expansion filesystem
        info = await fs._info(inner_path, **kwargs)
        return AutoExpandInfo(
            name=info.get("name", inner_path),
            size=info.get("size", 0),
            type=info.get("type", "file"),
            expanded=True,
        )

    @overload
    async def _ls(
        self, path: str, detail: Literal[True] = ..., **kwargs: Any
    ) -> list[AutoExpandInfo]: ...

    @overload
    async def _ls(self, path: str, detail: bool, **kwargs: Any) -> list[str]: ...

    async def _ls(
        self, path: str, detail: bool = True, **kwargs: Any
    ) -> list[str] | list[AutoExpandInfo]:
        """List contents of a path."""
        fs, inner_path, expanded_file = await self._parse_path(path)

        if fs is self.fs:
            # Check if path is an expandable file
            path = self._inner_path(path)
            ext = self._get_expansion_ext(path)
            if ext:
                try:
                    info = await self.fs._info(path)
                    if info.get("type") == "file":
                        # List contents of expanded file
                        expansion_fs = self._get_expansion_fs(path, ext)
                        items = await expansion_fs._ls("/", detail=True)

                        if not detail:
                            return [f"{path}/{item['name'].lstrip('/')}" for item in items]

                        return [
                            AutoExpandInfo(
                                name=f"{path}/{item['name'].lstrip('/')}",
                                size=item.get("size", 0),
                                type=item.get("type", "file"),
                                expanded=True,
                            )
                            for item in items
                        ]
                except FileNotFoundError:
                    pass

            # Regular directory listing
            # detail=True: every entry is an info dict
            listing = await self.fs._ls(inner_path, detail=True)
            items = [i for i in listing if isinstance(i, dict)]
            results: list[AutoExpandInfo] = []

            for item in items:
                name = item.get("name", "")
                item_ext = self._get_expansion_ext(name)
                is_expandable = item_ext and item.get("type") == "file"

                results.append(
                    AutoExpandInfo(
                        name=name,
                        size=item.get("size", 0),
                        type="directory" if is_expandable else item.get("type", "file"),
                        expanded=bool(is_expandable),
                        original_type=item.get("type", "file") if is_expandable else None,
                    )
                )

            if not detail:
                return [r["name"] for r in results]
            return results
        # Delegating to expansion filesystem: its names are relative to the expanded
        # file, so prefix them with that file's path to keep them reachable from here
        items = await fs._ls(inner_path, detail=True)

        def outer(name: str) -> str:
            return f"{expanded_file}/{name.lstrip('/')}"

        if not detail:
            return [outer(item["name"]) for item in items]

        return [
            AutoExpandInfo(
                name=outer(item.get("name", "")),
                size=item.get("size", 0),
                type=item.get("type", "file"),
                expanded=True,
            )
            for item in items
        ]

    async def _cat_file(
        self, path: str, start: int | None = None, end: int | None = None, **kwargs: Any
    ) -> bytes:
        """Read file contents."""
        fs, inner_path, _expanded_file = await self._parse_path(path)
        return await fs._cat_file(inner_path, start=start, end=end, **kwargs)

    async def _pipe_file(self, path: str, value: bytes, **kwargs: Any) -> None:
        """Write file contents."""
        fs, inner_path, _expanded_file = await self._parse_path(path)

        # Don't allow writing into expanded archives
        if fs is not self.fs:
            msg = "Cannot write into expanded archives"
            raise NotImplementedError(msg)

        await fs._pipe_file(inner_path, value, **kwargs)

    async def _exists(self, path: str) -> bool:
        """Check if path exists."""
        fs, inner_path, _expanded_file = await self._parse_path(path)
        return await fs._exists(inner_path)

    async def _isfile(self, path: str) -> bool:
        """Check if path is a file."""
        info = await self._info(path)
        return info.get("type") == "file"

    async def _isdir(self, path: str) -> bool:
        """Check if path is a directory."""
        info = await self._info(path)
        return info.get("type") == "directory"

    def register_expander(
        self,
        extension: str,
        protocol: str,
        kwargs_factory: ExpanderFactory,
    ) -> None:
        """Register a custom expander for a file extension.

        Args:
            extension: File extension (e.g., ".md", ".json")
            protocol: fsspec protocol to use for expansion
            kwargs_factory: Factory function that takes (fs, path) and returns
                           kwargs for creating the expansion filesystem
        """
        self.expanders[extension] = (protocol, kwargs_factory)

    def clear_cache(self) -> None:
        """Clear the expansion filesystem cache."""
        self._expansion_cache.clear()


if __name__ == "__main__":
    from pathlib import Path
    import tempfile

    from upathtools import core

    # Create test files
    with tempfile.TemporaryDirectory() as tmpdir:
        # Create a markdown file
        md_content = """\
# Section 1
Content 1
## Section 1.1
Content 1.1
# Section 2
Content 2
"""
        md_path = Path(tmpdir) / "readme.md"
        md_path.write_text(md_content)

        # Create AutoExpandFS with markdown support
        local_fs = core.filesystem("file")
        auto_fs = AutoExpandFS(
            fs=local_fs,
            expanders={
                ".md": (
                    "md",
                    lambda fs, path: {"fo": path, "target_protocol": main_protocol(fs)},
                ),
            },
        )

        # List directory - readme.md appears as directory
        print("Directory listing:")
        for item in auto_fs.ls(tmpdir, detail=True):
            print(f"  {item['name']} ({item['type']})")

        # List markdown sections
        print("\nMarkdown sections:")
        for item in auto_fs.ls(f"{tmpdir}/readme.md", detail=True):
            print(f"  {item['name']} ({item['type']})")
